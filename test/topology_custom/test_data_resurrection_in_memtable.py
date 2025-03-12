#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import json
import logging
import pytest
import time

from cassandra.cluster import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.topology.conftest import skip_mode
from test.topology.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts


logger = logging.getLogger(__name__)


async def run_test_cache_tombstone_gc(manager: ManagerClient, statement_pairs: list[tuple[str]]):
    """Test for cache garbage collecting tombstones which cover data in the memtable.

    1. Write a live row.
    2. Write a tombstone to 2/3 replica (fail the write on node3 via error injection).
    3. Run a repair so node3 also receives the tombstone.

    At this stage, node1 and node2 have both the live row and the tombstone in
    memtable, node3 has the live row in the memtable and the tombstone on disk.

    4. Read the row from each node with CL=LOCAL_ONE. This will create an entry in cache
       on node3, with the tombstone.
       Check that population didn't drop the tombstone! #23291
    5. Do another read round. This will use the existing entry in the cache.
       Check that the cache read didn't drop the tombstone! #23252
    """
    cmdline = ["--hinted-handoff-enabled", "0", "--cache-hit-rate-read-balancing", "0", "--logger-log-level", "debug_error_injection=trace"]

    nodes = await manager.servers_add(3, cmdline=cmdline)

    node1, node2, node3 = nodes

    cql = manager.get_cql()

    host1, host2, host3 = await wait_for_cql_and_get_hosts(cql, nodes, time.time() + 30)

    def execute_with_tracing(cql, statement, *args, **kwargs):
        kwargs['trace'] = True
        query_result = cql.execute(statement, *args, **kwargs)

        tracing = query_result.get_all_query_traces(max_wait_sec_per=900)
        page_traces = []
        for trace in tracing:
            trace_events = []
            for event in trace.events:
                trace_events.append(f"  {event.source} {event.source_elapsed} {event.description}")
            page_traces.append("\n".join(trace_events))
        logger.debug("Tracing {}:\n{}\n".format(statement, "\n".join(page_traces)))

    async with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = { 'enabled': true }") as ks:
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"
                    "     WITH speculative_retry = 'NONE'"
                    "     AND tombstone_gc = {'mode': 'immediate', 'propagation_delay_in_seconds': 0}"
                    "     AND compaction = {'class': 'NullCompactionStrategy'}")

        for write_statement, delete_statement in statement_pairs:
            execute_with_tracing(cql, write_statement.format(ks=ks))
            await manager.api.enable_injection(node3.ip_addr, "database_apply", one_shot=False)
            execute_with_tracing(cql, delete_statement.format(ks=ks))
            await manager.api.disable_injection(node3.ip_addr, "database_apply")

        def check_data(host, data):
            res = cql.execute(SimpleStatement(f"SELECT JSON * FROM {ks}.tbl WHERE pk = 0", consistency_level=ConsistencyLevel.LOCAL_ONE), host=host, trace=True)
            row_list = list(map(lambda row: json.loads(row[0]), res))
            tracing = res.get_all_query_traces(max_wait_sec_per=900)
            for trace in tracing:
                for event in trace.events:
                    # Make sure the read was executed on `host`.
                    assert event.source == host.address
            assert row_list == data

        def dump_mutation_fragments(description):
            for host in [host1, host2, host3]:
                res = cql.execute(SimpleStatement(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.tbl) WHERE pk = 0", consistency_level=ConsistencyLevel.LOCAL_ONE), host=host)
                logger.info(f"MUTATION_FRAGMENTS {description} for {host.address}:\n{'\n'.join(map(str, res))}")

        # Before repair: we expect node3 to have the deleted row as live.
        check_data(host1, [])
        check_data(host2, [])
        check_data(host3, [{'pk': 0, 'ck': 100, 'v': 999}])

        dump_mutation_fragments("before repair")

        await manager.api.tablet_repair(node1.ip_addr, ks, "tbl", "all", await_completion=True)

        # Give time for immediate-mode tombstone gc to take effect.
        # It needs tombstone.expiry < now(), with second resolution.
        time.sleep(2)

        dump_mutation_fragments("after repair")

        # Fist read - cache is populated with the tombstone
        check_data(host1, [])
        check_data(host2, [])
        check_data(host3, [])

        dump_mutation_fragments("after repair and after populating read")

        # Second read - cache should *not* garbage-collects the tombstone
        check_data(host1, [])
        check_data(host2, [])
        check_data(host3, [])


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cache_tombstone_gc_partition_tombstone(manager: ManagerClient):
    await run_test_cache_tombstone_gc(manager,
                                      [("INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, 100, 999)", "DELETE FROM {ks}.tbl WHERE pk = 0")])


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cache_tombstone_gc_row_tombstone(manager: ManagerClient):
    await run_test_cache_tombstone_gc(manager,
                                      [("INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, 100, 999)", "DELETE FROM {ks}.tbl WHERE pk = 0 AND ck = 100")])


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cache_tombstone_gc_range_tombstone(manager: ManagerClient):
    await run_test_cache_tombstone_gc(manager,
                                      [("INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, 100, 999)", "DELETE FROM {ks}.tbl WHERE pk = 0 AND ck > 0 AND ck < 1000")])


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cache_tombstone_gc_cell_tombstone(manager: ManagerClient):
    await run_test_cache_tombstone_gc(manager,
                                      [("UPDATE {ks}.tbl SET v = 999 WHERE pk = 0 AND ck = 100", "DELETE v FROM {ks}.tbl WHERE pk = 0 AND ck = 100")])


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cache_tombstone_gc_cell_tombstone_and_row_tombstone(manager: ManagerClient):
    await run_test_cache_tombstone_gc(manager,
                                      [
                                          ("INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, 100, 999)", "DELETE FROM {ks}.tbl WHERE pk = 0 AND ck = 100"),
                                          ("UPDATE {ks}.tbl SET v = 999 WHERE pk = 0 AND ck = 100", "DELETE v FROM {ks}.tbl WHERE pk = 0 AND ck = 100"),
                                      ])
