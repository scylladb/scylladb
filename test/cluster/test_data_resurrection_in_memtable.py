#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import json
import logging
import pytest
import time

from cassandra.cluster import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts, execute_with_tracing


logger = logging.getLogger(__name__)


async def run_test_cache_tombstone_gc(manager: ScyllaClusterManager, statement_pairs: list[tuple[str]]):
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

    nodes = await manager.servers_add(3, cmdline=cmdline, auto_rack_dc="dc1")

    node1, node2, node3 = nodes

    cql = manager.get_cql()

    host1, host2, host3 = await wait_for_cql_and_get_hosts(cql, nodes, time.time() + 30)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = { 'enabled': true }") as ks:
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"
                    "     WITH speculative_retry = 'NONE'"
                    "     AND tombstone_gc = {'mode': 'immediate', 'propagation_delay_in_seconds': 0}"
                    "     AND compaction = {'class': 'NullCompactionStrategy'}")

        for write_statement, delete_statement in statement_pairs:
            execute_with_tracing(cql, SimpleStatement(write_statement.format(ks=ks), consistency_level=ConsistencyLevel.ALL), log = True)
            await manager.api.enable_injection(node3.ip_addr, "database_apply", one_shot=False, parameters={"ks_name": ks, "cf_name": "tbl", "what": "throw"})
            execute_with_tracing(cql, SimpleStatement(delete_statement.format(ks=ks), consistency_level=ConsistencyLevel.LOCAL_QUORUM), log = True)
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

        dump_mutation_fragments("before repair")

        # Before repair: we expect node3 to have the deleted row as live.
        check_data(host1, [])
        check_data(host2, [])
        check_data(host3, [{'pk': 0, 'ck': 100, 'v': 999}])

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


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cache_tombstone_gc_partition_tombstone(manager: ScyllaClusterManager):
    await run_test_cache_tombstone_gc(manager,
                                      [("INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, 100, 999)", "DELETE FROM {ks}.tbl WHERE pk = 0")])


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cache_tombstone_gc_row_tombstone(manager: ScyllaClusterManager):
    await run_test_cache_tombstone_gc(manager,
                                      [("INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, 100, 999)", "DELETE FROM {ks}.tbl WHERE pk = 0 AND ck = 100")])


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cache_tombstone_gc_range_tombstone(manager: ScyllaClusterManager):
    await run_test_cache_tombstone_gc(manager,
                                      [("INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, 100, 999)", "DELETE FROM {ks}.tbl WHERE pk = 0 AND ck > 0 AND ck < 1000")])


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cache_tombstone_gc_cell_tombstone(manager: ScyllaClusterManager):
    await run_test_cache_tombstone_gc(manager,
                                      [("UPDATE {ks}.tbl SET v = 999 WHERE pk = 0 AND ck = 100", "DELETE v FROM {ks}.tbl WHERE pk = 0 AND ck = 100")])


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cache_tombstone_gc_cell_tombstone_and_row_tombstone(manager: ScyllaClusterManager):
    await run_test_cache_tombstone_gc(manager,
                                      [
                                          ("INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, 100, 999)", "DELETE FROM {ks}.tbl WHERE pk = 0 AND ck = 100"),
                                          ("UPDATE {ks}.tbl SET v = 999 WHERE pk = 0 AND ck = 100", "DELETE v FROM {ks}.tbl WHERE pk = 0 AND ck = 100"),
                                      ])


async def prepare_gc_eligible_tombstone(manager: ScyllaClusterManager, cql, nodes, ks: str):
    """Put a GC-eligible tombstone on disk on every replica."""
    cql.execute(SimpleStatement(f"DELETE FROM {ks}.tbl USING TIMESTAMP 2000 WHERE pk = 0 AND ck = 0",
                                consistency_level=ConsistencyLevel.ALL))
    # Keep the sstable from consisting of nothing but expired data. get_fully_expired_sstables()
    # drops such an sstable whole, without ever calling get_max_purgeable_timestamp(), and it looks
    # at the other sstables only -- never at the memtables -- so the purge check under test would
    # not run at all. One live row is enough to take that shortcut away.
    cql.execute(SimpleStatement(f"INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, 1, 1) USING TIMESTAMP 3000",
                                consistency_level=ConsistencyLevel.ALL))
    for node in nodes:
        await manager.api.keyspace_flush(node.ip_addr, ks, "tbl")

    # Repair records repair_time, and with propagation_delay 0 that is what gc_before becomes,
    # making the tombstone collectable. gc_before is compared against the tombstone's deletion
    # time, which has one-second resolution, so let the tombstone fall strictly behind.
    time.sleep(2)
    await manager.api.repair(nodes[0].ip_addr, ks, "tbl")
    time.sleep(2)


def insert_older_row(cql, ks: str):
    """The row the tombstone shadows, stamped older than it.

    This is the shape materialized view update generation produces: a live row written now, but
    carrying an old timestamp. It is what the memtable check in get_max_purgeable_timestamp()
    normally keeps the tombstone alive for.
    """
    cql.execute(SimpleStatement(f"INSERT INTO {ks}.tbl (pk, ck, v) VALUES (0, 0, 999) USING TIMESTAMP 1000",
                                consistency_level=ConsistencyLevel.ALL))


def sstable_fragments(cql, ks: str, host):
    """The fragments of row (0, 0) that live in sstables, i.e. what compaction left behind.

    Row (0, 1) is the sstable's live filler and is not of interest here.
    """
    res = cql.execute(SimpleStatement(
            f"SELECT mutation_source, mutation_fragment_kind, ck, metadata FROM MUTATION_FRAGMENTS({ks}.tbl) WHERE pk = 0",
            consistency_level=ConsistencyLevel.LOCAL_ONE), host=host)
    return [row for row in res if row.mutation_source.startswith("sstable") and row.ck == 0]


TBL_SCHEMA = ("     WITH speculative_retry = 'NONE'"
              "     AND tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': 0}"
              "     AND compaction = {'class': 'NullCompactionStrategy'}")

# gc_before is also capped by commitlog::min_gc_time() (tombstone_gc.cc, check_min()). A segment
# is only ignored there once its position falls behind the repair's replay position, and the active
# segment keeps growing, so the segment holding the DELETE keeps counting and its _cf_min_time for
# the table -- the time of that DELETE -- becomes gc_before. The tombstone is then never strictly
# older than gc_before and is kept regardless of what the memtable check decides, which is not what
# these tests are about. Running without a commitlog makes the cap time_point::max(), so gc_before
# is exactly repair_time. System keyspaces keep their commitlog.
#
# repair_time itself is the hint/batchlog flush time, and that flush is cached for
# repair_hints_batchlog_flush_cache_time_in_ms (60s by default): within that window repair skips the
# flush and reports the cached time, which in a test this short predates the DELETE. Disable the
# cache so every repair flushes and reports the time it did so.
NODE_CMDLINE = ["--enable-commitlog", "0",
                "--repair-hints-batchlog-flush-cache-time-in-ms", "0"]


KS_SCHEMA = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
             " AND tablets = {'enabled': false}")


async def test_major_compaction_without_flush_keeps_tombstone_over_memtable_row(manager: ScyllaClusterManager):
    """A major compaction that did not flush must keep consulting the memtables.

    A major compaction on a tombstone_gc=repair table is allowed to skip the memtable check,
    but only because it flushed first, which puts everything resident into its input set to be
    merged with the tombstones covering it. Run with flush_memtables=false there is no such
    merge: the memtable still holds a live row older than the tombstone, and collecting the
    tombstone would bring that row back as soon as the memtable is flushed.
    """
    nodes = await manager.servers_add(3, cmdline=NODE_CMDLINE, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, nodes, time.time() + 60)

    async with new_test_keyspace(manager, KS_SCHEMA) as ks:
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)){TBL_SCHEMA}")
        await prepare_gc_eligible_tombstone(manager, cql, nodes, ks)
        insert_older_row(cql, ks)

        for node in nodes:
            await manager.api.keyspace_compaction(node.ip_addr, ks, "tbl", flush_memtables=False)

        for host in hosts:
            assert sstable_fragments(cql, ks, host), \
                    f"tombstone was collected on {host.address} by a major compaction that did not flush"

        # The row is still in the memtable. Flushing it must not make it visible.
        for node in nodes:
            await manager.api.keyspace_flush(node.ip_addr, ks, "tbl")
        for host in hosts:
            res = cql.execute(SimpleStatement(f"SELECT * FROM {ks}.tbl WHERE pk = 0 AND ck = 0",
                                              consistency_level=ConsistencyLevel.LOCAL_ONE), host=host)
            assert list(res) == [], f"row was resurrected on {host.address}"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_major_compaction_with_flush_collects_tombstone_over_later_memtable_row(manager: ScyllaClusterManager):
    """A major compaction that flushed may collect the tombstone, even with an older row in the memtable.

    Same setup, except the older row is written *after* the major has flushed and selected its
    input sstables, so it is in neither. That is the only way a memtable can hold a row older
    than a GC-eligible tombstone once the major has flushed, and the compaction is allowed to
    ignore it: under tombstone_gc=repair, gc_before is derived from repair_time, which is the
    time repair flushed hints, so a legitimate write covered by that tombstone was already
    delivered and reconciled before gc_before could reach it. The write this test makes by hand
    is not reachable that way -- it is how the check is exercised, not a case that can occur.

    Consequently the row does become visible here once the tombstone is gone. That is the
    deliberate consequence of the narrowing, not a regression; what is asserted is that the
    tombstone was collected at all, which without the narrowing it would not be.
    """
    nodes = await manager.servers_add(3, cmdline=NODE_CMDLINE, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, nodes, time.time() + 60)

    async with new_test_keyspace(manager, KS_SCHEMA) as ks:
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)){TBL_SCHEMA}")
        await prepare_gc_eligible_tombstone(manager, cql, nodes, ks)

        # Pause each major right after it has flushed and registered its input sstables.
        for node in nodes:
            await manager.api.enable_injection(node.ip_addr, "major_compaction_wait", one_shot=True)
        compactions = [asyncio.create_task(manager.api.keyspace_compaction(node.ip_addr, ks, "tbl"))
                       for node in nodes]
        for node in nodes:
            await manager.api.wait_for_injection_enter(node.ip_addr, "major_compaction_wait")

        # Lands in a fresh memtable on every replica: after the flush, before the purge decision.
        insert_older_row(cql, ks)

        for node in nodes:
            await manager.api.message_injection(node.ip_addr, "major_compaction_wait")
        await asyncio.gather(*compactions)

        for host in hosts:
            assert not sstable_fragments(cql, ks, host), \
                    f"tombstone was not collected on {host.address} by a major compaction that flushed"
