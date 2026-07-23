#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import pytest
import time
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from contextlib import asynccontextmanager
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts


logger = logging.getLogger(__name__)


@asynccontextmanager
async def exclude_node_context(manager, ks_table, exclude_from_host):
    """Enable the database_apply injection on exclude_from_host for the duration of the context.
    Yields the Host object so callers can pass it as host= to run_async, ensuring the excluded
    node is always the coordinator. This prevents a race where the coordinator returns CL=ONE
    success before the background replica write reaches the excluded node, which could cause
    the write to land on the excluded node after the injection is disabled."""
    keyspace, table = ks_table.split(".", 1)
    if exclude_from_host is not None:
        await manager.api.enable_injection(exclude_from_host.address, "database_apply", one_shot=False, parameters={"ks_name": keyspace, "cf_name": table, "what": "throw"})

    try:
        yield exclude_from_host
    finally:
        if exclude_from_host is not None:
            await manager.api.disable_injection(exclude_from_host.address, "database_apply")


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_paging_with_partition_tombstone_span(manager: ManagerClient):
    cmdline = ["--hinted-handoff-enabled", "0",
               "--cache-hit-rate-read-balancing", "0",
               "--query-tombstone-page-limit", "10"]

    node1, node2 = await manager.servers_add(2, cmdline=cmdline, auto_rack_dc="dc1")
    cql = manager.get_cql()

    host1, host2 = await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 30)

    async with new_test_keyspace(manager, "with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 2} and tablets = { 'enabled': true }") as keyspace:
        async with new_test_table(manager, keyspace, 'pk int PRIMARY KEY, v int', " WITH tablets = {'max_tablet_count': 1}") as table:
            insert_partition_id = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
            delete_partition_id = cql.prepare(f"DELETE FROM {table} WHERE pk = ?")

            # We are setting up the below scenario (partitions are in ring order):
            # node1 [23, 33, 5, 28, --, --, --, 30, 11, 1, 19, 8 , 0, 2+]
            # node2 [23, 33, 5, 28, 10, 16, 13, 30, 11, 1, --, 8+, -, 2+]
            # - denotes no data for the partition
            # number denotes a dead partition
            # number+ denotes a live partition
            # After the first page, node1 will stop at partition 0, node2 will stop at partition 1
            # In ring order 1 < 0 so the next page should continue from 1 and correctly pick up the live partition 8+ in the next page.

            all_pks = [23, 33, 5, 28, 10, 16, 13, 30, 11, 1, 19, 8, 0, 2]

            exclude_from = {4: host1, 5: host1, 6: host1, 10: host2, 11: host2, 12: host2}
            for i, pk in enumerate(all_pks):
                async with exclude_node_context(manager, table, exclude_from.get(i)) as exclude_host:
                    statement = delete_partition_id.bind((pk,))
                    statement.consistency_level = ConsistencyLevel.ONE if exclude_host else ConsistencyLevel.ALL
                    await cql.run_async(statement, host=exclude_host)

            async with exclude_node_context(manager, table, host1) as exclude_host:
                statement = insert_partition_id.bind((8, 0))
                statement.consistency_level = ConsistencyLevel.ONE
                await cql.run_async(statement, host=exclude_host)

            statement = insert_partition_id.bind((2, 0))
            statement.consistency_level = ConsistencyLevel.ALL
            await cql.run_async(statement)

            async def check_rows(cql: Session, host: Host, expected_pks: list[int]) -> None:
                pks = [r.pk for r in await cql.run_async(f"SELECT pk FROM MUTATION_FRAGMENTS({table}) WHERE partition_region = 0 ALLOW FILTERING", host=host)]
                assert pks == expected_pks

            await check_rows(cql, host1, [23, 33, 5, 28,             30, 11, 1, 19, 8 , 0, 2])
            await check_rows(cql, host2, [23, 33, 5, 28, 10, 16, 13, 30, 11, 1,     8,     2])

            await manager.api.log(node1.ip_addr, "end check")

            select_statement = SimpleStatement(f"SELECT * FROM {table}", fetch_size=100)
            select_statement.consistency_level = ConsistencyLevel.ALL

            res = cql.execute(select_statement, host=host1)
            rows = [r.pk for r in res]

            assert rows == [8, 2]
