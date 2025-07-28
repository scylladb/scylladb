#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import asyncio
import pytest
import time
import logging
from test.topology.conftest import skip_mode
from test.pylib.util import wait_for_view
from test.topology.util import new_test_keyspace, new_test_table, new_materialized_view
from cassandra.cqltypes import Int32Type

logger = logging.getLogger(__name__)

async def insert_with_concurrency(cql, table, value_count, concurrency):
    ks = table.split(".")[0]
    def serialize_int(i):
        return Int32Type.serialize(i, cql.cluster.protocol_version)
    def get_replicas(key):
        return cql.cluster.metadata.get_replicas(ks, serialize_int(key))
    local_node = get_replicas(0)[0]
    logger.info(f"Starting writes with concurrency {concurrency}")
    async def do_inserts(m: int):
        m_count = value_count // concurrency
        if value_count % concurrency >= m:
            m_count += 1
        update_key = m
        # For each row in [0, value_count) with key % concurrency == m, insert a row with the same remainder m
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.tab (key, c) VALUES (?, ?)")
        inserted_count = 0
        while inserted_count < m_count:
            # Only remote updates hold on to memory, so try another key until the update is remote
            while local_node == get_replicas(update_key)[0]:
                update_key = update_key + concurrency
            await cql.run_async(insert_stmt, [0, update_key])
            inserted_count += 1
            update_key = update_key + concurrency
    tasks = [asyncio.create_task(do_inserts(i)) for i in range(concurrency)]

    await asyncio.gather(*tasks)
    logger.info(f"Finished writes with concurrency {concurrency}")

# This test reproduces issue #12379
# To quickly exceed the view update backlog limit, the test uses a minimal, 2 node
# cluster and lowers the limit using the "view_update_limit" error injection.
# Also, the "delay_before_remote_view_update" error injection is used to ensure that
# we're hitting a scenario where the remote view update finishes after the base write
# returns.
# The test steps are:
# 1. Create a table and with one partition and 200 rows
# 2. Create a materialized view and wait until it's built
# 3. Delete the entire partition in the base table, causing a large number of view updates
# The "view_update_limit" error injections will cause the test to fail due to a failed
# replica write if the view update limit is exceeded. If, thanks to throttling, we never
# exceed the limit, the test will pass
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_delete_partition_rows_from_table_with_mv(manager: ManagerClient) -> None:
    node_count = 2
    await manager.servers_add(node_count, config={'error_injections_at_startup': ['view_update_limit', 'delay_before_remote_view_update']})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, PRIMARY KEY (key, c))")
        await insert_with_concurrency(cql, f"{ks}.tab", 200, 100)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")

        await wait_for_view(cql, "mv_cf_view", node_count)

        logger.info(f"Deleting all rows from partition with key 0")
        await cql.run_async(f"DELETE FROM {ks}.tab WHERE key = 0", timeout=300)

# Test deleting a large partition when there is a view with the same partition
# key, and verify that view updates metrics is increased by exactly 1. Deleting
# a partition in this case is expected to generate one view update for deleting
# the corresponding view partition by a partition tombstone.
# Reproduces #8199
@pytest.mark.asyncio
@pytest.mark.parametrize("permuted", [False, True])
async def test_base_partition_deletion_with_metrics(manager: ManagerClient, permuted):
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as test_keyspace:
        async with new_test_table(manager, test_keyspace, 'p1 int, p2 int, c int, primary key ((p1,p2),c)') as table:
            # Insert into one base partition. We will delete the entire partition
            insert = cql.prepare(f'INSERT INTO {table} (p1,p2,c) VALUES (?,?,?)')
            # The view partition key is a permutation of the base partition key.
            async with new_materialized_view(manager, table, '*', '(p2,p1),c' if permuted else '(p1,p2),c', 'p1 is not null and p2 is not null and c is not null') as mv:
                # the metric total_view_updates_pushed_local is incremented by 1 for each 100 row view
                # updates, because it is collected in batches according to max_rows_for_view_updates.
                # To verify the behavior, we want the metric to increase by at least 2 without the optimization,
                # so 101 is the minimum value that works. With the optimization, we expect to have exactly 1 update
                # for any N.
                N = 101

                # all operations are on this single partition
                p1, p2 = 1, 10
                where_clause_table = f"WHERE p1={p1} AND p2={p2}"
                where_clause_mv = f"WHERE p2={p2} AND p1={p1}" if permuted else where_clause_table

                for i in range(N):
                    await cql.run_async(insert, [p1, p2, i])

                # Before the deletion, all N rows should exist in the base and the view
                allN = list(range(N))
                assert allN == [x.c for x in await cql.run_async(f"SELECT c FROM {table} {where_clause_table}")]
                assert allN == sorted([x.c for x in await cql.run_async(f"SELECT c FROM {mv} {where_clause_mv}")])

                metrics_before = await manager.metrics.query(server.ip_addr)
                updates_before = metrics_before.get('scylla_database_total_view_updates_pushed_local')

                await cql.run_async(f"DELETE FROM {table} {where_clause_table}")

                # After the deletion, all data should be gone from both base and view
                assert [] == list(await cql.run_async(f"SELECT c FROM {table} {where_clause_table}"))
                assert [] == list(await cql.run_async(f"SELECT c FROM {mv} {where_clause_mv}"))

                metrics_after = await manager.metrics.query(server.ip_addr)
                updates_after = metrics_after.get('scylla_database_total_view_updates_pushed_local')

                print(f"scylla_database_total_view_updates_pushed_local: {updates_before} -> {updates_after}")
                assert updates_after == updates_before + 1

# Perform a deletion of a base partition in a batch with deletion of individual rows. Verify the
# partition is deleted correctly and that a single update is generated for the view for deleting
# the whole partition, and no view updates for each row.
@pytest.mark.asyncio
async def test_base_partition_deletion_in_batch_with_delete_row_with_metrics(manager: ManagerClient):
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as test_keyspace:
        async with new_test_table(manager, test_keyspace, 'p int, c int, v int, primary key ((p,c),v)') as table:
            insert = cql.prepare(f'INSERT INTO {table} (p,c,v) VALUES (?,?,?)')
            # The view partition key is the same as the base partition key.
            async with new_materialized_view(manager, table, '*', '(p,c),v', 'p is not null and c is not null and v is not null') as mv:
                N = 101 # See comment above
                for i in range(N):
                    await cql.run_async(insert, [1, 10, i])

                # Before the deletion, all N rows should exist in the base and the view
                allN = list(range(N))
                assert allN == [x.v for x in await cql.run_async(f"SELECT v FROM {table} WHERE p=1 AND c=10")]
                assert allN == sorted([x.v for x in await cql.run_async(f"SELECT v FROM {mv} WHERE p=1 AND c=10")])

                metrics_before = await manager.metrics.query(server.ip_addr)
                updates_before = metrics_before.get('scylla_database_total_view_updates_pushed_local')

                # The batch deletes the entire partition and also, redundantly, deleting individual rows in the partition.
                # We expect the view update to contain only a single update for deleting the partition.
                cmd = 'BEGIN UNLOGGED BATCH '
                for i in range(100,500):
                    cmd += f'DELETE FROM {table} WHERE p=1 AND c=10 AND v={i}; '
                cmd += f'DELETE FROM {table} WHERE p=1 AND c=10; '
                cmd += 'APPLY BATCH;'
                await cql.run_async(cmd)

                # Verify the partition is deleted
                assert [] == list(await cql.run_async(f"SELECT v FROM {table} WHERE p=1 AND c=10"))
                assert [] == list(await cql.run_async(f"SELECT v FROM {mv} WHERE p=1 AND c=10"))

                # Verify there is a single view update
                metrics_after = await manager.metrics.query(server.ip_addr)
                updates_after = metrics_after.get('scylla_database_total_view_updates_pushed_local')

                print(f"scylla_database_total_view_updates_pushed_local: {updates_before} -> {updates_after}")
                assert updates_after == updates_before + 1
