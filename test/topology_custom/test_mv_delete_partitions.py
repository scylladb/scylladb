#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient

import asyncio
import pytest
import time
import logging
from test.topology.conftest import skip_mode
from cassandra.cqltypes import Int32Type

logger = logging.getLogger(__name__)

async def insert_with_concurrency(cql, values, concurrency):
    def serialize_int(i):
        return Int32Type.serialize(i, cql.cluster.protocol_version)
    def get_replicas(key):
        return cql.cluster.metadata.get_replicas("ks", serialize_int(key))
    local_node = get_replicas(0)[0]
    logger.info(f"Starting writes with concurrency {concurrency}")
    async def do_inserts(i: int):
        # Insert all rows with keys with the same modulo as i
        insert_stmt = cql.prepare(f"INSERT INTO ks.tab (key, c, v) VALUES (?, ?, ?)")
        i_steps = i
        while i_steps < values:
            # Only remote updates hold on to memory, so make all updates remote
            while local_node == get_replicas(i)[0]:
                i = i + concurrency
            await cql.run_async(insert_stmt, [0, i, i])
            i_steps += concurrency
            i = i + concurrency
    tasks = [asyncio.create_task(do_inserts(i)) for i in range(concurrency)]

    await asyncio.gather(*tasks)
    logger.info(f"Finished writes with concurrency {concurrency}")

async def wait_for_views(cql, mvs_count, node_count):
    deadline = time.time() + 120
    while time.time() < deadline:
        done = await cql.run_async(f"SELECT COUNT(*) FROM system_distributed.view_build_status WHERE status = 'SUCCESS' ALLOW FILTERING")
        logger.info(f"Views built: {done[0][0]}")
        if done[0][0] == node_count * mvs_count:
            return
        else:
            time.sleep(0.2)
    raise Exception("Timeout waiting for views to build")

# This test reproduces issue #12379
# The test scenario uses a 2 node cluster with "view_update_limit" error injections,
# which limits the amount of memory that can be used for view updates and causes
# view_update_generation_timeout_exception to be thrown when the limit is reached.
# The test steps are:
# 1. Create a table and with one partition and 200 rows
# 3. Create a materialized view and wait for it
# 4. Delete all partitions in the base table
# If the test doesn't cause a view_update_generation_timeout_exception due to using
# too much memory in step 3, it passes
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_delete_partition_rows_from_table_with_mv(manager: ManagerClient) -> None:
    node_count = 2
    await manager.servers_add(node_count, cmdline=['--error-injections-at-startup=view_update_limit'])
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE ks.tab (key int, c int, v int, PRIMARY KEY (key, c))")
    await insert_with_concurrency(cql, 200, 100)

    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab "
                    "WHERE c IS NOT NULL and key IS NOT NULL and v IS NOT NULL PRIMARY KEY (v, c, key) ")

    await wait_for_views(cql, 1, node_count)

    logger.info(f"Deleting all rows from partition with key 0")
    await cql.run_async(f"DELETE FROM ks.tab WHERE key = 0", timeout=300)

    await cql.run_async(f"DROP KEYSPACE ks")
