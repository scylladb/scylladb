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
from test.pylib.util import wait_for_view
from cassandra.cqltypes import Int32Type

logger = logging.getLogger(__name__)

async def insert_with_concurrency(cql, value_count, concurrency):
    def serialize_int(i):
        return Int32Type.serialize(i, cql.cluster.protocol_version)
    def get_replicas(key):
        return cql.cluster.metadata.get_replicas("ks", serialize_int(key))
    local_node = get_replicas(0)[0]
    logger.info(f"Starting writes with concurrency {concurrency}")
    async def do_inserts(m: int):
        m_count = value_count // concurrency
        if value_count % concurrency >= m:
            m_count += 1
        update_key = m
        # For each row in [0, value_count) with key % concurrency == m, insert a row with the same remainder m
        insert_stmt = cql.prepare(f"INSERT INTO ks.tab (key, c) VALUES (?, ?)")
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
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE ks.tab (key int, c int, PRIMARY KEY (key, c))")
    await insert_with_concurrency(cql, 200, 100)

    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab "
                    "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")

    await wait_for_view(cql, "mv_cf_view", node_count)

    logger.info(f"Deleting all rows from partition with key 0")
    await cql.run_async(f"DELETE FROM ks.tab WHERE key = 0", timeout=300)

    await cql.run_async(f"DROP KEYSPACE ks")
