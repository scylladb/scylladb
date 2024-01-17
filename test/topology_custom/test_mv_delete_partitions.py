
from test.pylib.manager_client import ManagerClient

import asyncio
import pytest
import time
import logging

logger = logging.getLogger(__name__)

async def insert_with_concurrency(cql, key, values, concurrency):
    logger.info(f"Starting writes for key {key} with concurrency {concurrency}")
    async def do_inserts(i: int):
        # Insert all rows with keys with the same modulo as i
        while i < values:
            await cql.run_async(f"INSERT INTO ks.tab (key, c, v) VALUES ({key}, {i}, {key + i})")
            i += concurrency
    tasks = [asyncio.create_task(do_inserts(i)) for i in range(concurrency)]

    await asyncio.gather(*tasks)
    logger.info(f"Finished writes for key {key} with concurrency {concurrency}")

async def wait_for_view(cql, base_table, view_table):
    logger.info(f"Waiting for view {view_table} to be built")
    while True:
        base_count = await cql.run_async(f"SELECT count(*) FROM {base_table} USING TIMEOUT 1h", timeout=600)
        view_count = await cql.run_async(f"SELECT count(*) FROM {view_table} USING TIMEOUT 1h", timeout=600)
        if base_count[0][0] == view_count[0][0]:
            logger.info(f"View {view_table} has been built")
            return
        time.sleep(0.2)

# This test reproduces issue #12379
@pytest.mark.asyncio
async def test_delete_partition_rows_from_table_with_mv(manager: ManagerClient) -> None:
    await manager.server_add()
    await manager.server_add()
    cql = manager.get_cql()
    mv_name_pref = "mv_cf_view"
    mvs_count = 50
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE ks.tab (key int, c int, v int, primary key (key, c))")
    for i in range(mvs_count):
        await cql.run_async(f"CREATE MATERIALIZED VIEW ks.{mv_name_pref}_{i} AS SELECT * FROM ks.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL and v IS NOT NULL PRIMARY KEY (v, c, key) ")

    for _ in range(10):
        for key in range(50):
            await insert_with_concurrency(cql, key, 1000, 100)

        for i in range(mvs_count):
            await wait_for_view(cql, f"ks.tab", f"ks.{mv_name_pref}_{i}")

        keys = ", ".join([str(key) for key in range(50)])
        logger.info(f"Deleting rows for keys {keys}")
        await cql.run_async(f"delete from ks.tab where key in ({keys})", timeout=3600)

        await cql.run_async(f"TRUNCATE ks.tab")
    await cql.run_async(f"DROP KEYSPACE ks")
