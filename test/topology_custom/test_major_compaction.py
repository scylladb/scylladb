#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import logging
import asyncio

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

async def disable_autocompaction_across_keyspaces(manager: ManagerClient, server_ip_addr: str, *keyspace_list):
    """Disable autocompaction that might interfere with testing"""

    logger.info("Disabling autocompaction across keyspaces")
    for ks in (*keyspace_list, "system", "system_schema"):
        await manager.api.disable_autocompaction(server_ip_addr, ks)

@pytest.mark.asyncio
@pytest.mark.parametrize("compaction_flush_all_tables_before_major_seconds", [0, 2, 10])
async def test_major_compaction_flush_all_tables(manager: ManagerClient, compaction_flush_all_tables_before_major_seconds):
    """
    1. Start server with configured compaction_flush_all_tables_before_major_seconds value
    2. Create table and insert few rows
    3. Run major compaction and verify if all tables were flushed
       - if compaction_flush_all_tables_before_major_seconds == 0, expect no flush to happen
       - if compaction_flush_all_tables_before_major_seconds == 2 or 10, expect all tables to be flushed
    4. Sleep for 2 seconds
    3. Run major compaction again and verify if all tables were flushed
       - if compaction_flush_all_tables_before_major_seconds == 0, expect no flush to happen
       - if compaction_flush_all_tables_before_major_seconds == 2, expect all tables to be flushed as 2 seconds have elapsed already
       - if compaction_flush_all_tables_before_major_seconds == 10, expect no flush to happen as only 2 seconds have elapsed
    """
    logger.info("Bootstrapping cluster")
    cfg = {'compaction_flush_all_tables_before_major_seconds' : compaction_flush_all_tables_before_major_seconds}
    server = (await manager.servers_add(1, config=cfg, cmdline=['--smp=1']))[0]

    logger.info("Creating table")
    ks = "test_flush_all_tables"
    cf = "t1"
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int PRIMARY KEY)")
    await disable_autocompaction_across_keyspaces(manager, server.ip_addr, ks)

    logger.info("Populating table")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{cf} (pk) VALUES ({k});") for k in range(256)])
    await manager.api.keyspace_flush(server.ip_addr, ks, cf)
    log = await manager.server_open_log(server.server_id)

    async def check_all_table_flush_in_major_compaction(expect_all_table_flush: bool):
        mark = await log.mark()

        logger.info("Start major compaction")
        await manager.api.keyspace_compaction(server.ip_addr, ks, cf)

        flush_log = await log.grep("Forcing new commitlog segment and flushing all tables", from_mark=mark)
        assert len(flush_log) == (1 if expect_all_table_flush else 0)

    # all tables should be flushed the first time unless compaction_flush_all_tables_before_major_seconds == 0
    await check_all_table_flush_in_major_compaction(compaction_flush_all_tables_before_major_seconds != 0)

    if compaction_flush_all_tables_before_major_seconds == 2:
        # let 2 seconds pass before trying again
        await asyncio.sleep(compaction_flush_all_tables_before_major_seconds)

    # for the second time, all tables should be flushed only if
    # compaction_flush_all_tables_before_major_seconds == 2 as only 2 seconds have passed
    await check_all_table_flush_in_major_compaction(compaction_flush_all_tables_before_major_seconds == 2)
