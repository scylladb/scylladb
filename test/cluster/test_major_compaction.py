#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import logging
import asyncio

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace, reconnect_driver

logger = logging.getLogger(__name__)

async def disable_autocompaction_across_keyspaces(manager: ManagerClient, server_ip_addr: str, *keyspace_list):
    """Disable autocompaction that might interfere with testing"""

    logger.info("Disabling autocompaction across keyspaces")
    for ks in (*keyspace_list, "system", "system_schema"):
        await manager.api.disable_autocompaction(server_ip_addr, ks)

@pytest.mark.asyncio
@pytest.mark.parametrize("consider_only_existing_data", [True, False])
@skip_mode('release', 'error injections are not supported in release mode')
async def test_major_compaction_consider_only_existing_data(manager: ManagerClient, consider_only_existing_data):
    """
    Test compactions drop tombstones when consider_only_existing_data is enabled.
    1. Create a single node cluster.
    2. Write some keys and then delete a few to create tombstones.
    3. Start major compaction with consider_only_existing_data=true but make it wait
        through error injection right after it has collected the sstables for compaction.
    4. Insert the deleted keys with backdated data into memtables and flush one
        of them into a new sstable that will not be a part of the compaction.
    5. Resume the major compaction and let it complete.
    6. Verify the results.
       - If consider_only_existing_data is False, the tombstones should not be purged and the backdated rows should not be visible
       - If consider_only_existing_data is True, the tombstones should be purged and the backdated rows should be visible
    """
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--tablets-initial-scale-factor=1' # The test assumes 1 compaction group per shard because of injection point trap
    ]
    server = (await manager.servers_add(1, cmdline=cmdline))[0]

    logger.info("Creating table")
    cf = "test_consider_only_existing_data"
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int PRIMARY KEY) WITH tombstone_gc = {{'mode': 'immediate'}}")
        await disable_autocompaction_across_keyspaces(manager, server.ip_addr, ks)

        logger.info("Populating table")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{cf} (pk) VALUES ({k});") for k in range(20)])
        await asyncio.gather(*[cql.run_async(f"DELETE FROM {ks}.{cf} WHERE pk = {k};") for k in range(10)])
        await manager.api.keyspace_flush(server.ip_addr, ks, cf)

        # let a second pass, so that the tombstones are eligible for gc
        await asyncio.sleep(1)

        # error injection to make compaction wait after collecting sstables
        injection = "major_compaction_wait"
        injection_handler = await inject_error_one_shot(manager.api, server.ip_addr, injection)

        logger.info("Start major compaction")
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        compaction_task = asyncio.create_task(manager.api.keyspace_compaction(server.ip_addr, ks, cf, consider_only_existing_data=consider_only_existing_data))
        # wait for the injection to pause the compaction
        await log.wait_for("major_compaction_wait: waiting", from_mark=mark, timeout=30)

        # insert new backdated rows with deleted keys and flush them
        # into a new sstable that will not be part of the major compaction
        logger.info("Insert backdated data into the table")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{cf} (pk) VALUES ({k}) USING TIMESTAMP 1;") for k in range(5)])
        await manager.api.keyspace_flush(server.ip_addr, ks, cf)

        # insert few more rows with deleted keys with backdated data into memtable
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{cf} (pk) VALUES ({k}) USING TIMESTAMP 1;") for k in range(5, 10)])

        # resume compaction
        await injection_handler.message()
        await compaction_task

        # evict cache to make backdated data visible for consider_only_existing_data mode
        if consider_only_existing_data:
            await manager.api.drop_sstable_caches(server.ip_addr)

        logger.info("Verify major compaction results")
        expected_count = 1 if consider_only_existing_data else 0
        for k in range(10):
            assert len(await cql.run_async(f"SELECT * FROM {ks}.{cf} WHERE pk = {k}")) == expected_count

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
    cf = "test_flush_all_tables"
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
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
            assert len(flush_log) == (2 if expect_all_table_flush else 0)

        # all tables should be flushed the first time unless compaction_flush_all_tables_before_major_seconds == 0
        await check_all_table_flush_in_major_compaction(compaction_flush_all_tables_before_major_seconds != 0)

        if compaction_flush_all_tables_before_major_seconds == 2:
            # let 2 seconds pass before trying again
            await asyncio.sleep(compaction_flush_all_tables_before_major_seconds)

        # for the second time, all tables should be flushed only if
        # compaction_flush_all_tables_before_major_seconds == 2 as only 2 seconds have passed
        await check_all_table_flush_in_major_compaction(compaction_flush_all_tables_before_major_seconds == 2)

# Testcase for https://github.com/scylladb/scylladb/issues/20197
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_shutdown_drain_during_compaction(manager: ManagerClient):
    """
    Test drain/shutdown during compaction doesn't throw any unexpected errors
    1. Create a single node cluster.
    2. Create a table and populate it.
    3. Inject error to make compaction wait right before updating compaction_history table
    4. Start compaction, wait for it to reach injection point
    5. Shutdown server and resume compaction.
    6. Verify that the shutdown did not throw any error except 'seastar::abort_requested_exception'
    """
    logger.info("Bootstrapping cluster")
    server = await manager.server_add(cmdline=['--smp=1'])

    logger.info("Creating table")
    cf = "test_shutdown_drain_during_compaction"
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int PRIMARY KEY);")
        await disable_autocompaction_across_keyspaces(manager, server.ip_addr, ks)

        logger.info("Populating table")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{cf} (pk) VALUES ({k});") for k in range(100)])
        await manager.api.keyspace_flush(server.ip_addr, ks, cf)

        # inject error to make compaction wait just before it updates the compaction_history table
        injection = "update_history_wait"
        injection_handler = await inject_error_one_shot(manager.api, server.ip_addr, injection)

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        # start compaction and wait for it to pause at the injection point
        logger.info("Start compaction")
        compaction_task = asyncio.create_task(manager.api.keyspace_compaction(server.ip_addr, ks, cf))
        await log.wait_for("update_history_wait: waiting", from_mark=mark, timeout=30)

        mark = await log.mark()
        # Start server shutdown
        logger.info("Shutdown server")
        stop_task = asyncio.create_task(manager.server_stop_gracefully(server.server_id))
        # wait until the shutdown drain request is sent to compaction_manager
        await log.wait_for("Asked to drain", from_mark=mark, timeout=30)
        # now resume compaction and let shutdown complete
        await injection_handler.message()
        # wait server to shutdown
        await stop_task
        # During shutdown, errors mentioning 'seastar::abort_requested_exception' is expected as we do abort the compaction midway.
        # Verify that the shutdown completed without any other unexpected errors
        assert len(await log.grep(expr="ERROR .*", filter_expr=".* seastar::abort_requested_exception \(abort requested\)", from_mark=mark)) == 0

        # For dropping the keyspace
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_alter_compaction_strategy_during_compaction(manager: ManagerClient):
    """
    Test ALTERing compaction strategy during compaction doesn't crash the server
    1. Create a single node cluster.
    2. Create a table with compaction strategy = TWCS and populate it.
    3. Inject error to make compaction wait when getting sstables for compaction.
    4. Start compaction, wait for it to reach injection point
    5. ALTER table to change compaction strategy to LCS
    6. Let compaction proceed and finish
    7. Verify no unexpected errors in logs
    """
    node1 = await manager.server_add(cmdline=['--logger-log-level', 'compaction=debug'])
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        logger.info("Create table")
        cf = "t1"
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int, ck int, val int, PRIMARY KEY (pk, ck)) WITH compaction={{'class': 'TimeWindowCompactionStrategy'}}")

        logger.info("Inject error to pause compaction midway")
        injection_name="twcs_get_sstables_for_compaction"
        await manager.api.enable_injection(node_ip=node1.ip_addr, injection=injection_name, one_shot=False)
        server_log = await manager.server_open_log(node1.server_id)

        logger.info("Populate table and start compaction")
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, ck, val) VALUES (?, ?, ?)")
        for i in range(20):
            for j in range(100):
                await cql.run_async(insert_stmt, (i, j, i * j))
        compaction_task = asyncio.create_task(manager.api.keyspace_compaction(node_ip=node1.ip_addr, keyspace=ks, table=cf))

        logger.info("Waiting for compaction to be suspended")
        await server_log.wait_for("twcs_get_sstables_for_compaction: waiting for message")

        logger.info("Alter compaction strategy")
        await cql.run_async(f"ALTER TABLE {ks}.{cf} WITH compaction = {{'class': 'LeveledCompactionStrategy'}};")

        logger.info("Resume compaction and wait for it to finish")
        await manager.api.message_injection(node_ip=node1.ip_addr, injection=injection_name)
        await manager.api.disable_injection(node_ip=node1.ip_addr, injection=injection_name)
        await compaction_task

# Testcase for https://github.com/scylladb/scylladb/issues/24501
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_disable_autocompaction_during_major_compaction(manager: ManagerClient):
    """
    Test disable autocompaction during a major compaction doesn't stop the major compaction.
    1. Create a single node cluster.
    2. Create a table and populate it.
    3. Inject error to make major compaction wait
    4. Start compaction, wait for it to reach injection point
    5. Disable autocompaction on the table
    6. Resume the major compaction and expect it to complete successfully
    """
    logger.info("Starting a single node cluster")
    server = await manager.server_add(cmdline=["--logger-log-level", "compaction=debug"])
    # disable autocompaction on system and system_schema keyspaces to avoid interference with testing
    await disable_autocompaction_across_keyspaces(manager, server.ip_addr, "system", "system_schema")

    cf = "cf"
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        logger.info("Creating table")
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int PRIMARY KEY)")

        logger.info("Populating table")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{cf} (pk) VALUES ({k});") for k in range(100)])
        await manager.api.keyspace_flush(server.ip_addr, ks, cf)

        logger.info("Inject error to make major compaction wait")
        injection = "major_compaction_wait"
        await manager.api.enable_injection(server.ip_addr, injection, False)

        logger.info("Start compaction and wait for it to pause at the injection point")
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        compaction_task = asyncio.create_task(manager.api.keyspace_compaction(server.ip_addr, ks, cf))
        await log.wait_for("major_compaction_wait: waiting", from_mark=mark, timeout=30)

        # Now that major compaction is in progress, disable autocompaction on the table
        logger.info("Disabling autocompaction on the table")
        await manager.api.disable_autocompaction(server.ip_addr, ks, cf)

        # now resume major compaction and it should complete successfully
        logger.info("Resuming major compaction")
        mark = await log.mark()
        await manager.api.message_injection(server.ip_addr, injection)
        await compaction_task
        await log.wait_for(f"Major {ks}.{cf} .* Compacted .*", from_mark=mark, timeout=30)

