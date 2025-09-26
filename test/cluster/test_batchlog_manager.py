#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import pytest
import logging
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for
from test.cluster.util import new_test_keyspace, reconnect_driver, wait_for_cql_and_get_hosts
from test.cluster.conftest import skip_mode

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_batchlog_replay_while_a_node_is_down(manager: ManagerClient) -> None:
    """ Test that batchlog replay handles the case when a node is down while replaying a batch.
        Reproduces issue #24599.
    1. Create a cluster with 3 nodes.
    2. Write a batch and inject an error to fail it before it's removed from the batchlog, so it
       needs to be replayed.
    3. Stop server 1.
    4. Server 0 tries to replay the batch. it sends the mutation to all replicas, but one of them is down,
       so it should fail.
    5. Bring server 1 back up.
    6. Verify that the batch is replayed and removed from the batchlog eventually.
    """

    cmdline=['--logger-log-level', 'batchlog_manager=trace']
    config = {'error_injections_at_startup': ['short_batchlog_manager_replay_interval'], 'write_request_timeout_in_ms': 2000}
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v int, PRIMARY KEY (key, c))")

        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "storage_proxy_fail_remove_from_batchlog", one_shot=False) for s in servers])

        # make sure the batch is replayed only after the server is stopped
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "skip_batch_replay", one_shot=False) for s in servers])

        s0_log = await manager.server_open_log(servers[0].server_id)

        try:
            await cql.run_async(f"BEGIN BATCH INSERT INTO {ks}.tab (key, c, v) VALUES (0,0,0); INSERT INTO {ks}.tab (key, c, v) VALUES (1,1,1); APPLY BATCH")
        except Exception as e:
            # injected error is expected
            logger.error(f"Error executing batch: {e}")

        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, "storage_proxy_fail_remove_from_batchlog") for s in servers])

        await manager.server_stop(servers[1].server_id)

        batchlog_row_count = (await cql.run_async("SELECT COUNT(*) FROM system.batchlog", host=hosts[0]))[0].count
        assert batchlog_row_count > 0

        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, "skip_batch_replay") for s in servers if s != servers[1]])

        # The batch is replayed while server 1 is down
        await s0_log.wait_for('Replaying batch', timeout=60)
        await asyncio.sleep(1)

        # Bring server 1 back up and verify that eventually the batch is replayed and removed from the batchlog
        await manager.server_start(servers[1].server_id)

        s0_mark = await s0_log.mark()
        await s0_log.wait_for('Finished replayAllFailedBatches', timeout=60, from_mark=s0_mark)

        async def batchlog_empty() -> bool:
            batchlog_row_count = (await cql.run_async("SELECT COUNT(*) FROM system.batchlog", host=hosts[0]))[0].count
            if batchlog_row_count == 0:
                return True
        await wait_for(batchlog_empty, time.time() + 60)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_batchlog_replay_aborted_on_shutdown(manager: ManagerClient) -> None:
    """ Similar to the previous test, but also verifies that the batchlog replay is aborted on shutdown,
        and node shutdown is not stuck.
    1. Create a cluster with 3 nodes.
    2. Write a batch and inject an error to fail it before it's removed from the batchlog, so it
       needs to be replayed.
    3. Stop server 1.
    4. Server 0 tries to replay the batch. it sends the mutation to all replicas, but one of them is down,
       so it should fail.
    5. Shut down server 0 gracefully, which should abort the batchlog replay which is in progress.
    6. Bring server 0 and server 1 back up.
    6. Verify that the batch is replayed and removed from the batchlog eventually.
    """

    cmdline=['--logger-log-level', 'batchlog_manager=trace']
    config = {'error_injections_at_startup': ['short_batchlog_manager_replay_interval'], 'write_request_timeout_in_ms': 2000}
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v int, PRIMARY KEY (key, c))")

        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "storage_proxy_fail_remove_from_batchlog", one_shot=False) for s in servers])

        # make sure the batch is replayed only after the server is stopped
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "skip_batch_replay", one_shot=False) for s in servers])

        s0_log = await manager.server_open_log(servers[0].server_id)

        try:
            await cql.run_async(f"BEGIN BATCH INSERT INTO {ks}.tab (key, c, v) VALUES (0,0,0); INSERT INTO {ks}.tab (key, c, v) VALUES (1,1,1); APPLY BATCH")
        except Exception as e:
            # injected error is expected
            logger.error(f"Error executing batch: {e}")

        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, "storage_proxy_fail_remove_from_batchlog") for s in servers])

        await manager.server_stop(servers[1].server_id)

        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, "skip_batch_replay") for s in servers if s != servers[1]])

        batchlog_row_count = (await cql.run_async("SELECT COUNT(*) FROM system.batchlog", host=hosts[0]))[0].count
        assert batchlog_row_count > 0

        # The batch is replayed while server 1 is down
        await s0_log.wait_for('Replaying batch', timeout=60)
        await asyncio.sleep(1)

        # verify shutdown is not stuck
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.server_start(servers[0].server_id)
        await manager.server_start(servers[1].server_id)

        cql = await reconnect_driver(manager)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        async def batchlog_empty() -> bool:
            batchlog_row_count = (await cql.run_async("SELECT COUNT(*) FROM system.batchlog", host=hosts[0]))[0].count
            if batchlog_row_count == 0:
                return True
        await wait_for(batchlog_empty, time.time() + 60)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_batchlog_replay_includes_cdc(manager: ManagerClient) -> None:
    """ Test that when a batch is replayed from the batchlog, it includes CDC mutations.
    1. Create a cluster with a single node.
    2. Create a table with CDC enabled.
    3. Write a batch and inject an error to fail it after it's written to the batchlog but before the mutation is applied.
    4. Wait for the batch to be replayed.
    5. Verify that the data is written to the base table.
    6. Verify that CDC mutations are also applied and visible in the CDC log table.
    """

    cmdline = ['--logger-log-level', 'batchlog_manager=trace']
    config = {'error_injections_at_startup': ['short_batchlog_manager_replay_interval'], 'write_request_timeout_in_ms': 2000}

    servers = await manager.servers_add(1, config=config, cmdline=cmdline)
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        # Create table with CDC enabled
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v int, PRIMARY KEY (key, c)) WITH cdc = {{'enabled': true}}")

        # Enable error injection to make the batch fail after writing to batchlog
        await manager.api.enable_injection(servers[0].ip_addr, "storage_proxy_fail_remove_from_batchlog", one_shot=False)

        # Execute a batch that will fail due to injection but be written to batchlog
        try:
            await cql.run_async(
                "BEGIN BATCH " +
                f"INSERT INTO {ks}.tab(key, c, v) VALUES (10, 20, 30); " +
                f"INSERT INTO {ks}.tab(key, c, v) VALUES (40, 50, 60); " +
                "APPLY BATCH"
            )
        except Exception as e:
            logger.info(f"Expected error executing batch: {e}")

        await manager.api.disable_injection(servers[0].ip_addr, "storage_proxy_fail_remove_from_batchlog")

        # Wait for data to appear in the base table
        async def data_written():
            result1 = await cql.run_async(f"SELECT * FROM {ks}.tab WHERE key = 10 AND c = 20")
            result2 = await cql.run_async(f"SELECT * FROM {ks}.tab WHERE key = 40 AND c = 50")
            if len(result1) > 0 and len(result2) > 0:
                return True
        await wait_for(data_written, time.time() + 60)

        # Check that CDC log table exists and has the CDC mutations
        cdc_table_name = f"{ks}.tab_scylla_cdc_log"

        # Wait for CDC mutations to be visible
        async def cdc_data_present():
            result1 = await cql.run_async(f"SELECT * FROM {cdc_table_name} WHERE key = 10 ALLOW FILTERING")
            result2 = await cql.run_async(f"SELECT * FROM {cdc_table_name} WHERE key = 40 ALLOW FILTERING")
            if len(result1) > 0 and len(result2) > 0:
                return True
        await wait_for(cdc_data_present, time.time() + 60)

        result1 = await cql.run_async(f"SELECT * FROM {cdc_table_name} WHERE key = 10 ALLOW FILTERING")
        assert len(result1) == 1, f"Expected 1 CDC mutation for key 10, got {len(result1)}"

        result2 = await cql.run_async(f"SELECT * FROM {cdc_table_name} WHERE key = 40 ALLOW FILTERING")
        assert len(result2) == 1, f"Expected 1 CDC mutation for key 40, got {len(result2)}"

@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_drop_mutations_for_dropped_table(manager: ManagerClient) -> None:
    """
    This test is an adjusted version of `test_batchlog_replay_while_a_node_is_down`.
    We want to verify that batchlog replay is aborted when the corresponding table has been dropped.

    This test reproduces scylladb/scylladb#24806.

    1. Create a cluster with 2 nodes, a keyspace, and a table.
    2. Enable error injections. We need to ensure that:
       - The mutations are not removed from the batchlog.
       - The mutations are not replayed from the batchlog before we drop the table.
    3. Write a batch. Because of step 2, the mutations will stay in the batchlog.
    4. Drop the table.
    5. Resume batchlog replay.
    6. Wait for the replay to finish. Verify that the batchlog is empty.

    Note: This test will most likely work even with a 1-node cluster, but let's
          use 2 nodes to make sure we're dealing with a realistic scenario.
    """

    cmdline=["--logger-log-level", "batchlog_manager=trace"]
    config = {"error_injections_at_startup": ["short_batchlog_manager_replay_interval"],
              "write_request_timeout_in_ms": 2000}

    servers = await manager.servers_add(2, config=config, cmdline=cmdline, auto_rack_dc="dc1")
    s1, _ = servers

    cql, hosts = await manager.get_ready_cql(servers)
    host1, _ = hosts

    async def get_batchlog_row_count():
        rows = await cql.run_async("SELECT COUNT(*) FROM system.batchlog", host=host1)
        row = rows[0]
        assert hasattr(row, "count")

        result = row.count
        logger.debug(f"Batchlow row count={result}")

        return result

    async def enable_injection(injection: str) -> None:
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, injection, one_shot=False) for s in servers])

    async def disable_injection(injection: str) -> None:
        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, injection) for s in servers])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.my_table (pk int, ck int, v int, PRIMARY KEY (pk, ck))")

        # Make sure the mutations stay in the batchlog.
        await enable_injection("storage_proxy_fail_remove_from_batchlog")
        # Make sure the mutations are not replayed too early (i.e. before dropping the table).
        await enable_injection("skip_batch_replay")

        s1_log = await manager.server_open_log(s1.server_id)

        try:
            await cql.run_async(f"BEGIN BATCH INSERT INTO {ks}.my_table (pk, ck, v) VALUES (0,0,0);"
                                f"INSERT INTO {ks}.my_table (pk, ck, v) VALUES (1,1,1); APPLY BATCH")
        except Exception as e:
            # Injected error is expected.
            logger.error(f"Error executing batch: {e}")

        # Once the mutations are in the batchlog, waiting to be replayed, we can disable this.
        await disable_injection("storage_proxy_fail_remove_from_batchlog")

        batchlog_row_count = await get_batchlog_row_count()
        assert batchlog_row_count > 0

        await cql.run_async(f"DROP TABLE {ks}.my_table")

        s1_mark = await s1_log.mark()
        # Once the table is dropped, we can resume the replay. The bug can
        # be triggered from now on (if it's present).
        await disable_injection("skip_batch_replay")

        # We don't need these, but let's keep them just so we know the replay
        # is really going on and the mutations don't just disappear.
        await s1_log.wait_for("Replaying batch", timeout=60, from_mark=s1_mark)
        await s1_log.wait_for("Finished replayAllFailedBatches", timeout=60, from_mark=s1_mark)

        async def batchlog_empty() -> bool:
            batchlog_row_count = await get_batchlog_row_count()
            return True if batchlog_row_count == 0 else None

        await wait_for(batchlog_empty, time.time() + 60)


@pytest.mark.asyncio
async def test_batchlog_traces(manager: ManagerClient) -> None:
    cmdline=['--logger-log-level', 'batchlog_manager=trace']
    config = {'write_request_timeout_in_ms': 2000}
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v int, PRIMARY KEY (key, c)) WITH tombstone_gc = {{'mode': 'repair'}}")

        for _ in range(30):
            await cql.run_async(f"BEGIN BATCH INSERT INTO {ks}.tab (key, c, v) VALUES (0,0,0); INSERT INTO {ks}.tab (key, c, v) VALUES (1,1,1); APPLY BATCH")

        await manager.api.repair(servers[0].ip_addr, ks, "tab")

    assert False
