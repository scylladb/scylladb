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
