# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
from test.pylib.manager_client import ManagerClient

import pytest
import logging
from uuid import UUID

from test.pylib.rest_client import inject_error_one_shot, read_barrier
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster import gather_safely
from test.topology.conftest import skip_mode
from test.topology.util import disable_schema_agreement_wait, new_test_keyspace, reconnect_driver

from cassandra.cluster import ConsistencyLevel, SimpleStatement

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_broken_bootstrap(manager: ManagerClient):
    server_a = await manager.server_add()
    server_b = await manager.server_add(start=False)

    async with new_test_keyspace(manager, "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.test"
        await manager.cql.run_async(f"CREATE TABLE {table} (a int PRIMARY KEY, b int)")
        for i in range(100):
            await manager.cql.run_async(f"INSERT INTO {table} (a, b) VALUES ({i}, {i})")
        await inject_error_one_shot(manager.api, server_a.ip_addr, "crash-before-bootstrapping-node-added")
        try:
            # Timeout fast since we do not expect the operation to complete
            # because the coordinator is dead by now due to the error injection
            # above
            await manager.server_start(server_b.server_id, timeout=5)
            pytest.fail("Expected server_add to fail")
        except Exception:
            pass

        await manager.server_stop(server_b.server_id)
        await manager.server_stop(server_a.server_id)

        stop_event = asyncio.Event()
        async def worker():
            logger.info("Worker started")
            while not stop_event.is_set():
                for i in range(100):
                    await manager.cql.run_async(f"INSERT INTO {table} (a, b) VALUES ({i}, {i})")
                    response = await manager.cql.run_async(f"SELECT * FROM {table} WHERE a = {i}")
                    assert response[0].b == i
                await asyncio.sleep(0.1)
            logger.info("Worker stopped")

        await manager.server_start(server_a.server_id)
        await manager.driver_connect()

        worker_task = asyncio.create_task(worker())

        await asyncio.sleep(20)
        stop_event.set()
        await worker_task


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_full_shutdown_during_replace(manager: ManagerClient):
    """
    Test that shutting down all live nodes during replace doesn't cause the IP address of the replacing node to be
    missing on restart. After restarts, the test performs requests with the replacing node as a pending replica, which
    would crash the request coordinators if the IP address was missing.

    Regression test for scylla-enterprise#5686. Note that in the reported issue `nodetool status` performed during
    stuck replace caused a crash, but the problem turned out to be more general.
    """
    logger.info('Adding servers')
    leader = await manager.server_add(property_file={'dc': 'dc1', 'rack': 'rack0'})
    follower, dead_server = await manager.servers_add(2, auto_rack_dc='dc1')
    live_servers = [leader, follower]

    logger.info('Waiting for driver')
    cql, [host] = await manager.get_ready_cql([follower])

    with disable_schema_agreement_wait(cql):  # This makes the test a bit faster.
        async with new_test_keyspace(manager, """WITH REPLICATION = {'class': 'NetworkTopologyStrategy',
                                     'replication_factor': 3} AND tablets = {'enabled': false}""", host) as ks:
            table = f'{ks}.test'
            await cql.run_async(f'CREATE TABLE {table} (a int PRIMARY KEY, b int)', host=host)

            logger.info(f'Stopping {dead_server}')
            await manager.server_stop_gracefully(dead_server.server_id)

            await manager.api.enable_injection(
                leader.ip_addr, 'topology_coordinator/write_both_read_old/before_global_token_metadata_barrier', True)

            replace_cfg = ReplaceConfig(replaced_id=dead_server.server_id, reuse_ip_addr=False, use_host_id=False)
            logger.info(f'Adding the server that will replace {dead_server}')
            replacing_server = await manager.server_add(
                replace_cfg, property_file=dead_server.property_file(), start=False)

            logger.info(f'Trying to replace {dead_server} with {replacing_server}')
            replacing_task = asyncio.create_task(manager.server_start(replacing_server.server_id))

            logger.info('Waiting for the topology coordinator to pause in write_both_read_old')
            leader_log = await manager.server_open_log(leader.server_id)
            await leader_log.wait_for(
                'topology_coordinator/write_both_read_old/before_global_token_metadata_barrier: waiting for message')

            logger.info(f'Stopping {live_servers + [replacing_server]}')
            await gather_safely(*(manager.server_stop(srv.server_id) for srv in live_servers + [replacing_server]))
            replacing_task.cancel()

            for srv in live_servers:
                await manager.server_update_config(srv.server_id, 'error_injections_at_startup',
                    ['topology_coordinator/write_both_read_old/before_global_token_metadata_barrier'])

            logger.info(f'Starting {live_servers}')
            await gather_safely(*(manager.server_start(srv.server_id) for srv in live_servers))

            # This is needed to ensure that the replacing node will be a pending replica for the requests below.
            logger.info(f'Waiting for {live_servers} to apply the latest topology state')
            await gather_safely(*[read_barrier(manager.api, srv.ip_addr) for srv in live_servers])

            await reconnect_driver(manager)
            logger.info('Waiting for driver')
            cql, hosts = await manager.get_ready_cql(live_servers)

            logger.info(f'Sending requests to {table} with {replacing_server} as a pending replica')
            for i in range(100):
                await cql.run_async(SimpleStatement(f'INSERT INTO {table} (a, b) VALUES ({i}, {i})',
                                                    consistency_level=ConsistencyLevel.ONE))
                response = await manager.cql.run_async(f"SELECT * FROM {table} WHERE a = {i}")
                assert response[0].b == i

            for srv in live_servers:
                await manager.api.message_injection(
                    srv.ip_addr, 'topology_coordinator/write_both_read_old/before_global_token_metadata_barrier')

            logger.info(f'Retrying replace of {dead_server}')
            new_server = await manager.server_add(replace_cfg, property_file=dead_server.property_file())
            live_servers.append(new_server)

            logger.info(f'Checking peers on {live_servers}')
            host_ids = [await manager.get_host_id(srv.server_id) for srv in live_servers]
            for srv, host in zip(live_servers, hosts):
                result = await cql.run_async('SELECT peer, host_id FROM system.peers', host=host)
                peers = {(row.peer, row.host_id) for row in result}
                expected = {(other.ip_addr, UUID(id)) for other, id in zip(live_servers, host_ids) if other != srv}
                assert peers == expected
