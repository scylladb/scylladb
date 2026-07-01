import asyncio
import pytest
import logging
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from cassandra.query import SimpleStatement # type: ignore
from cassandra.cluster import ConsistencyLevel # type: ignore
from cassandra.protocol import ReadFailure, ReadTimeout # type: ignore
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace, reconnect_driver


logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_node_shutdown_waits_for_pending_requests(manager: ManagerClient) -> None:
    """Reproducer for #16382"""

    logger.info('start two nodes')
    servers = await manager.servers_add(servers_num=2, auto_rack_dc="dc")
    cql = manager.get_cql()

    logger.info(f'wait for host for the node {servers[0]}, servers {servers}')
    h0 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]

    logger.info('create keyspace and table')
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f'create table {ks}.test_table (pk int primary key)')

        logger.info('insert test row into the table')
        await cql.run_async(f'insert into {ks}.test_table(pk) values (42)')

        logger.info(f'make storage_proxy::handle_read error injection on the node {servers[1]}')
        injection_handler = await inject_error_one_shot(
            manager.api, servers[1].ip_addr, 'storage_proxy::handle_read', parameters={'cf_name': 'test_table'})

        logger.info(f'start ConsistencyLevel.ALL read request on {servers[0]} as coordinator')
        read_future = cql.run_async(SimpleStatement(f'select pk from {ks}.test_table using timeout 1000ms',
                                                    consistency_level=ConsistencyLevel.ALL),
                                    host=h0)

        logger.info(f'wait until the read request hit storage_proxy::handle_read on the node {servers[1]}')
        log_file2 = await manager.server_open_log(servers[1].server_id)
        await manager.api.wait_for_injection_enter(servers[1].ip_addr, "storage_proxy::handle_read")

        logger.info(f'trigger shutdown of the node {servers[1]}')
        stop_future = asyncio.create_task(manager.server_stop_gracefully(servers[1].server_id))

        logger.info('wait until node shutdown stops accepting client requests')
        await log_file2.wait_for('Stop transport: shutdown rpc and cql server done', timeout=60)

        logger.info(f'release the read request')
        await injection_handler.message()

        # This range read can still fail while the replica is shutting down.
        # The fix below targets an already in-flight single-partition replica read
        # whose RPC reply must be drained before messaging shutdown; it does not
        # require a shutting-down replica to keep serving range-read follow-up work.
        logger.info(f'wait for read request')
        with pytest.raises((ReadFailure, ReadTimeout)):
            await read_future

        logger.info(f'wait for successful node {servers[1]} shutdown')
        await stop_future

        logger.info('done')

        # For dropping the keyspace
        await manager.server_start(servers[1].server_id)
        await reconnect_driver(manager)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_quorum_read_survives_replica_shutdown(manager: ManagerClient) -> None:
    """Reproducer for SCYLLADB-2956.

    A QUORUM read with RF=3 should survive one replica shutting down while handling
    the read. Current code closes internode RPC before the in-flight replica read
    can answer, so this test fails with a read error/timeout instead of returning
    the row.
    """

    logger.info('start three nodes')
    servers = await manager.servers_add(servers_num=3, auto_rack_dc="dc")
    cql, hosts = await manager.get_ready_cql(servers)

    injection = 'storage_proxy::handle_read'
    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"create table {ks}.test_table (pk int primary key) with speculative_retry = 'NONE'")
        await cql.run_async(SimpleStatement(f'insert into {ks}.test_table(pk) values (42)', consistency_level=ConsistencyLevel.ALL), host=hosts[0])

        for server in servers[1:]:
            await manager.api.enable_injection(server.ip_addr, injection, one_shot=True, parameters={'cf_name': 'test_table'})

        logger.info(f'start ConsistencyLevel.QUORUM read request on {servers[0]} as coordinator')
        read_future = cql.run_async(SimpleStatement(f'select pk from {ks}.test_table where pk = 42 using timeout 10s',
                                                    consistency_level=ConsistencyLevel.QUORUM), host=hosts[0])

        async def wait_for_replica_read(server):
            await manager.api.wait_for_injection_enter(server.ip_addr, injection)
            return server

        wait_tasks = [asyncio.create_task(wait_for_replica_read(server)) for server in servers[1:]]
        done, pending = await asyncio.wait(wait_tasks, timeout=60, return_when=asyncio.FIRST_COMPLETED)
        assert done, 'read did not reach any remote replica'
        target = next(iter(done)).result()
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        for server in servers[1:]:
            if server.server_id == target.server_id:
                continue
            await manager.api.disable_injection(server.ip_addr, injection)

        logger.info(f'trigger shutdown of the replica handling the read: {target}')
        log_file = await manager.server_open_log(target.server_id)
        stop_task = asyncio.create_task(manager.server_stop_gracefully(target.server_id))

        logger.info('wait until node shutdown stops accepting client requests')
        await log_file.wait_for('Stop transport: shutdown rpc and cql server done', timeout=60)

        logger.info('release the blocked read request')
        await manager.api.message_injection(target.ip_addr, injection)
        rows = await read_future
        assert [row.pk for row in rows] == [42]
        await stop_task
