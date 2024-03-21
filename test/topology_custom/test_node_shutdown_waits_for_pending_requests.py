import asyncio
import pytest
import logging
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from cassandra.query import SimpleStatement # type: ignore
from cassandra.cluster import ConsistencyLevel # type: ignore
from cassandra.protocol import ReadTimeout # type: ignore
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_node_shutdown_waits_for_pending_requests(manager: ManagerClient) -> None:
    """Reproducer for #16382"""

    logger.info('start two nodes')
    servers = await manager.servers_add(servers_num=2)
    cql = manager.get_cql()

    logger.info(f'wait for host for the node {servers[0]}, servers {servers}')
    h0 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]

    logger.info('create keyspace and table')
    await cql.run_async("create keyspace ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    await cql.run_async('create table ks.test_table (pk int primary key)')

    logger.info('insert test row into the table')
    await cql.run_async('insert into ks.test_table(pk) values (42)')

    logger.info(f'make storage_proxy::handle_read error injection on the node {servers[1]}')
    injection_handler = await inject_error_one_shot(
        manager.api, servers[1].ip_addr, 'storage_proxy::handle_read', parameters={'cf_name': 'test_table'})

    logger.info(f'start ConsistencyLevel.ALL read request on {servers[0]} as coordinator')
    read_future = cql.run_async(SimpleStatement('select pk from ks.test_table using timeout 1000ms',
                                                consistency_level=ConsistencyLevel.ALL),
                                host=h0)

    logger.info(f'wait until the read request hit storage_proxy::handle_read on the node {servers[1]}')
    log_file2 = await manager.server_open_log(servers[1].server_id)
    await log_file2.wait_for("storage_proxy::handle_read injection hit", timeout=60)

    logger.info(f'trigger shutdown of the node {servers[1]}')
    stop_future = asyncio.create_task(manager.server_stop_gracefully(servers[1].server_id))

    logger.info(f'wait until node shutdown process reaches the storage proxy verbs')
    await log_file2.wait_for("Shutting down storage proxy RPC verbs", timeout=60)

    logger.info(f'release the read request')
    await injection_handler.message()

    # We get a timeout instead of the actual response here.
    # This seems to be a flaw in the current Scylla code — when a node
    # is shutting down, the drain_on_shutdown method if storage_service is called before
    # storage_proxy::stop_remote. The drain_on_shutdown calls messaging_service::shutdown,
    # which means that although storage_proxy::stop_remote waits for current requests to complete,
    # client sockets are already closed so the responses can't be delivered to the clients.
    # We get a timeout and not a failure because digest_read_resolver::on_error has
    # a magic special case for error_kind::DISCONNECT:
    # "wait for timeout in hope that the client will issue speculative read"
    logger.info(f'wait for read request')
    with pytest.raises(ReadTimeout):
        await read_future

    logger.info(f'wait for successful node {servers[1]} shutdown')
    await stop_future

    logger.info('done')
