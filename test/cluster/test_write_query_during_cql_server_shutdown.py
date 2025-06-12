#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import pytest
import asyncio
import time

from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace
from test.cluster.test_tablets2 import inject_error_on
from cassandra.cluster import ConnectionException, NoHostAvailable  # type: ignore
from test.cluster.conftest import skip_mode

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_write_query_during_cql_server_shutdown(request: pytest.FixtureRequest, manager: ManagerClient) -> None:
    """
    Test query execution during cql connections shutdown.

     1. Start 3 servers
     2. Create a keyspace with replication factor 3
     3. Use error injection to pause write responses on 2 nodes of the cluster.
     4  Send a write query through the remaining node.
     5. Make sure the query coordinator started shutting down.
     6. Unpause the write responses from 2 nodes.
     7. Make sure request is completed successfully.
    """

    logger.info("Creating a new cluster")

    cmdline = [
        '--logger-log-level', 'debug_error_injection=debug',
    ]

    servers = await manager.servers_add(3, auto_rack_dc="dc1", cmdline=cmdline)

    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int primary key, v int)")

        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        target_host = hosts[2]
        target_server = servers[2]

        target_server_log = await manager.server_open_log(target_server.server_id)

        # Make sure responses on the other replicas will be delayed.
        servers_to_pause = [servers[1], servers[0]]
        await inject_error_on(manager, "storage_proxy_response_pause", servers_to_pause)
        logger.info(
            f"Pausing write responses on the replicas {servers_to_pause}")

        # Send a write query to the target node that will be shut down.
        async def do_send_query():
            logger.info(f"Sending a write query to the target node {target_server.server_id}")
            await cql.run_async(f"insert into {ks}.t (pk, v) values ({32765}, {17777})", host=target_host)

        write_task = asyncio.create_task(do_send_query())

        # Make sure nodes that have to pause the request response, got the write request and started waiting.
        for server in servers_to_pause:
            paused_server_logs = await manager.server_open_log(server.server_id)
            await paused_server_logs.wait_for("storage_proxy_response_pause: waiting for message")

        # Start shutdown of the query coordinator node
        async def do_shutdown():
            logger.info(f"Starting shutdown of node {target_server.server_id}")
            await manager.server_stop_gracefully(target_server.server_id)

        shutdown_task = asyncio.create_task(do_shutdown())

        # Wait for the shutdown to start
        await target_server_log.wait_for("init - Shutting down local storage")
        await asyncio.sleep(1)

        logger.info(f"Unblocking writes on the nodes {servers_to_pause}")
        for server in servers_to_pause:
            await manager.api.message_injection(server.ip_addr, 'storage_proxy_response_pause')

        logger.info("Waiting for write query to complete")
        await write_task

        logger.info("Waiting for the shutdown to complete")
        await shutdown_task


