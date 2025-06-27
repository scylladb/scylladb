#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import threading
import pytest
import asyncio
import time

from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import check_token_ring_and_group0_consistency, new_test_keyspace
from test.pylib.util import wait_for
from test.cluster.test_tablets2 import inject_error_on
from test.pylib.scylla_cluster import ReplaceConfig
from test.cluster.util import get_topology_coordinator
from cassandra.cluster import ConnectionException, NoHostAvailable  # type: ignore
from test.cluster.conftest import skip_mode

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_unfinished_writes_during_shutdown(request: pytest.FixtureRequest, manager: ManagerClient) -> None:
    """ Test a simultaneous topology change and write query during shutdown, which may cause the node to get stuck (see https://github.com/scylladb/scylladb/issues/23665).

     1. Create a keyspace with replication factor 3
     2. Start 3 servers
     3. Use error injection to pause the 3rd node on a topology change (`barrier_and_drain`)
     4  Trigger a topology change by adding a new node to the cluster.
     5. Make sure the topology change was paused on the node 3 (`barrier_and_drain`)
     6. Now with error injection, make sure node 2 will pause before sending a write acknowledgment.
     7. Send a write query to the node 3. (which already should be paused on the topology change operation)
     8. The query should have completed, but one write to node 2 should be remaining, making write_response_handler block the topology change in node 3
     9. Start node 3 shutdown. The shutdown should hang since the one of the replicas did not send the response and therefore the response write handler still holds the ERM.
    """
    logger.info("Creating a new cluster")

    cmdline = [
        '--logger-log-level', 'debug_error_injection=debug',
    ]

    servers = await manager.servers_add(3, auto_rack_dc="dc1", cmdline=cmdline)

    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int primary key, v int)")
        target_host = hosts[2]
        target_server = servers[2]

        # Make the target node stop before locking the ERM
        logger.info(
            f"Enabling injection 'pause_before_barrier_and_drain' on the target server {target_server}")
        target_server_log = await manager.server_open_log(target_server.server_id)
        await manager.api.enable_injection(target_server.ip_addr, "pause_before_barrier_and_drain", one_shot=True)

        async def do_add_node():
            logger.info("Adding a node to the cluster")
            try:
                await manager.server_add(property_file={"dc": "dc1", "rack": "rack4"})
            except Exception as exc:
                logger.error(f"Failed to add a new node: {exc}")

        # Start adding a new node to the cluster, causing a topology change that will issue a barrier and drain
        add_last_node_task = asyncio.create_task(do_add_node())

        # Wait for the topology change to start
        logger.info("Waiting for a topology change to start")
        await target_server_log.wait_for("pause_before_barrier_and_drain: waiting for message")

        # Now make sure responses on one of the replicas will be delayed
        server_to_pause = servers[1]
        await inject_error_on(manager, "storage_proxy_write_response_pause", [server_to_pause])
        logger.info(
            f"Pausing responses on one of the replicas {server_to_pause}")
        paused_server_logs = await manager.server_open_log(server_to_pause.server_id)

        # Now send a write query to the target node that will be shut down.
        await cql.run_async(f"insert into {ks}.t (pk, v) values ({32765}, {17777})", host=target_host)

        # Make sure the node that's response is paused, got the write request.
        await paused_server_logs.wait_for("storage_proxy_write_response_pause: waiting for message")

        # Start shutdown of the query coordinator
        async def do_shutdown():
            logger.info(f"Starting shutdown of node {target_server.server_id}")
            await manager.server_stop_gracefully(target_server.server_id)

        shutdown_task = asyncio.create_task(do_shutdown())

        # Wait for the shutdown to start
        await target_server_log.wait_for("Stop transport: done")

        # Unpause the coordinator to make it now continue with `barrier_and_drain` shutdown
        await manager.api.message_injection(target_server.ip_addr, 'pause_before_barrier_and_drain')

        logger.info(f"Unblocking writes on the node {server_to_pause}")
        await manager.api.message_injection(server_to_pause.ip_addr, 'storage_proxy_write_response_pause')

        logger.info("Waiting for the shutdown to complete")
        await shutdown_task

        logger.info("Cancelling addnode task")
        add_last_node_task.cancel()
