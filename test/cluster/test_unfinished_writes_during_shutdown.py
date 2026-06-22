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
from test.pylib.internal_types import ServerInfo
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
    """ Test a simultaneous topology change and write query during shutdown,
    which may cause the node to get stuck.

    The test runs twice on the same cluster:
    - First with the target node being a non-coordinator (hangs in
      uninit_messaging_service, see scylladb/scylladb#23665).
    - Then with the target node being the topology coordinator (hangs in
      do_drain -> wait_for_group0_stop, see SCYLLADB-1842).

    Each run:
     1. Enable pause_before_barrier_and_drain injection on the target.
     2. Trigger a topology change by adding a new node to the cluster.
     3. Wait for the first barrier_and_drain to hit the injection on
        the target.
     4. Pause write responses on all other running servers via injection.
     5. Send a write with CL=ONE to the target node. It completes
        immediately from the coordinator's local replica. The mutation
        is still sent to other replicas whose responses are paused,
        keeping the write_response_handler alive.
     6. Release the first barrier_and_drain (the write handler's ERM
        version is still current). Wait for the second
        barrier_and_drain — by now topology_state_load has installed
        a new token_metadata version, making the write handler's ERM
        stale.
     7. Start graceful shutdown of the target node.
     8. Disable the injection to release all paused barrier_and_drain
        handlers, then unblock the delayed write response.
     9. Verify shutdown completes within 15s (deadlock = test failure).
    """
    logger.info("Creating a new cluster")

    cmdline = [
        '--logger-log-level', 'debug_error_injection=debug',
    ]

    await manager.servers_add(2, auto_rack_dc="dc1", cmdline=cmdline)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2};") as ks:

        # Preconditions: exactly 2 running nodes, RF=2, so both nodes
        # are replicas for every partition. The target node is always a
        # replica (local write satisfies CL=ONE) and the other node is
        # always a replica (its paused response keeps the handler alive).
        # The caller restores these preconditions between runs.
        async def do_test(target_server: ServerInfo) -> ServerInfo:
            """Run the test scenario. Returns the newly added server."""
            other_server = next(s for s in running if s.server_id != target_server.server_id)
            target_host = next(h for h in hosts
                               if h.address == str(target_server.rpc_address))

            logger.info(
                f"Enabling injection 'pause_before_barrier_and_drain' on the target server {target_server}")
            target_server_log = await manager.server_open_log(target_server.server_id)
            await manager.api.enable_injection(target_server.ip_addr, "pause_before_barrier_and_drain", one_shot=False)

            # Start adding a new node, causing a topology change that issues barrier_and_drain
            logger.info("Adding a node to the cluster")
            add_last_node_task = asyncio.create_task(
                manager.server_add(property_file={"dc": "dc1", "rack": running[0].rack}))

            # Wait for the topology change to start
            logger.info("Waiting for a topology change to start")
            mark, _ = await target_server_log.wait_for("pause_before_barrier_and_drain: waiting for message")

            # Pause write responses on the other node so it keeps the
            # write_response_handler alive on the target.
            await inject_error_on(manager, "storage_proxy_write_response_pause", [other_server])
            other_server_log = await manager.server_open_log(other_server.server_id)

            # Send a write with CL=ONE so it completes from the coordinator's
            # local replica without waiting for the paused server.
            await cql.run_async(
                SimpleStatement(f"insert into {ks}.t (pk, v) values ({32765}, {17777})",
                                consistency_level=ConsistencyLevel.ONE),
                host=target_host)

            # Make sure the other replica got the write request and is paused.
            await other_server_log.wait_for("storage_proxy_write_response_pause: waiting for message")

            # Release the first barrier_and_drain — it completes because the write
            # handler holds the current version (not stale yet).
            await manager.api.message_injection(target_server.ip_addr, 'pause_before_barrier_and_drain')

            # Wait for the second barrier_and_drain. Between the first and second,
            # topology_state_load installs a new token_metadata version. The write
            # handler still holds the old version's ERM, which is now stale.
            await target_server_log.wait_for("pause_before_barrier_and_drain: waiting for message", from_mark=mark)

            # Start shutdown of the target node
            logger.info(f"Starting shutdown of node {target_server.server_id}")
            shutdown_task = asyncio.create_task(
                manager.server_stop_gracefully(target_server.server_id))

            # Wait for stop_transport to complete. At this point the local
            # messaging service is shut down, so MUTATION_DONE from the other
            # replica can no longer reach the coordinator — the write handler
            # has no chance to complete naturally. Next we release the paused
            # barrier_and_drain and unblock the write response. The
            # barrier_and_drain handler calls stale_versions_in_use() which
            # blocks until the stale ERM held by the write handler is released.
            # The test verifies that shutdown succeeds because the fix
            # forcefully cancels write handlers during storage_proxy shutdown,
            # releasing the stale ERM and unblocking stale_versions_in_use().
            await target_server_log.wait_for("Stop transport: done")

            # Disable the injection. This releases the paused barrier_and_drain
            # handler (and any subsequent ones that arrived during stop_transport)
            # so they don't block uninit_messaging_service.
            await manager.api.disable_injection(target_server.ip_addr, "pause_before_barrier_and_drain")

            logger.info(f"Unblocking writes on the node {other_server}")
            await manager.api.message_injection(other_server.ip_addr, 'storage_proxy_write_response_pause')

            logger.info("Waiting for the shutdown to complete")
            try:
                await asyncio.wait_for(shutdown_task, timeout=15)
            except asyncio.TimeoutError:
                # Deadlock reproduced — shutdown hung because stale_versions_in_use
                # blocks on the write handler holding a stale token_metadata version.
                # Kill all servers including any being bootstrapped (stuck because
                # the coordinator is dead). This unblocks the server-side addserver
                # handler so _after_test doesn't wait 120s for it.
                logger.info("Shutdown did not complete within the timeout, killing all servers")
                for s in await manager.all_servers() + await manager.starting_servers():
                    await manager.server_stop(s.server_id)
                pytest.fail(f"Shutdown did not complete within 15s — deadlock reproduced"
                            f" (target={target_server})")

            # Restart the target node before waiting for addnode — the addnode
            # needs raft quorum which requires the target to be alive.
            await manager.server_start(target_server.server_id)

            logger.info("Waiting for addnode to complete")
            return await add_last_node_task

        async def pick_target(is_coordinator: bool) -> ServerInfo:
            coordinator_host_id = await get_topology_coordinator(manager)
            for s in running:
                hid = await manager.get_host_id(s.server_id)
                if (hid == coordinator_host_id) == is_coordinator:
                    return s
            assert False, f"Could not find {'coordinator' if is_coordinator else 'non-coordinator'} among {running}"

        # Run 1: target is a non-coordinator node
        running = await manager.running_servers()
        cql, hosts = await manager.get_ready_cql(running)
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int primary key, v int)")
        target_server = await pick_target(is_coordinator=False)
        logger.info(f"=== Run 1: target={target_server} (non-coordinator) ===")
        new_server = await do_test(target_server)

        # Restore the cluster to its original state: decommission the added
        # node. do_test already restarted the target. This keeps 2 nodes with
        # RF=2, so both are replicas for every partition — same as run 1.
        await manager.decommission_node(new_server.server_id)

        # Run 2: target is the topology coordinator
        running = await manager.running_servers()
        cql, hosts = await manager.get_ready_cql(running)
        target_server = await pick_target(is_coordinator=True)
        logger.info(f"=== Run 2: target={target_server} (coordinator) ===")
        await do_test(target_server)
