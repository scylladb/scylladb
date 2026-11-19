#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging

import pytest
from aiohttp.client_exceptions import ServerDisconnectedError

from test.pylib.rest_client import HTTPError
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replica
from test.cluster.util import get_topology_coordinator, new_test_keyspace

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_coordinator_shutdown_fails_topology_command(manager: ScyllaClusterManager) -> None:
    """A topology command which fails because the coordinator node shuts down is
    logged at WARN, not at ERROR (SCYLLADB-6462).

    The shutdown aborts the group 0 abort source, which fails the read barrier of
    the command handler on the coordinator node. The coordinator must be aborted
    at that moment: the state monitor fiber, which also aborts it once it resumes
    from its own aborted wait, is held by an injection until the coordinator has
    handled the failure.
    """
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
    await manager.disable_tablet_balancing()

    host_ids = [await manager.get_host_id(s.server_id) for s in servers]
    coordinator = servers[host_ids.index(await get_topology_coordinator(manager))]
    log = await manager.server_open_log(coordinator.server_id)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                 "'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await manager.get_cql().run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")
        src_host, src_shard = await get_tablet_replica(manager, coordinator, ks, 'test', 0)
        dst_host = next(h for h in host_ids if h != src_host)

        # The migration's first global barrier parks its command handler on the
        # coordinator node, with the coordinator waiting for it.
        await manager.api.enable_injection(coordinator.ip_addr, "raft_topology_cmd_handler_pause_before_read_barrier",
                                           one_shot=True)
        await manager.api.enable_injection(coordinator.ip_addr, "raft_state_monitor_fiber_pause_on_abort", one_shot=True)
        move_task = asyncio.create_task(
            manager.api.move_tablet(coordinator.ip_addr, ks, 'test', src_host, src_shard, dst_host, 0, 0))
        await manager.api.wait_for_injection_enter(coordinator.ip_addr, "raft_topology_cmd_handler_pause_before_read_barrier")

        logger.info(f"Stopping the coordinator {coordinator}")
        mark = await log.mark()
        stop_task = asyncio.create_task(manager.server_stop_gracefully(coordinator.server_id))

        # Once the monitor fiber is held, group 0 is aborted, so the released
        # handler fails its read barrier and the coordinator handles the failure
        # while the fiber is still held.
        await manager.api.wait_for_injection_enter(coordinator.ip_addr, "raft_state_monitor_fiber_pause_on_abort")
        try:
            await manager.api.message_injection(coordinator.ip_addr, "raft_topology_cmd_handler_pause_before_read_barrier")
            await log.wait_for("topology change coordinator fiber got error", from_mark=mark, timeout=60)
        finally:
            # Release the fiber even if the line is missing, so that the test fails
            # on the wait and not on a node held by the injection.
            await manager.api.message_injection(coordinator.ip_addr, "raft_state_monitor_fiber_pause_on_abort")
            await stop_task

        assert await log.grep("WARN .* topology change coordinator fiber got error while being aborted", from_mark=mark)
        assert not await log.grep("ERROR .* topology change coordinator fiber got error", from_mark=mark)

        # The other node completes the migration once it has a quorum again.
        await manager.server_start(coordinator.server_id)
        try:
            await move_task
        except (HTTPError, ServerDisconnectedError) as e:
            logger.info(f"The move request failed with the coordinator: {e}")
        await manager.api.quiesce_topology(servers[0].ip_addr)
