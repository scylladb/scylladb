#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import time

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.cluster.util import get_topology_coordinator

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_rejected_snapshot_reloads_in_memory_state(manager: ScyllaClusterManager):
    """
    A transferred group 0 snapshot is persisted before raft decides whether to
    load it, and raft does not call load_snapshot() for a snapshot it rejects.
    Check that the in-memory state still follows the persisted one: a role
    created while a follower lags behind reaches its auth cache through a
    snapshot that raft rejects.
    """
    servers = await manager.servers_add(3, config=auth_config)
    cql, hosts = await manager.get_ready_cql(servers)

    coordinator_id = await get_topology_coordinator(manager)
    coordinator = await manager.find_server_by_host_id(servers, coordinator_id)
    coordinator_host = next(h for h in hosts if h.address == coordinator.ip_addr)
    follower = next(s for s in servers if s.server_id != coordinator.server_id)
    group0_id = (await cql.run_async(
        "SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'", host=coordinator_host))[0].value

    async def cached_roles(server):
        metrics = await manager.metrics.query(server.ip_addr)
        return metrics.get("scylla_auth_cache_roles", {'shard': '0'})

    async def wait_for_cached_roles(server, expected):
        async def cached():
            return True if await cached_roles(server) == expected else None
        await wait_for(cached, time.time() + 60)

    initial_roles = await cached_roles(follower)

    for s in servers:
        await manager.api.enable_injection(s.ip_addr, "raft_server_set_snapshot_thresholds", one_shot=False,
                                           parameters={'snapshot_threshold': '3', 'snapshot_trailing': '1'})

    logger.info("Making the follower lag behind and reject every snapshot it receives")
    await manager.api.enable_injection(follower.ip_addr, "raft_drop_incoming_append_entries_for_specified_group",
                                       one_shot=False, parameters={'value': group0_id})
    await manager.api.enable_injection(follower.ip_addr, "raft_reject_transferred_snapshot", one_shot=False)
    log = await manager.server_open_log(follower.server_id)
    mark = await log.mark()

    # A role statement returns only after a group 0 read barrier on every live node,
    # which the follower cannot complete while it lags. Its effects are committed
    # earlier, so the statements run in the background and are awaited at the end.
    # For the same reason no CQL may reach the follower.
    create_role = asyncio.ensure_future(cql.run_async(
        "CREATE ROLE lagging_role WITH PASSWORD = 'password' AND LOGIN = true", host=coordinator_host))
    await wait_for_cached_roles(coordinator, initial_roles + 1)
    for s in servers:
        if s.server_id != follower.server_id:
            await manager.api.client.post(f"/raft/trigger_snapshot/{group0_id}", host=s.ip_addr)

    # Once the follower answers again and asks for entries the leader no longer has,
    # the leader sends it a snapshot, which the follower persists and raft rejects.
    # Another command makes the leader send the follower entries right away.
    await manager.api.disable_injection(follower.ip_addr, "raft_drop_incoming_append_entries_for_specified_group")
    alter_role = asyncio.ensure_future(cql.run_async(
        "ALTER ROLE lagging_role WITH LOGIN = true", host=coordinator_host))
    await log.wait_for("transfer snapshot from .* completed", from_mark=mark)

    await wait_for_cached_roles(follower, initial_roles + 1)

    await manager.api.disable_injection(follower.ip_addr, "raft_reject_transferred_snapshot")
    await asyncio.gather(create_role, alter_role)
