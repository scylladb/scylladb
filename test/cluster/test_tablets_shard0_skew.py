#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for the host persisted in system.topology.tablets_shard0_skew_host, the one whose shard 0
# gets fewer tablets (tablets_group0_leader_shard_ratio). The topology coordinator moves it to itself.

import asyncio
import logging
import time

import pytest

from test.cluster.util import get_coordinator_host
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

DWELL_INJECTION = 'tablets_shard0_skew_dwell'


async def skew_host(cql, host) -> str | None:
    rows = await cql.run_async("SELECT tablets_shard0_skew_host FROM system.topology WHERE key = 'topology'", host=host)
    value = rows[0].tablets_shard0_skew_host if rows else None
    return str(value) if value else None


async def wait_for_skew_host(cql, host, expected: str) -> None:
    async def names_expected():
        actual = await skew_host(cql, host)
        logger.info(f"tablets_shard0_skew_host on {host}: {actual}, expected {expected}")
        return True if actual == expected else None
    await wait_for(names_expected, time.time() + 60, label=f"tablets_shard0_skew_host == {expected}")


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_shard0_skew_waits_before_following_new_coordinator(manager: ScyllaClusterManager) -> None:
    servers = await manager.servers_add(3)
    cql, hosts = await manager.get_ready_cql(servers)
    host_of = lambda server: next(h for h in hosts if h.address == server.ip_addr)

    await cql.run_async("ALTER CLUSTER WITH tablets_group0_leader_shard_ratio = 50")

    old_coordinator = await get_coordinator_host(manager)
    old_id = str(await manager.get_host_id(old_coordinator.server_id))
    await wait_for_skew_host(cql, host_of(old_coordinator), old_id)

    # A coordinator that has just taken over must not move the skew yet.
    for server in servers:
        await manager.api.enable_injection(server.ip_addr, DWELL_INJECTION, False, {'seconds': 3600})
    logs = {s.server_id: await manager.server_open_log(s.server_id) for s in servers}
    marks = {server_id: await log.mark() for server_id, log in logs.items()}
    await manager.server_stop_gracefully(old_coordinator.server_id)

    new_coordinator = await get_coordinator_host(manager)
    assert new_coordinator.server_id != old_coordinator.server_id
    new_id = str(await manager.get_host_id(new_coordinator.server_id))
    new_host = host_of(new_coordinator)

    await read_barrier(manager.api, new_coordinator.ip_addr)
    # The fiber logs once it has seen the target and decided to wait for the dwell.
    await logs[new_coordinator.server_id].wait_for(
            f"tablets shard0 skew: waiting 3600s before taking it over from {old_id}",
            from_mark=marks[new_coordinator.server_id], timeout=60)
    # The fiber re-checks every second, so it must keep holding off.
    for _ in range(4):
        assert await skew_host(cql, new_host) == old_id
        await asyncio.sleep(1)

    # Once the dwell is over the new coordinator takes the skew.
    await manager.api.disable_injection(new_coordinator.ip_addr, DWELL_INJECTION)
    await manager.api.enable_injection(new_coordinator.ip_addr, DWELL_INJECTION, False, {'seconds': 0})
    await wait_for_skew_host(cql, new_host, new_id)
