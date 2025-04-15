#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.cluster.util import get_topology_coordinator, trigger_stepdown

import pytest
import logging

from test.pylib.rest_client import read_barrier

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_load_stats_on_coordinator_failover(manager: ManagerClient):
    cfg = {
        'data_file_capacity': 7000000,
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval'],
    }
    servers = await manager.servers_add(3, config=cfg)
    host_ids = [await manager.get_host_id(s.server_id) for s in servers]
    cql = manager.get_cql()

    coord = await get_topology_coordinator(manager)
    coord_idx = host_ids.index(coord)
    await trigger_stepdown(manager, servers[coord_idx])

    async def get_capacity():
        rows = cql.execute(f"SELECT * FROM system.load_per_node WHERE node = {host_ids[coord_idx]}")
        return rows.one().storage_capacity

    # Check that query works when there is no leader yet, it should wait for election
    assert await get_capacity() == 7000000

    while True:
        coord2 = await get_topology_coordinator(manager)
        if coord2:
            break
        assert await get_capacity() == 7000000

    # Check that query works immediately after election, it should wait for stats to become available
    assert await get_capacity() == 7000000

    assert coord != coord2

    # Now "coord" has stats with capacity=70000000.
    # Change capacity and trigger failover back to "coord" and see that it doesn't
    # present stale stats. That's a serious bug because load balancer could make incorrect
    # decisions based on stale stats.

    await manager.server_update_config(servers[coord_idx].server_id, 'data_file_capacity', 3000000)
    logger.info("Waiting for load balancer to pick up new capacity")
    while True:
        if await get_capacity() == 3000000:
            break

    non_coord = None
    for h in host_ids:
        if h != coord and h != coord2:
            non_coord =  h
            break

    logger.info("Trigger stepdown of coord2")
    await trigger_stepdown(manager, servers[host_ids.index(coord2)])

    # Wait for election
    await read_barrier(manager.api, servers[host_ids.index(non_coord)].ip_addr)

    # Make sure "coord" gets the leadership again
    if await get_topology_coordinator(manager) == non_coord:
        await trigger_stepdown(manager, servers[host_ids.index(non_coord)])

    while True:
        # Check that the new leader doesn't work with stale stats
        assert await get_capacity() == 3000000
        coord3 = await get_topology_coordinator(manager)
        if coord3:
            break
