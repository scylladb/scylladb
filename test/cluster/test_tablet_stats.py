#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from test.pylib.manager_client import ManagerClient
from test.cluster.util import get_topology_coordinator, trigger_stepdown, new_test_keyspace, new_test_table

import pytest
import logging

from test.pylib.rest_client import read_barrier

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_load_stats_on_coordinator_failover(manager: ManagerClient):
    cfg = {
        'data_file_capacity': 7000000,
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        # The test overrides disk capacity but the disk usage remains real leading the disk_space_monitor
        # to announce 100% disk utilization and active OoS prevention mechanisms.
        'error_injections_at_startup': ['suppress_disk_space_threshold_checks'],
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


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_load_stats_refresh_during_shutdown(manager: ManagerClient):
    """Verify that _tablet_load_stats_refresh is properly joined during
    topology coordinator shutdown, even when a schema change notification
    triggers a refresh between run() completing and stop() being called.

    Reproduces the scenario using two injection points:
    - topology_coordinator_pause_before_stop: pauses after run() finishes
      but before stop() is called
    - refresh_tablet_load_stats_pause: holds refresh_tablet_load_stats()
      so it's still in-flight during shutdown

    Without the join in stop(), the refresh task outlives the coordinator
    and accesses freed memory.
    """
    servers = await manager.servers_add(3)
    await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        coord = await get_topology_coordinator(manager)
        host_ids = [await manager.get_host_id(s.server_id) for s in servers]
        coord_idx = host_ids.index(coord)
        coord_server = servers[coord_idx]

        log = await manager.server_open_log(coord_server.server_id)
        mark = await log.mark()

        # Injection B: pause between run() returning and stop() being called.
        await manager.api.enable_injection(
            coord_server.ip_addr, "topology_coordinator_pause_before_stop", one_shot=True)

        # Stepdown causes the topology coordinator to abort and shut down.
        logger.info("Triggering stepdown on coordinator")
        await trigger_stepdown(manager, coord_server)

        # Wait for injection B to fire. The coordinator has finished run() but
        # the schema change listener is still registered.
        mark, _ = await log.wait_for(
            "topology_coordinator_pause_before_stop: waiting", from_mark=mark)

        # Injection A: block refresh_tablet_load_stats() before it accesses _shared_tm.
        # Enable it now so it only catches the notification-triggered call.
        await manager.api.enable_injection(
            coord_server.ip_addr, "refresh_tablet_load_stats_pause", one_shot=True)

        # CREATE TABLE fires on_create_column_family on the old coordinator which
        # fire-and-forgets _tablet_load_stats_refresh.trigger() scheduling a task
        # via with_scheduling_group on the gossip scheduling group.
        logger.info("Issuing CREATE TABLE while coordinator is paused before stop()")
        async with new_test_table(manager, ks, "pk int PRIMARY KEY", reuse_tables=False):
            # Wait for injection A: refresh_tablet_load_stats() is now blocked before
            # accessing _shared_tm. The topology_coordinator is still alive (paused at B).
            await log.wait_for("refresh_tablet_load_stats_pause: waiting", from_mark=mark)

            # Release injection B: coordinator proceeds through stop().
            # Without the fix, stop() returns quickly and run_topology_coordinator
            # frees the topology_coordinator frame. With the fix, stop() blocks at
            # _tablet_load_stats_refresh.join() until injection A is released.
            logger.info("Releasing injection B: coordinator will stop")
            await manager.api.message_injection(
                coord_server.ip_addr, "topology_coordinator_pause_before_stop")

            # Release injection A: refresh_tablet_load_stats() resumes and accesses
            # this->_shared_tm via get_token_metadata_ptr(). Without the fix, 'this'
            # points to freed memory and ASan detects heap-use-after-free.
            logger.info("Releasing injection A: refresh resumes")
            await manager.api.message_injection(
                coord_server.ip_addr, "refresh_tablet_load_stats_pause")

            # If the bug is present, the node crashed. read_barrier will fail.
            await read_barrier(manager.api, coord_server.ip_addr)
