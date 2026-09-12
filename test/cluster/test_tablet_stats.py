#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import get_topology_coordinator, trigger_stepdown, new_test_keyspace, new_test_table

import pytest
import asyncio
import logging
import time

from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_load_stats_on_coordinator_failover(manager: ScyllaClusterManager):
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


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_load_stats_refresh_during_shutdown(manager: ScyllaClusterManager):
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

        # Injection B: pause between run() returning and stop() being called.
        await manager.api.enable_injection(
            coord_server.ip_addr, "topology_coordinator_pause_before_stop", one_shot=True)

        # Stepdown causes the topology coordinator to abort and shut down.
        logger.info("Triggering stepdown on coordinator")
        await trigger_stepdown(manager, coord_server)

        # Wait for injection B to fire. The coordinator has finished run() but
        # the schema change listener is still registered.
        await manager.api.wait_for_injection_enter(coord_server.ip_addr, "topology_coordinator_pause_before_stop")

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
            await manager.api.wait_for_injection_enter(coord_server.ip_addr, "refresh_tablet_load_stats_pause")

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


async def test_tablet_sizes_redirect_paged(manager: ScyllaClusterManager):
    """Reproduces the "redirect_to_leader() buffers the whole result in one
    RPC" bug reported against system.tablet_sizes: querying it from a
    non-leader node used to send a single unbounded read_mutation_data RPC to
    the group0 leader. With a small max_memory_for_unlimited_query, that RPC
    would fail before returning any row. Here the redirect must page instead,
    so the query succeeds and returns every tablet exactly once.
    """
    cfg = {'tablet_load_stats_refresh_interval_in_seconds': 1}
    servers = await manager.servers_add(3, config=cfg, auto_rack_dc="dc1")
    cql = manager.get_cql()
    driver_hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    driver_host_by_ip = {h.address: h for h in driver_hosts}

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
            "AND tablets = {'initial': 32}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        table_id = await manager.get_table_or_view_id(ks, 'test')

        coord_host_id = await get_topology_coordinator(manager)
        coord = await manager.find_server_by_host_id(servers, coord_host_id)
        non_coord = next(s for s in servers if s.server_id != coord.server_id)

        async def get_tablet_sizes(server):
            return await cql.run_async(f"SELECT * FROM system.tablet_sizes WHERE table_id = {table_id}",
                                        host=driver_host_by_ip[server.ip_addr])

        # Wait until load_stats has been refreshed for all 32 tablets.
        started = time.time()
        while True:
            rows = await get_tablet_sizes(coord)
            if len(rows) == 32 and all(len(r.missing_replicas) == 0 for r in rows):
                break
            assert time.time() - started < 120, "Timed out waiting for tablet_sizes to be populated"
            await asyncio.sleep(0.2)

        # Now that the cluster is up and tablet_sizes is populated, shrink the
        # non-leader's own query page size: redirect_to_leader() derives the
        # read_mutation_data page size from its own permit (i.e. this node's
        # config), so this forces the RPC to the leader to split into several
        # pages instead of materializing the whole table_sizes result in one
        # response. (allow_short_read makes get_page_size(), not the hard
        # limit, the actual per-page cutoff -- see check_local_limit().)
        await manager.server_update_config(non_coord.server_id,
                                            config_options={
                                                'query_page_size_in_bytes': 4096,
                                            })

        async def mutation_data_reads() -> float:
            metrics = await manager.metrics.query(coord.ip_addr)
            return metrics.get('scylla_storage_proxy_replica_reads', {'op_type': 'mutation_data'}) or 0

        reads_before = await mutation_data_reads()

        # Query through the non-leader: this exercises redirect_to_leader()'s
        # multi-page RPC loop against the leader.
        rows = await get_tablet_sizes(non_coord)

        reads_after = await mutation_data_reads()

        last_tokens = [r.last_token for r in rows]
        assert len(last_tokens) == 32, f"Expected 32 tablets, got {len(last_tokens)}"
        assert len(set(last_tokens)) == 32, "Duplicate tablets returned by redirected read"
        assert last_tokens == sorted(last_tokens), "Tablets not returned in last_token clustering order"

        # With the leader's own page size forced down to 4KB, a table with 32
        # tablets cannot possibly fit in a single read_mutation_data response,
        # so a correct redirect must issue more than one RPC to the leader.
        # (An unpaged redirect ignores the config and always does exactly one.)
        assert reads_after - reads_before > 1, \
            "redirect_to_leader() did not page: only one read_mutation_data RPC was issued to the leader"
