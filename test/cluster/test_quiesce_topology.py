#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Targeted tests for the enhanced quiesce_topology API.

The enhanced API ensures that after quiesce_topology returns:
- The tablet load balancer has evaluated cluster balance using freshly
  refreshed load stats from all live nodes.
- Any migrations, splits, or merges deemed necessary have completed.
- The topology is in a stable state with no pending transitions.
"""

from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_replica_count_by_host
from test.cluster.util import new_test_keyspace, get_topology_coordinator
import pytest
import asyncio
import logging

logger = logging.getLogger(__name__)

# Debug log levels needed for quiesce log messages
QUIESCE_DEBUG_CMDLINE = [
    '--logger-log-level', 'raft_topology=debug',
    '--logger-log-level', 'storage_service=debug',
]


@pytest.mark.asyncio
async def test_quiesce_waits_for_balancer(manager: ManagerClient):
    """
    Verify that quiesce_topology waits for the balancer to refresh stats
    and complete migrations.
    """
    logger.info("Starting cluster with 2 nodes")
    cfg = {'tablet_load_stats_refresh_interval_in_seconds': 1}
    servers = await manager.servers_add(2, config=cfg, cmdline=QUIESCE_DEBUG_CMDLINE)

    # Disable balancing so we can create imbalance without interference
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                 "'replication_factor': 1} AND tablets = {'initial': 16}") as ks:
        await manager.get_cql().run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")

        # Insert some data and flush to create sstables (so load stats have something to report)
        cql = manager.get_cql()
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({i}, {i})") for i in range(100)])
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        # Add a third node — creates imbalance (16 tablets on 2 nodes, 0 on new node)
        logger.info("Adding third node to create imbalance")
        servers.append(await manager.server_add(config=cfg, cmdline=QUIESCE_DEBUG_CMDLINE))

        logger.info(f"Calling quiesce_topology on node {servers[0].ip_addr}")
        # Enable balancing and quiesce — quiesce always does a fresh refresh,
        # so even if balancing finishes quickly, quiesce verifies correctness.
        await manager.enable_tablet_balancing()
        await manager.api.quiesce_topology(servers[0].ip_addr)

        # Verify balance: 16 tablets / 3 nodes = 5 or 6 per node
        replicas = await get_replica_count_by_host(manager, servers[0], ks, 't')
        logger.info(f"Replica distribution after quiesce: {replicas}")
        assert len(replicas) == 3, f"Expected tablets on all 3 nodes, got: {replicas}"
        for host_id, count in replicas.items():
            assert 5 <= count <= 6, f"Node {host_id} has {count} tablets, expected 5 or 6"

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_quiesce_blocks_until_refresh_completes(manager: ManagerClient):
    """
    Verify that quiesce blocks while the load stats refresh is stalled.
    Uses the refresh_tablet_load_stats_pause injection to pause the refresh,
    then releases it and confirms quiesce completes.
    """
    logger.info("Starting cluster with 2 nodes")
    # Use a very long refresh interval to prevent periodic refreshes from
    # consuming the injection before quiesce triggers one.
    cfg = {'tablet_load_stats_refresh_interval_in_seconds': 3600}
    servers = await manager.servers_add(2, config=cfg, cmdline=QUIESCE_DEBUG_CMDLINE)

    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                 "'replication_factor': 1} AND tablets = {'initial': 8}") as ks:
        await manager.get_cql().run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")

        cql = manager.get_cql()
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({i}, {i})") for i in range(100)])

        # Wait for initial stats refresh to complete (triggered on coordinator start).
        leader_host = await get_topology_coordinator(manager)
        leader = await manager.find_server_by_host_id(servers, leader_host)
        log = await manager.server_open_log(leader.server_id)
        await log.wait_for("Refreshed table load stats for all DC", timeout=30)

        # Enable the refresh pause injection on the leader
        await manager.api.enable_injection(leader.ip_addr, "refresh_tablet_load_stats_pause", one_shot=True)

        # Start quiesce — it should trigger a fresh refresh which will block on the injection
        await manager.enable_tablet_balancing()
        mark = await log.mark()
        quiesce_task = asyncio.create_task(
            manager.api.quiesce_topology(leader.ip_addr))

        # Wait for the quiesce-triggered refresh to start. Once we see this log,
        # the refresh is in progress and blocked by the injection.
        await log.wait_for("refresh_tablet_load_stats_pause: waiting", from_mark=mark, timeout=30)
        # Verify that quiesce is blocked while refresh is paused
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(quiesce_task), timeout=2)

        # Release the refresh by sending the message
        await manager.api.message_injection(leader.ip_addr, "refresh_tablet_load_stats_pause")

        # Quiesce should now complete
        await asyncio.wait_for(quiesce_task, timeout=60)
        logger.info("Quiesce completed after refresh was released")


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_quiesce_retries_until_balance_plan_is_empty(manager: ManagerClient):
    """
    Verify that quiesce retries after the balancer reports work. If the first
    request observes a non-empty plan, the API retries and only returns after a
    later request observes an empty plan.
    """
    logger.info("Starting cluster with 2 nodes")
    cfg = {'tablet_load_stats_refresh_interval_in_seconds': 1}
    servers = await manager.servers_add(2, config=cfg, cmdline=QUIESCE_DEBUG_CMDLINE)

    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                 "'replication_factor': 1} AND tablets = {'initial': 32}") as ks:
        await manager.get_cql().run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")

        # Insert data and flush
        cql = manager.get_cql()
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({i}, {i})") for i in range(200)])
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        # Add two more nodes — significant imbalance requiring balancing work.
        logger.info("Adding two more nodes to create significant imbalance")
        servers.append(await manager.server_add(config=cfg, cmdline=QUIESCE_DEBUG_CMDLINE))
        servers.append(await manager.server_add(config=cfg, cmdline=QUIESCE_DEBUG_CMDLINE))

        # Block the coordinator before enabling balancing. This makes the first
        # quiesce request run before the balancer can start migrations, so it
        # must observe a non-empty plan and retry.
        leader_host = await get_topology_coordinator(manager)
        leader = await manager.find_server_by_host_id(servers, leader_host)
        leader_log = await manager.server_open_log(leader.server_id)
        leader_mark = await leader_log.mark()
        await manager.api.enable_injection(leader.ip_addr,
                                           "topology_coordinator_pause_before_processing_backlog",
                                           one_shot=True)

        await manager.enable_tablet_balancing()
        await leader_log.wait_for("topology_coordinator_pause_before_processing_backlog: waiting", from_mark=leader_mark, timeout=30)

        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        quiesce_task = asyncio.create_task(manager.api.quiesce_topology(servers[0].ip_addr))
        # Verify that quiesce is blocked behind the paused coordinator
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(quiesce_task), timeout=2)
        await manager.api.message_injection(leader.ip_addr, "topology_coordinator_pause_before_processing_backlog")
        await asyncio.wait_for(quiesce_task, timeout=120)

        # Verify that at least one retry was needed.
        matches = await log.grep("topology not idle", from_mark=mark)
        assert matches, "Expected quiesce to retry after observing a non-empty plan"

        # Verify balance: all 4 nodes should have exactly 8 tablets each
        replicas = await get_replica_count_by_host(manager, servers[0], ks, 't')
        logger.info(f"Replica distribution after quiesce: {replicas}")
        assert len(replicas) == 4, f"Expected tablets on all 4 nodes, got: {replicas}"
        for host_id, count in replicas.items():
            assert count == 8, f"Node {host_id} has {count} tablets, expected 8"

        # Second quiesce should not trigger any work
        mark = await log.mark()
        await manager.api.quiesce_topology(servers[0].ip_addr)
        matches = await log.grep("topology not idle", from_mark=mark)
        assert not matches, "Second quiesce should not have retried"
