#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace
from test.pylib.tablets import get_tablet_replica
from test.pylib.rest_client import read_barrier

import asyncio
import json
import logging
import pytest


logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@pytest.mark.parametrize("migration_type", ["internode", "intranode"])
async def test_counter_updates_during_tablet_migration(manager: ManagerClient, migration_type: str):
    """
    Test that counter updates remain consistent during tablet migrations.

    This test performs concurrent counter increment operations on a single partition
    while simultaneously triggering a tablet migration, either between nodes or
    between shards on the same node.

    The test verifies that counter consistency is maintained throughout the migration
    process: no counter updates are lost, and the final counter value matches the total
    number of increments performed.
    """

    if migration_type == "intranode":
        node_count = 1
    else:
        node_count = 3

    cmdline = ['--smp', '2', '--logger-log-level', 'raft_topology=debug', '--logger-log-level', 'storage_service=debug']

    servers = await manager.servers_add(node_count, cmdline=cmdline)
    cql = manager.get_cql()
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets={'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.counters (pk int PRIMARY KEY, c counter)")

        stop_event = asyncio.Event()
        pk = 1  # Single partition key for all updates

        async def do_counter_updates():
            """Continuously update a single counter during migration"""
            update_count = 0

            while not stop_event.is_set():
                await asyncio.gather(*[cql.run_async(f"UPDATE {ks}.counters SET c = c + 1 WHERE pk = {pk}") for _ in range(100)])
                update_count += 100

            return update_count

        async def do_migration():
            """Perform the specified type of tablet migration"""
            try:
                tablet_token = 0  # the single tablet
                replica = await get_tablet_replica(manager, servers[0], ks, 'counters', tablet_token)
                src_host = replica[0]
                src_shard = replica[1]

                if migration_type == "internode":
                    # Find a different node to migrate to
                    all_host_ids = [await manager.get_host_id(server.server_id) for server in servers]
                    dst_host = next(host_id for host_id in all_host_ids if host_id != src_host)
                    dst_shard = 0
                else:  # migration_type == "intranode"
                    # Move to a different shard on the same node
                    dst_host = src_host
                    dst_shard = 1 - src_shard  # Switch between shard 0 and 1

                await manager.api.move_tablet(servers[0].ip_addr, ks, "counters", src_host, src_shard, dst_host, dst_shard, tablet_token)
            finally:
                stop_event.set()

        # Run counter updates and migration concurrently
        update_task = asyncio.create_task(do_counter_updates())
        await asyncio.sleep(0.5)
        await do_migration()
        total_updates = await update_task
        logger.info("Completed %d counter updates during migration", total_updates)

        # Verify no increments were lost - counter value should equal number of updates
        result = await cql.run_async(f"SELECT c FROM {ks}.counters WHERE pk = {pk}")
        actual_count = result[0].c

        assert actual_count == total_updates, f"Counter value mismatch: expected {total_updates}, got {actual_count}"

@pytest.mark.asyncio
async def test_counter_ids_reuse_in_single_rack(manager: ManagerClient):
    """
    Migrate a single counter tablet between 3 nodes in a single rack, performing counter updates on each node,
    and verify the updates use at most 2 different counter IDs.
    The counter ID should be reused when migrated to another node, except in the transition stage where 2 counter IDs
    may be used.
    """
    cmdline = ['--smp', '1', '--logger-log-level', 'raft_topology=debug', '--logger-log-level', 'storage_service=debug']
    servers = await manager.servers_add(3, cmdline=cmdline, property_file=[
        {'dc': 'dc1', 'rack': 'rack1'},
        {'dc': 'dc1', 'rack': 'rack1'},
        {'dc': 'dc1', 'rack': 'rack1'}
    ])
    cql, hosts = await manager.get_ready_cql(servers)
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets={'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.counters (pk int PRIMARY KEY, c counter)")

        pk = 1  # Single partition key for all updates
        tablet_token = 0  # single tablet
        total_updates = 0

        # Get all host IDs
        all_host_ids = [await manager.get_host_id(server.server_id) for server in servers]

        # Migrate the tablet between all 3 nodes
        for _ in range(3):
            await cql.run_async(f"UPDATE {ks}.counters SET c = c + 1 WHERE pk = {pk}")
            total_updates += 1

            # Get current tablet location
            replica = await get_tablet_replica(manager, servers[0], ks, 'counters', tablet_token)
            src_host = replica[0]
            src_shard = replica[1]

            # Migrate to the next node
            src_node_idx = all_host_ids.index(src_host)
            dst_node_idx = (src_node_idx + 1) % 3
            dst_host = all_host_ids[dst_node_idx]
            dst_shard = 0

            logger.info(f"Migrating tablet from node {src_node_idx} to node {dst_node_idx}")
            await manager.api.move_tablet(servers[0].ip_addr, ks, "counters", src_host, src_shard, dst_host, dst_shard, tablet_token)

        # Perform final counter updates after the last migration
        await cql.run_async(f"UPDATE {ks}.counters SET c = c + 1 WHERE pk = {pk}")
        total_updates += 1

        # Verify no counter updates were lost
        result = await cql.run_async(f"SELECT c FROM {ks}.counters WHERE pk = {pk}")
        actual_count = result[0].c
        assert actual_count == total_updates, f"Counter value mismatch: expected {total_updates}, got {actual_count}"

        # Ensure all tablet transitions are fully completed and committed on all nodes
        await manager.api.quiesce_topology(servers[0].ip_addr)
        await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])

        await asyncio.gather(*[manager.api.flush_keyspace(s.ip_addr, ks) for s in servers])

        # Get all counter IDs that were used
        counter_ids = set()
        for h in hosts:
            res = await cql.run_async(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.counters)", host=h)
            for row in res:
                if row.value:
                    value_dict = json.loads(row.value)
                    counter_ids.update({counter_shard["id"] for counter_shard in value_dict["c"]})

        logger.info(f"Unique counter IDs found: {counter_ids}")
        assert len(counter_ids) >= 1, f"Expected at least 1 counter ID, but found none"
        assert len(counter_ids) <= 2, f"Expected at most 2 counter IDs, but found {len(counter_ids)}: {counter_ids}"

@pytest.mark.asyncio
async def test_counter_ids_multi_rack(manager: ManagerClient):
    """
    Test counter IDs with 3 nodes in 3 different racks with RF=3.
    Each rack should use a different counter ID.
    """
    cmdline = ['--smp', '1', '--logger-log-level', 'raft_topology=debug', '--logger-log-level', 'storage_service=debug']
    servers = await manager.servers_add(3, cmdline=cmdline, property_file=[
        {'dc': 'dc1', 'rack': 'rack1'},
        {'dc': 'dc1', 'rack': 'rack2'},
        {'dc': 'dc1', 'rack': 'rack3'}
    ])
    cql, hosts = await manager.get_ready_cql(servers)
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets={'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.counters (pk int PRIMARY KEY, c counter)")

        pk = 1  # Single partition key for all updates
        total_updates = 0

        # Perform counter updates on each node
        for host in hosts:
            await cql.run_async(f"UPDATE {ks}.counters SET c = c + 1 WHERE pk = {pk}", host=host)
            total_updates += 1

        # Verify counter value
        result = await cql.run_async(f"SELECT c FROM {ks}.counters WHERE pk = {pk}")
        actual_count = result[0].c
        assert actual_count == total_updates, f"Counter value mismatch: expected {total_updates}, got {actual_count}"

        await asyncio.gather(*[manager.api.flush_keyspace(s.ip_addr, ks) for s in servers])

        # Collect counter IDs from all nodes
        counter_ids = set()
        for h in hosts:
            res = await cql.run_async(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.counters)", host=h)
            for row in res:
                if row.value:
                    value_dict = json.loads(row.value)
                    counter_ids.update({counter_shard["id"] for counter_shard in value_dict["c"]})

        logger.info(f"Unique counter IDs found: {counter_ids}")
        assert len(counter_ids) == 3, f"Expected exactly 3 counter IDs (one per rack), but found {len(counter_ids)}: {counter_ids}"
