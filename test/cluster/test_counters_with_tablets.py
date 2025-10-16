#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace
from test.pylib.tablets import get_tablet_replica

import asyncio
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
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

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
