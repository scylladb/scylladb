#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot, HTTPError, read_barrier
from test.pylib.tablets import get_all_tablet_replicas
from test.topology.conftest import skip_mode

import pytest
import asyncio
import logging
import time
import random

logger = logging.getLogger(__name__)

async def inject_error_one_shot_on(manager, error_name, servers):
    errs = [inject_error_one_shot(manager.api, s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)

async def disable_injection_on(manager, error_name, servers):
    errs = [manager.api.disable_injection(s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


async def get_tablet_count(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str):
    host = manager.cql.cluster.metadata.get_host(server.ip_addr)

    # read_barrier is needed to ensure that local tablet metadata on the queried node
    # reflects the finalized tablet movement.
    await read_barrier(manager.api, server.ip_addr)

    table_id = await manager.get_table_id(keyspace_name, table_name)
    rows = await manager.cql.run_async(f"SELECT tablet_count FROM system.tablets where "
                                       f"table_id = {table_id}", host=host)
    return rows[0].tablet_count

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_merge_simple(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'table=debug',
        '--logger-log-level', 'load_balancer=debug',
        '--target-tablet-size-in-bytes', '30000',
    ]
    servers = [await manager.server_add(config={
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
    }, cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1;")

    # Initial average table size of 400k (1 tablet), so triggers some splits.
    total_keys = 200
    keys = range(total_keys)
    def populate(keys):
        insert = cql.prepare(f"INSERT INTO test.test(pk, c) VALUES(?, ?)")
        for pk in keys:
            value = random.randbytes(2000)
            cql.execute(insert, [pk, value])
    populate(keys)

    async def check():
        logger.info("Checking table")
        cql = manager.get_cql()
        rows = await cql.run_async("SELECT * FROM test.test BYPASS CACHE;")
        assert len(rows) == len(keys)

    await check()

    await manager.api.flush_keyspace(servers[0].ip_addr, "test")

    tablet_count = await get_tablet_count(manager, servers[0], 'test', 'test')
    assert tablet_count == 1

    logger.info("Adding new server")
    servers.append(await manager.server_add(cmdline=cmdline))
    s1_host_id = await manager.get_host_id(servers[1].server_id)

    # Increases the chance of tablet migration concurrent with split
    await inject_error_one_shot_on(manager, "tablet_allocator_shuffle", servers)
    await inject_error_on(manager, "tablet_load_stats_refresh_before_rebalancing", servers)

    s1_log = await manager.server_open_log(servers[0].server_id)
    s1_mark = await s1_log.mark()

    # Now there's a split and migration need, so they'll potentially run concurrently.
    await manager.api.enable_tablet_balancing(servers[0].ip_addr)

    await check()
    time.sleep(2) # Give load balancer some time to do work

    await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark)

    await check()

    tablet_count = await get_tablet_count(manager, servers[0], 'test', 'test')
    assert tablet_count > 1

    # Allow shuffling of tablet replicas to make co-location work harder
    async def shuffle():
        await inject_error_on(manager, "tablet_allocator_shuffle", servers)
        time.sleep(2)
        await disable_injection_on(manager, "tablet_allocator_shuffle", servers)

    await shuffle()

    # This will allow us to simulate some balancing after co-location with shuffling, to make sure that
    # balancer won't break co-location.
    await inject_error_on(manager, "tablet_merge_completion_bypass", servers)

    # Shrinks table significantly, forcing merge.
    delete_keys = range(total_keys - 1)
    await asyncio.gather(*[cql.run_async(f"DELETE FROM test.test WHERE pk={k};") for k in delete_keys])
    keys = range(total_keys - 1, total_keys)

    # To avoid race of major with migration
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, "test")
        await manager.api.keyspace_compaction(server.ip_addr, "test")
    await manager.api.enable_tablet_balancing(servers[0].ip_addr)

    await s1_log.wait_for("Emitting resize decision of type merge", from_mark=s1_mark)
    # Waits for balancer to co-locate sibling tablets
    await s1_log.wait_for("All sibling tablets are co-located")
    # Do some shuffling to make sure balancer works with co-located tablets
    await shuffle()

    old_tablet_count = await get_tablet_count(manager, servers[0], 'test', 'test')
    s1_mark = await s1_log.mark()

    await inject_error_on(manager, "replica_merge_completion_wait", servers)
    await disable_injection_on(manager, "tablet_merge_completion_bypass", servers)

    await s1_log.wait_for('Detected tablet merge for table', from_mark=s1_mark)

    tablet_count = await get_tablet_count(manager, servers[0], 'test', 'test')
    assert tablet_count < old_tablet_count
    await check()

    # Reproduces https://github.com/scylladb/scylladb/issues/21867 that could cause compaction group
    # to be destroyed without being stopped first.
    # That's done by:
    #   1) Migrating a tablet to another node, and putting an artificial delay in cleanup stage when stopping groups
    #   2) Force tablet split, causing new groups to be added in a tablet being cleaned up
    # Without the fix, new groups are added to tablet being migrated away and never closed, potentially
    # resulting in an use-after-free.
    keys = range(total_keys)
    populate(keys)
    # Migrates a tablet to another node and put artificial delay on cleanup stage
    await manager.api.enable_injection(servers[0].ip_addr, "delay_tablet_compaction_groups_cleanup", one_shot=True)
    tablet_replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
    assert len(tablet_replicas) > 0
    t = tablet_replicas[0]
    migration_task = asyncio.create_task(
        manager.api.move_tablet(servers[0].ip_addr, "test", "test", *t.replicas[0], *(s1_host_id, 0), t.last_token))
    # Trigger split
    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, "test")
    try:
        await migration_task
    except:
        # move_tablet() fails if tablet is already in transit.
        # forgive if balancer decided to migrate the target tablet post split.
        pass

    await s1_log.wait_for('Merge completion fiber finished', from_mark=s1_mark)

    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, "test")
        await manager.api.keyspace_compaction(server.ip_addr, "test")
    await check()

# Multiple cycles of split and merge, with topology changes in parallel and RF > 1.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_split_and_merge_with_concurrent_topology_changes(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=info',
        '--logger-log-level', 'table=info',
        '--logger-log-level', 'raft_topology=info',
        '--logger-log-level', 'group0_raft_sm=info',
        '--logger-log-level', 'load_balancer=info',
        '--target-tablet-size-in-bytes', '30000',
    ]
    config = {
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
    }
    servers = [await manager.server_add(config=config, cmdline=cmdline),
               await manager.server_add(config=config, cmdline=cmdline),
               await manager.server_add(config=config, cmdline=cmdline)]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1;")

    async def perform_topology_ops():
        logger.info("Topology ops in background")
        server_id_to_decommission = servers[-1].server_id
        logger.info("Decommissioning old server with id {}".format(server_id_to_decommission))
        await manager.decommission_node(server_id_to_decommission)
        servers.pop()
        logger.info("Adding new server")
        servers.append(await manager.server_add(cmdline=cmdline))
        logger.info("Completed topology ops")

    for cycle in range(2):
        logger.info("Running split-merge cycle #{}".format(cycle))

        await manager.api.disable_tablet_balancing(servers[0].ip_addr)

        logger.info("Inserting data")
        # Initial average table size of (400k + metadata_overhead). Enough to trigger a few splits.
        total_keys = 200
        keys = range(total_keys)
        insert = cql.prepare(f"INSERT INTO test.test(pk, c) VALUES(?, ?)")
        for pk in keys:
            value = random.randbytes(2000)
            cql.execute(insert, [pk, value])

        async def check():
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async("SELECT * FROM test.test BYPASS CACHE;")
            assert len(rows) == len(keys)

        await check()

        logger.info("Flushing keyspace")
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, "test")

        tablet_count = await get_tablet_count(manager, servers[0], 'test', 'test')

        # Increases the chance of tablet migration concurrent with split
        await inject_error_on(manager, "tablet_allocator_shuffle", servers)
        await inject_error_on(manager, "tablet_load_stats_refresh_before_rebalancing", servers)

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        logger.info("Enabling balancing")
        # Now there's a split and migration need, so they'll potentially run concurrently.
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        topology_ops_task = asyncio.create_task(perform_topology_ops())

        await check()

        logger.info("Waiting for split")
        await disable_injection_on(manager, "tablet_allocator_shuffle", servers)
        await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark)

        logger.info("Waiting for topology ops")
        await topology_ops_task

        await check()

        old_tablet_count = tablet_count
        tablet_count = await get_tablet_count(manager, servers[0], 'test', 'test')
        assert tablet_count > old_tablet_count
        logger.info("Split increased number of tablets from {} to {}".format(old_tablet_count, tablet_count))

        # Allow shuffling of tablet replicas to make co-location work harder
        await inject_error_on(manager, "tablet_allocator_shuffle", servers)
        # This will allow us to simulate some balancing after co-location with shuffling, to make sure that
        # balancer won't break co-location.
        await inject_error_on(manager, "tablet_merge_completion_bypass", servers)

        logger.info("Deleting data")
        # Delete almost all keys, enough to trigger a few merges.
        delete_keys = range(total_keys - 1)
        await asyncio.gather(*[cql.run_async(f"DELETE FROM test.test WHERE pk={k};") for k in delete_keys])
        keys = range(total_keys - 1, total_keys)

        await disable_injection_on(manager, "tablet_allocator_shuffle", servers)

        # To avoid race of major with migration
        await manager.api.disable_tablet_balancing(servers[0].ip_addr)

        logger.info("Flushing keyspace and performing major")
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, "test")
            await manager.api.keyspace_compaction(server.ip_addr, "test")
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        logger.info("Waiting for merge decision")
        await s1_log.wait_for("Emitting resize decision of type merge", from_mark=s1_mark)
        # Waits for balancer to co-locate sibling tablets
        await s1_log.wait_for("All sibling tablets are co-located")
        # Do some shuffling to make sure balancer works with co-located tablets
        await inject_error_on(manager, "tablet_allocator_shuffle", servers)

        old_tablet_count = await get_tablet_count(manager, servers[0], 'test', 'test')

        topology_ops_task = asyncio.create_task(perform_topology_ops())

        await inject_error_on(manager, "replica_merge_completion_wait", servers)
        await disable_injection_on(manager, "tablet_merge_completion_bypass", servers)
        await disable_injection_on(manager, "tablet_allocator_shuffle", servers)

        await s1_log.wait_for('Detected tablet merge for table', from_mark=s1_mark)
        await s1_log.wait_for('Merge completion fiber finished', from_mark=s1_mark)

        logger.info("Waiting for topology ops")
        await topology_ops_task

        tablet_count = await get_tablet_count(manager, servers[0], 'test', 'test')
        assert tablet_count < old_tablet_count
        logger.info("Merge decreased number of tablets from {} to {}".format(old_tablet_count, tablet_count))
        await check()

        logger.info("Flushing keyspace and performing major")
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, "test")
            await manager.api.keyspace_compaction(server.ip_addr, "test")
        await check()
