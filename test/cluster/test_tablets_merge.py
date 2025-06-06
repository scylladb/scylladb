#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot, read_barrier
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas, get_tablet_count
from test.pylib.util import wait_for
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace, create_new_test_keyspace

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
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1;")

        # Initial average table size of 400k (1 tablet), so triggers some splits.
        total_keys = 200
        keys = range(total_keys)
        def populate(keys):
            insert = cql.prepare(f"INSERT INTO {ks}.test(pk, c) VALUES(?, ?)")
            for pk in keys:
                value = random.randbytes(2000)
                cql.execute(insert, [pk, value])
        populate(keys)

        async def check():
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {ks}.test BYPASS CACHE;")
            assert len(rows) == len(keys)

        await check()

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
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

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
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
        await asyncio.gather(*[cql.run_async(f"DELETE FROM {ks}.test WHERE pk={k};") for k in delete_keys])
        keys = range(total_keys - 1, total_keys)

        # To avoid race of major with migration
        await manager.api.disable_tablet_balancing(servers[0].ip_addr)

        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)
            await manager.api.keyspace_compaction(server.ip_addr, ks)
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        await s1_log.wait_for("Emitting resize decision of type merge", from_mark=s1_mark)
        # Waits for balancer to co-locate sibling tablets
        await s1_log.wait_for("All sibling tablets are co-located")
        # Do some shuffling to make sure balancer works with co-located tablets
        await shuffle()

        old_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        s1_mark = await s1_log.mark()

        await inject_error_on(manager, "replica_merge_completion_wait", servers)
        await disable_injection_on(manager, "tablet_merge_completion_bypass", servers)

        await s1_log.wait_for('Detected tablet merge for table', from_mark=s1_mark)

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
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
        tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        assert len(tablet_replicas) > 0
        t = tablet_replicas[0]
        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", *t.replicas[0], *(s1_host_id, 0), t.last_token))
        # Trigger split
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)
        try:
            await migration_task
        except:
            # move_tablet() fails if tablet is already in transit.
            # forgive if balancer decided to migrate the target tablet post split.
            pass

        await s1_log.wait_for('Merge completion fiber finished', from_mark=s1_mark)

        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)
            await manager.api.keyspace_compaction(server.ip_addr, ks)
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
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1;")

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
            insert = cql.prepare(f"INSERT INTO {ks}.test(pk, c) VALUES(?, ?)")
            for pk in keys:
                value = random.randbytes(2000)
                cql.execute(insert, [pk, value])

            async def check():
                logger.info("Checking table")
                cql = manager.get_cql()
                rows = await cql.run_async(f"SELECT * FROM {ks}.test BYPASS CACHE;")
                assert len(rows) == len(keys)

            await check()

            logger.info("Flushing keyspace")
            for server in servers:
                await manager.api.flush_keyspace(server.ip_addr, ks)

            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')

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
            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
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
            await asyncio.gather(*[cql.run_async(f"DELETE FROM {ks}.test WHERE pk={k};") for k in delete_keys])
            keys = range(total_keys - 1, total_keys)

            await disable_injection_on(manager, "tablet_allocator_shuffle", servers)

            # To avoid race of major with migration
            await manager.api.disable_tablet_balancing(servers[0].ip_addr)

            logger.info("Flushing keyspace and performing major")
            for server in servers:
                await manager.api.flush_keyspace(server.ip_addr, ks)
                await manager.api.keyspace_compaction(server.ip_addr, ks)
            await manager.api.enable_tablet_balancing(servers[0].ip_addr)

            logger.info("Waiting for merge decision")
            await s1_log.wait_for("Emitting resize decision of type merge", from_mark=s1_mark)
            # Waits for balancer to co-locate sibling tablets
            await s1_log.wait_for("All sibling tablets are co-located")
            # Do some shuffling to make sure balancer works with co-located tablets
            await inject_error_on(manager, "tablet_allocator_shuffle", servers)

            old_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')

            topology_ops_task = asyncio.create_task(perform_topology_ops())

            await inject_error_on(manager, "replica_merge_completion_wait", servers)
            await disable_injection_on(manager, "tablet_merge_completion_bypass", servers)
            await disable_injection_on(manager, "tablet_allocator_shuffle", servers)

            await s1_log.wait_for('Detected tablet merge for table', from_mark=s1_mark)
            await s1_log.wait_for('Merge completion fiber finished', from_mark=s1_mark)

            logger.info("Waiting for topology ops")
            await topology_ops_task

            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
            assert tablet_count < old_tablet_count
            logger.info("Merge decreased number of tablets from {} to {}".format(old_tablet_count, tablet_count))
            await check()

            logger.info("Flushing keyspace and performing major")
            for server in servers:
                await manager.api.flush_keyspace(server.ip_addr, ks)
                await manager.api.keyspace_compaction(server.ip_addr, ks)
            await check()

@pytest.mark.parametrize("racks", [2, 3])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_merge_cross_rack_migrations(manager: ManagerClient, racks):
    cmdline = ['--target-tablet-size-in-bytes', '30000',]
    config = {'error_injections_at_startup': ['short_tablet_stats_refresh_interval']}

    servers = []
    rf = racks
    for rack_id in range(0, racks):
        rack = f'rack{rack_id+1}'
        servers.extend(await manager.servers_add(3, config=config, cmdline=cmdline, property_file={'dc': 'mydc', 'rack': rack}))

    cql = manager.get_cql()
    ks = await create_new_test_keyspace(cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} AND tablets = {{'initial': 1}}")
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c blob) WITH compression = {{'sstable_compression': ''}};")

    await inject_error_on(manager, "forbid_cross_rack_migration_attempt", servers)

    total_keys = 400
    keys = range(total_keys)
    insert = cql.prepare(f"INSERT INTO {ks}.test(pk, c) VALUES(?, ?)")
    for pk in keys:
        value = random.randbytes(2000)
        cql.execute(insert, [pk, value])

    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, ks)

    async def finished_splitting():
        # FIXME: fragile since it's expecting on-disk size will be enough to produce a few splits.
        #   (raw_data=800k / target_size=30k) = ~26, lower power-of-two is 16. Compression was disabled.
        #   Per-table hints (min_tablet_count) can be used to improve this.
        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        return tablet_count >= 16 or None
    # Give enough time for split to happen in debug mode
    await wait_for(finished_splitting, time.time() + 120)

    delete_keys = range(total_keys - 1)
    await asyncio.gather(*[cql.run_async(f"DELETE FROM {ks}.test WHERE pk={k};") for k in delete_keys])
    keys = range(total_keys - 1, total_keys)

    old_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')

    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, ks)
        await manager.api.keyspace_compaction(server.ip_addr, ks)

    async def finished_merging():
        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        return tablet_count < old_tablet_count or None
    await wait_for(finished_merging, time.time() + 120)

# Reproduces use-after-free when migration right after merge, but concurrently to background
# merge completion handler.
# See: https://github.com/scylladb/scylladb/issues/24045
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_migration_running_concurrently_to_merge_completion_handling(manager: ManagerClient):
    cmdline = []
    cfg = {}
    servers = [await manager.server_add(cmdline=cmdline, config=cfg)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        assert tablet_count == 2

        old_tablet_count = tablet_count

        keys = range(100)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        await cql.run_async(f"ALTER KEYSPACE {ks} WITH tablets = {{'initial': 1}};")

        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()

        await manager.api.enable_injection(servers[0].ip_addr, "merge_completion_fiber", one_shot=True)
        await manager.api.enable_injection(servers[0].ip_addr, "replica_merge_completion_wait", one_shot=True)
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        servers.append(await manager.server_add(cmdline=cmdline, config=cfg))
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        async def finished_merging():
            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
            return tablet_count < old_tablet_count or None

        await wait_for(finished_merging, time.time() + 120)

        await manager.api.disable_tablet_balancing(servers[0].ip_addr)
        await manager.api.enable_injection(servers[0].ip_addr, "take_storage_snapshot", one_shot=True)

        await s0_log.wait_for(f"merge_completion_fiber: waiting", from_mark=s0_mark)

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        assert tablet_count == 1

        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        src_shard = replica[1]
        dst_shard = src_shard

        migration = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], src_shard, s1_host_id, dst_shard, tablet_token))

        await s0_log.wait_for(f"take_storage_snapshot: waiting", from_mark=s0_mark)

        await manager.api.message_injection(servers[0].ip_addr, "merge_completion_fiber")
        await s0_log.wait_for(f"Merge completion fiber finished", from_mark=s0_mark)

        await manager.api.message_injection(servers[0].ip_addr, "take_storage_snapshot")

        await migration

        rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
        assert len(rows) == len(keys)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_missing_data(manager: ManagerClient):

    # This is a test and reproducer for issue:
    # https://github.com/scylladb/scylladb/issues/23313

    logger.info('Bootstrapping cluster')
    cfg = { 'enable_tablets': True,
            'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
            }
    cmdline = [
        '--logger-log-level', 'load_balancer=debug',
        '--logger-log-level', 'debug_error_injection=debug',
    ]
    server = await manager.server_add(cmdline=cmdline, config=cfg)

    logger.info(f'server_id = {server.server_id}')

    cql = manager.get_cql()

    await manager.api.disable_tablet_balancing(server.ip_addr)

    inital_tablets = 32

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': {inital_tablets}}}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')

        await manager.api.disable_autocompaction(server.ip_addr, ks, 'test')

        # insert data
        pks = range(inital_tablets)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in pks])

        # flush the table
        await manager.api.flush_keyspace(server.ip_addr, ks)

        # force merge on the test table
        expected_tablet_count = inital_tablets // 2
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH tablets = {{'initial': {expected_tablet_count}}}")

        await manager.api.enable_tablet_balancing(server.ip_addr)

        # wait for merge to complete
        actual_tablet_count = 0
        started = time.time()
        while expected_tablet_count != actual_tablet_count:
            actual_tablet_count = await get_tablet_count(manager, server, ks, 'test')
            logger.debug(f'actual/expected tablet count: {actual_tablet_count}/{expected_tablet_count}')

            assert time.time() - started < 60, 'Timeout while waiting for tablet merge'

            await asyncio.sleep(.1)

        logger.info(f'Merged test table; new number of tablets: {expected_tablet_count}')

        # assert that the number of records has not changed
        qry = f'SELECT * FROM {ks}.test'
        logger.info(f'Running: {qry}')
        res = cql.execute(qry)
        missing = set(pks)
        rec_count = 0
        for row in res:
            rec_count += 1
            missing.discard(row.pk)

        assert rec_count == len(pks), f"received {rec_count} records instead of {len(pks)} while querying server {server.server_id}; missing keys: {missing}"
