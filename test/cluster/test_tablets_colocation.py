#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from cassandra.protocol import ConfigurationException, InvalidRequest, SyntaxException
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError, read_barrier, inject_error_one_shot
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas
from test.pylib.util import unique_name, wait_for
from test.cluster.conftest import skip_mode
from test.cluster.util import wait_for_cql_and_get_hosts, create_new_test_keyspace, new_test_keyspace, reconnect_driver
from contextlib import nullcontext as does_not_raise
import time
import pytest
import logging
import asyncio
import re
import requests
import random
import os
import glob
import shutil

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

async def repair_on_node(manager: ManagerClient, server: ServerInfo, servers: list[ServerInfo], keyspace, table = "test", ranges: str = ''):
    node = server.ip_addr
    await manager.servers_see_each_other(servers)
    live_nodes_wanted = [s.ip_addr for s in servers]
    live_nodes = await manager.api.get_alive_endpoints(node)
    live_nodes_wanted.sort()
    live_nodes.sort()
    assert live_nodes == live_nodes_wanted
    logger.info(f"Repair table on node {node} live_nodes={live_nodes} live_nodes_wanted={live_nodes_wanted}")
    await manager.api.repair(node, keyspace, table, ranges)

async def get_tablet_count(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str, is_view: bool = False):
    host = manager.cql.cluster.metadata.get_host(server.ip_addr)

    # read_barrier is needed to ensure that local tablet metadata on the queried node
    # reflects the finalized tablet movement.
    await read_barrier(manager.api, server.ip_addr)

    if is_view:
        table_id = await manager.get_view_id(keyspace_name, table_name)
    else:
        table_id = await manager.get_table_id(keyspace_name, table_name)
    rows = await manager.cql.run_async(f"SELECT tablet_count FROM system.tablets where "
                                       f"table_id = {table_id}", host=host)
    return rows[0].tablet_count

@pytest.mark.asyncio
async def assert_base_view_colocation(manager: ManagerClient, server: ServerInfo, ks: str, base: str, view: str):
    async def replicas_eq():
        base_replicas = await get_all_tablet_replicas(manager, server, ks, base)
        view_replicas = await get_all_tablet_replicas(manager, server, ks, view, True)
        if base_replicas == view_replicas:
            return True
    await wait_for(replicas_eq, time.time() + 60)

@pytest.mark.asyncio
async def assert_colocation(manager: ManagerClient, server: ServerInfo, ks: str, base: str, child: str):
    async def replicas_eq():
        base_replicas = await get_all_tablet_replicas(manager, server, ks, base)
        child_replicas = await get_all_tablet_replicas(manager, server, ks, child)
        if base_replicas == child_replicas:
            return True
    await wait_for(replicas_eq, time.time() + 60)

@pytest.mark.skip(reason="TODO MICHAEL Cannot drop non existing keyspace")
@pytest.mark.asyncio
async def test_colocation_with_topology_changes(manager: ManagerClient):
    cfg = {'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
    ]
    servers = await manager.servers_add(3, config=cfg, cmdline=cmdline)

    cql = manager.get_cql()

    for server in servers:
        await manager.api.enable_injection(server.ip_addr, "tablet_allocator_shuffle", one_shot=False)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 4}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")

        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

        new_servers = await manager.servers_add(3, config=cfg, cmdline=cmdline)

        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

@pytest.mark.asyncio
async def test_drop_base_table(manager: ManagerClient):
    cfg = {'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
        '--logger-log-level', 'load_balancer=debug',
    ]
    servers = await manager.servers_add(1, config=cfg, cmdline=cmdline)

    cql = manager.get_cql()

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }} AND tablets = {{'initial': 2}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")
        time.sleep(1)
        await cql.run_async(f"DROP TABLE {ks}.test")

@pytest.mark.asyncio
async def test_base_view_colocation(manager: ManagerClient):
    cfg = {'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
        '--logger-log-level', 'load_balancer=debug',
    ]
    servers = await manager.servers_add(3, config=cfg, cmdline=cmdline)

    cql = manager.get_cql()

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3 }} AND tablets = {{'initial': 2}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH tablets={{'min_tablet_count':8}};")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        assert 8 == await get_tablet_count(manager, servers[0], ks, 'test')
        assert 8 == await get_tablet_count(manager, servers[0], ks, 'tv', True)

        await assert_base_view_colocation(manager, servers[0], ks, 'test', 'tv')

@pytest.mark.asyncio
async def test_keyspace_rf_change(manager: ManagerClient):
    cfg = {'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
        '--logger-log-level', 'load_balancer=debug',
    ]
    servers = await manager.servers_add(3, config=cfg, cmdline=cmdline)

    cql = manager.get_cql()
    res = await cql.run_async("SELECT data_center FROM system.local")
    this_dc = res[0].data_center

    from_rf = 1
    to_rf = 3

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': {from_rf}}} AND tablets = {{'initial': 2}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        for t in replicas:
            assert len(t.replicas) == from_rf
        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

        rf = from_rf
        while rf < to_rf:
            await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': {rf+1}}} AND tablets = {{'initial': 2}}")
            rf += 1

            replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
            for t in replicas:
                assert len(t.replicas) == rf
            await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

@pytest.mark.parametrize("move_table", ["base", "child"])
@pytest.mark.asyncio
async def test_move_tablet(manager: ManagerClient, move_table: str):
    cfg = {'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
    ]
    servers = [await manager.server_add(config=cfg, cmdline=cmdline)]
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")

        if move_table == 'base':
            table = 'test'
        else:
            table = 'test_colocation'

        servers.append(await manager.server_add(config=cfg, cmdline=cmdline))
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        logger.info("Migrate the tablet to node 2")
        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, table, tablet_token)
        await manager.api.move_tablet(servers[0].ip_addr, ks, table, replica[0], replica[1], s1_host_id, 0, tablet_token)
        logger.info("Migration done")

        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

@pytest.mark.asyncio
async def test_add_tablet_replica(manager: ManagerClient):
    cfg = {'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
    ]
    servers = [await manager.server_add(config=cfg, cmdline=cmdline)]
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")

        servers.append(await manager.server_add(config=cfg, cmdline=cmdline))
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        logger.info("Adding replica to tablet")
        tablet_token = 0 # Doesn't matter since there is one tablet
        await manager.api.add_tablet_replica(servers[0].ip_addr, ks, 'test', s1_host_id, 0, tablet_token)

        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

@pytest.mark.asyncio
async def test_multiple_colocation_groups(manager: ManagerClient):
    cfg = {'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
        '--logger-log-level', 'load_balancer=debug',
    ]
    servers = await manager.servers_add(6, config=cfg, cmdline=cmdline)

    cql = manager.get_cql()

    for server in servers:
        await manager.api.enable_injection(server.ip_addr, "tablet_allocator_shuffle", one_shot=False)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 4}") as ks:

        await cql.run_async(f"CREATE TABLE {ks}.test1 (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test1_1colocation (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test1_2colocation (pk int PRIMARY KEY, c int);")

        await cql.run_async(f"CREATE TABLE {ks}.test2 (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test2_1colocation (pk int PRIMARY KEY, c int);")

        await cql.run_async(f"CREATE TABLE {ks}.test3 (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test3_1colocation (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test3_2colocation (pk int PRIMARY KEY, c int);")

        await manager.api.disable_tablet_balancing(servers[0].ip_addr)
        await assert_colocation(manager, servers[0], ks, 'test1', 'test1_1colocation')
        await assert_colocation(manager, servers[0], ks, 'test1', 'test1_2colocation')

        await assert_colocation(manager, servers[0], ks, 'test2', 'test2_1colocation')

        await assert_colocation(manager, servers[0], ks, 'test3', 'test3_1colocation')
        await assert_colocation(manager, servers[0], ks, 'test3', 'test3_2colocation')

@pytest.mark.parametrize("repair_table", ["base", "child"])
@pytest.mark.repair
@pytest.mark.asyncio
async def test_tablet_repair(manager: ManagerClient, repair_table: str):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'repair=trace',
        '--task-ttl-in-seconds', '3600',    # Make sure the test passes with non-zero task_ttl.
    ]
    servers = await manager.servers_add(3, cmdline=cmdline)

    await inject_error_on(manager, "tablet_allocator_shuffle", servers)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': 2} AND tablets = {'initial': 32}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")

        if repair_table == 'base':
            table = 'test'
        else:
            table = 'test_colocation'

        logger.info("Populating table")

        keys = range(256)

        stmt = cql.prepare(f"INSERT INTO {ks}.{table} (pk, c) VALUES (?, ?)")
        stmt.consistency_level = ConsistencyLevel.ONE

        # Repair runs concurrently with tablet shuffling which exercises issues with serialization
        # of repair and tablet migration.
        #
        # We do it 30 times because it's been experimentally shown to be enough to trigger the issue with high probability.
        # Lack of proper synchronization would manifest as repair failure with the following cause:
        #
        #   failed_because=std::runtime_error (multishard_writer: No shards for token 7505809055260144771 of test.test)
        #
        # ...which indicates that repair tried to stream data to a node which is no longer a tablet replica.
        repair_cycles = 30
        for i in range(repair_cycles):
            # Write concurrently with repair to increase the chance of repair having some discrepancy to resolve and send writes.
            inserts_future = asyncio.gather(*[cql.run_async(stmt, [k, i]) for k in keys])

            # Disable in the background so that repair is started with migrations in progress.
            # We need to disable balancing so that repair which blocks on migrations eventually gets unblocked.
            # Otherwise, shuffling would keep the topology busy forever.
            disable_balancing_future = asyncio.create_task(manager.api.disable_tablet_balancing(servers[0].ip_addr))

            await repair_on_node(manager, servers[0], servers, ks)

            await inserts_future
            await disable_balancing_future
            await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        key_count = len(keys)
        stmt = cql.prepare(f"SELECT * FROM {ks}.{table};")
        stmt.consistency_level = ConsistencyLevel.ALL
        rows = await cql.run_async(stmt)
        assert len(rows) == key_count
        for r in rows:
            assert r.c == repair_cycles - 1

@pytest.mark.parametrize("split_table", ["base", "child"])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_split_and_merge(manager: ManagerClient, split_table: str):
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
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1;")

        if split_table == 'base':
            table, other_table = 'test', 'test_colocation'
        else:
            table, other_table = 'test_colocation', 'test'

        # Initial average table size of 400k (1 tablet), so triggers some splits.
        total_keys = 200
        keys = range(total_keys)
        def populate(keys):
            insert = cql.prepare(f"INSERT INTO {ks}.{table}(pk, c) VALUES(?, ?)")
            for pk in keys:
                value = random.randbytes(2000)
                cql.execute(insert, [pk, value])
        populate(keys)

        async def check():
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {ks}.{table} BYPASS CACHE;")
            assert len(rows) == len(keys)

        await check()

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        tablet_count = await get_tablet_count(manager, servers[0], ks, table)
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

        tablet_count = await get_tablet_count(manager, servers[0], ks, table)
        assert tablet_count > 1

        other_tablet_count = await get_tablet_count(manager, servers[0], ks, other_table)
        assert tablet_count == other_tablet_count

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
        await asyncio.gather(*[cql.run_async(f"DELETE FROM {ks}.{table} WHERE pk={k};") for k in delete_keys])
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

        old_tablet_count = await get_tablet_count(manager, servers[0], ks, table)
        s1_mark = await s1_log.mark()

        await inject_error_on(manager, "replica_merge_completion_wait", servers)
        await disable_injection_on(manager, "tablet_merge_completion_bypass", servers)

        await s1_log.wait_for('Detected tablet merge for table', from_mark=s1_mark)

        tablet_count = await get_tablet_count(manager, servers[0], ks, table)
        assert tablet_count < old_tablet_count
        await check()

        other_tablet_count = await get_tablet_count(manager, servers[0], ks, other_table)
        assert tablet_count == other_tablet_count
