#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from cassandra.protocol import ConfigurationException, InvalidRequest, SyntaxException
from cassandra.query import SimpleStatement, ConsistencyLevel
from contextlib import asynccontextmanager
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError, read_barrier, inject_error_one_shot
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas, get_base_table, get_tablet_count
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

async def disable_tablet_balancing(manager, servers):
    await asyncio.gather(*[manager.api.disable_tablet_balancing(s.ip_addr) for s in servers])

async def enable_tablet_balancing(manager, servers):
    await asyncio.gather(*[manager.api.enable_tablet_balancing(s.ip_addr) for s in servers])

@asynccontextmanager
async def no_tablet_balancing(manager, servers):
    await disable_tablet_balancing(manager, servers);
    try:
        yield
    finally:
        await enable_tablet_balancing(manager, servers)

async def get_tablet_info(manager, table_id, token):
    base_table = await get_base_table(manager, table_id)
    rows = await manager.cql.run_async(f"SELECT * FROM system.tablets where table_id = {base_table}")
    for row in rows:
        if row.last_token >= token:
            return row
    raise "Tablet not found"

async def wait_for_tablet_stage(manager, table_id, token, stage):
    async def tablet_is_in_stage():
        ti = await get_tablet_info(manager, table_id, token)
        if ti.stage == stage:
            return True
    await wait_for(tablet_is_in_stage, time.time() + 60)

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

    min_tablet_count = 8

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3 }} AND tablets = {{'initial': 2}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH tablets={{'min_tablet_count':{min_tablet_count}}};")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        base_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        view_tablet_count = await get_tablet_count(manager, servers[0], ks, 'tv')

        assert base_tablet_count >= min_tablet_count
        assert base_tablet_count == view_tablet_count

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
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        new_servers = await manager.servers_add(3, config=cfg, cmdline=cmdline)

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
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        for t in replicas:
            assert len(t.replicas) == from_rf

        rf = from_rf
        while rf < to_rf:
            await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': {rf+1}}} AND tablets = {{'initial': 2}}")
            rf += 1

            replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
            for t in replicas:
                assert len(t.replicas) == rf

@pytest.mark.parametrize("move_table", ["base", "child"])
@pytest.mark.asyncio
async def test_move_tablet(manager: ManagerClient, move_table: str):
    cfg = {'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
    ]
    servers = await manager.servers_add(2, config=cfg, cmdline=cmdline)
    await asyncio.gather(*[manager.api.disable_tablet_balancing(s.ip_addr) for s in servers])

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        if move_table == 'base':
            table = 'test'
            is_view = False
        else:
            table = 'tv'
            is_view = True

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, table, tablet_token, is_view)
        if replica[0] == s0_host_id:
            dst_host_id = s1_host_id
        else:
            dst_host_id = s0_host_id

        logger.info(f"Migrate the tablet to {dst_host_id}")
        await manager.api.move_tablet(servers[0].ip_addr, ks, table, replica[0], replica[1], dst_host_id, 0, tablet_token)
        logger.info("Migration done")

        new_replica = await get_tablet_replica(manager, servers[0], ks, table, tablet_token, is_view)
        assert new_replica[0] == dst_host_id

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
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        servers.append(await manager.server_add(config=cfg, cmdline=cmdline))
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        old_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        for tablet in old_replicas:
            assert len(tablet.replicas) == 1

        logger.info("Adding replica to tablet")
        tablet_token = 0 # Doesn't matter since there is one tablet
        await manager.api.add_tablet_replica(servers[0].ip_addr, ks, 'test', s1_host_id, 0, tablet_token)

        new_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        for tablet in new_replicas:
            assert len(tablet.replicas) == 2

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

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 4}") as ks:

        await cql.run_async(f"CREATE TABLE {ks}.test1 (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv11 AS SELECT * FROM {ks}.test1 WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv12 AS SELECT * FROM {ks}.test1 WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        await cql.run_async(f"CREATE TABLE {ks}.test2 (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv21 AS SELECT * FROM {ks}.test2 WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        await cql.run_async(f"CREATE TABLE {ks}.test3 (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv31 AS SELECT * FROM {ks}.test3 WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv32 AS SELECT * FROM {ks}.test3 WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        await manager.api.disable_tablet_balancing(servers[0].ip_addr)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.parametrize(
    "with_merge",
    [
        pytest.param(False, id="no_merge"),
        pytest.param(True, id="with_merge", marks=pytest.mark.skip(reason="issue #17265")),
    ],
)
async def test_tablet_split_and_merge(manager: ManagerClient, with_merge: bool):
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
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=1 AND bloom_filter_fp_chance=1;")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

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
            rows = await cql.run_async(f"SELECT * FROM {ks}.tv BYPASS CACHE;")
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

        async with no_tablet_balancing(manager, servers):
            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
            assert tablet_count > 1
            other_tablet_count = await get_tablet_count(manager, servers[0], ks, 'tv')
            assert tablet_count == other_tablet_count

        if not with_merge:
            return

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
        async with no_tablet_balancing(manager, servers):
            for server in servers:
                await manager.api.flush_keyspace(server.ip_addr, ks)
                await manager.api.keyspace_compaction(server.ip_addr, ks)

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

        async with no_tablet_balancing(manager, servers):
            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
            assert tablet_count < old_tablet_count
            other_tablet_count = await get_tablet_count(manager, servers[0], ks, 'tv')
            assert tablet_count == other_tablet_count

            await check()

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_revert_migration(manager: ManagerClient):
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
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        table = 'test'

        total_keys = 200
        keys = range(total_keys)
        def populate(keys, tab):
            insert = cql.prepare(f"INSERT INTO {ks}.{tab}(pk, c) VALUES(?, ?)")
            for pk in keys:
                value = pk+1
                cql.execute(insert, [pk, value])
        populate(keys, 'test')

        async def check():
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {ks}.test BYPASS CACHE;")
            assert len(rows) == len(keys)
            rows = await cql.run_async(f"SELECT * FROM {ks}.tv BYPASS CACHE;")
            assert len(rows) == len(keys)

        await check()

        await inject_error_one_shot_on(manager, "stream_tablet_fail", servers)
        await inject_error_on(manager, "stream_tablet_move_to_cleanup", servers)

        servers.append(await manager.server_add(config=cfg, cmdline=cmdline))
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        logger.info("Migrate the tablet to node 2")
        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, table, tablet_token)
        await manager.api.move_tablet(servers[0].ip_addr, ks, table, replica[0], replica[1], s1_host_id, 0, tablet_token)
        logger.info("Migration done")

        await check()
