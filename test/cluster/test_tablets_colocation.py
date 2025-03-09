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
    rows = await manager.cql.run_async(f"SELECT * FROM system.tablets where table_id = {table_id}")
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

async def assert_base_view_colocation(manager: ManagerClient, server: ServerInfo, ks: str, base: str, view: str):
    async def replicas_eq():
        base_replicas = await get_all_tablet_replicas(manager, server, ks, base)
        view_replicas = await get_all_tablet_replicas(manager, server, ks, view, True)
        if base_replicas == view_replicas:
            return True
    await wait_for(replicas_eq, time.time() + 60)

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
        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')
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

    min_tablet_count = 8

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3 }} AND tablets = {{'initial': 2}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH tablets={{'min_tablet_count':{min_tablet_count}}};")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        base_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        view_tablet_count = await get_tablet_count(manager, servers[0], ks, 'tv', True)

        assert base_tablet_count >= min_tablet_count
        assert base_tablet_count == view_tablet_count

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
    servers = await manager.servers_add(2, config=cfg, cmdline=cmdline)
    await asyncio.gather(*[manager.api.disable_tablet_balancing(s.ip_addr) for s in servers])

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")

        if move_table == 'base':
            table = 'test'
        else:
            table = 'test_colocation'

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, table, tablet_token)
        if replica[0] == s0_host_id:
            dst_host_id = s1_host_id
        else:
            dst_host_id = s0_host_id

        logger.info(f"Migrate the tablet to {dst_host_id}")
        await manager.api.move_tablet(servers[0].ip_addr, ks, table, replica[0], replica[1], dst_host_id, 0, tablet_token)
        logger.info("Migration done")

        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

        new_replica = await get_tablet_replica(manager, servers[0], ks, table, tablet_token)
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
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")

        servers.append(await manager.server_add(config=cfg, cmdline=cmdline))
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        old_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        for tablet in old_replicas:
            assert len(tablet.replicas) == 1

        logger.info("Adding replica to tablet")
        tablet_token = 0 # Doesn't matter since there is one tablet
        await manager.api.add_tablet_replica(servers[0].ip_addr, ks, 'test', s1_host_id, 0, tablet_token)

        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

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
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_migrate_table_while_other_colocated_table_is_in_repair(manager: ManagerClient, repair_table: str):
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
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")

        base_table_id = await manager.get_table_id(ks, 'test')
        child_table_id = await manager.get_table_id(ks, 'test_colocation')

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        if replica[0] == s0_host_id:
            replica_ip = servers[0].ip_addr
            dst_host_id = s1_host_id
        else:
            replica_ip = servers[1].ip_addr
            dst_host_id = s0_host_id

        if repair_table == 'base':
            repair_table_name = 'test'
            repair_table_id = base_table_id
            move_table_name = 'test_colocation'
        else:
            repair_table_name = 'test_colocation'
            repair_table_id = child_table_id
            move_table_name = 'test'

        repair_wait = "repair_tablet_wait"
        await inject_error_on(manager, repair_wait, servers)

        repair_task = asyncio.create_task(manager.api.tablet_repair(replica_ip, ks, repair_table_name, tablet_token))
        await wait_for_tablet_stage(manager, repair_table_id, tablet_token, "repair")

        if repair_table == 'base':
            # We can't migrate the child while the base table is in repair. This should fail.
            try:
                await manager.api.move_tablet(servers[0].ip_addr, ks, move_table_name, replica[0], replica[1], dst_host_id, 0, tablet_token)
                raise "move_tablet of child should fail because the base table is busy with repair"
            except HTTPError as e:
                assert 'is in transition' in e.message
            except:
                raise

            await disable_injection_on(manager, repair_wait, servers)
            await repair_task
            await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')
        else:
            # The base table should wait in 'allow_write_both_read_old' until child is done with the repair and then it
            # will continue coordinating the migration.
            move_task = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, 'test', replica[0], replica[1], dst_host_id, 0, tablet_token))
            await wait_for_tablet_stage(manager, base_table_id, tablet_token, "allow_write_both_read_old")

            await disable_injection_on(manager, repair_wait, servers)
            await repair_task
            await move_task

            await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')
            new_replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
            assert new_replica[0] == dst_host_id

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_create_colocated_table_while_base_is_migrating(manager: ManagerClient):
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
        base_table_id = await manager.get_table_id(ks, 'test')

        total_keys = 100
        keys = range(total_keys)
        def populate(table, keys):
            insert = cql.prepare(f"INSERT INTO {ks}.{table}(pk, c) VALUES(?, ?)")
            for pk in keys:
                cql.execute(insert, [pk, pk+1])

        populate('test', keys)

        async def check(table):
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {ks}.{table} BYPASS CACHE;")
            assert len(rows) == len(keys)

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        tablet_token = 0
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        if replica[0] == s0_host_id:
            replica_ip = servers[0].ip_addr
            dst_host_id = s1_host_id
        else:
            replica_ip = servers[1].ip_addr
            dst_host_id = s0_host_id

        streaming_wait_injection = "stream_tablet_wait"
        await inject_error_on(manager, streaming_wait_injection, servers)
        move_task = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, 'test', replica[0], replica[1], dst_host_id, 0, tablet_token))
        await wait_for_tablet_stage(manager, base_table_id, tablet_token, "streaming")

        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")
        populate('test_colocation', keys)

        await disable_injection_on(manager, streaming_wait_injection, servers)
        await move_task

        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')
        new_replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        assert new_replica[0] == dst_host_id

        await check('test')
        await check('test_colocation')

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

        async with no_tablet_balancing(manager, servers):
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
        async with no_tablet_balancing(manager, servers):
            for server in servers:
                await manager.api.flush_keyspace(server.ip_addr, ks)
                await manager.api.keyspace_compaction(server.ip_addr, ks)

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

        async with no_tablet_balancing(manager, servers):
            tablet_count = await get_tablet_count(manager, servers[0], ks, table)
            assert tablet_count < old_tablet_count
            other_tablet_count = await get_tablet_count(manager, servers[0], ks, other_table)
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
        await cql.run_async(f"CREATE TABLE {ks}.test_colocation (pk int PRIMARY KEY, c int);")

        table = 'test'

        total_keys = 200
        keys = range(total_keys)
        def populate(keys, tab):
            insert = cql.prepare(f"INSERT INTO {ks}.{tab}(pk, c) VALUES(?, ?)")
            for pk in keys:
                value = pk+1
                cql.execute(insert, [pk, value])
        populate(keys, 'test')
        populate(keys, 'test_colocation')

        async def check():
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {ks}.test BYPASS CACHE;")
            assert len(rows) == len(keys)
            rows = await cql.run_async(f"SELECT * FROM {ks}.test_colocation BYPASS CACHE;")
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

        await assert_colocation(manager, servers[0], ks, 'test', 'test_colocation')

        await check()
