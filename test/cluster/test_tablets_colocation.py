#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from contextlib import asynccontextmanager
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.tablets import get_tablet_replica, get_base_table, get_tablet_count, get_tablet_info
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace
import time
import pytest
import logging
import asyncio
import random
from cassandra.query import SimpleStatement, ConsistencyLevel

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
    await disable_tablet_balancing(manager, servers)
    try:
        yield
    finally:
        await enable_tablet_balancing(manager, servers)

async def wait_for_tablet_stage(manager, server, keyspace_name, table_name, token, stage):
    async def tablet_is_in_stage():
        ti = await get_tablet_info(manager, server, keyspace_name, table_name, token)
        if ti.stage == stage:
            return True
    await wait_for(tablet_is_in_stage, time.time() + 60)

# Test the when creating MVs that have the same partition key as the base table
# they are co-located with the base table.
# We create multiple views, some with the same partition key and some not, and
# check that those views with the same partition key are co-located by reading
# their tablet map from system.tablets and checking they have base_table set.
@pytest.mark.asyncio
async def test_base_view_colocation(manager: ManagerClient):
    cfg = {'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
        '--logger-log-level', 'load_balancer=debug',
    ]
    servers = await manager.servers_add(1, config=cfg, cmdline=cmdline)

    cql = manager.get_cql()

    min_tablet_count = 8

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1 } AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int, c int, v int, PRIMARY KEY(pk, c)) WITH tablets={{'min_tablet_count':{min_tablet_count}}};")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv1 AS SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL PRIMARY KEY (pk, c)")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv2 AS SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL PRIMARY KEY (pk, v, c)")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv3 AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk)")

        base_id = await manager.get_table_id(ks, 'test')
        tv1_id = await manager.get_view_id(ks, 'tv1')
        tv2_id = await manager.get_view_id(ks, 'tv2')
        tv3_id = await manager.get_view_id(ks, 'tv3')

        # tv1 and tv2 are co-located with the base table because they have the same partition key
        assert base_id == (await get_base_table(manager, tv1_id))
        assert base_id == (await get_base_table(manager, tv2_id))

        # tv3 is not co-located with the base table because it has a different partition key
        assert tv3_id == (await get_base_table(manager, tv3_id))

        base_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        assert base_tablet_count == (await get_tablet_count(manager, servers[0], ks, 'tv1'))
        assert base_tablet_count == (await get_tablet_count(manager, servers[0], ks, 'tv2'))

        await cql.run_async(f"DROP MATERIALIZED VIEW {ks}.tv1")
        await cql.run_async(f"DROP MATERIALIZED VIEW {ks}.tv2")
        await cql.run_async(f"DROP MATERIALIZED VIEW {ks}.tv3")

# Create co-located base and view tables with a single tablet each in a 2 node
# cluster, and request to move the tablet of some table (either base or view,
# according to the parameter) to the other node. It should succeed and move
# both tablets to be co-located on the other node.  After they are moved we
# stop the other node, remaining only with the one node that should hold the
# base and view tablets, and verify we can read both tables from this node.
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
        # The base and view table should be co-located on one of the nodes with a single tablet each.
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c)")

        row_count = 100
        insert = cql.prepare(f"INSERT INTO {ks}.test(pk, c) VALUES(?, ?)")
        for pk in range(row_count):
            cql.execute(insert, [pk, pk+1])

        if move_table == 'base':
            table = 'test'
        else:
            table = 'tv'

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        # Request to move one of the tablets to the other node. it should move
        # both tablets because they must remain co-located.

        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, table, tablet_token)
        if replica[0] == s0_host_id:
            src_server = 0
            dst_host_id = s1_host_id
        else:
            src_server = 1
            dst_host_id = s0_host_id

        logger.info(f"Migrate the tablet to {dst_host_id}")
        await manager.api.move_tablet(servers[0].ip_addr, ks, table, replica[0], replica[1], dst_host_id, 0, tablet_token)
        logger.info("Migration done")

        new_replica = await get_tablet_replica(manager, servers[0], ks, table, tablet_token)
        assert new_replica[0] == dst_host_id

        # Now the dst node should hold both tablets. Stop the other node and
        # verify we can read from both tables.
        await manager.server_stop(servers[src_server].server_id)

        rows = await cql.run_async(f"SELECT * FROM {ks}.test")
        assert len(rows) == row_count

        rows = await cql.run_async(f"SELECT * FROM {ks}.tv")
        assert len(rows) == row_count

        # Start the server back because we need quorom when dropping the keyspace
        await manager.server_start(servers[src_server].server_id)

# Basic test of tablet split and merge of co-located tablets.
# Create co-located base and view tablets and populate the base table with enough data to trigger tablet splits.
# The view table has a filter so it's empty and wouldn't be a candidate for tablet split normally, but it should
# be split because it's co-located with the base table and their combined sizes are large enough.
# We verify that the tablets of both tables are split, and the tables have the same tablet count.
# Then delete some keys and verify the tablets of both tables are merged.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.parametrize(
    "with_merge",
    [
        pytest.param(False, id="no_merge"),
        pytest.param(True, id="with_merge", marks=pytest.mark.xfail(reason="issue #17265")),
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
        'tablet_load_stats_refresh_interval_in_seconds': 1
    }, cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=1 AND bloom_filter_fp_chance=1;")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL AND pk > 1000000 PRIMARY KEY (pk, c)")

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
        await asyncio.sleep(2) # Give load balancer some time to do work

        await s1_log.wait_for(f"Detected tablet split for table {ks}.test", from_mark=s1_mark, timeout=60)
        await s1_log.wait_for(f"Detected tablet split for table {ks}.tv", from_mark=s1_mark, timeout=60)

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
            await asyncio.sleep(2)
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

        await s1_log.wait_for("Emitting resize decision of type merge", from_mark=s1_mark, timeout=60)
        # Waits for balancer to co-locate sibling tablets
        await s1_log.wait_for("All sibling tablets are co-located", timeout=60)
        # Do some shuffling to make sure balancer works with co-located tablets
        await shuffle()

        old_tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        s1_mark = await s1_log.mark()

        await inject_error_on(manager, "replica_merge_completion_wait", servers)
        await disable_injection_on(manager, "tablet_merge_completion_bypass", servers)

        await s1_log.wait_for(f"Detected tablet merge for table {ks}.test", from_mark=s1_mark, timeout=60)
        await s1_log.wait_for(f"Detected tablet merge for table {ks}.tv", from_mark=s1_mark, timeout=60)

        async with no_tablet_balancing(manager, servers):
            tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
            assert tablet_count < old_tablet_count
            other_tablet_count = await get_tablet_count(manager, servers[0], ks, 'tv')
            assert tablet_count == other_tablet_count

            await check()

# Test creating a co-located table while the base table is migrating.  We
# create a table with a single tablet and start migrating it to the other node.
# While it's in some transition stage, we hold it and create a co-located view.
# We verify we can continue read and write to both tables. Then we complete the
# migration and verify everything continues to work as expected.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.parametrize("wait_stage", [("streaming", "stream_tablet_wait"), ("cleanup", "cleanup_tablet_wait")])
async def test_create_colocated_table_while_base_is_migrating(manager: ManagerClient, wait_stage):
    cfg = {'enable_tablets': True, 'tablet_load_stats_refresh_interval_in_seconds': 1 }
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'raft_topology=debug',
    ]
    servers = await manager.servers_add(2, config=cfg, cmdline=cmdline)
    await asyncio.gather(*[manager.api.disable_tablet_balancing(s.ip_addr) for s in servers])

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        total_keys = 100
        keys = range(total_keys)
        def populate(table, keys):
            insert = cql.prepare(f"INSERT INTO {ks}.{table}(pk, c) VALUES(?, ?)")
            for pk in keys:
                cql.execute(insert, [pk, pk+1])

        populate('test', keys)

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

        wait_injection = wait_stage[1]
        await inject_error_on(manager, wait_injection, servers)
        move_task = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, 'test', replica[0], replica[1], dst_host_id, 0, tablet_token))
        await wait_for_tablet_stage(manager, servers[0], ks, 'test', tablet_token, wait_stage[0])

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (pk, c) WITH synchronous_updates = true")
        await asyncio.sleep(2)

        # check we can write and read from the base table during the migration, and
        # that the view is also updated
        cql.execute(f"INSERT INTO {ks}.test(pk, c) VALUES(200, 201)")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk=200")
        assert len(rows) == 1 and rows[0].c == 201

        await cql.run_async(f"SELECT * FROM {ks}.tv WHERE pk=200 BYPASS CACHE")
        assert len(rows) == 1 and rows[0].c == 201

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        await disable_injection_on(manager, wait_injection, servers)
        await move_task

        base_id = await manager.get_table_id(ks, 'test')
        tv_id = await manager.get_view_id(ks, 'tv')

        new_replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        assert new_replica[0] == dst_host_id

        rows = await cql.run_async(f"SELECT * FROM {ks}.tv")
        assert len(rows) == total_keys+1

        cql.execute(f"INSERT INTO {ks}.test(pk, c) VALUES(300, 301)")
        rows = await cql.run_async(f"SELECT * FROM {ks}.tv WHERE pk=300 BYPASS CACHE")
        assert len(rows) == 1 and rows[0].c == 301

        src_host_id, dst_host_id = dst_host_id, replica[0]
        await manager.api.move_tablet(servers[0].ip_addr, ks, 'test', src_host_id, 0, dst_host_id, 0, tablet_token)

        cql.execute(f"INSERT INTO {ks}.test(pk, c) VALUES(400, 401)")
        rows = await cql.run_async(f"SELECT * FROM {ks}.tv WHERE pk=400 BYPASS CACHE")
        assert len(rows) == 1 and rows[0].c == 401

# Test basic tablet repair of a co-located base and view table.
# 1. start 2 nodes
# 1. Create a base table and a co-located view table with a single tablet and RF=2 (replica on each node)
# 2. write data to the base table while one node is down
# 3. bring the node back up - it is now missing some data
# 4. run tablet repair on the base table
# 5. verify both the base table and the view contain the missing data on the node that was down
@pytest.mark.asyncio
async def test_repair_colocated_base_and_view(manager: ManagerClient):
    cfg = {'enable_tablets': True}
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'repair=debug',
    ]
    servers = await manager.servers_add(2, config=cfg, cmdline=cmdline, auto_rack_dc="dc1")
    await asyncio.gather(*[manager.api.disable_tablet_balancing(s.ip_addr) for s in servers])

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.tv AS SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL PRIMARY KEY (pk, c)")

        cql.execute(SimpleStatement(f"INSERT INTO {ks}.test(pk, c) VALUES(1, 10)", consistency_level=ConsistencyLevel.ONE))
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)
        await manager.api.flush_keyspace(servers[1].ip_addr, ks)

        # Stop node 2 and write data while it is down
        await manager.server_stop(servers[1].server_id)

        cql.execute(SimpleStatement(f"INSERT INTO {ks}.test(pk, c) VALUES(2, 20)", consistency_level=ConsistencyLevel.ONE))
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        # Start node 2 back up
        await manager.server_start(servers[1].server_id)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        await manager.servers_see_each_other(servers)

        # At this point, node 2 is missing pk=2
        # Verify that pk=2 is missing on node 2 before repair
        # Connect directly to server 2 to check its data
        cql_server2 = await manager.get_cql_exclusive(servers[1])
        rows = await cql_server2.run_async(SimpleStatement(f"SELECT * FROM {ks}.test", consistency_level=ConsistencyLevel.ONE))
        pks = set(row.pk for row in rows)
        assert 1 in pks and 2 not in pks

        # Trigger repair of the single tablet
        tablet_token = 0
        await manager.api.tablet_repair(servers[0].ip_addr, ks, 'test', tablet_token)

        # Verify the view is repaired on server 2
        rows = await cql_server2.run_async(SimpleStatement(f"SELECT * FROM {ks}.tv", consistency_level=ConsistencyLevel.ONE))
        pks = set(row.pk for row in rows)
        assert 1 in pks and 2 in pks
