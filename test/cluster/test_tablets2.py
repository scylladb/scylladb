#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.internal_types import HostID, ServerInfo, ServerNum
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot, HTTPError, read_barrier
from test.pylib.util import wait_for_cql_and_get_hosts, unique_name
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas
from test.cluster.conftest import skip_mode
from test.cluster.util import reconnect_driver, create_new_test_keyspace, new_test_keyspace

import pytest
import asyncio
import logging
import time
import random
import os
import glob
from collections import defaultdict
from collections.abc import Iterable
from contextlib import asynccontextmanager
import itertools

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

async def load_repair_history(cql, hosts):
    all_rows = []
    for host in hosts:
        logging.info(f'Query hosts={host}');
        rows = await cql.run_async("SELECT * from system.repair_history", host=host)
        all_rows += rows
    for row in all_rows:
        logging.info(f"Got repair_history_entry={row}")
    return all_rows

async def safe_server_stop_gracefully(manager, server_id, timeout: float = 60, reconnect: bool = False):
    # Explicitly close the driver to avoid reconnections if scylla fails to update gossiper state on shutdown.
    # It's a problem until https://github.com/scylladb/scylladb/issues/15356 is fixed.
    manager.driver_close()
    await manager.server_stop_gracefully(server_id, timeout)
    cql = None
    if reconnect:
        cql = await reconnect_driver(manager)
    return cql

async def safe_rolling_restart(manager, servers, with_down):
    # https://github.com/scylladb/python-driver/issues/230 is not fixed yet, so for sake of CI stability,
    # driver must be reconnected after rolling restart of servers.
    await manager.rolling_restart(servers, with_down)
    cql = await reconnect_driver(manager)
    return cql

@pytest.mark.asyncio
async def test_tablet_metadata_propagates_with_schema_changes_in_snapshot_mode(manager: ManagerClient):
    """Test that you can create a table and insert and query data"""

    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_proxy=trace',
        '--logger-log-level', 'cql_server=trace',
        '--logger-log-level', 'query_processor=trace',
        '--logger-log-level', 'gossip=trace',
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'raft_topology=trace',
        '--logger-log-level', 'messaging_service=trace',
        '--logger-log-level', 'rpc=trace',
        ]
    servers = await manager.servers_add(3, cmdline=cmdline)

    s0 = servers[0].server_id
    not_s0 = servers[1:]

    # s0 should miss schema and tablet changes
    cql = await safe_server_stop_gracefully(manager, s0, reconnect=True)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 100}") as ks:
        # force s0 to catch up later from the snapshot and not the raft log
        await inject_error_one_shot_on(manager, 'raft_server_force_snapshot', not_s0)
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        keys = range(10)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, 1);") for k in keys])

        rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
        assert len(list(rows)) == len(keys)
        for r in rows:
            assert r.c == 1

        manager.driver_close()
        await manager.server_start(s0, wait_others=2)
        await manager.driver_connect(server=servers[0])
        cql = manager.get_cql()
        await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)

        # Trigger a schema change to invoke schema agreement waiting to make sure that s0 has the latest schema
        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as test_dummy:
            await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, 2);", execution_profile='whitelist')
                                for k in keys])

            rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
            assert len(rows) == len(keys)
            for r in rows:
                assert r.c == 2

            conn_logger = logging.getLogger("conn_messages")
            conn_logger.setLevel(logging.DEBUG)
            try:
                # Check that after rolling restart the tablet metadata is still there
                await manager.rolling_restart(servers)

                cql = await reconnect_driver(manager)

                await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)

                await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, 3);", execution_profile='whitelist')
                                    for k in keys])

                rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
                assert len(rows) == len(keys)
                for r in rows:
                    assert r.c == 3
            finally:
                conn_logger.setLevel(logging.INFO)


@pytest.mark.asyncio
async def test_scans(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        keys = range(100)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        rows = await cql.run_async(f"SELECT count(*) FROM {ks}.test;")
        assert rows[0].count == len(keys)

        rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk


@pytest.mark.asyncio
async def test_table_drop_with_auto_snapshot(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = { 'auto_snapshot': True }
    servers = await manager.servers_add(3, config = cfg)

    cql = manager.get_cql()

    # Increases the chance of tablet migration concurrent with schema change
    await inject_error_on(manager, "tablet_allocator_shuffle", servers)

    for i in range(3):
        await cql.run_async("DROP KEYSPACE IF EXISTS test;")
        await cql.run_async("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8 };")
        await cql.run_async("CREATE TABLE IF NOT EXISTS test.tbl_sample_kv (id int, value text, PRIMARY KEY (id));")
        await cql.run_async("INSERT INTO test.tbl_sample_kv (id, value) VALUES (1, 'ala');")

    await cql.run_async("DROP KEYSPACE test;")


@pytest.mark.asyncio
async def test_topology_changes(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 32}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Populating table")

        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])
        expected_rows = await cql.run_async(f"SELECT * FROM {ks}.test;")

        async def check():
            logger.info("Checking table")
            rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
            assert rows == expected_rows
            assert len(rows) == len(keys)
            for r in rows:
                assert r.c == r.pk

        await inject_error_on(manager, "tablet_allocator_shuffle", servers)

        logger.info("Adding new server")
        await manager.server_add()

        await check()

        logger.info("Adding new server")
        await manager.server_add()

        await check()
        time.sleep(5) # Give load balancer some time to do work
        await check()

        await manager.decommission_node(servers[0].server_id)

        await check()

async def get_two_servers_to_move_tablet(manager: ManagerClient):
    """
    The first server in servers list is source node to move the tablet from. The second server is the dest node.
    """
    logger.info("Bootstrapping cluster")
    cmdline = ['--enable-file-stream', 'false']
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

    servers.append(await manager.server_add(cmdline=cmdline))

    key = 7 # Whatever
    tablet_token = 0 # Doesn't matter since there is one tablet
    await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, 0)")
    rows = await cql.run_async(f"SELECT pk from {ks}.test")
    assert len(list(rows)) == 1

    replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
    logger.info(f'{replica=}')

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    s1_host_id = await manager.get_host_id(servers[1].server_id)

    # Make sure servers[0] is the source node to move the tablet from
    if replica[0] != s0_host_id:
        servers.reverse()
        s0_host_id, s1_host_id = s1_host_id, s0_host_id

    dst_shard = 0

    return (servers, cql, s0_host_id, s1_host_id, replica, tablet_token, dst_shard, ks)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_streaming_rx_error_no_failed_message_with_fail_stream_plan(manager: ManagerClient):
    servers, cql, s0_host_id, s1_host_id, replica, tablet_token, dst_shard, ks = await get_two_servers_to_move_tablet(manager)

    await manager.api.enable_injection(servers[0].ip_addr, "stream_session_ignore_failed_message", one_shot=True)
    await manager.api.enable_injection(servers[1].ip_addr, "stream_session_ignore_failed_message", one_shot=True)
    await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments_rx_error", one_shot=True)

    s1_log = await manager.server_open_log(servers[1].server_id)
    s1_mark = await s1_log.mark()

    migration_task = asyncio.create_task(
        manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token, timeout=30))

    await s1_log.wait_for('stream_manager: Failed stream_session for stream_plan', from_mark=s1_mark)
    s1_mark = await s1_log.mark()

    await manager.api.disable_injection(servers[1].ip_addr, "stream_mutation_fragments_rx_error")

    logger.info("Waiting for migration to finish")
    await migration_task
    logger.info("Migration done")

    # Sanity test
    rows = await cql.run_async(f"SELECT pk from {ks}.test")
    assert len(list(rows)) == 1

    await cql.run_async(f"TRUNCATE {ks}.test")
    rows = await cql.run_async(f"SELECT pk from {ks}.test")
    assert len(list(rows)) == 0

    # Verify that there is no data resurrection
    rows = await cql.run_async(f"SELECT pk from {ks}.test")
    assert len(list(rows)) == 0

    # Verify that moving the tablet back works
    await manager.api.move_tablet(servers[0].ip_addr, ks, "test", s1_host_id, dst_shard, replica[0], replica[1], tablet_token)
    rows = await cql.run_async(f"SELECT pk from {ks}.test")
    assert len(list(rows)) == 0

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_streaming_rx_error_no_failed_message_no_fail_stream_plan_hang(manager: ManagerClient):
    servers, cql, s0_host_id, s1_host_id, replica, tablet_token, dst_shard, ks = await get_two_servers_to_move_tablet(manager)

    await manager.api.enable_injection(servers[0].ip_addr, "stream_session_ignore_failed_message", one_shot=True)
    await manager.api.enable_injection(servers[1].ip_addr, "stream_session_ignore_failed_message", one_shot=True)
    await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments_rx_error", one_shot=True)

    await manager.api.enable_injection(servers[0].ip_addr, "stream_mutation_fragments_skip_fail_stream_plan", one_shot=True)
    await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments_skip_fail_stream_plan", one_shot=True)

    s1_log = await manager.server_open_log(servers[1].server_id)
    s1_mark = await s1_log.mark()

    migration_task = asyncio.create_task(
        manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token, timeout=10))

    try:
        logger.info("Waiting for migration to finish")
        await migration_task
        assert False # The move tablet is not supposed to finish
    except TimeoutError:
        logger.info("Migration timeout as expected")

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_streaming_is_guarded_by_topology_guard(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'raft_topology=trace',
        '--enable-file-stream', 'false',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        servers.append(await manager.server_add(cmdline=cmdline))

        key = 7 # Whatever
        tablet_token = 0 # Doesn't matter since there is one tablet
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, 0)")
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == 1

        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)
        dst_shard = 0

        await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments", one_shot=True)
        s1_log = await manager.server_open_log(servers[1].server_id)
        s1_mark = await s1_log.mark()

        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token))

        # Wait for the replica-side writer of streaming to reach a place where it already
        # received writes from the leaving replica but haven't applied them yet.
        # Once the writer reaches this place, it will wait for the message_injection() call below before proceeding.
        # The place we block the writer in should not hold to erm or topology_guard because that will block the migration
        # below and prevent test from proceeding.
        await s1_log.wait_for('stream_mutation_fragments: waiting', from_mark=s1_mark)
        s1_mark = await s1_log.mark()

        # Should cause streaming to fail and be retried while leaving behind the replica-side writer.
        await manager.api.inject_disconnect(servers[0].ip_addr, servers[1].ip_addr)

        logger.info("Waiting for migration to finish")
        await migration_task
        logger.info("Migration done")

        # Sanity test
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == 1

        await cql.run_async(f"TRUNCATE {ks}.test")
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == 0

        # Release abandoned streaming
        await manager.api.message_injection(servers[1].ip_addr, "stream_mutation_fragments")
        await s1_log.wait_for('stream_mutation_fragments: done', from_mark=s1_mark)

        # Verify that there is no data resurrection
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == 0

        # Verify that moving the tablet back works
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", s1_host_id, dst_shard, replica[0], replica[1], tablet_token)
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == 0


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_table_dropped_during_streaming(manager: ManagerClient):
    """
    Verifies that load balancing recovers when table is dropped during streaming phase of tablet migration.
    Recovering means that state machine is not stuck and later migrations can proceed.
    """

    logger.info("Bootstrapping cluster")
    servers = [await manager.server_add()]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE TABLE {ks}.test2 (pk int PRIMARY KEY, c int);")

        servers.append(await manager.server_add())

        logger.info("Populating tables")
        key = 7 # Whatever
        value = 3 # Whatever
        tablet_token = 0 # Doesn't matter since there is one tablet
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, {value})")
        await cql.run_async(f"INSERT INTO {ks}.test2 (pk, c) VALUES ({key}, {value})")
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(list(rows)) == 1
        rows = await cql.run_async(f"SELECT pk from {ks}.test2")
        assert len(list(rows)) == 1

        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)

        await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments", one_shot=True)
        s1_log = await manager.server_open_log(servers[1].server_id)
        s1_mark = await s1_log.mark()

        logger.info("Starting tablet migration")
        s1_host_id = await manager.get_host_id(servers[1].server_id)
        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, 0, tablet_token))

        # Wait for the replica-side writer of streaming to reach a place where it already
        # received writes from the leaving replica but haven't applied them yet.
        # Once the writer reaches this place, it will wait for the message_injection() call below before proceeding.
        # We want to drop the table while streaming is deep in the process, where it will attempt to apply writes
        # to the dropped table.
        await s1_log.wait_for('stream_mutation_fragments: waiting', from_mark=s1_mark)

        # Streaming blocks table drop, so we can't wait here.
        drop_task = cql.run_async(f"DROP TABLE {ks}.test")

        # Release streaming as late as possible to increase probability of drop causing problems.
        await s1_log.wait_for('Dropping', from_mark=s1_mark)

        # Unblock streaming
        await manager.api.message_injection(servers[1].ip_addr, "stream_mutation_fragments")
        await drop_task

        logger.info("Waiting for migration to finish")
        try:
            await migration_task
        except HTTPError as e:
            assert 'Tablet map not found' in e.message

        logger.info("Verifying that moving the other tablet works")
        replica = await get_tablet_replica(manager, servers[0], ks, 'test2', tablet_token)
        s0_host_id = await manager.get_host_id(servers[0].server_id)
        assert replica[0] == s0_host_id
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test2", replica[0], replica[1], s1_host_id, 0, tablet_token)

        logger.info("Verifying tablet replica")
        replica = await get_tablet_replica(manager, servers[0], ks, 'test2', tablet_token)
        assert replica == (s1_host_id, 0)

@pytest.mark.repair
@pytest.mark.asyncio
async def test_tablet_repair(manager: ManagerClient):
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

        logger.info("Populating table")

        keys = range(256)

        stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
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
        stmt = cql.prepare(f"SELECT * FROM {ks}.test;")
        stmt.consistency_level = ConsistencyLevel.ALL
        rows = await cql.run_async(stmt)
        assert len(rows) == key_count
        for r in rows:
            assert r.c == repair_cycles - 1

# Reproducer for race between split and repair: https://github.com/scylladb/scylladb/issues/19378
# Verifies repair will not complete with sstables that still require split, causing split
# execution to fail.
@pytest.mark.repair
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_concurrent_tablet_repair_and_split(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'raft_topology=debug',
        '--target-tablet-size-in-bytes', '1024',
    ]
    servers = await manager.servers_add(3, cmdline=cmdline, config={
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
    })

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                        "'replication_factor': 2} AND tablets = {'initial': 32}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Populating table")

        keys = range(5000) # Enough keys to trigger repair digest mismatch with a high chance.
        stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
        stmt.consistency_level = ConsistencyLevel.ONE

        await inject_error_on(manager, "tablet_load_stats_refresh_before_rebalancing", servers)

        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()

        await asyncio.gather(*[cql.run_async(stmt, [k, -1]) for k in keys])

        # split decision is sstable size based, so data must be flushed first
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)

        await manager.api.enable_injection(servers[0].ip_addr, "tablet_split_finalization_postpone", False)
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        logger.info("Waiting for split prepare...")
        await s0_log.wait_for('Setting split ready sequence number to', from_mark=s0_mark)
        s0_mark = await s0_log.mark()
        logger.info("Waited for split prepare")

        # Balancer is re-enabled later for split execution
        await asyncio.create_task(manager.api.disable_tablet_balancing(servers[0].ip_addr))

        # Write concurrently with repair to increase the chance of repair having some discrepancy to resolve and send writes.
        inserts_future = asyncio.gather(*[cql.run_async(stmt, [k, 1]) for k in keys])

        await repair_on_node(manager, servers[0], servers, ks)

        await inserts_future

        logger.info("Waiting for split execute...")
        await manager.api.disable_injection(servers[0].ip_addr, "tablet_split_finalization_postpone")
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)
        await s0_log.wait_for('Detected tablet split for table', from_mark=s0_mark)
        await inject_error_one_shot_on(manager, "tablet_split_finalization_postpone", servers)
        logger.info("Waited for split execute...")

        key_count = len(keys)
        stmt = cql.prepare(f"SELECT * FROM {ks}.test;")
        stmt.consistency_level = ConsistencyLevel.ALL
        rows = await cql.run_async(stmt)
        assert len(rows) == key_count

@pytest.mark.repair
@pytest.mark.asyncio
async def test_tablet_missing_data_repair(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--hinted-handoff-enabled', 'false',
        ]
    servers = [await manager.server_add(cmdline=cmdline),
               await manager.server_add(cmdline=cmdline),
               await manager.server_add(cmdline=cmdline)]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': 3} AND tablets = {'initial': 32}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        keys_list = [range(0, 100), range(100, 200), range(200, 300)]
        keys_for_server = dict([(s.server_id, keys_list[idx]) for idx, s in enumerate(servers)])
        keys = range(0, 300)

        async def insert_with_down(down_server):
            logger.info(f"Stopped server {down_server.server_id}")
            logger.info(f"Insert into server {down_server.server_id}")
            await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});")
                                for k in keys_for_server[down_server.server_id]])

        cql = await safe_rolling_restart(manager, servers, with_down=insert_with_down)

        await repair_on_node(manager, servers[0], servers, ks)

        async def check_with_down(down_node):
            logger.info("Checking table")
            query = SimpleStatement(f"SELECT * FROM {ks}.test;", consistency_level=ConsistencyLevel.ONE)
            rows = await cql.run_async(query)
            assert len(rows) == len(keys)
            for r in rows:
                assert r.c == r.pk

        cql = await safe_rolling_restart(manager, servers, with_down=check_with_down)


@pytest.mark.repair
@pytest.mark.asyncio
async def test_tablet_repair_history(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = [await manager.server_add(), await manager.server_add(), await manager.server_add()]

    rf = 3
    tablets = 8

    cql = manager.get_cql()
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} AND tablets = {{'initial': {tablets}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH tombstone_gc = {{'mode':'repair'}};")

        logger.info("Populating table")

        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        logging.info(f'Got hosts={hosts}');

        await repair_on_node(manager, servers[0], servers, ks)

        all_rows = await load_repair_history(cql, hosts)
        assert len(all_rows) == rf * tablets

@pytest.mark.repair
@pytest.mark.asyncio
async def test_tablet_repair_ranges_selection(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = [await manager.server_add(), await manager.server_add()]

    rf = 2
    tablets = 4
    nr_ranges = 0;

    cql = manager.get_cql()
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} AND tablets = {{'initial': {tablets}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH tombstone_gc = {{'mode':'repair'}};")

        logger.info("Populating table")

        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        logging.info(f'Got hosts={hosts}');

        await repair_on_node(manager, servers[0], servers, ks, ranges='-4611686018427387905:-1,4611686018427387903:9223372036854775807')
        nr_ranges = nr_ranges + 2
        await repair_on_node(manager, servers[0], servers, ks, ranges='-2000:-1000,1000:2000')
        nr_ranges = nr_ranges + 2
        await repair_on_node(manager, servers[0], servers, ks, ranges='3000:-3000')
        # The wrap around range (3000, -3000] will produce the following intersection range
        # range=(minimum token,-4611686018427387905] ranges_specified={(3000,+inf), (-inf, -3000]} intersection_ranges=(minimum token,-4611686018427387905]
        # range=(-4611686018427387905,-1] ranges_specified={(3000,+inf), (-inf, -3000]} intersection_ranges=(-4611686018427387905,-3000]
        # range=(-1,4611686018427387903] ranges_specified={(3000,+inf), (-inf, -3000]} intersection_ranges=(3000,4611686018427387903]
        # range=(4611686018427387903,9223372036854775807] ranges_specified={(3000,+inf), (-inf, -3000]} intersection_ranges=(4611686018427387903,9223372036854775807]
        nr_ranges = nr_ranges + 4

        all_rows = await load_repair_history(cql, hosts)
        assert len(all_rows) == rf * nr_ranges;

@pytest.mark.asyncio
async def test_tablet_cleanup(manager: ManagerClient):
    cmdline = ['--smp=2', '--commitlog-sync=batch']

    logger.info("Start first node")
    servers = [await manager.server_add(cmdline=cmdline)]
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info("Populate table")
    cql = manager.get_cql()
    n_tablets = 32
    n_partitions = 1000
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': {n_tablets}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk) VALUES ({k});") for k in range(1000)])

        logger.info("Start second node")
        servers.append(await manager.server_add())

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        logger.info("Read system.tablets")
        tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        assert len(tablet_replicas) == n_tablets

        # Randomly select half of all tablets.
        sample = random.sample(tablet_replicas, n_tablets // 2)
        moved_tokens = [x.last_token for x in sample]
        moved_src = [x.replicas[0] for x in sample]
        moved_dst = [(s1_host_id, random.choice([0, 1])) for _ in sample]

        # Migrate the selected tablets to second node.
        logger.info("Migrate half of all tablets to second node")
        for t, s, d in zip(moved_tokens, moved_src, moved_dst):
            await manager.api.move_tablet(servers[0].ip_addr, ks, "test", *s, *d, t)

        # Sanity check. All data we inserted should be still there.
        assert n_partitions == (await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test"))[0].count

        # Wipe data on second node.
        logger.info("Wipe data on second node")
        await manager.server_stop_gracefully(servers[1].server_id, timeout=120)
        await manager.server_wipe_sstables(servers[1].server_id, ks, "test")
        await manager.server_start(servers[1].server_id)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        await manager.servers_see_each_other(servers)
        partitions_after_loss = (await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test"))[0].count
        assert partitions_after_loss < n_partitions

        # Migrate all tablets back to their original position.
        # Check that this doesn't resurrect cleaned data.
        logger.info("Migrate the migrated tablets back")
        for t, s, d in zip(moved_tokens, moved_dst, moved_src):
            await manager.api.move_tablet(servers[0].ip_addr, ks, "test", *s, *d, t)
        assert partitions_after_loss == (await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test"))[0].count

        # Kill and restart first node.
        # Check that this doesn't resurrect cleaned data.
        logger.info("Brutally restart first node")
        await manager.server_stop(servers[0].server_id)
        await manager.server_start(servers[0].server_id)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        await manager.servers_see_each_other(servers)
        assert partitions_after_loss == (await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test"))[0].count

        # Bonus: check that commitlog_cleanups doesn't have any garbage after restart.
        assert 0 == (await cql.run_async("SELECT COUNT(*) FROM system.commitlog_cleanups", host=hosts[0]))[0].count

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_cleanup_failure(manager: ManagerClient):
    cmdline = ['--smp=1']

    servers = [await manager.server_add(cmdline=cmdline)]

    cql = manager.get_cql()
    n_tablets = 1
    n_partitions = 1000
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': {n_tablets}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk) VALUES ({k});") for k in range(n_partitions)])

        await inject_error_one_shot_on(manager, "tablet_cleanup_failure", servers)

        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()

        servers.append(await manager.server_add())

        tablet_token = 0
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        s1_host_id = await manager.get_host_id(servers[1].server_id)
        dst_shard = 0
        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token))

        logger.info("Waiting for injected cleanup failure...")
        await s0_log.wait_for('Cleanup failed for tablet', from_mark=s0_mark)

        logger.info("Waiting for cleanup success on retry...")
        await s0_log.wait_for(f'Cleaned up tablet .* of table {ks}.test successfully.', from_mark=s0_mark)

        logger.info("Waiting for cleanup success on retry...")
        await s0_log.wait_for('updating topology state: Finished tablet migration', from_mark=s0_mark)

        logger.info("Waiting for migration task...")
        await migration_task

        assert n_partitions == (await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test"))[0].count

        node_workdir = await manager.server_get_workdir(servers[0].server_id)
        table_dir = glob.glob(os.path.join(node_workdir, "data", ks, "test-*"))[0]
        logger.info(f"Table dir: {table_dir}")
        ssts = glob.glob(os.path.join(table_dir, "*-Data.db"))
        logger.info("Guarantee source node of migration left no sstables undeleted")
        assert len(ssts) == 0

@pytest.mark.asyncio
async def test_tablet_resharding(manager: ManagerClient):
    cmdline = ['--smp=3']
    config = {'enable_tablets': True}
    servers = await manager.servers_add(1, cmdline=cmdline)
    server = servers[0]

    logger.info("Populate table")
    cql = manager.get_cql()
    n_tablets = 32
    n_partitions = 1000
    ks = await create_new_test_keyspace(cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': {n_tablets}}}")
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY);")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk) VALUES ({k});") for k in range(n_partitions)])

    await manager.server_stop_gracefully(server.server_id, timeout=120)
    await manager.server_update_cmdline(server.server_id, ['--smp=2'])

    await manager.server_start(
        server.server_id,
        expected_error="Detected a tablet with invalid replica shard, reducing shard count with tablet-enabled tables is not yet supported. Replace the node instead.")

async def get_tablet_count(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str):
    host = manager.cql.cluster.metadata.get_host(server.ip_addr)

    # read_barrier is needed to ensure that local tablet metadata on the queried node
    # reflects the finalized tablet movement.
    await read_barrier(manager.api, server.ip_addr)

    table_id = await manager.get_table_id(keyspace_name, table_name)
    rows = await manager.cql.run_async(f"SELECT tablet_count FROM system.tablets where "
                                       f"table_id = {table_id}", host=host)
    return rows[0].tablet_count

@pytest.mark.parametrize("injection_error", ["foreach_compaction_group_wait", "major_compaction_wait"])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_split(manager: ManagerClient, injection_error: str):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'table=debug',
        '--target-tablet-size-in-bytes', '1024',
    ]
    servers = [await manager.server_add(config={
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
    }, cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # enough to trigger multiple splits with max size of 1024 bytes.
        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        async def check():
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
            assert len(rows) == len(keys)
            for r in rows:
                assert r.c == r.pk

        await check()

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        assert tablet_count == 1

        logger.info("Adding new server")
        servers.append(await manager.server_add(cmdline=cmdline))

        # Increases the chance of tablet migration concurrent with split
        await inject_error_one_shot_on(manager, "tablet_allocator_shuffle", servers)
        await inject_error_on(manager, "tablet_load_stats_refresh_before_rebalancing", servers)

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        await manager.api.enable_injection(servers[0].ip_addr, injection_error, one_shot=True)
        compaction_task = asyncio.create_task(manager.api.keyspace_compaction(servers[0].ip_addr, ks))
        await s1_log.wait_for(f"{injection_error}: waiting", from_mark=s1_mark)

        # Now there's a split and migration need, so they'll potentially run concurrently.
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        await check()
        time.sleep(5) # Give load balancer some time to do work

        await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark)

        await check()

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        assert tablet_count > 1

        await manager.api.message_injection(servers[0].ip_addr, injection_error)
        await s1_log.wait_for(f"{injection_error}: released", from_mark=s1_mark)
        await compaction_task

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_correctness_of_tablet_split_finalization_after_restart(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'table=debug',
        '--target-tablet-size-in-bytes', '1024',
    ]
    servers = [await manager.server_add(config={
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval'],
    }, cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    servers.append(await manager.server_add(config={
        'error_injections_at_startup': ['delay_split_compaction']
    }, cmdline=cmdline))

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH compaction = {{'class': 'NullCompactionStrategy'}};")

        # enough to trigger multiple splits with max size of 1024 bytes.
        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        async def check():
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
            assert len(rows) == len(keys)
            for r in rows:
                assert r.c == r.pk

        await check()

        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        assert tablet_count == 2

        await manager.api.enable_injection(servers[0].ip_addr, "tablet_load_stats_refresh_before_rebalancing", one_shot=False)

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        await manager.api.enable_injection(servers[0].ip_addr, "tablet_split_finalization_postpone", one_shot=False)
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        await s1_log.wait_for('Finalizing resize decision for table', from_mark=s1_mark)

        # Delays refresh of tablet stats, so balancer works with whichever it got last.
        await manager.api.disable_injection(servers[0].ip_addr, "tablet_load_stats_refresh_before_rebalancing")
        await manager.api.disable_injection(servers[0].ip_addr, "short_tablet_stats_refresh_interval")
        time.sleep(1)
        await manager.api.disable_tablet_balancing(servers[0].ip_addr)

        await manager.server_stop_gracefully(servers[1].server_id, timeout=120)
        await manager.server_start(servers[1].server_id)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        await manager.servers_see_each_other(servers)

        await manager.api.disable_injection(servers[0].ip_addr, "tablet_split_finalization_postpone")
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark)

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        assert tablet_count > 2

        await check()

@pytest.mark.parametrize("injection_error", ["foreach_compaction_group_wait", "major_compaction_wait"])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_concurrent_tablet_migration_and_major(manager: ManagerClient, injection_error):
    logger.info("Bootstrapping cluster")
    cmdline = []
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        async def check():
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
            assert len(rows) == len(keys)
            for r in rows:
                assert r.c == r.pk

        await check()

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        logger.info("Adding new server")
        servers.append(await manager.server_add(cmdline=cmdline))
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        await manager.api.enable_injection(servers[0].ip_addr, injection_error, one_shot=True)
        logger.info("Started major compaction")
        compaction_task = asyncio.create_task(manager.api.keyspace_compaction(servers[0].ip_addr, ks))
        await s1_log.wait_for(f"{injection_error}: waiting", from_mark=s1_mark)

        tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')

        t = tablet_replicas[0]
        logger.info("Migrating tablet")
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", *t.replicas[0], *(s1_host_id, 0), t.last_token)

        await manager.api.message_injection(servers[0].ip_addr, injection_error)
        await s1_log.wait_for(f"{injection_error}: released", from_mark=s1_mark)
        await compaction_task

        if injection_error == "major_compaction_wait":
            logger.info("Check that major was successfully aborted on migration")
            await s1_log.wait_for(f"Compaction for {ks}/test was stopped due to: tablet cleanup", from_mark=s1_mark)

        await check()

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_concurrent_table_drop_and_major(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    injection_error = "major_compaction_wait"
    cmdline = ['--logger-log-level', 'compaction_manager=debug',]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        await manager.api.enable_injection(servers[0].ip_addr, injection_error, one_shot=True)
        logger.info("Started major compaction")
        compaction_task = asyncio.create_task(manager.api.keyspace_compaction(servers[0].ip_addr, ks))
        await s1_log.wait_for(f"{injection_error}: waiting", from_mark=s1_mark)

        logger.info("Dropping table")
        await cql.run_async(f"DROP TABLE {ks}.test")

        await manager.api.message_injection(servers[0].ip_addr, injection_error)
        await s1_log.wait_for(f"{injection_error}: released", from_mark=s1_mark)
        await compaction_task

        if injection_error == "major_compaction_wait":
            logger.info("Check that major was successfully aborted on migration")
            await s1_log.wait_for(f"ongoing compactions for table {ks}.test .* due to table removal", from_mark=s1_mark)

async def assert_tablet_count_metric_value_for_shards(manager: ManagerClient, server: ServerInfo, expected_count_per_shard: list[int]):
    tablet_count_metric_name = "scylla_tablets_count"
    metrics = await manager.metrics.query(server.ip_addr)
    for shard_id in range(0, len(expected_count_per_shard)):
        expected_tablet_count = expected_count_per_shard[shard_id]
        tablet_count = metrics.get(name=tablet_count_metric_name, labels=None, shard=str(shard_id))
        assert int(tablet_count) == expected_tablet_count

async def get_tablet_tokens_from_host_on_shard(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str, shard: int) -> list[int]:
    host = await manager.get_host_id(server.server_id)
    table_tablets = await get_all_tablet_replicas(manager, server, keyspace_name, table_name)
    tokens = []
    for tablet_replica in table_tablets:
        for host_id, shard_id in tablet_replica.replicas:
            if host_id == host and shard_id == shard:
                tokens.append(tablet_replica.last_token)
    return tokens

async def get_tablet_count_per_shard_for_host(manager: ManagerClient, server: ServerInfo, full_tables: dict[str, list[str]], shards_count: int = 2) -> list[int]:
    dict_result = await get_tablet_count_per_shard_for_hosts(manager, [server], full_tables, shards_count)
    return dict_result[server.server_id]

async def get_tablet_count_per_shard_for_hosts(manager: ManagerClient, servers: Iterable[ServerInfo], full_tables: dict[str, list[str]], shards_per_node: int = 2) -> dict[ServerNum, list[int]]:
    result = dict[ServerNum, list[int]]()
    hosts = dict[HostID, ServerNum]()
    server1 = None

    for server in servers:
        if not server1:
            server1 = server
        result[server.server_id] = [0] * shards_per_node
        hosts[await manager.get_host_id(server.server_id)] = server.server_id

    for keyspace, tables in full_tables.items():
        for table in tables:
            table_tablets = await get_all_tablet_replicas(manager, server1, keyspace, table)
            for tablet_replica in table_tablets:
                for host_id, shard_id in tablet_replica.replicas:
                    if host_id in hosts:
                        result[hosts[host_id]][shard_id] += 1

    return result

def get_shard_that_has_tablets(tablet_count_per_shard: list[int]) -> int:
    for shard_id in range(0, len(tablet_count_per_shard)):
        if tablet_count_per_shard[shard_id] > 0:
            return shard_id
    return -1

@pytest.mark.asyncio
async def test_tablet_count_metric_per_shard(manager: ManagerClient):
    # Given two running servers
    shards_count = 4
    cmdline = ['--smp=4']
    servers = await manager.servers_add(2, cmdline=cmdline)

    # And given disabled load balancing
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    # When two tables are created
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.mytable1 (col1 timestamp, col2 text, col3 blob, PRIMARY KEY (col1));")
        await cql.run_async(f"CREATE TABLE {ks}.mytable2 (col1 timestamp, col2 text, col3 blob, PRIMARY KEY (col1));")

        # Then tablet count metric for each shard depicts the actual state
        tables = { ks: ["mytable1", "mytable2"] }
        expected_count_per_shard_for_host_0 = await get_tablet_count_per_shard_for_host(manager, servers[0], tables, shards_count)
        await assert_tablet_count_metric_value_for_shards(manager, servers[0], expected_count_per_shard_for_host_0)

        expected_count_per_shard_for_host_1 = await get_tablet_count_per_shard_for_host(manager, servers[1], tables, shards_count)
        await assert_tablet_count_metric_value_for_shards(manager, servers[1], expected_count_per_shard_for_host_1)

        # When third table is created
        await cql.run_async(f"CREATE TABLE {ks}.mytable3 (col1 timestamp, col2 text, col3 blob, PRIMARY KEY (col1));")

        # Then tablet count metric for each shard depicts the actual state
        tables = { ks: ["mytable1", "mytable2", "mytable3"] }
        expected_count_per_shard_for_host_0 = await get_tablet_count_per_shard_for_host(manager, servers[0], tables, shards_count)
        await assert_tablet_count_metric_value_for_shards(manager, servers[0], expected_count_per_shard_for_host_0)

        expected_count_per_shard_for_host_1 = await get_tablet_count_per_shard_for_host(manager, servers[1], tables, shards_count)
        await assert_tablet_count_metric_value_for_shards(manager, servers[1], expected_count_per_shard_for_host_1)

        # When one of tables is dropped
        await cql.run_async(f"DROP TABLE {ks}.mytable2;")

        # Then tablet count metric for each shard depicts the actual state
        tables = { ks: ["mytable1", "mytable3"] }
        expected_count_per_shard_for_host_0 = await get_tablet_count_per_shard_for_host(manager, servers[0], tables, shards_count)
        await assert_tablet_count_metric_value_for_shards(manager, servers[0], expected_count_per_shard_for_host_0)

        expected_count_per_shard_for_host_1 = await get_tablet_count_per_shard_for_host(manager, servers[1], tables, shards_count)
        await assert_tablet_count_metric_value_for_shards(manager, servers[1], expected_count_per_shard_for_host_1)

        # And when moving tablets from one shard of src_host to (dest_host, shard_3)
        shard_id_to_move = get_shard_that_has_tablets(expected_count_per_shard_for_host_0)
        if shard_id_to_move != -1:
            src_server = servers[0]
            dest_server = servers[1]
            src_expected_count_per_shard = expected_count_per_shard_for_host_0
            dest_expected_count_per_shard = expected_count_per_shard_for_host_1
        else:
            shard_id_to_move = get_shard_that_has_tablets(expected_count_per_shard_for_host_1)
            src_server = servers[1]
            dest_server = servers[0]
            src_expected_count_per_shard = expected_count_per_shard_for_host_1
            dest_expected_count_per_shard = expected_count_per_shard_for_host_0


        tokens_on_shard_to_move = {
            "mytable1" : await get_tablet_tokens_from_host_on_shard(manager, src_server, ks, "mytable1", shard_id_to_move),
            "mytable3" : await get_tablet_tokens_from_host_on_shard(manager, src_server, ks, "mytable3", shard_id_to_move)
        }

        count_of_tokens_on_src_shard_to_move = len(tokens_on_shard_to_move["mytable1"]) + len(tokens_on_shard_to_move["mytable3"])
        assert count_of_tokens_on_src_shard_to_move > 0

        src_host_id = await manager.get_host_id(src_server.server_id)
        dest_host_id = await manager.get_host_id(dest_server.server_id)
        for table_name, tokens in tokens_on_shard_to_move.items():
            for token in tokens:
                await manager.api.move_tablet(node_ip=src_server.ip_addr, ks=ks, table=table_name, src_host=src_host_id, src_shard=shard_id_to_move, dst_host=dest_host_id, dst_shard=3, token=token)

        # And when ensuring that local tablet metadata on the queried node reflects the finalized tablet movement
        await read_barrier(manager.api, servers[0].ip_addr)
        await read_barrier(manager.api, servers[1].ip_addr)

        # Then tablet count metric is adjusted to depict that situation on src_host - all tablets from selected shard have been moved
        src_expected_count_per_shard[shard_id_to_move] = 0
        await assert_tablet_count_metric_value_for_shards(manager, src_server, src_expected_count_per_shard)

        # And then tablet count metric is increased on dest_host - tablets have been moved to shard_3
        dest_expected_count_per_shard[3] += count_of_tokens_on_src_shard_to_move
        await assert_tablet_count_metric_value_for_shards(manager, dest_server, dest_expected_count_per_shard)

@pytest.mark.parametrize("primary_replica_only", [False, True])
async def test_tablet_load_and_stream(manager: ManagerClient, primary_replica_only):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'table=debug',
        '--smp', '1',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()

    async def create_table(tablet_count : int) -> str:
        # Creates multiple tablets in the same shard
        ks_name = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}" \
                            f" AND tablets = {{ 'initial': {tablet_count} }};")
        await cql.run_async(f"CREATE TABLE {ks_name}.test (pk int PRIMARY KEY, c int);")
        return ks_name

    ks = await create_table(5) # 5 is rounded up to next power-of-two

    # Populate tablets
    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

    async def check(ks_name: str):
        logger.info("Checking table")
        cql = manager.get_cql()
        rows = await cql.run_async(f"SELECT * FROM {ks_name}.test BYPASS CACHE;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await manager.api.flush_keyspace(servers[0].ip_addr, ks)
    await check(ks)

    node_workdir = await manager.server_get_workdir(servers[0].server_id)

    ks2 = await create_table(16)

    cql = await safe_server_stop_gracefully(manager, servers[0].server_id)

    table_dir = glob.glob(os.path.join(node_workdir, "data", ks, "test-*"))[0]
    logger.info(f"Table dir: {table_dir}")

    dst_table_dir = glob.glob(os.path.join(node_workdir, "data", ks2, "test-*"))[0]
    logger.info(f"Dst table dir: {dst_table_dir}")

    def move_sstables_to_upload(table_dir: str, dst_table_dir: str):
        logger.info("Moving sstables to upload dir of destination table")
        table_upload_dir = os.path.join(dst_table_dir, "upload")
        for sst in glob.glob(os.path.join(table_dir, "*-Data.db")):
            for src_path in glob.glob(os.path.join(table_dir, sst.removesuffix("-Data.db") + "*")):
                dst_path = os.path.join(table_upload_dir, os.path.basename(src_path))
                logger.info(f"Moving sstable file {src_path} to {dst_path}")
                os.rename(src_path, dst_path)

    move_sstables_to_upload(table_dir, dst_table_dir)

    await manager.server_start(servers[0].server_id)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    rows = await cql.run_async(f"SELECT * FROM {ks}.test BYPASS CACHE;")
    assert len(rows) == 0

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info("Adding new server")
    servers.append(await manager.server_add(cmdline=cmdline))

    # Trigger concurrent migration and load-and-stream

    await manager.api.enable_tablet_balancing(servers[0].ip_addr)

    await manager.api.load_new_sstables(servers[0].ip_addr, ks2, "test", primary_replica_only)

    time.sleep(1)

    await check(ks2)

    await asyncio.gather(*[cql.run_async(f"drop keyspace {i}") for i in [ks, ks2]])

@pytest.mark.asyncio
async def test_storage_service_api_uneven_ownership_keyspace_and_table_params_used(manager: ManagerClient):
    # Given two running servers
    shards_count = 4
    cmdline = ['--smp=4']
    servers = await manager.servers_add(2, cmdline=cmdline)

    # When table is created with initial tablets set to 1
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.mytable1 (col1 timestamp, col2 text, col3 blob, PRIMARY KEY (col1));")

        # And when ownership for this table is queried
        actual_ownerships = await manager.api.get_ownership(servers[0].ip_addr, ks, "mytable1")

        # Then ensure that returned ownerships is 0.0 and 1.0 (which node gets 0.0 and 1.0 is unspecified)
        expected_ips = {servers[0].ip_addr, servers[1].ip_addr}
        expected_ownerships = [0.0, 1.0]
        delta = 0.0001
        already_verified = set()

        sorted_actual_ownerships = sorted(actual_ownerships, key=lambda e: e["value"])
        assert len(sorted_actual_ownerships) == len(expected_ownerships)

        for i in range(0, len(sorted_actual_ownerships)):
            entry = sorted_actual_ownerships[i]
            actual_ip = entry["key"]
            actual_ownership = float(entry["value"])

            assert actual_ip in expected_ips
            assert actual_ip not in already_verified
            assert actual_ownership == pytest.approx(expected_ownerships[i], abs=delta)

            already_verified.add(actual_ip)

@pytest.mark.asyncio
async def test_tablet_storage_freeing(manager: ManagerClient):
    logger.info("Start first node")
    servers = [await manager.server_add()]
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logger.info("Create a table with two tablets and populate it with a moderate amount of data.")
    n_tablets = 2
    n_partitions = 1000
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': {n_tablets}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v text) WITH compression = {{'sstable_compression': ''}};")
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, v) VALUES (?, ?);")
        payload = "a"*10000

        max_concurrency = 100
        for batch in itertools.batched(range(n_partitions), max_concurrency):
            await asyncio.gather(*[cql.run_async(insert_stmt, [k, payload]) for k in batch])
        await manager.api.keyspace_flush(servers[0].ip_addr, ks)

        logger.info("Start second node.")
        servers.append(await manager.server_add())
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        logger.info("Check the table's disk usage on first node.")
        size_before = await manager.server_get_sstables_disk_usage(servers[0].server_id, ks, "test")
        assert size_before > n_partitions * len(payload)

        logger.info("Read system.tablets.")
        tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        assert len(tablet_replicas) == n_tablets

        logger.info("Migrate one of the two tablets from the first node to the second node.")
        t = tablet_replicas[0]
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", *t.replicas[0], *(s1_host_id, 0), t.last_token)

        logger.info("Verify that the table's disk usage on first node shrunk by about half.")
        size_after = await manager.server_get_sstables_disk_usage(servers[0].server_id, ks, "test")
        assert size_before * 0.33 < size_after < size_before * 0.66

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tombstone_gc_disabled_on_pending_replica(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = [await manager.server_add()]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH gc_grace_seconds = 0;")

        servers.append(await manager.server_add())

        key = 7 # Whatever
        tablet_token = 0 # Doesn't matter since there is one tablet
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, 1) USING timestamp 9")
        rows = await cql.run_async(f"SELECT pk from {ks}.test")
        assert len(rows) == 1

        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)
        dst_shard = 0

        await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments", one_shot=True)
        s1_log = await manager.server_open_log(servers[1].server_id)
        s1_mark = await s1_log.mark()

        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token))

        await s1_log.wait_for('stream_mutation_fragments: waiting', from_mark=s1_mark)
        s1_mark = await s1_log.mark()

        # write a tombstone with timestamp X to DB
        await cql.run_async(f'DELETE FROM {ks}.test USING timestamp 10 WHERE pk = {key}')

        # flush both servers
        for s in servers:
            await manager.api.flush_keyspace(s.ip_addr, ks)

        await asyncio.sleep(1)

        # major compact both servers
        for s in servers:
            await manager.api.keyspace_compaction(s.ip_addr, ks)

        # write backdated data to test.test with timestamp X-1 with the same key as the tombstone
        await cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({key}, 0) USING timestamp 9')

        # release streaming
        await manager.api.message_injection(servers[1].ip_addr, "stream_mutation_fragments")
        await s1_log.wait_for('stream_mutation_fragments: done', from_mark=s1_mark)

        logger.info("Waiting for migration to finish")
        await migration_task
        logger.info("Migration done")

        for s in servers:
            await manager.api.flush_keyspace(s.ip_addr, ks)

        # verify result
        rows = await cql.run_async(f'SELECT pk, c FROM {ks}.test WHERE pk = {key};')
        assert len(rows) == 0

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_schema_change_during_cleanup(manager: ManagerClient):
    logger.info("Start first node")
    servers = [await manager.server_add()]
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Populating table")

        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        logger.info("Start second node.")
        servers.append(await manager.server_add())
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        await inject_error_on(manager, "delay_tablet_compaction_groups_cleanup", servers)

        logger.info("Read system.tablets.")
        tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        assert len(tablet_replicas) == 1

        logger.info("Migrating one tablet to another node.")
        t = tablet_replicas[0]
        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", *t.replicas[0], *(s1_host_id, 0), t.last_token))

        logger.info("Waiting for log")
        await s1_log.wait_for('Initiating tablet cleanup of', from_mark=s1_mark, timeout=120)
        time.sleep(1)
        await cql.run_async(f"ALTER TABLE {ks}.test WITH gc_grace_seconds = 0;")
        await migration_task

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tombstone_gc_correctness_during_tablet_split(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'table=debug',
        '--target-tablet-size-in-bytes', '5000',
    ]
    servers = [await manager.server_add(config={
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
    }, cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH gc_grace_seconds=0;")

        await manager.api.disable_autocompaction(servers[0].ip_addr, ks)

        keys = range(100)

        logger.info("Generating sstable with shadowed data")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        logger.info("Generating another sstable with tombstones")
        await asyncio.gather(*[cql.run_async(f"DELETE FROM {ks}.test WHERE pk={k};") for k in keys])
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        async def assert_empty_table():
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {ks}.test BYPASS CACHE;")
            assert len(rows) == 0

        await assert_empty_table()

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        assert tablet_count == 1

        await manager.api.enable_injection(servers[0].ip_addr, "tablet_load_stats_refresh_before_rebalancing", one_shot=False)
        await manager.api.enable_injection(servers[0].ip_addr, "tablet_split_finalization_postpone", one_shot=False)

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        # Waits for tombstones to be expired.
        time.sleep(1)

        await manager.api.enable_injection(servers[0].ip_addr, "split_sstable_rewrite", one_shot=False)

        logger.info("Enable balancing so split will be emitted")
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        logger.info("Waits for split of sstable containing expired tombstones")
        await s1_log.wait_for(f"split_sstable_rewrite: waiting", from_mark=s1_mark)
        s1_mark = await s1_log.mark()
        await manager.api.message_injection(servers[0].ip_addr, "split_sstable_rewrite")
        await s1_log.wait_for(f"split_sstable_rewrite: released", from_mark=s1_mark)

        logger.info("Pause split of sstable containing deleted data")
        await s1_log.wait_for(f"split_sstable_rewrite: waiting", from_mark=s1_mark)
        s1_mark = await s1_log.mark()

        logger.info("Force compaction of split sstable containing expired tombstone")
        await manager.api.stop_compaction(servers[0].ip_addr, "SPLIT")
        await manager.api.keyspace_compaction(servers[0].ip_addr, ks)

        await s1_log.wait_for(f"split_sstable_rewrite: released", from_mark=s1_mark)

        await manager.api.disable_injection(servers[0].ip_addr, "split_sstable_rewrite")

        await manager.api.disable_injection(servers[0].ip_addr, "tablet_split_finalization_postpone")
        await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark)

        tablet_count = await get_tablet_count(manager, servers[0], ks, 'test')
        assert tablet_count > 1

        logger.info("Verify data is not resurrected")
        await assert_empty_table()

async def create_cluster(manager: ManagerClient, num_dcs: int, num_racks: int, nodes_per_rack: int) -> dict[ServerNum, ServerInfo]:
    logger.debug(f"Creating cluster: num_dcs={num_dcs} num_racks={num_racks} nodes_per_rack={nodes_per_rack}")
    servers: dict[ServerNum, ServerInfo] = dict()
    for dc in range(1, num_dcs + 1):
        for rack in range(1, num_racks + 1):
            rack_servers = await manager.servers_add(nodes_per_rack, property_file={"dc": f"dc{dc}", "rack": f"rack{rack}"})
            for s in rack_servers:
                servers[s.server_id] = s
    logger.debug(f"Created servers={list(servers.values())}")
    return servers


class TestContext:
    def __init__(self, ks: str, table: str, rf: int, initial_tablets: int, num_keys: int):
        self.ks = ks
        self.table = table
        self.rf = rf
        self.initial_tablets = initial_tablets
        self.num_keys = num_keys


@asynccontextmanager
async def create_and_populate_table(manager: ManagerClient, rf: int = 3, initial_tablets: int = 64, num_keys: int = 0):
    ks = ""
    table = unique_name()
    if not num_keys:
        num_keys = initial_tablets * 4

    logger.info(f"Creating table and populating data {ks}.{table}: rf={rf} initial_tablets={initial_tablets} num_keys={num_keys}")

    cql = manager.get_cql()
    try:
        ks = await create_new_test_keyspace(cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} AND tablets = {{'initial': {initial_tablets}}}")
        await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, c int)")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{table} (pk, c) VALUES ({k}, 1);") for k in range(num_keys)])
        yield TestContext(ks, table, rf, initial_tablets, num_keys)
    finally:
        await cql.run_async(f"DROP KEYSPACE {ks}")


def get_expected_replicas_per_server(live_servers: Iterable[ServerInfo], dead_servers: Iterable[ServerInfo], initial_tablets: int, rf: int) -> dict[ServerNum, float]:
    total_replicas = initial_tablets * rf
    nodes_per_rack: defaultdict[str, set[ServerNum]] = defaultdict(set)
    for s in live_servers:
        nodes_per_rack[s.rack].add(s.server_id)
    replicas_per_rack = total_replicas / len(nodes_per_rack.keys())
    result: dict[ServerNum, float] = dict()
    for rack_servers in nodes_per_rack.values():
        replicas_per_node = replicas_per_rack / len(rack_servers)
        for id in rack_servers:
            result[id] = replicas_per_node
    for s in dead_servers:
        result[s.server_id] = 0
    return result


def verify_replicas_per_server(desc: str, expected_replicas_per_server: dict[ServerNum, float], tablet_count: dict[ServerNum, list[int]], initial_tablets: int, rf: int, shards_per_node: int = 2):
    logger.debug(f"{desc}: expected_replicas_per_server={expected_replicas_per_server} tablet_count={tablet_count}")
    total = 0
    for server_id, host_replicas in tablet_count.items():
        expected_replicas_per_shard = expected_replicas_per_server[server_id] / shards_per_node
        for i in host_replicas:
            total += i
            assert abs(i - expected_replicas_per_shard) <= 1
    assert total == initial_tablets * rf


@pytest.mark.asyncio
async def test_decommission_rack_basic(manager: ManagerClient):
    """
    Test decommissioning of all nodes in a rack
    when there are enough remaining racks to satisfy
    the replication factor.
    """
    logger.info("Bootstrapping multi-rack cluster")
    num_racks = 3
    nodes_per_rack = 2
    rf = num_racks - 1

    all_servers = await create_cluster(manager, 1, num_racks, nodes_per_rack)
    async with create_and_populate_table(manager, rf=rf) as ctx:
        logger.info("Verify tablet replicas distribution")
        tables = {ctx.ks: [ctx.table]}
        expected_replicas_per_server = get_expected_replicas_per_server(all_servers.values(), [], ctx.initial_tablets, ctx.rf)
        tablet_count = await get_tablet_count_per_shard_for_hosts(manager, all_servers.values(), tables)
        verify_replicas_per_server("Before decommission", expected_replicas_per_server, tablet_count, ctx.initial_tablets, ctx.rf)

        decommision_rack = f"rack{num_racks}"
        logger.debug(f"Decommissioning rack={decommision_rack}")
        live_servers: dict[ServerNum, ServerInfo] = dict()
        dead_servers: dict[ServerNum, ServerInfo] = dict()
        for id, s in all_servers.items():
            if s.rack == decommision_rack:
                logger.debug(f"Decommissioning server={s}")
                await manager.decommission_node(s.server_id)
                dead_servers[id] = s
            else:
                live_servers[id] = s

        logger.info("Verify tablet replicas distribution")
        expected_replicas_per_server = get_expected_replicas_per_server(live_servers.values(), dead_servers.values(), ctx.initial_tablets, ctx.rf)
        tablet_count = await get_tablet_count_per_shard_for_hosts(manager, live_servers.values(), tables)
        verify_replicas_per_server("After decommission", expected_replicas_per_server, tablet_count, ctx.initial_tablets, ctx.rf)

@pytest.mark.asyncio
async def test_decommission_rack_after_adding_new_rack(manager: ManagerClient):
    """
    Test decommissioning a rack, after a rack with new nodes is added
    """
    logger.info("Bootstrapping multi-rack cluster")
    initial_num_racks = 3
    num_racks = initial_num_racks + 1
    nodes_per_rack = 2
    rf = initial_num_racks

    initial_servers = await create_cluster(manager, 1, initial_num_racks, nodes_per_rack)
    async with create_and_populate_table(manager, rf=rf) as ctx:
        logger.debug("Temporarily disable tablet load balancing")
        node1 = sorted(initial_servers.values(), key=lambda s: s.server_id)[0]
        await manager.api.disable_tablet_balancing(node1.ip_addr)

        logger.info("Add a new rack")
        new_rack = f"rack{num_racks}"
        # copy initial_servers into all_servers, don't just assign it (by reference)
        all_servers: list[ServerInfo] = list(initial_servers.values())
        new_rack_servers = await manager.servers_add(nodes_per_rack, property_file={"dc": "dc1", "rack": new_rack})
        all_servers.extend(new_rack_servers)

        logger.info("Verify tablet replicas distribution")
        tables = {ctx.ks: [ctx.table]}
        expected_replicas_per_server = get_expected_replicas_per_server(initial_servers.values(), new_rack_servers, ctx.initial_tablets, ctx.rf)
        tablet_count = await get_tablet_count_per_shard_for_hosts(manager, all_servers, tables)
        verify_replicas_per_server("Before decommission", expected_replicas_per_server, tablet_count, ctx.initial_tablets, ctx.rf)

        logger.debug("Reenable tablet load balancing")
        await manager.api.enable_tablet_balancing(node1.ip_addr)

        live_servers: dict[ServerNum, ServerInfo] = dict()
        dead_servers: dict[ServerNum, ServerInfo] = dict()
        decommision_rack = f"rack{initial_num_racks}"
        logger.debug(f"Decommissioning rack={decommision_rack}")
        for s in all_servers:
            if s.rack == decommision_rack:
                logger.debug(f"Decommissioning server={s}")
                await manager.decommission_node(s.server_id)
                dead_servers[s.server_id] = s
            else:
                live_servers[s.server_id] = s

        logger.info("Verify tablet replicas distribution")
        expected_replicas_per_server = get_expected_replicas_per_server(live_servers.values(), dead_servers.values(), ctx.initial_tablets, ctx.rf)
        tablet_count = await get_tablet_count_per_shard_for_hosts(manager, all_servers, tables)
        verify_replicas_per_server("After decommission", expected_replicas_per_server, tablet_count, ctx.initial_tablets, ctx.rf)

@pytest.mark.asyncio
async def test_decommission_not_enough_racks(manager: ManagerClient):
    """
    Test that decommissioning a rack fails if the number of rack is
    insufficient to satisfy replication factor, even if the number of
    nodes is sufficient.
    Reproduces https://github.com/scylladb/scylladb/issues/19475
    """
    logger.info("Bootstrapping multi-rack cluster")
    num_racks = 3
    nodes_per_rack = 2
    rf = num_racks

    all_servers = await create_cluster(manager, 1, num_racks, nodes_per_rack)
    async with create_and_populate_table(manager, rf=rf) as ctx:
        logger.info("Verify tablet replicas distribution")
        tables = {ctx.ks: [ctx.table]}
        expected_replicas_per_server = get_expected_replicas_per_server(all_servers.values(), [], ctx.initial_tablets, ctx.rf)
        tablet_count = await get_tablet_count_per_shard_for_hosts(manager, all_servers.values(), tables)
        verify_replicas_per_server("Before decommission", expected_replicas_per_server, tablet_count, ctx.initial_tablets, ctx.rf)

        live_servers: dict[ServerNum, ServerInfo] = dict()
        dead_servers: dict[ServerNum, ServerInfo] = dict()
        decommision_rack = f"rack{num_racks}"
        decommision_count = 0
        for s in all_servers.values():
            if s.rack == decommision_rack:
                logger.debug(f"Decommissioning server={s}")
                decommision_count += 1
                expected_error = "Unable to find new replica for tablet" if decommision_count == nodes_per_rack else None
                await manager.decommission_node(s.server_id, expected_error=expected_error)
                if not expected_error:
                    dead_servers[s.server_id] = s
                else:
                    live_servers[s.server_id] = s
            else:
                live_servers[s.server_id] = s

        logger.info("Verify tablet replicas distribution")
        expected_replicas_per_server = get_expected_replicas_per_server(live_servers.values(), dead_servers.values(), ctx.initial_tablets, ctx.rf)
        tablet_count = await get_tablet_count_per_shard_for_hosts(manager, all_servers.values(), tables)
        verify_replicas_per_server("After decommission", expected_replicas_per_server, tablet_count, ctx.initial_tablets, ctx.rf)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_cleanup_vs_snapshot_race(manager: ManagerClient):
    cmdline = ['--smp=1']

    servers = [await manager.server_add(cmdline=cmdline)]

    cql = manager.get_cql()
    n_tablets = 1
    n_partitions = 1000
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': {n_tablets}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk) VALUES ({k});") for k in range(n_partitions)])

        await inject_error_one_shot_on(manager, "tablet_cleanup_failure_post_deletion", servers)

        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()

        servers.append(await manager.server_add())

        tablet_token = 0
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        s1_host_id = await manager.get_host_id(servers[1].server_id)
        dst_shard = 0
        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token))

        logger.info("Waiting for injected cleanup failure...")
        await s0_log.wait_for('Cleanup failed for tablet', from_mark=s0_mark)

        await manager.api.take_snapshot(servers[0].ip_addr, ks, "test_snapshot")
