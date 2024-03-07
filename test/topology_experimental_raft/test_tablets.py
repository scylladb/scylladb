#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot, HTTPError
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts, read_barrier
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas
from test.topology.conftest import skip_mode
from test.topology.util import reconnect_driver

import pytest
import asyncio
import logging
import time
import random
import os
import glob
from typing import NamedTuple


logger = logging.getLogger(__name__)


async def inject_error_one_shot_on(manager, error_name, servers):
    errs = [inject_error_one_shot(manager.api, s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)

async def repair_on_node(manager: ManagerClient, server: ServerInfo, servers: list[ServerInfo]):
    node = server.ip_addr
    await manager.servers_see_each_other(servers)
    live_nodes_wanted = [s.ip_addr for s in servers]
    live_nodes = await manager.api.get_alive_endpoints(node)
    live_nodes_wanted.sort()
    live_nodes.sort()
    assert live_nodes == live_nodes_wanted
    logger.info(f"Repair table on node {node} live_nodes={live_nodes} live_nodes_wanted={live_nodes_wanted}")
    await manager.api.repair(node, "test", "test")

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
    await manager.server_stop_gracefully(s0)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 100};")

    # force s0 to catch up later from the snapshot and not the raft log
    await inject_error_one_shot_on(manager, 'raft_server_force_snapshot', not_s0)
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    keys = range(10)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 1);") for k in keys])

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(list(rows)) == len(keys)
    for r in rows:
        assert r.c == 1

    manager.driver_close()
    await manager.server_start(s0, wait_others=2)
    await manager.driver_connect(server=servers[0])
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)

    # Trigger a schema change to invoke schema agreement waiting to make sure that s0 has the latest schema
    await cql.run_async("CREATE KEYSPACE test_dummy WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")

    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 2);", execution_profile='whitelist')
                           for k in keys])

    rows = await cql.run_async("SELECT * FROM test.test;")
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

        await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 3);", execution_profile='whitelist')
                               for k in keys])

        rows = await cql.run_async("SELECT * FROM test.test;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == 3
    finally:
        conn_logger.setLevel(logging.INFO)

    await cql.run_async("DROP KEYSPACE test;")
    await cql.run_async("DROP KEYSPACE test_dummy;")


@pytest.mark.asyncio
async def test_scans(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    keys = range(100)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    rows = await cql.run_async("SELECT count(*) FROM test.test;")
    assert rows[0].count == len(keys)

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == r.pk

    await cql.run_async("DROP KEYSPACE test;")


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
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 32};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    async def check():
        logger.info("Checking table")
        rows = await cql.run_async("SELECT * FROM test.test;")
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

    await cql.run_async("DROP KEYSPACE test;")


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_streaming_is_guarded_by_topology_guard(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'raft_topology=trace',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    servers.append(await manager.server_add(cmdline=cmdline))

    key = 7 # Whatever
    tablet_token = 0 # Doesn't matter since there is one tablet
    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({key}, 0)")
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 1

    replica = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet_token)

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    s1_host_id = await manager.get_host_id(servers[1].server_id)
    dst_shard = 0

    await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments", one_shot=True)
    s1_log = await manager.server_open_log(servers[1].server_id)
    s1_mark = await s1_log.mark()

    migration_task = asyncio.create_task(
        manager.api.move_tablet(servers[0].ip_addr, "test", "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token))

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
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 1

    await cql.run_async("TRUNCATE test.test")
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 0

    # Release abandoned streaming
    await manager.api.message_injection(servers[1].ip_addr, "stream_mutation_fragments")
    await s1_log.wait_for('stream_mutation_fragments: done', from_mark=s1_mark)

    # Verify that there is no data resurrection
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 0

    # Verify that moving the tablet back works
    await manager.api.move_tablet(servers[0].ip_addr, "test", "test", s1_host_id, dst_shard, replica[0], replica[1], tablet_token)
    rows = await cql.run_async("SELECT pk from test.test")
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
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")
    await cql.run_async("CREATE TABLE test.test2 (pk int PRIMARY KEY, c int);")

    servers.append(await manager.server_add())

    logger.info("Populating tables")
    key = 7 # Whatever
    value = 3 # Whatever
    tablet_token = 0 # Doesn't matter since there is one tablet
    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({key}, {value})")
    await cql.run_async(f"INSERT INTO test.test2 (pk, c) VALUES ({key}, {value})")
    rows = await cql.run_async("SELECT pk from test.test")
    assert len(list(rows)) == 1
    rows = await cql.run_async("SELECT pk from test.test2")
    assert len(list(rows)) == 1

    replica = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet_token)

    await manager.api.enable_injection(servers[1].ip_addr, "stream_mutation_fragments", one_shot=True)
    s1_log = await manager.server_open_log(servers[1].server_id)
    s1_mark = await s1_log.mark()

    logger.info("Starting tablet migration")
    s1_host_id = await manager.get_host_id(servers[1].server_id)
    migration_task = asyncio.create_task(
        manager.api.move_tablet(servers[0].ip_addr, "test", "test", replica[0], replica[1], s1_host_id, 0, tablet_token))

    # Wait for the replica-side writer of streaming to reach a place where it already
    # received writes from the leaving replica but haven't applied them yet.
    # Once the writer reaches this place, it will wait for the message_injection() call below before proceeding.
    # We want to drop the table while streaming is deep in the process, where it will attempt to apply writes
    # to the dropped table.
    await s1_log.wait_for('stream_mutation_fragments: waiting', from_mark=s1_mark)

    # Streaming blocks table drop, so we can't wait here.
    drop_task = cql.run_async("DROP TABLE test.test")

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
    replica = await get_tablet_replica(manager, servers[0], 'test', 'test2', tablet_token)
    s0_host_id = await manager.get_host_id(servers[0].server_id)
    assert replica[0] == s0_host_id
    await manager.api.move_tablet(servers[0].ip_addr, "test", "test2", replica[0], replica[1], s1_host_id, 0, tablet_token)

    logger.info("Verifying tablet replica")
    replica = await get_tablet_replica(manager, servers[0], 'test', 'test2', tablet_token)
    assert replica == (s1_host_id, 0)

@pytest.mark.repair
@pytest.mark.asyncio
async def test_tablet_repair(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = [await manager.server_add(), await manager.server_add(), await manager.server_add()]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': 2} AND tablets = {'initial': 32};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    await repair_on_node(manager, servers[0], servers)

    async def check():
        logger.info("Checking table")
        rows = await cql.run_async("SELECT * FROM test.test;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await check()

    await cql.run_async("DROP KEYSPACE test;")

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
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': 3} AND tablets = {'initial': 32};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    keys_list = [range(0, 100), range(100, 200), range(200, 300)]
    keys_for_server = dict([(s.server_id, keys_list[idx]) for idx, s in enumerate(servers)])
    keys = range(0, 300)

    async def insert_with_down(down_server):
        logger.info(f"Stopped server {down_server.server_id}")
        logger.info(f"Insert into server {down_server.server_id}")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});")
                               for k in keys_for_server[down_server.server_id]])

    await manager.rolling_restart(servers, with_down=insert_with_down)

    await repair_on_node(manager, servers[0], servers)

    async def check_with_down(down_node):
        logger.info("Checking table")
        query = SimpleStatement("SELECT * FROM test.test;", consistency_level=ConsistencyLevel.ONE)
        rows = await cql.run_async(query)
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await manager.rolling_restart(servers, with_down=check_with_down)


@pytest.mark.repair
@pytest.mark.asyncio
async def test_tablet_repair_history(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = [await manager.server_add(), await manager.server_add(), await manager.server_add()]

    rf = 3
    tablets = 8

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': {}}} AND tablets = {{'initial': {}}};".format(rf, tablets))
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int) WITH tombstone_gc = {'mode':'repair'};")

    logger.info("Populating table")

    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f'Got hosts={hosts}');

    await repair_on_node(manager, servers[0], servers)

    async def check_repair_history():
        all_rows = []
        for host in hosts:
            logging.info(f'Query hosts={host}');
            rows = await cql.run_async("SELECT * from system.repair_history", host=host)
            all_rows += rows
        for row in all_rows:
            logging.info(f"Got repair_history_entry={row}")
        assert len(all_rows) == rf * tablets

    await check_repair_history()

    await cql.run_async("DROP KEYSPACE test;")

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
    await cql.run_async("CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': {}}};".format(n_tablets))
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY);")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk) VALUES ({k});") for k in range(1000)])

    logger.info("Start second node")
    servers.append(await manager.server_add())

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    s1_host_id = await manager.get_host_id(servers[1].server_id)

    logger.info("Read system.tablets")
    tablet_replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
    assert len(tablet_replicas) == n_tablets

    # Randomly select half of all tablets.
    sample = random.sample(tablet_replicas, n_tablets // 2)
    moved_tokens = [x.last_token for x in sample]
    moved_src = [x.replicas[0] for x in sample]
    moved_dst = [(s1_host_id, random.choice([0, 1])) for _ in sample]

    # Migrate the selected tablets to second node.
    logger.info("Migrate half of all tablets to second node")
    for t, s, d in zip(moved_tokens, moved_src, moved_dst):
        await manager.api.move_tablet(servers[0].ip_addr, "test", "test", *s, *d, t)

    # Sanity check. All data we inserted should be still there.
    assert n_partitions == (await cql.run_async("SELECT COUNT(*) FROM test.test"))[0].count

    # Wipe data on second node.
    logger.info("Wipe data on second node")
    await manager.server_stop_gracefully(servers[1].server_id, timeout=120)
    await manager.server_wipe_sstables(servers[1].server_id, "test", "test")
    await manager.server_start(servers[1].server_id)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)
    partitions_after_loss = (await cql.run_async("SELECT COUNT(*) FROM test.test"))[0].count
    assert partitions_after_loss < n_partitions

    # Migrate all tablets back to their original position.
    # Check that this doesn't resurrect cleaned data.
    logger.info("Migrate the migrated tablets back")
    for t, s, d in zip(moved_tokens, moved_dst, moved_src):
        await manager.api.move_tablet(servers[0].ip_addr, "test", "test", *s, *d, t)
    assert partitions_after_loss == (await cql.run_async("SELECT COUNT(*) FROM test.test"))[0].count

    # Kill and restart first node.
    # Check that this doesn't resurrect cleaned data.
    logger.info("Brutally restart first node")
    await manager.server_stop(servers[0].server_id)
    await manager.server_start(servers[0].server_id)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)
    assert partitions_after_loss == (await cql.run_async("SELECT COUNT(*) FROM test.test"))[0].count

    # Bonus: check that commitlog_cleanups doesn't have any garbage after restart.
    assert 0 == (await cql.run_async("SELECT COUNT(*) FROM system.commitlog_cleanups", host=hosts[0]))[0].count

@pytest.mark.asyncio
async def test_tablet_resharding(manager: ManagerClient):
    cmdline = ['--smp=3']
    config = {'experimental_features': ['consistent-topology-changes', 'tablets']}
    servers = await manager.servers_add(1, cmdline=cmdline)
    server = servers[0]

    logger.info("Populate table")
    cql = manager.get_cql()
    n_tablets = 32
    n_partitions = 1000
    await cql.run_async(f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': {n_tablets}}};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY);")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk) VALUES ({k});") for k in range(n_partitions)])

    await manager.server_stop_gracefully(server.server_id, timeout=120)
    await manager.server_update_cmdline(server.server_id, ['--smp=2'])

    await manager.server_start(
        server.server_id,
        expected_error="Detected a tablet with invalid replica shard, reducing shard count with tablet-enabled tables is not yet supported. Replace the node instead.")

async def get_tablet_count(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str):
    host = manager.cql.cluster.metadata.get_host(server.ip_addr)

    # read_barrier is needed to ensure that local tablet metadata on the queried node
    # reflects the finalized tablet movement.
    await read_barrier(manager.cql, host)

    table_id = await manager.get_table_id(keyspace_name, table_name)
    rows = await manager.cql.run_async(f"SELECT tablet_count FROM system.tablets where "
                                       f"table_id = {table_id}", host=host)
    return rows[0].tablet_count

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_split(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'table=debug',
        '--target-tablet-size-in-bytes', '1024',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    # enough to trigger multiple splits with max size of 1024 bytes.
    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    async def check():
        logger.info("Checking table")
        cql = manager.get_cql()
        rows = await cql.run_async("SELECT * FROM test.test;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await check()

    await manager.api.flush_keyspace(servers[0].ip_addr, "test")

    tablet_count = await get_tablet_count(manager, servers[0], 'test', 'test')
    assert tablet_count == 1

    logger.info("Adding new server")
    servers.append(await manager.server_add(cmdline=cmdline))

    # Increases the chance of tablet migration concurrent with split
    await inject_error_one_shot_on(manager, "tablet_allocator_shuffle", servers)
    await inject_error_on(manager, "tablet_load_stats_refresh_before_rebalancing", servers)

    s1_log = await manager.server_open_log(servers[0].server_id)
    s1_mark = await s1_log.mark()

    # Now there's a split and migration need, so they'll potentially run concurrently.
    await manager.api.enable_tablet_balancing(servers[0].ip_addr)

    await check()
    time.sleep(5) # Give load balancer some time to do work

    await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark)

    await check()

    tablet_count = await get_tablet_count(manager, servers[0], 'test', 'test')
    assert tablet_count > 1

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

async def get_tablet_count_per_shard_for_host(shards_count: int, manager: ManagerClient, server: ServerInfo, full_tables: dict[str: list[str]]) -> list[int]:
    host = await manager.get_host_id(server.server_id)
    result = [0] * shards_count

    for keyspace, tables in full_tables.items():
        for table in tables:
            table_tablets = await get_all_tablet_replicas(manager, server, keyspace, table)
            for tablet_replica in table_tablets:
                for host_id, shard_id in tablet_replica.replicas:
                    if host_id == host:
                        result[shard_id] += 1;
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
    await cql.run_async("CREATE KEYSPACE testing WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    await cql.run_async("CREATE TABLE testing.mytable1 (col1 timestamp, col2 text, col3 blob, PRIMARY KEY (col1));")
    await cql.run_async("CREATE TABLE testing.mytable2 (col1 timestamp, col2 text, col3 blob, PRIMARY KEY (col1));")

    # Then tablet count metric for each shard depicts the actual state
    tables = { "testing": ["mytable1", "mytable2"] }
    expected_count_per_shard_for_host_0 = await get_tablet_count_per_shard_for_host(shards_count, manager, servers[0], tables)
    await assert_tablet_count_metric_value_for_shards(manager, servers[0], expected_count_per_shard_for_host_0)

    expected_count_per_shard_for_host_1 = await get_tablet_count_per_shard_for_host(shards_count, manager, servers[1], tables)
    await assert_tablet_count_metric_value_for_shards(manager, servers[1], expected_count_per_shard_for_host_1)

    # When third table is created
    await cql.run_async("CREATE TABLE testing.mytable3 (col1 timestamp, col2 text, col3 blob, PRIMARY KEY (col1));")

    # Then tablet count metric for each shard depicts the actual state
    tables = { "testing": ["mytable1", "mytable2", "mytable3"] }
    expected_count_per_shard_for_host_0 = await get_tablet_count_per_shard_for_host(shards_count, manager, servers[0], tables)
    await assert_tablet_count_metric_value_for_shards(manager, servers[0], expected_count_per_shard_for_host_0)

    expected_count_per_shard_for_host_1 = await get_tablet_count_per_shard_for_host(shards_count, manager, servers[1], tables)
    await assert_tablet_count_metric_value_for_shards(manager, servers[1], expected_count_per_shard_for_host_1)

    # When one of tables is dropped
    await cql.run_async("DROP TABLE testing.mytable2;")

    # Then tablet count metric for each shard depicts the actual state
    tables = { "testing": ["mytable1", "mytable3"] }
    expected_count_per_shard_for_host_0 = await get_tablet_count_per_shard_for_host(shards_count, manager, servers[0], tables)
    await assert_tablet_count_metric_value_for_shards(manager, servers[0], expected_count_per_shard_for_host_0)

    expected_count_per_shard_for_host_1 = await get_tablet_count_per_shard_for_host(shards_count, manager, servers[1], tables)
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
        "mytable1" : await get_tablet_tokens_from_host_on_shard(manager, src_server, "testing", "mytable1", shard_id_to_move),
        "mytable3" : await get_tablet_tokens_from_host_on_shard(manager, src_server, "testing", "mytable3", shard_id_to_move)
    }

    count_of_tokens_on_src_shard_to_move = len(tokens_on_shard_to_move["mytable1"]) + len(tokens_on_shard_to_move["mytable3"])
    assert count_of_tokens_on_src_shard_to_move > 0

    src_host_id = await manager.get_host_id(src_server.server_id)
    dest_host_id = await manager.get_host_id(dest_server.server_id)
    for table_name, tokens in tokens_on_shard_to_move.items():
        for token in tokens:
            await manager.api.move_tablet(node_ip=src_server.ip_addr, ks="testing", table=table_name, src_host=src_host_id, src_shard=shard_id_to_move, dst_host=dest_host_id, dst_shard=3, token=token)

    # And when ensuring that local tablet metadata on the queried node reflects the finalized tablet movement
    host0 = manager.get_cql().cluster.metadata.get_host(servers[0].ip_addr)
    host1 = manager.get_cql().cluster.metadata.get_host(servers[1].ip_addr)
    await read_barrier(manager.get_cql(), host0)
    await read_barrier(manager.get_cql(), host1)

    # Then tablet count metric is adjusted to depict that situation on src_host - all tablets from selected shard have been moved
    src_expected_count_per_shard[shard_id_to_move] = 0
    await assert_tablet_count_metric_value_for_shards(manager, src_server, src_expected_count_per_shard)

    # And then tablet count metric is increased on dest_host - tablets have been moved to shard_3
    dest_expected_count_per_shard[3] += count_of_tokens_on_src_shard_to_move
    await assert_tablet_count_metric_value_for_shards(manager, dest_server, dest_expected_count_per_shard)

async def test_tablet_load_and_stream(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'table=debug',
        '--smp', '1',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    # Creates multiple tablets in the same shard
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}" \
                        " AND tablets = {'initial': 5};") # 5 is rounded up to next power-of-two
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    # Populate tablets
    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    async def check():
        logger.info("Checking table")
        cql = manager.get_cql()
        rows = await cql.run_async("SELECT * FROM test.test BYPASS CACHE;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await manager.api.flush_keyspace(servers[0].ip_addr, "test")
    await check()

    node_workdir = await manager.server_get_workdir(servers[0].server_id)

    await manager.server_stop_gracefully(servers[0].server_id)

    table_dir = glob.glob(os.path.join(node_workdir, "data", "test", "test-*"))[0]
    logger.info(f"Table dir: {table_dir}")

    def move_sstables_to_upload(table_dir: str):
        logger.info("Moving sstables to upload dir")
        table_upload_dir = os.path.join(table_dir, "upload")
        for sst in glob.glob(os.path.join(table_dir, "*-Data.db")):
            for src_path in glob.glob(os.path.join(table_dir, sst.removesuffix("-Data.db") + "*")):
                dst_path = os.path.join(table_upload_dir, os.path.basename(src_path))
                logger.info(f"Moving sstable file {src_path} to {dst_path}")
                os.rename(src_path, dst_path)

    move_sstables_to_upload(table_dir)

    await manager.server_start(servers[0].server_id)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    rows = await cql.run_async("SELECT * FROM test.test BYPASS CACHE;")
    assert len(rows) == 0

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info("Adding new server")
    servers.append(await manager.server_add(cmdline=cmdline))

    # Trigger concurrent migration and load-and-stream

    await manager.api.enable_tablet_balancing(servers[0].ip_addr)

    await manager.api.load_new_sstables(servers[0].ip_addr, "test", "test")

    time.sleep(1)

    await check()
