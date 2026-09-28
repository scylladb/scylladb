#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from cassandra.query import SimpleStatement, ConsistencyLevel # type: ignore

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

import pytest
import asyncio
import logging

from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import start_writes
from test.cluster.util import create_new_test_keyspace, get_topology_coordinator

logger = logging.getLogger(__name__)


async def create_keyspace(cql, initial_tablets, rf):
    return await create_new_test_keyspace(cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"
                        f" AND tablets = {{'initial': {initial_tablets}}};")


async def create_keyspaces(cql) -> dict[int, str]:
    """Create a keyspace with a `test` table for each replication factor, keyed by it."""
    # The tests use RF=1 to check that losing a tablet's only replica doesn't crash
    # anything, and RF=2 and RF=3 to check that the data survives.
    ks_by_rf = {}
    for rf in [1, 2, 3]:
        ks = await create_keyspace(cql, 32, rf=rf)
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        ks_by_rf[rf] = ks
    return ks_by_rf


async def insert_rows(cql, table: str, keys):
    await asyncio.gather(*[cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, c) VALUES ({k}, {k});",
                                                         consistency_level=ConsistencyLevel.ALL)) for k in keys])


async def check_rows(cql, table: str, keys, cl):
    """Check that `table` holds exactly `keys`, each with c == pk."""
    rows = await cql.run_async(SimpleStatement(f"SELECT * FROM {table};", consistency_level=cl), all_pages=True)
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == r.pk


async def test_removenode_with_coordinator_restart(manager: ScyllaClusterManager):
    """
    Verifies that removenode can proceed when the coordinator is restarted
    with some normal nodes down, so cannot obtain table stats for them.
    Tablet draining should still be able to make progress
    as long as all non-excluded nodes are up. This verifies that capacity
    is obtained per node and not in all-or-nothing fashion.
    """
    logger.info("Bootstrapping cluster")
    cmdline = ['--logger-log-level', 'load_balancer=debug']

    servers = await manager.servers_add(3, cmdline=cmdline)
    cql = manager.get_cql()

    ks1 = await create_keyspace(cql, 3, rf=1)
    await cql.run_async(f"CREATE TABLE {ks1}.test (pk int PRIMARY KEY, c int);")

    logger.info('Stopping a node to be removed')
    await manager.server_stop(servers[2].server_id, convict=True)

    logger.info('Restarting leader')
    raft_leader_host_id = await get_topology_coordinator(manager)
    for s in servers:
        if raft_leader_host_id == await manager.get_host_id(s.server_id):
            await manager.server_restart(s.server_id)
            break

    logger.info('Removing a node')
    await manager.remove_node(servers[1].server_id, servers[2].server_id)


async def test_replace(manager: ScyllaClusterManager):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'raft_topology=trace',
    ]

    config = {"rf_rack_valid_keyspaces": False}
    servers = await manager.servers_add(3, cmdline=cmdline, config=config)

    cql = manager.get_cql()

    # We want RF=2 table to validate that quorum reads work after replacing node finishes
    # bootstrap which indicates that bootstrap waits for rebuilt.
    # Otherwise, some reads would fail to find a quorum.
    ks_by_rf = await create_keyspaces(cql)
    ks3 = ks_by_rf[3]
    await cql.run_async(f"CREATE TABLE {ks3}.test2 (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    for ks in ks_by_rf.values():
        await insert_rows(cql, f"{ks}.test", keys)

    async def check_ks(ks):
        logger.info(f"Checking {ks}")
        await check_rows(cql, f"{ks}.test", keys, ConsistencyLevel.QUORUM)

    async def check():
        # RF=1 keyspace will experience data loss so don't check it.
        # We include it in the test only to check that the system doesn't crash.
        for rf, ks in ks_by_rf.items():
            if rf > 1:
                await check_ks(ks)

    await check()

    # Disable migrations concurrent with replace since we don't handle nodes going down during migration yet.
    # See https://github.com/scylladb/scylladb/issues/16527
    await manager.disable_tablet_balancing()

    finish_writes = await start_writes(cql, ks3, "test2")

    logger.info('Replacing a node')
    await manager.server_stop_gracefully(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = True)
    servers.append(await manager.server_add(replace_cfg, config=config))
    servers = servers[1:]

    key_count = await finish_writes()
    await check_rows(cql, f"{ks3}.test2", range(key_count), ConsistencyLevel.QUORUM)

    await check()

    # Verify that QUORUM reads from RF=3 table work when replacing finished and we down a single node.
    # This validates that replace waits for tablet rebuilt before finishing bootstrap, otherwise some reads
    # would fail to find a quorum.
    logger.info('Downing a node')
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_not_sees_other_server(servers[1].ip_addr, servers[0].ip_addr)
    await manager.server_not_sees_other_server(servers[2].ip_addr, servers[0].ip_addr)

    await check_ks(ks3)


async def test_removenode(manager: ScyllaClusterManager):
    logger.info("Bootstrapping cluster")
    cmdline = ['--logger-log-level', 'storage_service=trace']

    config = {"rf_rack_valid_keyspaces": False}

    # 4 nodes so that we can find new tablet replica for the RF=3 table on removenode
    servers = await manager.servers_add(4, cmdline=cmdline, config=config)

    cql = manager.get_cql()

    ks_by_rf = await create_keyspaces(cql)
    ks3 = ks_by_rf[3]

    logger.info("Populating table")

    keys = range(256)
    for ks in ks_by_rf.values():
        await insert_rows(cql, f"{ks}.test", keys)

    async def check():
        # RF=1 table "test" will experience data loss so don't check it.
        # We include it to check that the system doesn't crash.
        for rf, ks in ks_by_rf.items():
            if rf > 1:
                logger.info(f"Checking the RF={rf} keyspace")
                await check_rows(cql, f"{ks}.test", keys, ConsistencyLevel.ONE)

    await check()

    # Disable migrations concurrent with removenode since we don't handle nodes going down during migration yet.
    # See https://github.com/scylladb/scylladb/issues/16527
    await manager.disable_tablet_balancing()

    logger.info('Removing a node')
    await manager.server_stop(servers[0].server_id, convict=True)
    await manager.remove_node(servers[1].server_id, servers[0].server_id)
    servers = servers[1:]

    await check()


async def test_removenode_with_ignored_node(manager: ScyllaClusterManager):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
    ]

    # 5 nodes because we need a quorum with 2 nodes down.
    # 4 nodes would be enough to not lose data with RF=3.
    servers = await manager.servers_add(5, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"}
    ])

    cql = manager.get_cql()

    ks = await create_keyspace(cql, 32, rf=3)
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(512)
    await insert_rows(cql, f"{ks}.test", keys)

    async def check():
        logger.info("Checking")
        await check_rows(cql, f"{ks}.test", keys, ConsistencyLevel.ONE)

    await check()

    # Disable migrations concurrent with removenode since we don't handle nodes going down during migration yet.
    # See https://github.com/scylladb/scylladb/issues/16527
    await manager.disable_tablet_balancing()

    logger.info('Removing a node with another node down')
    await manager.server_stop(servers[0].server_id, convict=True) # removed
    await manager.server_stop(servers[1].server_id, convict=True) # ignored
    await manager.remove_node(servers[2].server_id, servers[0].server_id, [servers[1].ip_addr])

    await manager.others_not_see_server(servers[1].ip_addr)
    servers = servers[1:]

    await check()

    logger.info('Removing a node')
    await manager.remove_node(servers[1].server_id, servers[0].server_id)

    await check()
