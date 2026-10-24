#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from cassandra import WriteFailure, WriteTimeout # type: ignore
from cassandra.query import SimpleStatement, ConsistencyLevel # type: ignore

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

import pytest
import asyncio
import logging

from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import start_writes
from test.cluster.util import (create_new_test_keyspace, get_topology_coordinator,
                               FeatureConfig, FeatureConfigurations, feature_configs)

logger = logging.getLogger(__name__)


FEATURE_CONFIGS = feature_configs(FeatureConfigurations.EVENTUAL_CONSISTENCY,
                                  FeatureConfigurations.STRONG_CONSISTENCY)


async def create_keyspace(cql, initial_tablets, rf, feature_config: FeatureConfig = FeatureConfig()):
    return await create_new_test_keyspace(cql, feature_config.get_keyspace_opts(
                        f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"
                        f" AND tablets = {{'initial': {initial_tablets}}};"))


async def create_keyspaces(cql, feature_config: FeatureConfig) -> dict[int, str]:
    """Create a keyspace with a `test` table for each replication factor, keyed by it."""
    # The eventually consistent tests use RF=1 to check that losing a tablet's only replica
    # doesn't crash anything, and RF=2 and RF=3 to check that the data survives.
    #
    # For now, a strongly consistent tablet whose raft group loses its majority with a
    # removed or replaced node cannot be rebuilt: the configuration change that adds the new
    # replica needs a quorum the group no longer has, so the rebuild waits for good and so
    # does the node operation. With one node lost that is every RF=1 and RF=2 tablet on it,
    # so a strongly consistent test keeps only the RF=3 keyspace. Once recovery from
    # majority loss is supported, the other keyspaces can be brought back.
    rfs = [3] if feature_config.strongly_consistent else [1, 2, 3]
    ks_by_rf = {}
    for rf in rfs:
        ks = await create_keyspace(cql, 32, rf=rf, feature_config=feature_config)
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        ks_by_rf[rf] = ks
    return ks_by_rf


async def insert_rows(cql, feature_config: FeatureConfig, table: str, keys):
    # Strongly consistent writes must use QUORUM.
    cl = ConsistencyLevel.QUORUM if feature_config.strongly_consistent else ConsistencyLevel.ALL
    await asyncio.gather(*[cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, c) VALUES ({k}, {k});",
                                                         consistency_level=cl)) for k in keys])


async def check_partitions(cql, table: str, keys, cl=None, host=None):
    """Check that the partition of every key in `table` holds one row with c == pk.

    A query per key, as strongly consistent queries can only target a single partition, and
    at most 100 of them at a time, as in test.cluster.util: thousands at once make the test
    flaky. `cl` None reads at the session's default.
    """
    semaphore = asyncio.Semaphore(100)

    async def read(k):
        async with semaphore:
            return await cql.run_async(SimpleStatement(f"SELECT * FROM {table} WHERE pk = {k};", consistency_level=cl),
                                       host=host)

    results = await asyncio.gather(*[read(k) for k in keys])
    for k, rows in zip(keys, results):
        assert len(rows) == 1 and rows[0].c == k, f"Unexpected rows for pk={k}: {rows}"


async def check_rows(cql, feature_config: FeatureConfig, table: str, keys, cl=None, host=None):
    """Check that `table` holds exactly `keys`, each with c == pk.

    `cl` applies to eventually consistent tables only.
    """
    if feature_config.strongly_consistent:
        # Read at the session's default LOCAL_QUORUM, which is linearizable: a strongly
        # consistent read at ONE is served by the local replica without a read barrier and
        # may miss a write just acknowledged.
        await check_partitions(cql, table, keys, host=host)
    else:
        rows = await cql.run_async(SimpleStatement(f"SELECT * FROM {table};", consistency_level=cl), all_pages=True)
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk


async def check_every_replica_serves_data(manager: ScyllaClusterManager, ks: str, table: str, keys):
    """Read every key of a strongly consistent table at CL=ONE on every node.

    A strongly consistent read at ONE is served by the local replica, so it passes only if
    the data reached every replica, the rebuilt ones included. A node which isn't a replica
    of the key's tablet would forward the read to one, so every node must hold every tablet.

    A read at ONE has no read barrier and may miss the latest writes, so every node passes a
    read barrier of every tablet's raft group first.
    """
    servers = await manager.running_servers()
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = {await manager.get_host_id(s.server_id) for s in servers}

    tablets = await get_all_tablet_replicas(manager, servers[0], ks, table)
    for tablet in tablets:
        assert tablet.raft_group_id, f"Tablet {tablet.last_token} of {ks}.{table} has no raft group"
        assert {h for h, _ in tablet.replicas} == host_ids, \
            f"Tablet {tablet.last_token} has replicas {tablet.replicas}, expected one on every node: {host_ids}"

    # With no timeout, a read barrier of a strongly consistent group hits an internal error
    # (SCYLLADB-4759).
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr, t.raft_group_id, timeout=60)
                           for s in servers for t in tablets])
    for host in hosts:
        await check_partitions(cql, f"{ks}.{table}", keys, cl=ConsistencyLevel.ONE, host=host)


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


@pytest.mark.parametrize("feature_config", feature_configs(
    FeatureConfigurations.EVENTUAL_CONSISTENCY,
    FeatureConfigurations.STRONG_CONSISTENCY.with_marks(pytest.mark.skip_bug(
        link="https://scylladb.atlassian.net/browse/SCYLLADB-5638",
        reason="the replacing node can join a tablet's raft group without its storage and abort"))))
async def test_replace(manager: ScyllaClusterManager, feature_config: FeatureConfig):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'raft_topology=trace',
    ]

    config = feature_config.get_cluster_cfg({"rf_rack_valid_keyspaces": False})
    servers = await manager.servers_add(3, cmdline=cmdline, config=config)

    cql = manager.get_cql()

    # The eventually consistent variant has an RF=2 keyspace to validate that quorum reads
    # work after the replacing node finishes bootstrap, which indicates that bootstrap waits
    # for rebuild. Otherwise, some reads would fail to find a quorum.
    ks_by_rf = await create_keyspaces(cql, feature_config)
    ks3 = ks_by_rf[3]
    await cql.run_async(f"CREATE TABLE {ks3}.test2 (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    for ks in ks_by_rf.values():
        await insert_rows(cql, feature_config, f"{ks}.test", keys)

    async def check_ks(ks):
        logger.info(f"Checking {ks}")
        await check_rows(cql, feature_config, f"{ks}.test", keys, ConsistencyLevel.QUORUM)

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

    # FIXME: stopping the replaced node can fail a strongly consistent write in flight on it.
    # A graceful shutdown aborts writes bounced to another shard of the node (SCYLLADB-5405)
    # and writes forwarded to the node from another one (SCYLLADB-5594), which fail with
    # WriteTimeout or WriteFailure. Retry such writes until both are fixed.
    sc_shutdown_errors = (WriteTimeout, WriteFailure) if feature_config.strongly_consistent else ()
    finish_writes = await start_writes(cql, ks3, "test2", retried_write_errors=sc_shutdown_errors)

    logger.info('Replacing a node')
    await manager.server_stop_gracefully(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = True)
    servers.append(await manager.server_add(replace_cfg, config=config))
    servers = servers[1:]

    key_count = await finish_writes()
    await check_rows(cql, feature_config, f"{ks3}.test2", range(key_count), ConsistencyLevel.QUORUM)

    await check()

    if feature_config.strongly_consistent:
        # Every tablet is rebuilt on the replacing node. The rows of test were all written
        # before the replace; test2 also has rows written while the tablets were being rebuilt.
        await check_every_replica_serves_data(manager, ks3, 'test', keys)
        await check_every_replica_serves_data(manager, ks3, 'test2', range(key_count))

    # Verify that QUORUM reads from RF=3 table work when replacing finished and we down a single node.
    # This validates that replace waits for tablet rebuilt before finishing bootstrap, otherwise some reads
    # would fail to find a quorum.
    logger.info('Downing a node')
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_not_sees_other_server(servers[1].ip_addr, servers[0].ip_addr)
    await manager.server_not_sees_other_server(servers[2].ip_addr, servers[0].ip_addr)

    await check_ks(ks3)


@pytest.mark.parametrize("feature_config", FEATURE_CONFIGS)
async def test_removenode(manager: ScyllaClusterManager, feature_config: FeatureConfig):
    logger.info("Bootstrapping cluster")
    cmdline = ['--logger-log-level', 'storage_service=trace']

    config = feature_config.get_cluster_cfg({"rf_rack_valid_keyspaces": False})

    # 4 nodes so that we can find new tablet replica for the RF=3 table on removenode
    servers = await manager.servers_add(4, cmdline=cmdline, config=config)

    cql = manager.get_cql()

    ks_by_rf = await create_keyspaces(cql, feature_config)
    ks3 = ks_by_rf[3]

    logger.info("Populating table")

    keys = range(256)
    for ks in ks_by_rf.values():
        await insert_rows(cql, feature_config, f"{ks}.test", keys)

    async def check():
        # RF=1 table "test" will experience data loss so don't check it.
        # We include it to check that the system doesn't crash.
        for rf, ks in ks_by_rf.items():
            if rf > 1:
                logger.info(f"Checking the RF={rf} keyspace")
                await check_rows(cql, feature_config, f"{ks}.test", keys, ConsistencyLevel.ONE)

    await check()

    # Disable migrations concurrent with removenode since we don't handle nodes going down during migration yet.
    # See https://github.com/scylladb/scylladb/issues/16527
    await manager.disable_tablet_balancing()

    logger.info('Removing a node')
    await manager.server_stop(servers[0].server_id, convict=True)
    await manager.remove_node(servers[1].server_id, servers[0].server_id)
    servers = servers[1:]

    await check()

    if feature_config.strongly_consistent:
        await check_every_replica_serves_data(manager, ks3, 'test', keys)


@pytest.mark.parametrize("feature_config", FEATURE_CONFIGS)
async def test_removenode_with_ignored_node(manager: ScyllaClusterManager, feature_config: FeatureConfig):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
    ]

    # 5 nodes because we need a quorum with 2 nodes down.
    # 4 nodes would be enough to not lose data with RF=3.
    #
    # The removed and the ignored node are both in r1, and RF=3 places one replica of each
    # tablet in every rack, so no tablet loses two replicas. That matters for strong
    # consistency: for now, a tablet with both nodes among its replicas would lose the
    # majority of its raft group and could not be rebuilt, so the removenode would hang
    # (see create_keyspaces()).
    servers = await manager.servers_add(5, cmdline=cmdline, config=feature_config.get_cluster_cfg(), property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"}
    ])

    cql = manager.get_cql()

    ks = await create_keyspace(cql, 32, rf=3, feature_config=feature_config)
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(512)
    await insert_rows(cql, feature_config, f"{ks}.test", keys)

    async def check():
        logger.info("Checking")
        await check_rows(cql, feature_config, f"{ks}.test", keys, ConsistencyLevel.ONE)

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
