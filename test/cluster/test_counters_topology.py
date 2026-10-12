#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Counters on multiple nodes: their values survive restarts and topology
# operations, and concurrent updates through several coordinators add up.
# The keyspaces use vnodes, as with tablets counters move by tablet
# migration, which test_counters_with_tablets.py covers.

import asyncio
import glob
import os
import time

import pytest
from cassandra import ConsistencyLevel  # type: ignore
from cassandra.policies import FallthroughRetryPolicy  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.util import new_test_keyspace, vnodes_keyspace
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

KEYS = 100


def counter_update(table: str, cl: ConsistencyLevel) -> SimpleStatement:
    # A timed out counter update must not be retried: it may have been applied.
    return SimpleStatement(f"UPDATE {table} SET c = c + %s WHERE pk = %s", consistency_level=cl,
                           retry_policy=FallthroughRetryPolicy())


async def add_to_counters(cql, table: str, delta_of_pk, cl=ConsistencyLevel.QUORUM, host=None):
    update = counter_update(table, cl)
    await asyncio.gather(*[cql.run_async(update, [delta_of_pk(pk), pk], host=host) for pk in range(KEYS)])


async def check_counters(cql, table: str, cl=ConsistencyLevel.ALL, host=None):
    """Every counter equals its key, also after another +1, -1 round."""
    select = SimpleStatement(f"SELECT pk, c FROM {table}", consistency_level=cl)
    assert sorted(await cql.run_async(select, host=host)) == [(pk, pk) for pk in range(KEYS)]
    await add_to_counters(cql, table, lambda pk: 1, cl=cl, host=host)
    await add_to_counters(cql, table, lambda pk: -1, cl=cl, host=host)
    assert sorted(await cql.run_async(select, host=host)) == [(pk, pk) for pk in range(KEYS)]


async def counters_table(manager: ScyllaClusterManager, ks: str) -> str:
    table = f"{ks}.t"
    await manager.get_cql().run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c counter)")
    return table


# Counter batches at QUORUM keep their values over several rounds of rolling
# restarts, which reload them from the commitlog and the sstables.
# Reproduces https://github.com/scylladb/scylladb/issues/4436
async def test_counters_survive_rolling_restarts(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, property_file={"dc": "dc1", "rack": "r1"})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, vnodes_keyspace(2)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, c counter) WITH compression = {{'sstable_compression': 'SnappyCompressor'}}")
        batch = SimpleStatement(f"BEGIN COUNTER BATCH {' '.join(f'UPDATE {ks}.t SET c = c + 1 WHERE pk = {k};' for k in range(4))} APPLY BATCH",
                                consistency_level=ConsistencyLevel.QUORUM, retry_policy=FallthroughRetryPolicy())
        select = SimpleStatement(f"SELECT pk, c FROM {ks}.t", consistency_level=ConsistencyLevel.QUORUM)
        expected = 0
        for _ in range(3):
            for _ in range(10):
                await cql.run_async(batch)
            expected += 10
            for s in servers:
                await manager.server_restart(s.server_id)
                await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
                assert sorted(await cql.run_async(select)) == [(k, expected) for k in range(4)]


# Increments through one coordinator and decrements through the two others,
# all concurrent, at CL=ONE or QUORUM: CL=ALL reads see the exact sums.
@pytest.mark.parametrize("cl", [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM], ids=["one", "quorum"])
async def test_concurrent_counter_updates_through_all_nodes(manager: ScyllaClusterManager, cl):
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        table = await counters_table(manager, ks)
        update = counter_update(table, cl)
        rows, rounds = 25, 20

        async def updates(host, delta):
            for _ in range(rounds):
                await asyncio.gather(*[cql.run_async(update, [delta, pk], host=host) for pk in range(rows)])
        await asyncio.gather(updates(hosts[0], 2), updates(hosts[1], -1), updates(hosts[2], -3))
        select = SimpleStatement(f"SELECT pk, c FROM {table}", consistency_level=ConsistencyLevel.ALL)
        assert sorted(await cql.run_async(select)) == [(pk, rounds * (2 - 1 - 3)) for pk in range(rows)]


async def only_node_reads(manager, servers, node, table):
    """Check the counters through `node` alone, with the other nodes stopped."""
    others = [s for s in servers if s.server_id != node.server_id]
    for s in others:
        await manager.server_stop_gracefully(s.server_id)
    cql = await manager.get_cql_exclusive(node)
    try:
        await check_counters(cql, table, cl=ConsistencyLevel.ONE)
    finally:
        cql.cluster.shutdown()
    for s in others:
        await manager.server_start(s.server_id)
    await manager.driver_connect()


# Counter values stay right after a topology operation moves their data:
# replacing, removing, adding or decommissioning a node, or repairing or
# rebuilding a node that lost them.
@pytest.mark.parametrize("op", ["replace", "remove", "add", "decommission", "repair", "rebuild"])
async def test_counters_after_topology_operation(manager: ScyllaClusterManager, op: str):
    rf = {"decommission": 1, "repair": 3, "rebuild": 3}.get(op, 2)
    servers = await manager.servers_add(3, property_file={"dc": "dc1", "rack": "r1"})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, vnodes_keyspace(rf)) as ks:
        table = await counters_table(manager, ks)
        if op != "repair":
            await add_to_counters(cql, table, lambda pk: pk)

        if op == "replace":
            await manager.server_stop_gracefully(servers[2].server_id)
            replace_cfg = ReplaceConfig(replaced_id=servers[2].server_id, reuse_ip_addr=False, use_host_id=True)
            servers[2] = await manager.server_add(replace_cfg=replace_cfg, property_file={"dc": "dc1", "rack": "r1"})
        elif op == "remove":
            await manager.server_stop_gracefully(servers[2].server_id)
            await manager.remove_node(servers[0].server_id, servers[2].server_id)
            servers.pop()
        elif op == "add":
            servers.append(await manager.server_add(property_file={"dc": "dc1", "rack": "r1"}))
        elif op == "decommission":
            await manager.decommission_node(servers[1].server_id)
            servers.pop(1)
        elif op == "repair":
            # The third node misses the updates, gets them by repair.
            await manager.server_stop_gracefully(servers[2].server_id)
            await add_to_counters(cql, table, lambda pk: pk, cl=ConsistencyLevel.TWO)
            await manager.server_start(servers[2].server_id)
            await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            await manager.api.repair(servers[2].ip_addr, ks, "t")
            await only_node_reads(manager, servers, servers[2], table)
        elif op == "rebuild":
            # The third node loses its sstables of the table, gets them back by rebuild.
            for s in servers:
                await manager.api.keyspace_flush(s.ip_addr, ks)
            workdir = await manager.server_get_workdir(servers[2].server_id)
            await manager.server_stop_gracefully(servers[2].server_id)
            for f in glob.glob(os.path.join(workdir, 'data', ks, 't-*', '*')):
                if os.path.isfile(f):
                    os.unlink(f)
            await manager.server_start(servers[2].server_id)
            await manager.api.rebuild_node(servers[2].ip_addr, timeout=300)
            await only_node_reads(manager, servers, servers[2], table)

        await wait_for_cql_and_get_hosts(manager.get_cql(), servers, time.time() + 60)
        await check_counters(manager.get_cql(), table)
