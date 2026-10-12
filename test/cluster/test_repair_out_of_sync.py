#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Row-level repair of vnodes keyspaces through "nodetool repair", ported
# from scylla-dtest repair_test.py: a node that missed a write gets it from
# repair, a whole cluster or only the local datacenter, and partitions that
# are empty on one node and hold only purgeable tombstones on the other
# need no repair. As on dtest nodes, hinted handoff is off and the commitlog
# is synced in batch mode, so only repair can bring the missed data.

import asyncio
import subprocess
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

CONFIG = {"hinted_handoff_enabled": False, "commitlog_sync": "batch", "tablets_mode_for_new_keyspaces": "disabled"}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def nodetool(manager: ScyllaClusterManager, server, *args):
    """Runs "scylla nodetool" against the node, like ccm's node.nodetool()."""
    exe = await manager.server_get_exe(server.server_id)
    return await asyncio.to_thread(subprocess.run, [exe, "nodetool", "-h", server.ip_addr, *args],
                                   check=True, capture_output=True, text=True)


def connect(manager: ScyllaClusterManager, server, exclusive=False):
    """A session whose statements default to consistency level ONE, as dtest sessions do."""
    if exclusive:
        cql = manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    else:
        cql = manager.con_gen([server.ip_addr]).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql


def create_cf(cql, table):
    """The dtests' create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})."""
    cql.execute(f"CREATE COLUMNFAMILY {table} (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {} "
                "AND read_repair_chance=0.000000")
    cql.execute(f"SELECT * FROM {table} LIMIT 1")


def insert_c1c2(cql, keys, consistency):
    stmt = cql.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
    stmt.consistency_level = consistency
    execute_concurrent_with_args(cql, stmt, [[f"k{k}"] for k in keys], concurrency=20)


def query_c1c2(cql, key, consistency):
    rows = list(cql.execute(SimpleStatement(f"SELECT c1, c2 FROM ks.cf WHERE key='k{key}'", consistency_level=consistency)))
    assert len(rows) == 1 and tuple(rows[0]) == ("value1", "value2"), rows


async def check_rows_on_node(manager: ScyllaClusterManager, servers, node, rows, found=(), missings=(), restart=True):
    """Stops the other nodes and checks the data the node has by itself."""
    stopped = []
    running = {s.server_id for s in await manager.running_servers()}
    for s in servers:
        if s.server_id in running and s.server_id != node.server_id:
            stopped.append(s)
            await manager.server_stop_gracefully(s.server_id)
    cql = connect(manager, node)
    result = list(cql.execute(f"SELECT * FROM ks.cf LIMIT {rows * 2}"))
    assert len(result) == rows, len(result)
    for k in found:
        query_c1c2(cql, k, ConsistencyLevel.ONE)
    for k in missings:
        res = list(cql.execute(SimpleStatement(f"SELECT c1, c2 FROM ks.cf WHERE key='k{k}'", consistency_level=ConsistencyLevel.ONE)))
        assert not res, res
    cql.shutdown()
    if restart:
        for s in stopped:
            await manager.server_start(s.server_id)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_simple_repair(manager: ScyllaClusterManager):
    """
    A node that was down while one key was written has 2000 of the 2001 keys,
    the other two nodes have all; after "nodetool repair ks" on a node with
    the key, the node has it too. The dtest's sequential and parallel
    variants run the same repair.
    """
    servers = await manager.servers_add(3, config=CONFIG, property_file=[{"dc": "datacenter1", "rack": f"rack{i}"} for i in (1, 2, 3)])
    node1, node2, node3 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")
    create_cf(cql, "ks.cf")

    insert_c1c2(cql, range(1000), ConsistencyLevel.ALL)
    await nodetool(manager, node3, "flush")
    await manager.server_stop_gracefully(node3.server_id)
    insert_c1c2(cql, (1000,), ConsistencyLevel.TWO)
    await manager.server_start(node3.server_id)
    insert_c1c2(cql, range(1001, 2001), ConsistencyLevel.ALL)
    for s in servers:
        await nodetool(manager, s, "flush")

    await check_rows_on_node(manager, servers, node3, 2000, missings=[1000])
    await check_rows_on_node(manager, servers, node1, 2001, found=[1000])
    await check_rows_on_node(manager, servers, node2, 2001, found=[1000])

    await nodetool(manager, node1, "repair", "ks")

    await check_rows_on_node(manager, servers, node3, 2001, found=[1000], restart=False)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_empty_vs_gcable_repair(manager: ScyllaClusterManager):
    """
    Repairing a partition that is empty on one node and only has tombstones
    older than gc_grace_seconds on the other needs no repair (CASSANDRA-8979):
    after the repair, no rows are returned, for a table with
    gc_grace_seconds=1 and one with the default. The dtest's sequential and
    parallel variants run the same repair.
    """
    servers = await manager.servers_add(2, config=CONFIG, property_file=[{"dc": "datacenter1", "rack": f"rack{i}"} for i in (1, 2)])
    node1, node2 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':2}")
    # Compaction is disabled, so the tombstones are not purged.
    cql.execute("""
        CREATE TABLE ks.cf1 (
            key text,
            c1 text,
            c2 text,
            PRIMARY KEY (key, c1)
        )
        WITH gc_grace_seconds=1
        AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
    """)
    time.sleep(0.5)
    cql.execute("""
        CREATE TABLE ks.cf2 (
            key text,
            c1 text,
            c2 text,
            PRIMARY KEY (key, c1)
        )
        WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
    """)
    time.sleep(0.5)

    # Only node1 gets the data and its tombstones.
    await manager.server_stop_gracefully(node2.server_id)
    for cf in ["cf1", "cf2"]:
        for i in range(10):
            for j in range(1000):
                cql.execute(SimpleStatement(f"INSERT INTO ks.{cf} (key, c1, c2) VALUES ('k{i}', 'v{j}', 'value')", consistency_level=ConsistencyLevel.ONE))
        await nodetool(manager, node1, "flush")
        # Half of the partitions get a partition tombstone, the rest row tombstones.
        for i in range(5):
            cql.execute(SimpleStatement(f"DELETE FROM ks.{cf} WHERE key='k{i}'", consistency_level=ConsistencyLevel.ONE))
        await nodetool(manager, node1, "flush")
        for i in range(5, 10):
            for j in range(1000):
                cql.execute(SimpleStatement(f"DELETE FROM ks.{cf} WHERE key='k{i}' AND c1='v{j}'", consistency_level=ConsistencyLevel.ONE))
        await nodetool(manager, node1, "flush")

    # Wait until cf1's tombstones are past gc_grace_seconds.
    time.sleep(2)

    await manager.server_start(node2.server_id)
    await nodetool(manager, node2, "repair", "ks")

    for cf in ["cf1", "cf2"]:
        for i in range(10):
            res = list(cql.execute(SimpleStatement(f"SELECT c1, c2 FROM ks.{cf} WHERE key='k{i}'", consistency_level=ConsistencyLevel.ALL)))
            assert not res, res


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_local_dc_repair(manager: ScyllaClusterManager):
    """
    With 2 nodes in dc1 and one in each of dc2 and dc3, a node of dc1 that
    missed a write gets it from "nodetool repair -local ks" run on the
    other node of dc1.
    """
    layout = [("dc1", "rack1"), ("dc1", "rack2"), ("dc2", "rack1"), ("dc3", "rack1")]
    servers = await manager.servers_add(4, config=CONFIG, property_file=[{"dc": dc, "rack": rack} for dc, rack in layout])
    node1, node2, node3, node4 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1, 'dc3':1};")
    cql.execute("USE ks")
    create_cf(cql, "cf")

    insert_c1c2(cql, range(1000), ConsistencyLevel.ALL)
    await nodetool(manager, node2, "flush")
    await manager.server_stop_gracefully(node2.server_id)
    insert_c1c2(cql, (1000,), ConsistencyLevel.THREE)
    await manager.server_start(node2.server_id)
    insert_c1c2(cql, range(1001, 2001), ConsistencyLevel.ALL)
    for s in servers:
        await nodetool(manager, s, "flush")

    await check_rows_on_node(manager, servers, node2, 2000, missings=[1000])
    for node in [node1, node3, node4]:
        await check_rows_on_node(manager, servers, node, 2001, found=[1000])

    await nodetool(manager, node1, "repair", "-local", "ks")

    await check_rows_on_node(manager, servers, node2, 2001, found=[1000], restart=False)
