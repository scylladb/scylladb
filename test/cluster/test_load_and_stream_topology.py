#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Load-and-stream of sstables written by clusters of other sizes, ported from
# scylla-dtest migration_test.py (TestLoadAndStream): sstables of a 4-node
# cluster are loaded into 2 nodes, sstables of a 2-node cluster into 4
# nodes, also through a single node, into nodes with different shard counts,
# into tables with a materialized view or a secondary index, and to the
# primary replicas only followed by repair. One test per dtest.
# The sstables, in the md format, are in
# test/resource/sstables/load_and_stream/<source cluster>/<source node>; each
# holds part of 1000 rows ('k<n>', 'customtext1<n>', 'customtext2<n>') of
# keyspace1.standard1 (cassandra-stress data in "-c-s-smp2"). Like the
# dtests, hinted handoff is off, the commitlog is synced in batch mode, and
# keyspaces use vnodes; refresh is run with nodetool.

import asyncio
import glob
import logging
import os
import shutil
import subprocess
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

SSTABLES_DIR = os.path.join(os.path.dirname(__file__), '..', 'resource', 'sstables', 'load_and_stream')
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "hinted_handoff_enabled": False, "commitlog_sync": "batch"}
KS, TABLE = "keyspace1", "standard1"
ROWS = 1000


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def start_cluster(manager: ScyllaClusterManager, racks, dc="datacenter1"):
    return await manager.servers_add(len(racks), config=CONFIG, property_file=[{"dc": dc, "rack": r} for r in racks])


def create_c1c2_table(cql, rf):
    cql.execute(f"CREATE KEYSPACE {KS} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}")
    cql.execute(f"CREATE COLUMNFAMILY {KS}.{TABLE} (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute(f"SELECT * FROM {KS}.{TABLE} LIMIT 1")


def create_stress_compatible_table(cql, rf):
    cql.execute(f"CREATE KEYSPACE {KS} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}")
    cql.execute(f"""CREATE TABLE {KS}.{TABLE}(
        key blob PRIMARY KEY, "C0" blob, "C1" blob, "C2" blob, "C3" blob, "C4" blob
        ) WITH bloom_filter_fp_chance = 0.01
        AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
        AND comment = ''
        AND compaction = {{'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '100'}}
        AND compression = {{}}
        AND crc_check_chance = 1.0
        AND default_time_to_live = 0
        AND gc_grace_seconds = 864000
        AND max_index_interval = 2048
        AND memtable_flush_period_in_ms = 0
        AND min_index_interval = 128
        AND speculative_retry = '99.0PERCENTILE'""")


async def nodetool(manager: ScyllaClusterManager, server, *args):
    exe = await manager.server_get_exe(server.server_id)
    res = await asyncio.to_thread(subprocess.run, [exe, "nodetool", "-h", server.ip_addr, *args], capture_output=True, text=True)
    assert res.returncode == 0, f"nodetool {' '.join(args)} failed: {res.stderr}"
    return res.stdout


async def copy_sstables_to_node(manager: ScyllaClusterManager, server, source):
    workdir = await manager.server_get_workdir(server.server_id)
    upload_dir = os.path.join(glob.glob(os.path.join(workdir, 'data', KS, f'{TABLE}-*'))[0], 'upload')
    os.makedirs(upload_dir, exist_ok=True)
    src_dir = os.path.join(SSTABLES_DIR, source, TABLE)
    for name in os.listdir(src_dir):
        shutil.copy2(os.path.join(src_dir, name), os.path.join(upload_dir, name))


async def load_and_stream(manager: ScyllaClusterManager, server, primary_replica_only=False):
    """nodetool refresh --load-and-stream, and the start and end of it in the log."""
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    args = ["refresh", "--load-and-stream"] + (["--primary-replica-only"] if primary_replica_only else []) + ["--", KS, TABLE]
    await nodetool(manager, server, *args)
    await log.wait_for(f"Loading new SSTables for keyspace={KS}, table={TABLE}, load_and_stream=true, "
                       f"primary_replica_only={str(primary_replica_only).lower()}", from_mark=mark, timeout=10)
    await log.wait_for(rf"(?:storage_service|sstables_loader) - Done loading new SSTables for keyspace={KS}, table={TABLE}, "
                       r"load_and_stream=true.*status=(.*)", from_mark=mark, timeout=10)


async def copy_and_load(manager, server, source, primary_replica_only=False):
    await copy_sstables_to_node(manager, server, source)
    await load_and_stream(manager, server, primary_replica_only)


async def wait_for_view(manager: ScyllaClusterManager, cql, servers, view, timeout=120):
    query = SimpleStatement(f"SELECT host_id, status FROM system_distributed.view_build_status "
                            f"WHERE keyspace_name = '{KS}' AND view_name = '{view}'", consistency_level=ConsistencyLevel.QUORUM)
    deadline = time.time() + timeout
    while len({r.host_id for r in cql.execute(query) if r.status == "SUCCESS"}) < len(servers):
        assert time.time() < deadline, f"view {view} is not built"
        await asyncio.sleep(0.5)


async def check_view_rows(manager: ScyllaClusterManager, servers, view):
    """The view has all the rows, read through each node; view updates of the
    loaded data are asynchronous, so retry for a minute."""
    for s in servers:
        cql = manager.con_gen([s.ip_addr]).connect()
        statement = SimpleStatement(f"SELECT COUNT(*) FROM {KS}.{view}", consistency_level=ConsistencyLevel.ONE)
        for attempt in range(60):
            count = cql.execute(statement).one()[0]
            if count == ROWS:
                break
            assert attempt < 59, f"Expected {ROWS} rows in {KS}.{view} on {s}, got {count}"
            await asyncio.sleep(1)


def query_c1c2(cql, n):
    rows = list(cql.execute(SimpleStatement(f"SELECT c1, c2 FROM {KS}.{TABLE} WHERE key='k{n}'", consistency_level=ConsistencyLevel.QUORUM)))
    assert [tuple(r) for r in rows] == [(f"customtext1{n}", f"customtext2{n}")]


def check_data(cql, view=None, index=False):
    for n in range(ROWS):
        query_c1c2(cql, n)
        if view:
            rows = cql.execute(SimpleStatement(f"select key from {KS}.{view} where c1 = 'customtext1{n}' and key='k{n}'",
                                               consistency_level=ConsistencyLevel.QUORUM))
            assert [r.key for r in rows] == [f"k{n}"]
        if index:
            rows = cql.execute(SimpleStatement(f"select key from {KS}.{TABLE} where c2 = 'customtext2{n}'",
                                               consistency_level=ConsistencyLevel.QUORUM))
            assert [r.key for r in rows] == [f"k{n}"]


MV = "test_mv"
INDEX = "c2_ind"


def create_view_or_index(cql, kind):
    if kind == "mv":
        cql.execute(f"CREATE MATERIALIZED VIEW {KS}.{MV} AS SELECT c1 FROM {KS}.{TABLE} "
                    "where c1 IS NOT NULL and key IS NOT NULL PRIMARY KEY (c1, key)")
    elif kind == "index":
        cql.execute(f"CREATE INDEX {INDEX} ON {KS}.{TABLE} (c2)")


async def check_view_or_index(manager, cql, servers, kind):
    if kind:
        view = MV if kind == "mv" else f"{INDEX}_index"
        await wait_for_view(manager, cql, servers, view)
        await check_view_rows(manager, servers, view)
    check_data(cql, view=MV if kind == "mv" else None, index=kind == "index")


async def decrease_cluster(manager: ScyllaClusterManager, kind):
    """The sstables of a 4-node cluster with RF=3 are loaded into a 2-node
    cluster with RF=2, two source nodes' worth through each node."""
    servers = await start_cluster(manager, ["rack1", "rack2"])
    node1, node2 = servers
    cql = manager.get_cql()
    create_c1c2_table(cql, rf=2)
    create_view_or_index(cql, kind)
    for source in ["node1", "node2"]:
        await copy_and_load(manager, node1, f"from-cluster-4-nodes/{source}")
    for source in ["node3", "node4"]:
        await copy_and_load(manager, node2, f"from-cluster-4-nodes/{source}")
    await check_view_or_index(manager, cql, servers, kind)


GATE = [pytest.mark.tier3, pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")]


def gate(f):
    for mark in reversed(GATE):
        f = mark(f)
    return f


# Ported from scylla-dtest migration_test.py::TestLoadAndStream::test_load_and_stream_decrease_cluster.
@gate
async def test_load_and_stream_decrease_cluster(manager: ScyllaClusterManager):
    await decrease_cluster(manager, None)


# Ported from scylla-dtest migration_test.py::TestLoadAndStream::test_load_and_stream_decrease_cluster_with_mv.
# The table has a materialized view.
@gate
async def test_load_and_stream_decrease_cluster_with_mv(manager: ScyllaClusterManager):
    await decrease_cluster(manager, "mv")


# Ported from scylla-dtest migration_test.py::TestLoadAndStream::test_load_and_stream_decrease_cluster_with_index_view.
# The table has a secondary index.
@gate
async def test_load_and_stream_decrease_cluster_with_index_view(manager: ScyllaClusterManager):
    await decrease_cluster(manager, "index")


async def increase_cluster(manager: ScyllaClusterManager, kind):
    """The sstables of a 2-node cluster with RF=2 are loaded into a 4-node
    cluster with RF=2 through node4 and node1."""
    servers = await start_cluster(manager, ["rack1", "rack1", "rack2", "rack2"])
    node1, node4 = servers[0], servers[3]
    cql = manager.get_cql()
    create_c1c2_table(cql, rf=2)
    create_view_or_index(cql, kind)
    await copy_sstables_to_node(manager, node4, "from-cluster-2-nodes/node1")
    await copy_sstables_to_node(manager, node1, "from-cluster-2-nodes/node2")
    for node in [node1, node4]:
        await load_and_stream(manager, node)
    await check_view_or_index(manager, cql, servers, kind)


# Ported from scylla-dtest migration_test.py::TestLoadAndStream::test_load_and_stream_increase_cluster.
@gate
async def test_load_and_stream_increase_cluster(manager: ScyllaClusterManager):
    await increase_cluster(manager, None)


# Ported from scylla-dtest migration_test.py::TestLoadAndStream::test_load_and_stream_increase_cluster_with_mv.
# The table has a materialized view.
@gate
async def test_load_and_stream_increase_cluster_with_mv(manager: ScyllaClusterManager):
    await increase_cluster(manager, "mv")


# Ported from scylla-dtest migration_test.py::TestLoadAndStream::test_load_and_stream_increase_cluster_with_index.
# The table has a secondary index.
@gate
async def test_load_and_stream_increase_cluster_with_index(manager: ScyllaClusterManager):
    await increase_cluster(manager, "index")


# Ported from scylla-dtest migration_test.py::TestLoadAndStream::test_load_and_stream_asymmetric_cluster.
# A 4-node cluster whose nodes 2, 3 and 4 are restarted with 2, 3 and 4
# shards; sstables of a 2-node RF=2 cluster are loaded through node1 and
# node3. As in the dtest, both source nodes' sstables are copied to node1's
# upload directory, so the second load-and-stream (on node3) finds an empty
# upload directory and node2's sstables stay unloaded in node1's.
@gate
async def test_load_and_stream_asymmetric_cluster(manager: ScyllaClusterManager):
    servers = await start_cluster(manager, ["rack1", "rack1", "rack2", "rack2"])
    node1, node3 = servers[0], servers[2]
    for i, node in enumerate(servers[1:]):
        await manager.server_stop_gracefully(node.server_id)
        await manager.others_not_see_server(node.ip_addr)
        await manager.server_start(node.server_id, cmdline_options_override=["--smp", str(i + 2)])
        await manager.servers_see_each_other(await manager.running_servers())
    cql, _ = await manager.get_ready_cql(servers)
    create_c1c2_table(cql, rf=2)
    for source, node in zip(["node1", "node2"], [node1, node3]):
        await copy_sstables_to_node(manager, node1, f"from-cluster-2-nodes/{source}")
        await load_and_stream(manager, node)
    check_data(cql)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_load_and_stream_increase_cluster_test_data_with_smp2(manager: ScyllaClusterManager):
    """The cassandra-stress sstables of a 2-node cluster with SMP 2 are
    loaded into a 4-node cluster in three racks with RF=3."""
    servers = await start_cluster(manager, ["rack1", "rack1", "rack2", "rack3"], dc="dc1")
    node3, node4 = servers[2:]
    create_stress_compatible_table(manager.get_cql(), rf=3)
    await copy_sstables_to_node(manager, node4, "from-cluster-2-nodes-c-s-smp2/node1")
    await copy_sstables_to_node(manager, node3, "from-cluster-2-nodes-c-s-smp2/node2")
    for node in [node3, node4]:
        await load_and_stream(manager, node)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_load_and_stream_from_one_node_increase_cluster(manager: ScyllaClusterManager):
    """All the sstables of a 2-node cluster are loaded into a 4-node cluster
    through one node, one source node at a time."""
    servers = await start_cluster(manager, ["rack1", "rack1", "rack2", "rack2"])
    node3 = servers[2]
    cql = manager.get_cql()
    create_c1c2_table(cql, rf=2)
    for source in ["from-cluster-2-nodes/node1", "from-cluster-2-nodes/node2"]:
        await copy_and_load(manager, node3, source)
    check_data(cql)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_load_and_stream_primary_replica_only(manager: ScyllaClusterManager):
    """
    The sstables of a node are streamed to the primary replicas only, and a
    primary-range repair on every node brings the data to all replicas.
    """
    servers = await start_cluster(manager, ["rack1", "rack1", "rack2"], dc="dc1")
    node2 = servers[1]
    cql = manager.get_cql()
    create_c1c2_table(cql, rf=2)
    await copy_and_load(manager, node2, "from-cluster-2-nodes/node1", primary_replica_only=True)
    for node in servers:
        await nodetool(manager, node, "repair", "-pr")
    check_data(cql)
