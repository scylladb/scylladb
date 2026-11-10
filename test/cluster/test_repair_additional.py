#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Repair of vnodes keyspaces through "nodetool repair", ported from
# scylla-dtest repair_additional_test.py: a table created while a node was
# down is repaired to it together with its schema, repair whose master is
# stopped midway, repair with a dead node, rows added to a cluster with a
# large partition, and repair of a single node. As on the dtest nodes,
# hinted handoff is off and sstable key validation is on. Data is checked on
# each node alone: the cluster is stopped and only that node is started.

import asyncio
import logging
import os
import random
import re
import string
import subprocess
import time

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, Unavailable
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
from cassandra.cluster import EXEC_PROFILE_DEFAULT, NoHostAvailable
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.util import dump_sstables, get_table_dir, stop_servers_gracefully
from test.cqlpy.util import run_cqlsh_at
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {"hinted_handoff_enabled": False, "enable_sstable_key_validation": True,
                  "tablets_mode_for_new_keyspaces": "disabled"}
# Clusters the dtests populate with a node count instead of a topology use
# the SimpleSnitch, with every node in datacenter1/rack1.
SIMPLE_SNITCH = {"endpoint_snitch": "SimpleSnitch"}
# So compaction start and end are logged, as in the dtests.
CMDLINE = ["--logger-log-level", "compaction=debug"]
NUM_OF_COLUMNS = 50
PARTITIONS = 100
ROWS_IN_PARTITION = 20
BIG_PARTITION_ROWS = 10000


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def rack_layout(dc, racks):
    return [{"dc": dc, "rack": r} for r in racks]


async def nodetool(manager: ScyllaClusterManager, server, *args, check=True):
    """Runs "scylla nodetool" against the node, like ccm's node.nodetool()."""
    exe = await manager.server_get_exe(server.server_id)
    return await asyncio.to_thread(subprocess.run, [exe, "nodetool", "-h", server.ip_addr, *args],
                                   check=check, capture_output=True, text=True)


def connect(manager: ScyllaClusterManager, server, exclusive=False, keyspace=None):
    if exclusive:
        cql = manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    else:
        cql = manager.con_gen([server.ip_addr]).connect()
    # Statements default to consistency level ONE, as in dtest sessions.
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    if keyspace:
        cql.set_keyspace(keyspace)
    return cql


def create_cf(cql, table, columns=("c1", "c2")):
    """The dtests' create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})."""
    cols = "".join(f", {c} text" for c in columns)
    cql.execute(f"CREATE COLUMNFAMILY {table} (key varchar PRIMARY KEY{cols}) WITH comment='test cf' "
                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {} "
                "AND read_repair_chance=0.000000")
    cql.execute(f"SELECT * FROM {table} LIMIT 1")


def insert_c1c2(cql, keys, consistency, cf="cf"):
    stmt = cql.prepare(f"INSERT INTO ks.{cf} (key, c1, c2) VALUES (?, 'value1', 'value2')")
    stmt.consistency_level = consistency
    execute_concurrent_with_args(cql, stmt, [[f"k{k}"] for k in keys], concurrency=20)


async def stop_all(manager: ScyllaClusterManager):
    """Stops the running servers gracefully and in parallel, as ccm's
    cluster.stop() does."""
    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in await manager.running_servers()))


async def start_all(manager: ScyllaClusterManager, servers):
    running = {s.server_id for s in await manager.running_servers()}
    for s in servers:
        if s.server_id not in running:
            await manager.server_start(s.server_id)


async def check_rows_on_node(manager: ScyllaClusterManager, node, rows, consistency_level=ConsistencyLevel.ONE):
    """Stops the cluster and starts only the node (which runs reshape after a
    repair, if needed), then counts the rows it has by itself."""
    await stop_all(manager)
    await manager.server_start(node.server_id)
    cql = connect(manager, node, exclusive=True, keyspace="ks")
    result = list(cql.execute(SimpleStatement(f"SELECT * FROM cf LIMIT {rows * 2}", consistency_level=consistency_level)))
    cql.shutdown()
    assert len(result) == rows, len(result)


async def repair_schema(manager: ScyllaClusterManager, repair_node):
    """
    A table created, and written to, while a node was down reaches that node
    with its data by repair, started on a node that has the table or on the
    node that missed it.
    """
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=CMDLINE,
                                        property_file=rack_layout("dc1", ["rack1", "rack2", "rack3"]))
    node1, node2, node3 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")

    # Create the table and the data on node1 and node3 only.
    await nodetool(manager, node2, "flush")
    await manager.server_stop_gracefully(node2.server_id)
    create_cf(cql, "ks.cf")
    insert_c1c2(cql, range(1000, 2000), ConsistencyLevel.QUORUM)

    await manager.server_start(node2.server_id)
    await nodetool(manager, servers[repair_node], "repair", "ks")

    for node in servers:
        await check_rows_on_node(manager, node, 1000)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_schema.
# The repair starts on a node that has the table.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_schema(manager: ScyllaClusterManager):
    await repair_schema(manager, 0)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_schema_2.
# The repair starts on the node that missed the table.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_schema_2(manager: ScyllaClusterManager):
    await repair_schema(manager, 1)


async def two_nodes_with_different_keys(manager: ScyllaClusterManager):
    """The dtests' populate(2) with RF=2, then 1000 keys only on node1 and
    another 1000 keys only on node2."""
    servers = await manager.servers_add(2, config={**DEFAULT_CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE,
                                        property_file=rack_layout("datacenter1", ["rack1", "rack1"]))
    node1, node2 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':2}")
    create_cf(cql, "ks.cf")
    cql.shutdown()

    await nodetool(manager, node2, "flush")
    await manager.server_stop_gracefully(node2.server_id)
    cql1 = connect(manager, node1, exclusive=True, keyspace="ks")
    insert_c1c2(cql1, range(1000, 2000), ConsistencyLevel.ONE)
    cql1.shutdown()
    await manager.server_start(node2.server_id)
    await nodetool(manager, node1, "flush")
    await manager.server_stop_gracefully(node1.server_id)
    cql2 = connect(manager, node2, exclusive=True, keyspace="ks")
    insert_c1c2(cql2, range(2000, 3000), ConsistencyLevel.ONE)
    cql2.shutdown()
    await manager.server_start(node1.server_id)
    return servers


async def repair_kill(manager: ScyllaClusterManager, kill_master):
    """Stopping a node of a repair, its master (node1) or the other node,
    soon after it starts stops the repair, but does not otherwise cause
    problems on the node left: it still has its 1000 keys, and up to the 1000
    keys of the other node, depending on how far the repair got."""
    node1, node2 = await two_nodes_with_different_keys(manager)
    stopped, left = (node1, node2) if kill_master else (node2, node1)
    log = await manager.server_open_log(node1.server_id)
    mark = await log.mark()
    repair = asyncio.create_task(nodetool(manager, node1, "repair", "ks", check=False))
    await log.wait_for("starting user-requested repair", from_mark=mark)
    await asyncio.sleep(random.uniform(0.0, 0.5))
    await manager.server_stop_gracefully(stopped.server_id)
    await repair

    cql = connect(manager, left, exclusive=True, keyspace="ks")
    count = len(list(cql.execute("SELECT * FROM cf LIMIT 3000")))
    cql.shutdown()
    assert 1000 <= count <= 2000
    await manager.server_start(stopped.server_id)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_kill_1.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_kill_1(manager: ScyllaClusterManager):
    await repair_kill(manager, kill_master=True)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_kill_2.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_kill_2(manager: ScyllaClusterManager):
    await repair_kill(manager, kill_master=False)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_kill_3.
# A repair master stopped right after the repair starts shuts down cleanly,
# without failing an assertion (#699).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_kill_3(manager: ScyllaClusterManager):
    node1, _ = await two_nodes_with_different_keys(manager)
    log = await manager.server_open_log(node1.server_id)
    mark = await log.mark()
    repair = asyncio.create_task(nodetool(manager, node1, "repair", "ks", check=False))
    await log.wait_for("starting user-requested repair", from_mark=mark)
    await manager.server_stop_gracefully(node1.server_id)
    await repair
    assert not await log.grep("Assertion .* failed.")


async def repair_with_down_node(manager: ScyllaClusterManager, racks, rf, down):
    """Nodes 1 and 2 got different data (1000 keys each, written with the
    other one down). Repair on node 2 fails while node `down` is down, but
    repairs what it can: after that node is back, repairing it makes all 2000
    keys available."""
    servers = await manager.servers_add(len(racks), config=DEFAULT_CONFIG, cmdline=CMDLINE,
                                        property_file=rack_layout("dc1", racks))
    node1, node2 = servers[:2]
    down = servers[down]
    cql = connect(manager, node1)
    cql.execute(f"CREATE KEYSPACE ks WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    create_cf(cql, "ks.cf")
    cql.shutdown()

    await nodetool(manager, node2, "flush")
    await manager.server_stop_gracefully(node2.server_id)
    cql1 = connect(manager, node1, exclusive=True, keyspace="ks")
    insert_c1c2(cql1, range(1000), ConsistencyLevel.ONE)
    cql1.shutdown()
    # Let the CL=ONE delayed replication succeed; the down node 2 is not a write target.
    await wait_background_writes_done(manager, node1)

    await manager.server_start(node2.server_id)
    await nodetool(manager, node1, "flush")
    await manager.server_stop_gracefully(node1.server_id)
    cql2 = connect(manager, node2, exclusive=True, keyspace="ks")
    insert_c1c2(cql2, range(1000, 2000), ConsistencyLevel.ONE)
    cql2.shutdown()
    await wait_background_writes_done(manager, node2)
    await manager.server_start(node1.server_id)

    await nodetool(manager, down, "flush")
    await manager.server_stop_gracefully(down.server_id)

    def count_on_node2():
        cql2 = connect(manager, node2, exclusive=True, keyspace="ks")
        n = len(list(cql2.execute("SELECT * from cf")))
        cql2.shutdown()
        return n

    assert (await nodetool(manager, node2, "repair", "ks", check=False)).returncode != 0

    await manager.server_start(down.server_id)

    await nodetool(manager, down, "repair", "ks")
    assert count_on_node2() == 2000


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_with_down_nodes_1a.
# Three nodes with RF=2, node 3 down.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_with_down_nodes_1a(manager: ScyllaClusterManager):
    await repair_with_down_node(manager, ["r1", "r1", "r2"], rf=2, down=2)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_with_down_nodes_2a.
# Four nodes with RF=3, node 4 down.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_with_down_nodes_2a(manager: ScyllaClusterManager):
    await repair_with_down_node(manager, ["r1", "r1", "r2", "r3"], rf=3, down=3)


async def repair_fails_with_replica_down(manager: ScyllaClusterManager, nodes, rf):
    """With node 3 down, a repair on node 2 fails: some of its ranges have a
    replica down. Default node configuration, as in the dtests."""
    servers = await manager.servers_add(nodes, config={"tablets_mode_for_new_keyspaces": "disabled", **SIMPLE_SNITCH},
                                        cmdline=CMDLINE, property_file=rack_layout("datacenter1", ["rack1"] * nodes))
    cql = connect(manager, servers[0])
    cql.execute(f"CREATE KEYSPACE ks WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    cql.execute("CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    # The dtests' create_cf() checks that the table exists by reading it.
    cql.execute("SELECT * FROM ks.cf LIMIT 1")
    cql.shutdown()
    await manager.server_stop_gracefully(servers[2].server_id)
    assert (await nodetool(manager, servers[1], "repair", "ks", check=False)).returncode != 0


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_with_down_nodes_1.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_with_down_nodes_1(manager: ScyllaClusterManager):
    await repair_fails_with_replica_down(manager, nodes=3, rf=2)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_with_down_nodes_2.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_with_down_nodes_2(manager: ScyllaClusterManager):
    await repair_fails_with_replica_down(manager, nodes=4, rf=3)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_with_down_nodes_2b.
# Four nodes with RF=3; nodes 1 and 2 got different keys written at CL=TWO.
# With node 4 down, repairs on node 2 and on node 3 fail, but still repair
# between the live replicas: node 2 ends with all 2000 keys.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_with_down_nodes_2b(manager: ScyllaClusterManager):
    servers = await manager.servers_add(4, config={**DEFAULT_CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE,
                                        property_file=rack_layout("datacenter1", ["rack1"] * 4))
    node1, node2, node3, node4 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")
    create_cf(cql, "ks.cf")
    cql.shutdown()

    await nodetool(manager, node2, "flush")
    await manager.server_stop_gracefully(node2.server_id)
    cql1 = connect(manager, node1, exclusive=True, keyspace="ks")
    insert_c1c2(cql1, range(1000), ConsistencyLevel.TWO)
    cql1.shutdown()
    # Let the CL=TWO delayed replication succeed; the down node 2 is not a write target.
    await wait_background_writes_done(manager, node1)
    await manager.server_start(node2.server_id)
    await nodetool(manager, node1, "flush")
    await manager.server_stop_gracefully(node1.server_id)
    cql2 = connect(manager, node2, exclusive=True, keyspace="ks")
    insert_c1c2(cql2, range(1000, 2000), ConsistencyLevel.TWO)
    cql2.shutdown()
    await wait_background_writes_done(manager, node2)
    await manager.server_start(node1.server_id)
    await nodetool(manager, node4, "flush")
    await manager.server_stop_gracefully(node4.server_id)

    for node in (node2, node3):
        assert (await nodetool(manager, node, "repair", "ks", check=False)).returncode != 0
    cql2 = connect(manager, node2, exclusive=True, keyspace="ks")
    assert len(list(cql2.execute("SELECT * from cf"))) == 2000
    cql2.shutdown()


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_during_update.
# A repair of two replicas with different data, running while more keys are
# written to both at CL=TWO, leaves both with all the keys.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_during_update(manager: ScyllaClusterManager, build_mode):
    num_keys = 1000 if build_mode in DEBUG_MODES else 10000
    (node1, node2), cql = await two_replicas(manager)
    create_cf(cql, "ks.cf")
    cql.shutdown()

    cql1 = await only_node_up(manager, node1, node2)
    insert_c1c2(cql1, range(num_keys), ConsistencyLevel.ONE)
    cql1.shutdown()
    await manager.server_start(node2.server_id)
    cql2 = await only_node_up(manager, node2, node1)
    insert_c1c2(cql2, range(num_keys, 2 * num_keys), ConsistencyLevel.ONE)
    cql2.shutdown()
    await manager.server_start(node1.server_id)

    repair = asyncio.create_task(nodetool(manager, node1, "repair", "ks", check=False))
    count = 2 * num_keys
    cql = connect(manager, node1, keyspace="ks")
    add_keys = 1 if build_mode in DEBUG_MODES else num_keys // 10
    while not repair.done():
        prev_count, count = count, count + add_keys
        await asyncio.to_thread(insert_c1c2, cql, range(prev_count, count), ConsistencyLevel.TWO)
    await repair
    cql.shutdown()
    await check_rows_on_node(manager, node1, count)
    await check_rows_on_node(manager, node2, count)


def insert_row_statement(table, pk, ck, random_string):
    return ("insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({pk}, {ck}, {int_values}, "
            "[{pk}, {ck}], {{'{s}'}}, {{{ck}: '{s}'}})").format(
        table_name=table,
        columns=", ".join(f"c{n}" for n in range(1, NUM_OF_COLUMNS)),
        int_values=", ".join(f"{n}" for n in range(1, NUM_OF_COLUMNS)),
        pk=pk, ck=ck, s=random_string)


def prefill_table_data(cql, partition_range_end, rows_in_partition, partition_range_start=1):
    def random_string():
        return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
    stmts = [(insert_row_statement("ks.cf", i, k, random_string()), ())
             for i in range(partition_range_start, partition_range_end + 1) for k in range(1, rows_in_partition + 1)]
    execute_concurrent(cql, stmts, concurrency=20, raise_on_first_error=True)


async def wait_background_writes_done(manager: ScyllaClusterManager, node, timeout=60):
    """The dtests' _wait_background_writes_done(): waits until the
    coordinator node has no in-flight background writes (writes whose CL was
    already acked)."""
    metric = "scylla_storage_proxy_coordinator_background_writes{"
    # A sum of 0 over no matching lines would pass: a renamed metric must fail here.
    assert any(metric in line for line in (await manager.metrics.query(node.ip_addr)).lines), \
        f"{node.ip_addr} does not export the background_writes gauge"
    deadline = time.time() + timeout
    while (await repair_metrics(manager, node, [re.escape(metric)]))[re.escape(metric)] != 0:
        assert time.time() < deadline, f"[{node.ip_addr}] Waiting for background writes to finish"
        await asyncio.sleep(0.5)


async def repair_metrics(manager: ScyllaClusterManager, server, names):
    """Sums the node's metrics whose lines match each name, like the dtests'
    get_node_metrics()."""
    metrics = await manager.metrics.query(server.ip_addr)
    res = {name: 0 for name in names}
    for line in metrics.lines:
        if line.startswith("#"):
            continue
        for name in names:
            if re.search(name, line):
                res[name] += float(line.split()[-1])
    return res


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_large_partition_new_rows(manager: ScyllaClusterManager):
    """
    New rows written while node 2 was down, to a table with 100 partitions of
    20 rows and one of 10000 rows, reach node 2 by repair on node 2: node 2
    receives exactly the new rows and sends none.
    """
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=CMDLINE,
                                        property_file=rack_layout("dc1", ["rack1", "rack2", "rack3"]))
    node1, repaired_node = servers[0], servers[1]
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")
    cql.execute("create table ks.cf (pk int, ck int, {}, clist list<int>, cset set<text>, cmap map<int, text>, PRIMARY KEY(pk, ck))".format(
        ", ".join(f"c{n} int" for n in range(1, NUM_OF_COLUMNS))))
    prefill_table_data(cql, PARTITIONS, ROWS_IN_PARTITION)
    big_partition = PARTITIONS + 1
    prefill_table_data(cql, big_partition, BIG_PARTITION_ROWS, partition_range_start=big_partition)
    cql.shutdown()
    total_rows = PARTITIONS * ROWS_IN_PARTITION + BIG_PARTITION_ROWS

    for s in servers:
        await nodetool(manager, s, "flush")
    await manager.server_stop_gracefully(repaired_node.server_id)

    cql = connect(manager, node1)
    cql.set_keyspace("ks")
    num_of_new_rows = 50
    for i in range(1, num_of_new_rows + 1):
        cql.execute(f"insert into cf (pk, ck) values ({big_partition + i}, {random.randint(1, ROWS_IN_PARTITION)})")
    cql.shutdown()

    await manager.server_start(repaired_node.server_id)
    await nodetool(manager, repaired_node, "repair", "ks")
    total_rows += num_of_new_rows

    # The repaired node receives the new rows and sends nothing.
    metrics = await repair_metrics(manager, repaired_node, ["tx_row_nr", "rx_row_nr", "tx_hashes_nr", "rx_hashes_nr"])
    assert metrics["tx_row_nr"] <= 0, metrics
    assert 0 < metrics["rx_row_nr"] <= num_of_new_rows, metrics

    for node in [node1, repaired_node]:
        await check_rows_on_node(manager, node, total_rows)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_one_node(manager: ScyllaClusterManager):
    """
    "nodetool repair" of a single node of a 3-node cluster with RF=3 repairs
    all its token ranges, primary or not: after keys were written to each
    node alone, every node has all the keys.
    """
    servers = await manager.servers_add(3, config={"hinted_handoff_enabled": False, "tablets_mode_for_new_keyspaces": "disabled", **SIMPLE_SNITCH},
                                        cmdline=CMDLINE, property_file=rack_layout("datacenter1", ["rack1"] * 3))
    num_nodes = len(servers)
    cql = connect(manager, servers[0])
    cql.execute(f"CREATE KEYSPACE ks WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'datacenter1' : {num_nodes} }};")
    cql.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY, v int) WITH compaction = {'class': 'NullCompactionStrategy'} AND speculative_retry = 'NONE'")
    cql.shutdown()

    num_keys = 100 * num_nodes

    async def on_node_alone(node, fn):
        others = [s for s in servers if s.server_id != node.server_id]
        await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in others))
        cql1 = connect(manager, node, exclusive=True, keyspace="ks")
        try:
            return fn(cql1)
        finally:
            cql1.shutdown()
            for s in others:
                await manager.server_start(s.server_id)

    def read(cql1):
        res = cql1.execute(SimpleStatement("SELECT * from ks.tbl", consistency_level=ConsistencyLevel.ONE))
        return [(r.pk, r.v) for r in sorted(res)]

    expected_values = []
    for i, node in enumerate(servers):
        expected_values.append([(pk, pk) for pk in range(i, num_keys, num_nodes)])

        def write(cql1, values=expected_values[i]):
            insert = cql1.prepare("INSERT INTO ks.tbl (pk, v) VALUES (?, ?)")
            for k, v in values:
                cql1.execute(insert, (k, v))
        await on_node_alone(node, write)

    for i, node in enumerate(servers):
        assert await on_node_alone(node, read) == expected_values[i]

    await nodetool(manager, random.choice(servers), "repair")

    for node in servers:
        assert await on_node_alone(node, read) == [(i, i) for i in range(num_keys)]


def repair_stats(log_lines):
    """The RPC calls (summed) and the duration (the longest) of the repairs
    logged in the "stats: repair_reason=repair" lines, like the dtest's
    get_repair_rpc_calls() and get_repair_duration()."""
    def value(line, key):
        m = re.search(rf"{key}=(\d+\.\d+|\d+)", line)
        return float(m.group(1)) if m else 0
    return sum(value(line, "rpc_call_nr") for line in log_lines), max((value(line, "duration") for line in log_lines), default=0)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_small_table_optimization.
# Two nodes in each of three DCs, a keyspace with two replicas in each: a
# repair of the keyspace with small_table_optimization makes over ten times
# fewer RPC calls, and finishes over ten times faster, than one without.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_small_table_optimization(manager: ScyllaClusterManager):
    servers = await manager.servers_add(6, config={"tablets_mode_for_new_keyspaces": "disabled"},
                                        property_file=[{"dc": dc, "rack": "rack1"} for dc in ("dc1", "dc1", "dc2", "dc2", "dc3", "dc3")])
    cql = connect(manager, servers[0])
    cql.execute("CREATE KEYSPACE test_repair WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2, 'dc3': 2}")
    create_cf(cql, "test_repair.cf")
    stats = []
    for node, optimization in [(servers[0], False), (servers[1], True)]:
        log = await manager.server_open_log(node.server_id)
        mark = await log.mark()
        params = {"small_table_optimization": "true"} if optimization else {}
        repair_id = await manager.api.client.post_json("/storage_service/repair_async/test_repair", host=node.ip_addr, params=params)
        assert await manager.api.client.get_json("/storage_service/repair_status", host=node.ip_addr, params={"id": str(repair_id)}) == "SUCCESSFUL"
        stats.append(repair_stats([line for line, _ in await log.grep("stats: repair_reason=repair", from_mark=mark)]))
    (regular_calls, regular_duration), (optimized_calls, optimized_duration) = stats
    assert regular_calls > 10 * optimized_calls, "Repair with small_table_optimization is supposed to have fewer rpc calls"
    assert regular_duration > 10 * optimized_duration, "Repair with small_table_optimization is supposed to finish faster"


async def two_replicas(manager: ScyllaClusterManager):
    """Two nodes in racks of their own and a keyspace with RF=2, as the dtests'
    populate(generate_cluster_topology(dc_num=1, rack_num=2)) and create_ks(session, "ks", 2)."""
    servers = await manager.servers_add(2, config=DEFAULT_CONFIG, cmdline=CMDLINE,
                                        property_file=rack_layout("dc1", ["rack1", "rack2"]))
    cql = connect(manager, servers[0])
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':2}")
    return servers, cql


async def only_node_up(manager: ScyllaClusterManager, node, other):
    """Flushes and stops other, and connects to node alone, as the dtests'
    other.flush(); other.stop(); patient_exclusive_cql_connection(node, "ks")."""
    await nodetool(manager, other, "flush")
    await manager.server_stop_gracefully(other.server_id)
    return connect(manager, node, exclusive=True, keyspace="ks")


async def repair_diverged_replicas(manager: ScyllaClusterManager, servers, change, check_changed, check_unchanged):
    """The dtests' scenario for two replicas: with node2 down, change the data
    on node1 (checked there), then check that node2 alone still has the old
    data, repair from node2 and check that node2 alone has the change. Returns
    the session of the last check, still open, with node1 stopped."""
    node1, node2 = servers
    cql1 = await only_node_up(manager, node1, node2)
    for stmt in change:
        cql1.execute(SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE))
    check_changed(list(cql1.execute("SELECT * from cf")))
    cql1.shutdown()

    await manager.server_start(node2.server_id)
    cql2 = await only_node_up(manager, node2, node1)
    check_unchanged(list(cql2.execute("SELECT * from cf")))
    cql2.shutdown()

    await manager.server_start(node1.server_id)
    await nodetool(manager, node2, "repair", "ks")
    cql2 = await only_node_up(manager, node2, node1)
    check_changed(list(cql2.execute("SELECT * from cf")))
    cql2.shutdown()


def key_row(key, c1, c2):
    def check(result):
        assert [(r.key, r.c1, r.c2) for r in result] == [(key, c1, c2)], result
    return check


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_cell_update.
# A cell updated on one of two replicas while the other was down is repaired
# to the other one.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_cell_update(manager: ScyllaClusterManager):
    servers, cql = await two_replicas(manager)
    create_cf(cql, "ks.cf")
    cql.execute(SimpleStatement("INSERT INTO ks.cf (key, c1, c2) VALUES ('key', 'hello', 'hi')", consistency_level=ConsistencyLevel.ALL))
    cql.shutdown()
    await repair_diverged_replicas(manager, servers, ["INSERT INTO cf (key, c1, c2) VALUES ('key', 'new', 'yo')"],
                                   key_row("key", "new", "yo"), key_row("key", "hello", "hi"))


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_cell_delete.
# A cell deleted on one of two replicas while the other was down: the
# deletion is repaired to the other one.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_cell_delete(manager: ScyllaClusterManager):
    servers, cql = await two_replicas(manager)
    create_cf(cql, "ks.cf")
    cql.execute(SimpleStatement("INSERT INTO ks.cf (key, c1, c2) VALUES ('key', 'hello', 'hi')", consistency_level=ConsistencyLevel.ALL))
    cql.shutdown()
    await repair_diverged_replicas(manager, servers, ["DELETE c1 FROM cf WHERE key ='key'"],
                                   key_row("key", None, "hi"), key_row("key", "hello", "hi"))


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_row_delete.
# A CQL row deleted on one of two replicas while the other was down: the
# row tombstone is repaired to the other one.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_row_delete(manager: ScyllaClusterManager):
    servers, cql = await two_replicas(manager)
    cql.execute("CREATE TABLE ks.cf (name text, pet text, age int, PRIMARY KEY ((name), pet)) WITH compression = {} AND read_repair_chance = 0.0;")
    for pet, age in [("kitty", 5), ("adamdami", 1)]:
        cql.execute(SimpleStatement(f"INSERT INTO ks.cf (name, pet, age) VALUES ('nadav', '{pet}', {age})", consistency_level=ConsistencyLevel.ALL))
    cql.shutdown()

    def deleted(result):
        assert [(r.name, r.pet, r.age) for r in result] == [("nadav", "adamdami", 1)], result

    def not_deleted(result):
        assert len(result) == 2, result
    await repair_diverged_replicas(manager, servers, ["DELETE FROM cf WHERE name = 'nadav' AND pet = 'kitty'"], deleted, not_deleted)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_partition_delete.
# Partitions deleted on one of two replicas while the other was down: the
# partition tombstones are repaired to the other one.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_partition_delete(manager: ScyllaClusterManager):
    servers, cql = await two_replicas(manager)
    create_cf(cql, "ks.cf")
    for k in (1, 2, 3):
        cql.execute(SimpleStatement(f"INSERT INTO ks.cf (key, c1, c2) VALUES ('k{k}', 'v{k}1', 'v{k}2')", consistency_level=ConsistencyLevel.ALL))
    cql.shutdown()

    def not_deleted(result):
        assert len(result) == 3, result
    await repair_diverged_replicas(manager, servers, ["DELETE FROM cf WHERE key = 'k1';", "DELETE FROM cf WHERE key = 'k3';"],
                                   key_row("k2", "v21", "v22"), not_deleted)


async def c1_cells(manager: ScyllaClusterManager, node):
    """The c1 cells in the sstables of ks.cf on node, as the dtest's _cells_with_name()."""
    return [ce["columns"]["c1"] for p in await dump_sstables(manager, node, "ks", "cf")
            for ce in p.get("clustering_elements", []) if "c1" in ce["columns"]]


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_ttl_update.
# As test_repair_cell_update, with the update setting a TTL: the sstables
# show the cell with its original TTL on node1, without a TTL on node2
# before the repair, and with the same TTL on node2 after it.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_ttl_update(manager: ScyllaClusterManager):
    servers, cql = await two_replicas(manager)
    node1, node2 = servers
    create_cf(cql, "ks.cf")
    cql.execute(SimpleStatement("INSERT INTO ks.cf (key, c1, c2) VALUES ('key', 'hello', 'hi')", consistency_level=ConsistencyLevel.ALL))
    cql.shutdown()

    cql1 = await only_node_up(manager, node1, node2)
    cql1.execute(SimpleStatement("UPDATE cf using TTL 1234 SET c1='new' WHERE key = 'key'", consistency_level=ConsistencyLevel.ONE))
    key_row("key", "new", "hi")(list(cql1.execute("SELECT * from cf")))
    cql1.shutdown()
    await nodetool(manager, node1, "flush")
    cells = await c1_cells(manager, node1)
    assert cells, "TTL set in sstable"
    c1_cell_ttl = cells[0]
    assert c1_cell_ttl.get("ttl") == "1234s", "TTL set to 1234"

    await manager.server_start(node2.server_id)
    cql2 = await only_node_up(manager, node2, node1)
    key_row("key", "hello", "hi")(list(cql2.execute("SELECT * from cf")))
    cql2.shutdown()
    cells = await c1_cells(manager, node2)
    if cells:
        assert "expiry" not in cells[0], "TTL should not be set"

    await manager.server_start(node1.server_id)
    await nodetool(manager, node2, "repair", "ks")
    cql2 = await only_node_up(manager, node2, node1)
    key_row("key", "new", "hi")(list(cql2.execute("SELECT * from cf")))
    cql2.shutdown()
    await nodetool(manager, node2, "flush")
    assert c1_cell_ttl in await c1_cells(manager, node2), "expected c1 value and timeout in sstable"


def assert_repair_option_pr_rows(cql, min_count, max_count, consistency_level=ConsistencyLevel.ONE):
    rows = list(cql.execute(SimpleStatement("SELECT * FROM cf", consistency_level=consistency_level)))
    count = cql.execute(SimpleStatement("SELECT count(*) from cf", consistency_level=consistency_level)).one()[0]
    assert count == len(rows), f"count {count} must be equal to len(rows)\nrows: {rows}"
    assert min_count <= count <= max_count, f"expected pr repair to repair between {min_count} to {max_count} rows, but count is {count}"


def connect_local_one(manager: ScyllaClusterManager, node):
    cql = connect(manager, node, exclusive=True, keyspace="ks")
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.LOCAL_ONE
    return cql


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_option_pr.
# Two nodes with RF=2 and 1000 keys written to each alone: a "-pr" repair on
# node1 repairs only the ranges node1 is primary for (about half the keys
# reach each node), and a second one on node2 repairs the rest.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_option_pr(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config={**DEFAULT_CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE,
                                        property_file=rack_layout("datacenter1", ["rack1", "rack1"]))
    node1, node2 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':2}")
    create_cf(cql, "ks.cf")
    cql.shutdown()

    cql1 = await only_node_up(manager, node1, node2)
    insert_c1c2(cql1, range(1000, 2000), ConsistencyLevel.ONE)
    cql1.shutdown()
    await nodetool(manager, node1, "flush")
    await manager.server_start(node2.server_id)
    cql2 = await only_node_up(manager, node2, node1)
    insert_c1c2(cql2, range(2000, 3000), ConsistencyLevel.ONE)
    cql2.shutdown()
    await manager.server_start(node1.server_id)

    await nodetool(manager, node1, "repair", "ks", "--partitioner-range")
    for node, other in [(node2, node1), (node1, node2)]:
        cql = await only_node_up(manager, node, other)
        assert_repair_option_pr_rows(cql, 1200, 1800)
        cql.shutdown()
        await manager.server_start(other.server_id)

    await nodetool(manager, node2, "repair", "ks", "--partitioner-range")
    await check_rows_on_node(manager, node1, 2000)
    await check_rows_on_node(manager, node2, 2000)


def dc_layout(nodes_per_dc):
    """ccm's populate([n, n, ...]): nodes_per_dc[i] nodes in dc<i+1>, all in rack1."""
    return [{"dc": f"dc{d + 1}", "rack": "rack1"} for d, n in enumerate(nodes_per_dc) for _ in range(n)]


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_option_pr_dc_host.
# "-pr" combined with "-dc" or "-hosts" is refused; "-pr -local" works, and
# makes "-pr" pick one primary per dc: on three DCs of two nodes, "-pr
# -local" repairs of node1_1 and node1_2 together repair all of dc1.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_option_pr_dc_host(manager: ScyllaClusterManager):
    servers = await manager.servers_add(6, config=DEFAULT_CONFIG, cmdline=CMDLINE, property_file=dc_layout([2, 2, 2]))
    node1_1, node1_2 = servers[:2]
    cql = connect(manager, node1_1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'dc1':2, 'dc2':2, 'dc3':2}")
    create_cf(cql, "ks.cf")
    cql.shutdown()

    for opts in (["--in-dc", "dc1,dc3"], ["--in-hosts", node1_1.ip_addr], ["--in-dc", "dc1"]):
        assert (await nodetool(manager, node1_1, "repair", "ks", *opts, "--partitioner-range", check=False)).returncode != 0, opts
    await nodetool(manager, node1_1, "repair", "ks", "--in-local-dc", "--partitioner-range")

    for s in servers[1:]:
        await manager.server_stop_gracefully(s.server_id)
    cql1 = connect_local_one(manager, node1_1)
    insert_c1c2(cql1, range(1000, 2000), ConsistencyLevel.LOCAL_ONE)
    cql1.shutdown()
    await nodetool(manager, node1_1, "flush")
    await manager.server_start(node1_2.server_id)
    await manager.server_stop_gracefully(node1_1.server_id)
    cql2 = connect_local_one(manager, node1_2)
    insert_c1c2(cql2, range(2000, 3000), ConsistencyLevel.LOCAL_ONE)
    cql2.shutdown()
    await start_all(manager, servers)

    await nodetool(manager, node1_1, "repair", "ks", "--in-local-dc", "--partitioner-range")
    for node, other in [(node1_2, node1_1), (node1_1, node1_2)]:
        await nodetool(manager, other, "flush")
        await manager.server_stop_gracefully(other.server_id)
        cql = connect_local_one(manager, node)
        assert_repair_option_pr_rows(cql, 1200, 1800, consistency_level=ConsistencyLevel.LOCAL_ONE)
        cql.shutdown()
        await manager.server_start(other.server_id)

    await nodetool(manager, node1_2, "repair", "ks", "--in-local-dc", "--partitioner-range")
    await check_rows_on_node(manager, node1_1, 2000, consistency_level=ConsistencyLevel.LOCAL_ONE)
    await check_rows_on_node(manager, node1_2, 2000, consistency_level=ConsistencyLevel.LOCAL_ONE)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_option_pr_multi_dc.
# Without "-local", a "-pr" repair on one node of a multi-DC cluster repairs
# only the ranges it is the cluster-wide primary for; a "-pr" repair on every
# node repairs everything.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_option_pr_multi_dc(manager: ScyllaClusterManager, build_mode):
    num_dcs, num_keys = (2, 1000) if build_mode in DEBUG_MODES else (3, 3000)
    servers = await manager.servers_add(2 * num_dcs, config=DEFAULT_CONFIG, cmdline=CMDLINE, property_file=dc_layout([2] * num_dcs))
    node1_1, node1_2 = servers[:2]
    cql = connect(manager, node1_1)
    replication = ", ".join(f"'dc{d + 1}':2" for d in range(num_dcs))
    cql.execute(f"CREATE KEYSPACE ks WITH replication={{'class':'NetworkTopologyStrategy', {replication}}}")
    create_cf(cql, "ks.cf")
    cql.shutdown()

    for s in servers[1:]:
        await manager.server_stop_gracefully(s.server_id)
    cql1 = connect_local_one(manager, node1_1)
    insert_c1c2(cql1, range(num_keys, 2 * num_keys), ConsistencyLevel.LOCAL_ONE)
    cql1.shutdown()
    await nodetool(manager, node1_1, "flush")
    await manager.server_start(node1_2.server_id)
    await manager.server_stop_gracefully(node1_1.server_id)
    cql2 = connect_local_one(manager, node1_2)
    insert_c1c2(cql2, range(2 * num_keys, 3 * num_keys), ConsistencyLevel.LOCAL_ONE)
    cql2.shutdown()
    await start_all(manager, servers)

    await nodetool(manager, node1_1, "repair", "ks", "--partitioner-range")
    await nodetool(manager, node1_2, "flush")
    await manager.server_stop_gracefully(node1_2.server_id)
    cql1 = connect_local_one(manager, node1_1)
    assert_repair_option_pr_rows(cql1, int(num_keys * 1.05), int(num_keys * 1.667), consistency_level=ConsistencyLevel.LOCAL_ONE)
    cql1.shutdown()
    await manager.server_start(node1_2.server_id)

    for node in servers[1:]:
        await nodetool(manager, node, "repair", "ks", "--partitioner-range")
    for node in servers:
        await check_rows_on_node(manager, node, 2 * num_keys, consistency_level=ConsistencyLevel.LOCAL_ONE)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_option_cf.
# A repair given a list of tables repairs only those tables; one without a
# list repairs them all.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_option_cf(manager: ScyllaClusterManager):
    (node1, node2), cql = await two_replicas(manager)
    for cf in ("cf1", "cf2", "cf3"):
        create_cf(cql, f"ks.{cf}", columns=("c1",))
    cql.shutdown()

    def insert(cql, suffix):
        for t in (1, 2, 3):
            cql.execute(SimpleStatement(f"INSERT INTO cf{t} (key, c1) VALUES ('k{t}1{suffix}', 'v{t}1{suffix}')", consistency_level=ConsistencyLevel.ONE))

    def counts(cql):
        return [len(list(cql.execute(f"SELECT * from cf{t}"))) for t in (1, 2, 3)]

    cql1 = await only_node_up(manager, node1, node2)
    insert(cql1, "")
    cql1.shutdown()
    await nodetool(manager, node1, "flush")
    await manager.server_start(node2.server_id)
    cql2 = await only_node_up(manager, node2, node1)
    insert(cql2, "a")
    cql2.shutdown()
    await manager.server_start(node1.server_id)

    await nodetool(manager, node1, "repair", "ks", "cf1", "cf3")
    cql2 = await only_node_up(manager, node2, node1)
    assert counts(cql2) == [2, 1, 2]
    cql2.shutdown()
    await manager.server_start(node1.server_id)
    cql1 = await only_node_up(manager, node1, node2)
    assert counts(cql1) == [2, 1, 2]
    cql1.shutdown()

    await manager.server_start(node2.server_id)
    await nodetool(manager, node1, "repair", "ks")
    cql2 = await only_node_up(manager, node2, node1)
    assert counts(cql2)[1] == 2
    cql2.shutdown()


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_option_invalid_ks_cf.
# A repair of a keyspace or table that does not exist fails.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_option_invalid_ks_cf(manager: ScyllaClusterManager):
    (node1, _), cql = await two_replicas(manager)
    create_cf(cql, "ks.cf", columns=("c1",))
    for args in (["ks", "badcf"], ["badks"], ["badks", "cf", "badcf"]):
        assert (await nodetool(manager, node1, "repair", *args, check=False)).returncode != 0, args
    await nodetool(manager, node1, "repair", "ks", "cf")
    cql.shutdown()


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_option_dc.
# With two nodes in dc1 and one in each of dc2 and dc3, a row written only to
# node1 reaches by a "-dc dc1,dc3" repair the nodes of dc1 and dc3 only; a
# repair naming an unknown dc, or not the local one, fails; a repair without
# options reaches every dc, and a "-local" one only the local dc.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_option_dc(manager: ScyllaClusterManager):
    servers = await manager.servers_add(4, config=DEFAULT_CONFIG, cmdline=CMDLINE, property_file=[
        {"dc": "dc1", "rack": "rack1_1"}, {"dc": "dc1", "rack": "rack1_2"}, {"dc": "dc2", "rack": "rack2"}, {"dc": "dc3", "rack": "rack3"}])
    node1, node2, node3, node4 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2' : 1, 'dc3': 1};")
    create_cf(cql, "ks.cf", columns=("c1",))

    async def stop(*nodes):
        for n in nodes:
            await nodetool(manager, n, "flush")
            await manager.server_stop_gracefully(n.server_id)

    async def start(*nodes):
        for n in nodes:
            await manager.server_start(n.server_id)

    def count_on(node):
        c = connect(manager, node, exclusive=True, keyspace="ks")
        n = len(list(c.execute("SELECT * from cf")))
        c.shutdown()
        return n

    async def counts_on_each(nodes, prev):
        """Each node's rows, read with only that node up: the node is started
        and the previous one (prev, the only one up) stopped."""
        res = []
        for n in nodes:
            await start(n)
            await stop(prev)
            res.append(count_on(n))
            prev = n
        return res

    await stop(node2, node3, node4)
    cql.execute(SimpleStatement("INSERT INTO ks.cf (key, c1) VALUES ('k11', 'v11')", consistency_level=ConsistencyLevel.ONE))
    cql.shutdown()
    await start(node2, node3, node4)

    await nodetool(manager, node1, "repair", "ks", "--in-dc", "dc1,dc3")
    await stop(node1, node3, node4)
    assert count_on(node2) == 1, "cf on node2"
    assert await counts_on_each([node3, node4], node2) == [0, 1]

    await start(node1, node2, node3)
    for dcs in ("dc1,baddc", "dc2,dc3"):
        assert (await nodetool(manager, node1, "repair", "ks", "--in-dc", dcs, check=False)).returncode != 0, dcs

    await nodetool(manager, node1, "repair", "ks")
    await stop(node1, node2, node4)
    assert count_on(node3) == 1, "cf on node3"

    await start(node1)
    await stop(node3)
    cql1 = connect(manager, node1, exclusive=True, keyspace="ks")
    cql1.execute(SimpleStatement("INSERT INTO cf (key, c1) VALUES ('k12', 'v12')", consistency_level=ConsistencyLevel.ONE))
    cql1.shutdown()
    await start(node2, node3, node4)
    await nodetool(manager, node1, "repair", "ks", "--in-local-dc")
    await stop(node1, node3, node4)
    assert count_on(node2) == 2, "cf on node2"
    assert await counts_on_each([node3, node4], node2) == [1, 1]


async def repair_multiple(manager: ScyllaClusterManager, partitioner_range):
    """1000 keys written to each of three RF=3 replicas alone, then repairs
    started in parallel on all three nodes: every node ends with all keys."""
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=CMDLINE,
                                        property_file=rack_layout("dc1", ["rack1", "rack2", "rack3"]))
    node1, node2, node3 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")
    create_cf(cql, "ks.cf")
    cql.shutdown()

    for n in (node2, node3):
        await nodetool(manager, n, "flush")
        await manager.server_stop_gracefully(n.server_id)
    for node, keys, prev in [(node1, range(1000, 2000), None), (node2, range(2000, 3000), node1), (node3, range(3000, 4000), node2)]:
        if prev:
            await manager.server_start(node.server_id)
            await nodetool(manager, prev, "flush")
            await manager.server_stop_gracefully(prev.server_id)
        c = connect(manager, node, exclusive=True, keyspace="ks")
        insert_c1c2(c, keys, ConsistencyLevel.ONE)
        c.shutdown()
        if node is node1:
            await nodetool(manager, node1, "flush")
    await start_all(manager, servers)

    opts = ["--partitioner-range"] if partitioner_range else []
    await asyncio.gather(*(nodetool(manager, n, "repair", "ks", *opts) for n in servers))
    for n in servers:
        await check_rows_on_node(manager, n, 3000)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_multiple.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_multiple(manager: ScyllaClusterManager):
    await repair_multiple(manager, partitioner_range=False)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_multiple_pr.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_multiple_pr(manager: ScyllaClusterManager):
    await repair_multiple(manager, partitioner_range=True)


async def nodes_in_racks(manager: ScyllaClusterManager, smps, config=DEFAULT_CONFIG):
    """One node per rack in dc1, as generate_cluster_topology(dc_num=1, rack_num=len(smps)),
    started with the given shard counts (None: the default, 2 as in ccm)."""
    servers = []
    for i, smp in enumerate(smps):
        servers.append(await manager.server_add(config=config, cmdline=CMDLINE + (["--smp", str(smp)] if smp else []),
                                                property_file={"dc": "dc1", "rack": f"rack{i + 1}"}))
    return servers


# The shard counts of the dtests' _diff_shard_count variants: the last node
# (the one repaired from) has 3 shards, the others 2.
SAME_SHARDS_2, DIFF_SHARDS_2 = [None, None], [2, 3]
SAME_SHARDS_3, DIFF_SHARDS_3 = [None, None, None], [2, 2, 3]


async def check_repair_tx_rx_rows(manager: ScyllaClusterManager, node, expected_tx_row_nr, expected_rx_row_nr):
    """The rows the node sent and received in all its repairs, summed over the
    "stats: repair_reason=repair" lines of its log."""
    log = await manager.server_open_log(node.server_id)
    tx = rx = 0
    for line, _ in await log.grep("stats: repair_reason=repair"):
        tx += int(re.search(r"tx_row_nr=(\d*)", line).group(1))
        rx += int(re.search(r"rx_row_nr=(\d*)", line).group(1))
    assert (tx, rx) == (expected_tx_row_nr, expected_rx_row_nr)


async def create_ks_cf(manager: ScyllaClusterManager, node, rf):
    cql = connect(manager, node)
    cql.execute(f"CREATE KEYSPACE ks WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    create_cf(cql, "ks.cf")
    return cql


async def repair_one_row(manager: ScyllaClusterManager, smps, delete):
    """10000 keys on both replicas; then one more key inserted (or one key
    deleted) on node1 alone. A repair on node2 receives that one row and
    sends none."""
    node1, node2 = await nodes_in_racks(manager, smps)
    nr_rows = 10000
    cql = await create_ks_cf(manager, node1, 2)
    insert_c1c2(cql, range(nr_rows if delete else nr_rows - 1), ConsistencyLevel.ALL)
    cql.shutdown()
    cql1 = await only_node_up(manager, node1, node2)
    if delete:
        cql1.execute(SimpleStatement("DELETE FROM cf WHERE key ='key1'", consistency_level=ConsistencyLevel.ONE))
    else:
        insert_c1c2(cql1, range(nr_rows - 1, nr_rows), ConsistencyLevel.ONE)
    cql1.shutdown()
    await manager.server_start(node2.server_id)
    await nodetool(manager, node2, "repair", "ks")
    await check_repair_tx_rx_rows(manager, node2, expected_tx_row_nr=0, expected_rx_row_nr=1)
    await check_rows_on_node(manager, node1, nr_rows)
    await check_rows_on_node(manager, node2, nr_rows)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_one_missing_row.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_one_missing_row(manager: ScyllaClusterManager):
    await repair_one_row(manager, SAME_SHARDS_2, delete=False)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_one_missing_row_diff_shard_count.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_one_missing_row_diff_shard_count(manager: ScyllaClusterManager):
    await repair_one_row(manager, DIFF_SHARDS_2, delete=False)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_one_deleted_row.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_one_deleted_row(manager: ScyllaClusterManager):
    await repair_one_row(manager, SAME_SHARDS_2, delete=True)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_one_deleted_row_diff_shard_count.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_one_deleted_row_diff_shard_count(manager: ScyllaClusterManager):
    await repair_one_row(manager, DIFF_SHARDS_2, delete=True)


async def write_alone(manager: ScyllaClusterManager, servers, steps):
    """The dtests' sequence of writes to single nodes. Starts with only the
    first step's node up (the others are flushed and stopped); each step
    (node, keys, consistency, stop) starts its node, then flushes and stops the
    node `stop` (if any), and writes the keys through its node alone."""
    for step, (node, keys, cl, stop) in enumerate(steps):
        running = {s.server_id for s in await manager.running_servers()}
        if node.server_id not in running:
            await manager.server_start(node.server_id)
        if stop:
            await nodetool(manager, stop, "flush")
            await manager.server_stop_gracefully(stop.server_id)
        c = connect(manager, node, exclusive=True, keyspace="ks")
        insert_c1c2(c, keys, cl)
        c.shutdown()
        if step == 0:
            # self.cluster.flush(): node1 is the only node up.
            await nodetool(manager, node, "flush")


async def stop_all_but(manager: ScyllaClusterManager, servers, node):
    for s in servers:
        if s.server_id != node.server_id:
            await nodetool(manager, s, "flush")
            await manager.server_stop_gracefully(s.server_id)


async def repair_disjoint_row_2nodes(manager: ScyllaClusterManager, smps):
    """1000 different keys on each of two replicas: a repair on node2 sends
    1000 rows and receives 1000."""
    servers = await nodes_in_racks(manager, smps)
    node1, node2 = servers
    (await create_ks_cf(manager, node1, 2)).shutdown()
    await stop_all_but(manager, servers, node1)
    await write_alone(manager, servers, [(node1, range(1000), ConsistencyLevel.ONE, None),
                                         (node2, range(1000, 2000), ConsistencyLevel.ONE, node1)])
    await manager.server_start(node1.server_id)
    await nodetool(manager, node2, "repair", "ks")
    await check_repair_tx_rx_rows(manager, node2, expected_tx_row_nr=1000, expected_rx_row_nr=1000)
    await check_rows_on_node(manager, node1, 2000)
    await check_rows_on_node(manager, node2, 2000)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_disjoint_row_2nodes.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_disjoint_row_2nodes(manager: ScyllaClusterManager):
    await repair_disjoint_row_2nodes(manager, SAME_SHARDS_2)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_disjoint_row_2nodes_diff_shard_count.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_disjoint_row_2nodes_diff_shard_count(manager: ScyllaClusterManager):
    await repair_disjoint_row_2nodes(manager, DIFF_SHARDS_2)


async def repair_disjoint_row_3nodes(manager: ScyllaClusterManager, smps):
    """1000 different keys on each of three replicas: a repair on node3
    sends 4000 rows and receives 2000. Returns the nodes, node3 alone up."""
    servers = await nodes_in_racks(manager, smps)
    node1, node2, node3 = servers
    (await create_ks_cf(manager, node1, 3)).shutdown()
    await stop_all_but(manager, servers, node1)
    await write_alone(manager, servers, [(node1, range(1000, 2000), ConsistencyLevel.ONE, None),
                                         (node2, range(2000, 3000), ConsistencyLevel.ONE, node1),
                                         (node3, range(3000, 4000), ConsistencyLevel.ONE, node2)])
    await start_all(manager, servers)
    await nodetool(manager, node3, "repair", "ks")
    await check_repair_tx_rx_rows(manager, node3, expected_tx_row_nr=4000, expected_rx_row_nr=2000)
    for node in servers:
        await check_rows_on_node(manager, node, 3000)
    return servers


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_disjoint_row_3nodes.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_disjoint_row_3nodes(manager: ScyllaClusterManager):
    await repair_disjoint_row_3nodes(manager, SAME_SHARDS_3)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_disjoint_row_3nodes_diff_shard_count.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_disjoint_row_3nodes_diff_shard_count(manager: ScyllaClusterManager):
    await repair_disjoint_row_3nodes(manager, DIFF_SHARDS_3)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_no_streaming_on_second_repair.
# After test_repair_disjoint_row_3nodes, a second repair on node3 sends and
# receives no rows.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_no_streaming_on_second_repair(manager: ScyllaClusterManager):
    servers = await repair_disjoint_row_3nodes(manager, SAME_SHARDS_3)
    await start_all(manager, servers)
    await nodetool(manager, servers[2], "repair", "ks")
    await check_repair_tx_rx_rows(manager, servers[2], expected_tx_row_nr=4000, expected_rx_row_nr=2000)


async def repair_joint_row_3nodes_same_value(manager: ScyllaClusterManager, smps):
    """Keys 10-15 on node1, 25-30 on node2, 15-20 on nodes 1 and 3 and 20-25
    on nodes 2 and 3, the same values on both: a repair on node3 receives the
    10 rows only nodes 1 and 2 have and sends 20."""
    servers = await nodes_in_racks(manager, smps)
    node1, node2, node3 = servers
    (await create_ks_cf(manager, node1, 3)).shutdown()
    await stop_all_but(manager, servers, node1)
    await write_alone(manager, servers, [(node1, range(10, 15), ConsistencyLevel.ONE, None),
                                         (node2, range(25, 30), ConsistencyLevel.ONE, node1)])
    # Keys 20-25 at CL=TWO through node3 with node2 up, then 15-20 through
    # node1 with node3 up.
    await manager.server_start(node3.server_id)
    c = connect(manager, node3, exclusive=True, keyspace="ks")
    insert_c1c2(c, range(20, 25), ConsistencyLevel.TWO)
    c.shutdown()
    await nodetool(manager, node2, "flush")
    await manager.server_stop_gracefully(node2.server_id)
    await manager.server_start(node1.server_id)
    c = connect(manager, node1, exclusive=True, keyspace="ks")
    insert_c1c2(c, range(15, 20), ConsistencyLevel.TWO)
    c.shutdown()
    await manager.server_start(node2.server_id)

    await nodetool(manager, node3, "repair", "ks")
    await check_repair_tx_rx_rows(manager, node3, expected_tx_row_nr=20, expected_rx_row_nr=10)
    for node in servers:
        await check_rows_on_node(manager, node, 20)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_joint_row_3nodes_1.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_joint_row_3nodes_1(manager: ScyllaClusterManager):
    await repair_joint_row_3nodes_same_value(manager, SAME_SHARDS_3)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_joint_row_3nodes_1_diff_shard_count.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_joint_row_3nodes_1_diff_shard_count(manager: ScyllaClusterManager):
    await repair_joint_row_3nodes_same_value(manager, DIFF_SHARDS_3)


async def repair_joint_row_3nodes_diff_value(manager: ScyllaClusterManager, smps):
    """Keys 10-20 on node1, 20-30 on node2 and 15-25 on node3, the overlaps
    written separately (so with different timestamps): a repair on node3
    receives 20 rows and sends 40."""
    servers = await nodes_in_racks(manager, smps)
    node1, node2, node3 = servers
    (await create_ks_cf(manager, node1, 3)).shutdown()
    await stop_all_but(manager, servers, node1)
    await write_alone(manager, servers, [(node1, range(10, 20), ConsistencyLevel.ONE, None),
                                         (node2, range(20, 30), ConsistencyLevel.ONE, node1),
                                         (node3, range(15, 25), ConsistencyLevel.ONE, node2)])
    await start_all(manager, servers)
    await nodetool(manager, node3, "repair", "ks")
    await check_repair_tx_rx_rows(manager, node3, expected_tx_row_nr=40, expected_rx_row_nr=20)
    for node in servers:
        await check_rows_on_node(manager, node, 20)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_joint_row_3nodes_2.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_joint_row_3nodes_2(manager: ScyllaClusterManager):
    await repair_joint_row_3nodes_diff_value(manager, SAME_SHARDS_3)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_joint_row_3nodes_2_diff_shard_count.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_joint_row_3nodes_2_diff_shard_count(manager: ScyllaClusterManager):
    await repair_joint_row_3nodes_diff_value(manager, DIFF_SHARDS_3)


async def repair_same_row_diff_value_3nodes(manager: ScyllaClusterManager, smps):
    """The same three rows written with different values to each of three
    replicas alone (populate(3), hinted handoff off): a repair on node3
    receives 6 rows and sends 12, and the rows it writes compact."""
    config = {"hinted_handoff_enabled": False, "tablets_mode_for_new_keyspaces": "disabled", **SIMPLE_SNITCH}
    servers = []
    for smp in smps:
        servers.append(await manager.server_add(config=config, cmdline=CMDLINE + (["--smp", str(smp)] if smp else []),
                                                property_file={"dc": "datacenter1", "rack": "rack1"}))
    node1, node2, node3 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 };")
    cql.execute("CREATE TABLE ks.tb (pk int, ck int, c0 int, c1 int, PRIMARY KEY(pk, ck));")
    cql.shutdown()
    nr_rows = 3
    await manager.server_stop_gracefully(node2.server_id)
    await manager.server_stop_gracefully(node3.server_id)
    for node, value, stop in [(node1, 1, None), (node2, 2, node1), (node3, 3, node2)]:
        if stop:
            await manager.server_start(node.server_id)
            await manager.server_stop_gracefully(stop.server_id)
        c = connect(manager, node)
        for ck in range(nr_rows):
            c.execute(f"INSERT into ks.tb (pk,ck,c0,c1) values (0, {ck}, {value}, {value})")
        c.shutdown()
        if node is node1:
            await nodetool(manager, node1, "flush")
    await start_all(manager, servers)
    await nodetool(manager, node3, "repair", "ks")
    for s in servers:
        await nodetool(manager, s, "compact")
    await check_repair_tx_rx_rows(manager, node3, expected_tx_row_nr=4 * nr_rows, expected_rx_row_nr=2 * nr_rows)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_same_row_diff_value_3nodes.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_same_row_diff_value_3nodes(manager: ScyllaClusterManager):
    await repair_same_row_diff_value_3nodes(manager, SAME_SHARDS_3)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_same_row_diff_value_3nodes_diff_shard_count.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_same_row_diff_value_3nodes_diff_shard_count(manager: ScyllaClusterManager):
    await repair_same_row_diff_value_3nodes(manager, DIFF_SHARDS_3)


async def repair_and_check_completed(manager: ScyllaClusterManager, node, ks, cf, mark, aux_cf=None):
    """The dtests' _run_repair_and_check_completed(): a repair of the table,
    which shuts down its off-strategy compaction updater and completes; then
    optionally a repair of another table."""
    await nodetool(manager, node, "repair", ks, cf)
    log = await manager.server_open_log(node.server_id)
    await log.wait_for("repair - repair.*: Started to shutdown off-strategy compaction updater",
                       "repair - repair.*: Finished to shutdown off-strategy compaction updater",
                       "repair - repair.* completed successfully", from_mark=mark, timeout=120)
    if aux_cf:
        await nodetool(manager, node, "repair", ks, aux_cf)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_triggering_off_strategy_compaction.
# A repair of a table that received rows starts off-strategy compaction of
# the table (here triggered through the REST API instead of waiting for its
# timer); a repair of another table does not get in the way.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_triggering_off_strategy_compaction(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config={**DEFAULT_CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE,
                                        property_file=rack_layout("datacenter1", ["rack1", "rack1"]))
    node1, node2 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':2}")
    create_cf(cql, "ks.cf1")
    create_cf(cql, "ks.cf2")
    cql.shutdown()

    cql1 = await only_node_up(manager, node1, node2)
    insert_c1c2(cql1, range(1000, 2000), ConsistencyLevel.ONE, cf="cf1")
    insert_c1c2(cql1, range(1000, 2000), ConsistencyLevel.ONE, cf="cf2")
    cql1.shutdown()
    await manager.server_start(node2.server_id)

    log = await manager.server_open_log(node2.server_id)
    mark = await log.mark()
    await repair_and_check_completed(manager, node2, "ks", "cf1", mark, aux_cf="cf2")
    try:
        await manager.api.client.post("/storage_service/keyspace_offstrategy_compaction/ks", host=node2.ip_addr, params={"cf": "cf1"})
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Triggering off-strategy compaction on {node2.ip_addr} failed: {e}")
    await log.wait_for("Starting off-strategy compaction for ks.cf1", "Done with off-strategy compaction for ks.cf1",
                       from_mark=mark, timeout=360)


async def stop_others(manager: ScyllaClusterManager, servers, node):
    """Stops the running nodes other than node (without flushing first, as
    ccm's node.stop()), and returns them."""
    running = {s.server_id for s in await manager.running_servers()}
    stopped = [s for s in servers if s.server_id != node.server_id and s.server_id in running]
    for s in stopped:
        await manager.server_stop_gracefully(s.server_id)
    return stopped


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_abort.
# 3000 different keys on each of three replicas; a repair started on node3 and
# terminated through the REST API soon after it starts leaves at most 9000
# keys on every node.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_abort(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config={**DEFAULT_CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE,
                                        property_file=rack_layout("datacenter1", ["rack1"] * 3))
    node1, node2, node3 = servers
    (await create_ks_cf(manager, node1, 3)).shutdown()
    keys_unit = 3000
    await stop_all_but(manager, servers, node1)
    await write_alone(manager, servers, [(node1, range(keys_unit), ConsistencyLevel.ONE, None),
                                         (node2, range(keys_unit, 2 * keys_unit), ConsistencyLevel.ONE, node1),
                                         (node3, range(2 * keys_unit, 3 * keys_unit), ConsistencyLevel.ONE, node2)])
    await start_all(manager, servers)

    async def checking_keys_num(less_than_num=None):
        for node in servers:
            stopped = await stop_others(manager, servers, node)
            c = connect(manager, node, exclusive=True, keyspace="ks")
            result = list(c.execute(f"SELECT * FROM cf LIMIT {3 * keys_unit * 2}"))
            c.shutdown()
            if less_than_num:
                assert len(result) <= less_than_num
            for s in stopped:
                await manager.server_start(s.server_id)

    log = await manager.server_open_log(node3.server_id)
    mark = await log.mark()
    repair = asyncio.create_task(nodetool(manager, node3, "repair", "ks", check=False))
    await log.wait_for("Repair 5 out of|Started to repair 1 out of", from_mark=mark, timeout=200)
    # The dtest aborts the repair with curl, as here.
    url = f"http://{node3.ip_addr}:10000/storage_service/force_terminate_repair"
    await asyncio.to_thread(subprocess.getoutput, f'curl -X POST  --header "Accept: application/json" {url}')
    await asyncio.wait_for(repair, 120)
    time.sleep(10)
    for s in await manager.running_servers():
        await nodetool(manager, s, "flush")
    await checking_keys_num(less_than_num=keys_unit * 3)


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_while_table_is_dropped.
# Tables dropped while repairs of their keyspace run (one on node1 and two on
# node2, in parallel) are skipped: the repairs succeed, the dropped tables
# are gone and the remaining table has all its rows on both repaired nodes.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_while_table_is_dropped(manager: ScyllaClusterManager):
    # The dtest's config update() returns None, so only batch commitlog is set.
    # Scylla's default auto_snapshot (test.py turns it off), as for the dtest:
    # the dropped tables are snapshotted.
    servers = await manager.servers_add(3, config={"tablets_mode_for_new_keyspaces": "disabled", **SIMPLE_SNITCH,
                                                   "commitlog_sync": "batch", "commitlog_sync_batch_window_in_ms": 5,
                                                   "auto_snapshot": True},
                                        cmdline=CMDLINE, property_file=rack_layout("datacenter1", ["rack1"] * 3))
    node1, node2, _ = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")
    await nodetool(manager, node2, "flush")
    await manager.server_stop_gracefully(node2.server_id)
    create_cf(cql, "ks.cf")
    insert_c1c2(cql, range(2000), ConsistencyLevel.QUORUM)
    delete_table_num = 8
    for i in range(delete_table_num):
        create_cf(cql, f"ks.cf_del{i}")
        insert_c1c2(cql, range(2000), ConsistencyLevel.QUORUM, cf=f"cf_del{i}")
    await manager.server_start(node2.server_id)

    async def repair_all(node):
        """ccm's node.repair() without a keyspace: a vnode repair, then a
        tablet repair, of all keyspaces."""
        await nodetool(manager, node, "repair")
        await nodetool(manager, node, "cluster", "repair")

    log2 = await manager.server_open_log(node2.server_id)
    repairs = [asyncio.create_task(repair_all(n)) for n in (node1, node2, node2)]
    _, matches = await log2.wait_for("sync data for keyspace=ks, status=started|starting user-requested repair for keyspace ks,")
    m = re.search(r"\[(?:.*uuid=)?(?P<uuid>[\w-]+)\]", matches[0][0])
    assert m is not None, f"Could not find repair task uuid in '{matches[0][0]}'"
    task_uuid = m.group("uuid")
    for i in range(delete_table_num):
        cql.execute(f"DROP TABLE ks.cf_del{i}")
    cql.shutdown()
    await asyncio.gather(*repairs)
    await log2.wait_for(f"repair - repair.*{task_uuid}.* completed successfully$")

    for node, other in [(node1, node2), (node2, node1)]:
        running = {s.server_id for s in await manager.running_servers()}
        if other.server_id in running:
            await manager.server_stop_gracefully(other.server_id)
        if node.server_id not in running:
            await manager.server_start(node.server_id)
        c = connect(manager, node, exclusive=True, keyspace="ks")
        for i in range(delete_table_num):
            out = await asyncio.to_thread(run_cqlsh_at, node.ip_addr, 9042, f"describe table ks.cf_del{i}")
            assert re.search(rf"cf_del{i}.* not found", out.stdout + out.stderr), out.stdout + out.stderr
            with pytest.raises(InvalidRequest):
                list(c.execute(SimpleStatement(f"SELECT * FROM cf_del{i} LIMIT 1", consistency_level=ConsistencyLevel.ONE)))
        result = list(c.execute(SimpleStatement("SELECT * FROM cf LIMIT 3000", consistency_level=ConsistencyLevel.ONE)))
        c.shutdown()
        assert len(result) == 2000, len(result)


async def verify_repair_tx_rx_rows(manager: ScyllaClusterManager, node, expected_tx_row_nr, expected_rx_row_nr):
    """The dtests' verify_repair_tx_rx_rows(): the node's repair row metrics
    are at most the expected numbers, and non-zero where those are."""
    metrics = await repair_metrics(manager, node, ["tx_row_nr", "rx_row_nr", "tx_hashes_nr", "rx_hashes_nr"])
    assert metrics["tx_row_nr"] <= expected_tx_row_nr, metrics
    assert metrics["rx_row_nr"] <= expected_rx_row_nr, metrics
    if expected_rx_row_nr > 0:
        assert metrics["rx_row_nr"] > 0, metrics
    if expected_tx_row_nr > 0:
        assert metrics["tx_row_nr"] > 0, metrics


def write_table_updates(cql, partitions_range_end, rows_in_partition, num_of_updates, partitions_range_start=1):
    """The dtests' write_table_updates(): updates (or nulls) a random int
    column of random rows."""
    for _ in range(num_of_updates):
        column = random.randint(1, NUM_OF_COLUMNS - 1)
        value = random.choice(["NULL", random.randint(0, 500000)])
        pk = random.randint(partitions_range_start, partitions_range_end)
        cql.execute(f"update cf set c{column} = {value} where pk={pk} and ck={random.randint(1, rows_in_partition)}")


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_large_partition_existing_rows.
# Updates of existing rows, written while node 2 was down, to a table with
# 100 partitions of 20 rows and one of 10000 rows, reach node 2 by repair
# on node 2: it receives at most the updated rows and sends at most twice as
# many.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_large_partition_existing_rows(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=CMDLINE,
                                        property_file=rack_layout("dc1", ["rack1", "rack2", "rack3"]))
    node1, repaired_node = servers[0], servers[1]
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':3}")
    cql.execute("create table ks.cf (pk int, ck int, {}, clist list<int>, cset set<text>, cmap map<int, text>, PRIMARY KEY(pk, ck))".format(
        ", ".join(f"c{n} int" for n in range(1, NUM_OF_COLUMNS))))
    prefill_table_data(cql, PARTITIONS, ROWS_IN_PARTITION)
    big_partition = PARTITIONS + 1
    prefill_table_data(cql, big_partition, BIG_PARTITION_ROWS, partition_range_start=big_partition)
    cql.shutdown()
    total_rows = PARTITIONS * ROWS_IN_PARTITION + BIG_PARTITION_ROWS

    for s in servers:
        await nodetool(manager, s, "flush")
    await manager.server_stop_gracefully(repaired_node.server_id)
    num_of_updates = 50
    cql = connect(manager, node1, keyspace="ks")
    write_table_updates(cql, PARTITIONS, ROWS_IN_PARTITION, num_of_updates)
    write_table_updates(cql, big_partition, BIG_PARTITION_ROWS, num_of_updates, partitions_range_start=big_partition)
    cql.shutdown()

    await manager.server_start(repaired_node.server_id)
    await nodetool(manager, repaired_node, "repair", "ks")
    num_of_total_updates = 2 * num_of_updates
    await verify_repair_tx_rx_rows(manager, repaired_node, num_of_total_updates * 2, num_of_total_updates)
    for node in [node1, repaired_node]:
        await check_rows_on_node(manager, node, total_rows)


async def repair_api(manager: ScyllaClusterManager, node, keyspace, ignore_nodes=()):
    """The dtests' _run_repair_api(): a repair through the REST API, with
    ignore_nodes, waited for until it succeeds."""
    params = {"ignore_nodes": ",".join(n.ip_addr for n in ignore_nodes)} if ignore_nodes else {}
    repair_id = await manager.api.client.post_json(f"/storage_service/repair_async/{keyspace}", host=node.ip_addr, params=params)
    deadline = time.time() + 120
    while await manager.api.client.get_json("/storage_service/repair_status", host=node.ip_addr, params={"id": str(repair_id)}) != "SUCCESSFUL":
        assert time.time() < deadline, f"repair {repair_id} did not complete"
        await asyncio.sleep(1)


async def cluster_with_table(manager: ScyllaClusterManager):
    """The dtests' _setup_cluster_with_table(): three racks, hinted handoff
    off, batch commitlog, RF=3."""
    servers = await manager.servers_add(3, config={"hinted_handoff_enabled": False, "tablets_mode_for_new_keyspaces": "disabled",
                                                   "commitlog_sync": "batch", "commitlog_sync_batch_window_in_ms": 5},
                                        cmdline=CMDLINE, property_file=rack_layout("dc1", ["rack1", "rack2", "rack3"]))
    (await create_ks_cf(manager, servers[0], 3)).shutdown()
    return servers


def count_rows(manager: ScyllaClusterManager, node, consistency_level):
    c = connect(manager, node, keyspace="ks")
    n = len(list(c.execute(SimpleStatement("SELECT * FROM cf", consistency_level=consistency_level))))
    c.shutdown()
    return n


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_ignore_nodes.
# A repair through the REST API ignoring a down node succeeds, and repairs
# the rows between the live nodes.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_ignore_nodes(manager: ScyllaClusterManager):
    node1, node2, node3 = await cluster_with_table(manager)
    await manager.server_stop_gracefully(node2.server_id)
    c = connect(manager, node1, keyspace="ks")
    insert_c1c2(c, range(1000), ConsistencyLevel.TWO)
    assert len(list(c.execute(SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.TWO)))) == 1000
    c.shutdown()
    await manager.server_start(node2.server_id)
    await manager.server_stop_gracefully(node3.server_id)
    await repair_api(manager, node2, "ks", ignore_nodes=[node3])
    assert count_rows(manager, node2, ConsistencyLevel.ONE) == 1000


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_ignore_nodes_errors.
# A repair ignoring every other node succeeds and repairs nothing; ignoring
# only the down node, it repairs the rows from the live one.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_ignore_nodes_errors(manager: ScyllaClusterManager):
    node1, node2, node3 = await cluster_with_table(manager)
    await nodetool(manager, node2, "flush")
    await manager.server_stop_gracefully(node2.server_id)
    c = connect(manager, node1, keyspace="ks")
    insert_c1c2(c, range(1000), ConsistencyLevel.TWO)
    assert len(list(c.execute(SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.TWO)))) == 1000
    c.shutdown()
    await nodetool(manager, node3, "flush")
    await manager.server_stop_gracefully(node3.server_id)
    await manager.server_start(node2.server_id)
    await repair_api(manager, node2, "ks", ignore_nodes=[node1, node3])
    await nodetool(manager, node1, "flush")
    await manager.server_stop_gracefully(node1.server_id)
    assert count_rows(manager, node2, ConsistencyLevel.ONE) == 0
    await manager.server_start(node1.server_id)
    await repair_api(manager, node2, "ks", ignore_nodes=[node3])
    await nodetool(manager, node1, "flush")
    await manager.server_stop_gracefully(node1.server_id)
    assert count_rows(manager, node2, ConsistencyLevel.ONE) == 1000


# Matches the peers of a repair log line, a list of addresses or host ids in
# parentheses or square brackets.
PEERS_RE = r"[\[(](?P<peers>(?:(?:,\s*)?[\w.-]+)*)[)\]]"


def remove_files_in_folder(folder):
    for f in os.listdir(folder):
        if os.path.isfile(os.path.join(folder, f)):
            os.remove(os.path.join(folder, f))


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_streams_data_from_closest_node.
# Repair, and repair-based replace, fetch rows from the nodes of the same dc
# first: on two DCs of two nodes, a node whose sstables were removed repairs
# from the other node of its dc first, and a node replacing one of dc2
# fetches only from the other node of dc2.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_streams_data_from_closest_node(manager: ScyllaClusterManager):
    config = {**DEFAULT_CONFIG, "enable_repair_based_node_ops": True,
              "allowed_repair_based_node_ops": "bootstrap,replace,removenode,decommission,rebuild"}
    servers = await manager.servers_add(4, config=config, cmdline=CMDLINE, property_file=[
        {"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack2"}, {"dc": "dc2", "rack": "rack1"}, {"dc": "dc2", "rack": "rack2"}])
    node1_1, node1_2, node2_1, node2_2 = servers
    cql = connect(manager, node1_1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'dc1':2, 'dc2':2}")
    create_cf(cql, "ks.cf")
    insert_c1c2(cql, range(1000), ConsistencyLevel.ALL)
    cql.shutdown()
    for s in servers:
        await nodetool(manager, s, "flush")
    table_folder = await get_table_dir(manager, node1_2, "ks", "cf")
    await manager.server_stop_gracefully(node1_2.server_id)
    remove_files_in_folder(table_folder)
    await manager.server_start(node1_2.server_id)
    await manager.api.client.post("/system/logger/repair", host=node1_2.ip_addr, params={"level": "debug"})

    log = await manager.server_open_log(node1_2.server_id)
    await repair_and_check_completed(manager, node1_2, "ks", "cf", await log.mark())
    host_id1_1 = await manager.get_host_id(node1_1.server_id)
    matches = await log.grep(rf"Started Row Level Repair \(Master\).+ peers={PEERS_RE}, repair_meta_id")
    assert matches
    for _, m in matches:
        peers = re.split(r"\s*,\s*", m.group("peers"))
        assert peers[0] in (node1_1.ip_addr, host_id1_1), "Missing rows should be fetched from a node from the same dc first"

    for s in servers:
        await nodetool(manager, s, "flush")
    await manager.server_stop_gracefully(node2_2.server_id)
    new_node = await manager.server_add(
        replace_cfg=ReplaceConfig(replaced_id=node2_2.server_id, reuse_ip_addr=False, use_host_id=True),
        config=config, cmdline=["--logger-log-level", "repair=debug"], property_file={"dc": "dc2", "rack": "rack2"})
    new_log = await manager.server_open_log(new_node.server_id)
    await new_log.wait_for("initialization completed")
    host_id2_1 = await manager.get_host_id(node2_1.server_id)
    matches = await new_log.grep(rf"repair .+ keyspace=ks, .+ peers={PEERS_RE}, live_peers")
    assert matches
    for _, m in matches:
        peers = set(re.split(r"\s*,\s*", m.group("peers")))
        assert peers in ({node2_1.ip_addr}, {host_id2_1}), "New node should fetched data only from the same dc"


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_postpone_reshape_sstables_created_by_repair.
# The sstables a repair writes to a node whose sstables were removed are
# reshaped by off-strategy compaction after the node restarts, not by a
# synchronous reshape at boot (b6828e899ae2).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_postpone_reshape_sstables_created_by_repair(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config={"tablets_mode_for_new_keyspaces": "disabled", **SIMPLE_SNITCH},
                                        cmdline=CMDLINE, property_file=rack_layout("datacenter1", ["rack1", "rack1"]))
    node1, node2 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':2}")
    create_cf(cql, "ks.cf")
    rows = 10000
    insert_c1c2(cql, range(rows), ConsistencyLevel.ONE)
    cql.shutdown()
    for s in servers:
        await nodetool(manager, s, "flush")
    remove_files_in_folder(await get_table_dir(manager, node1, "ks", "cf"))
    # Restart node1: it holds the removed files open and could still read them.
    await manager.server_stop_gracefully(node1.server_id)
    await manager.server_start(node1.server_id)

    log2 = await manager.server_open_log(node2.server_id)
    await repair_and_check_completed(manager, node2, "ks", "cf", await log2.mark())
    await manager.server_stop_gracefully(node1.server_id)
    log1 = await manager.server_open_log(node1.server_id)
    mark = await log1.mark()
    await manager.server_start(node1.server_id)
    assert await log1.grep("Starting off-strategy compaction for ks.cf", from_mark=mark), \
        "Expected that off-strategy compaction is started, but it was not started"
    assert await log1.grep("Reshape ks.cf", from_mark=mark), "Expected that reshape is started, but it was not started"
    cql = connect(manager, node1, keyspace="ks")
    assert cql.execute(SimpleStatement("SELECT count(*) FROM cf", consistency_level=ConsistencyLevel.QUORUM)).one()[0] == rows
    cql.shutdown()


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_compacts_data.
# A repair sends a partition whose rows were deleted on one replica as the
# single tombstone (it compacts the data it sends); with
# enable_compacting_data_for_streaming_and_repair off, it sends the 20 rows
# and the tombstone.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_compacts_data(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config={"hinted_handoff_enabled": False, "tablets_mode_for_new_keyspaces": "disabled"},
                                        cmdline=CMDLINE, property_file=rack_layout("dc1", ["rack1", "rack2"]))
    node1, node2 = servers
    cql = connect(manager, node1)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':2}")
    cql.execute("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = {'class': 'NullCompactionStrategy'}")

    async def write_data_to_node1(pk):
        await manager.server_stop_gracefully(node2.server_id)
        for ck in range(10):
            cql.execute(f"INSERT INTO ks.tbl (pk, ck, v) VALUES ({pk}, {ck}, 0)")
        await nodetool(manager, node1, "flush")
        cql.execute(f"DELETE FROM ks.tbl WHERE pk = {pk}")
        await nodetool(manager, node1, "flush")
        await manager.server_start(node2.server_id)

    names = ["tx_row_nr", "rx_row_nr", "tx_hashes_nr", "rx_hashes_nr"]
    for pk, sent in [(0, 1), (1, 21)]:
        await write_data_to_node1(pk)
        if pk == 1:
            cql.execute("UPDATE system.config SET value = '0' WHERE name = 'enable_compacting_data_for_streaming_and_repair'")
        base = await repair_metrics(manager, node1, names)
        await nodetool(manager, node1, "repair", "ks", "tbl")
        await verify_repair_tx_rx_rows(manager, node1, base["tx_row_nr"] + sent, base["rx_row_nr"])
    cql.shutdown()


# Ported from scylla-dtest repair_additional_test.py::TestRepairAdditional::test_repair_one_node_alter_rf.
# Keys written with RF=1 are each on one node; after the RF is raised to 3,
# a "nodetool repair" of a single node brings all keys to every node.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_repair_one_node_alter_rf(manager: ScyllaClusterManager):
    config = {"hinted_handoff_enabled": False, "tablets_mode_for_new_keyspaces": "disabled",
              "rf_rack_valid_keyspaces": False, "force_capacity_based_balancing": True}
    servers = await manager.servers_add(3, config=config, cmdline=CMDLINE, property_file=rack_layout("dc1", ["r1", "r1", "r2"]))
    num_nodes = len(servers)
    num_keys = 100 * num_nodes
    cql = connect(manager, servers[0])
    cql.execute("CREATE KEYSPACE ks WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 };")
    cql.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY, v int) WITH compaction = {'class': 'NullCompactionStrategy'} AND speculative_retry = 'NONE'")
    insert = cql.prepare("INSERT INTO ks.tbl (pk, v) VALUES (?, ?)")
    for pk in range(num_keys):
        cql.execute(insert, (pk, pk))
    cql.shutdown()

    total_found = 0
    for node in servers:
        stopped = await stop_others(manager, servers, node)
        c = connect(manager, node, exclusive=True, keyspace="ks")
        for pk in range(num_keys):
            try:
                c.execute(SimpleStatement(f"SELECT * from ks.tbl WHERE pk={pk}", consistency_level=ConsistencyLevel.ONE))
                total_found += 1
            except (NoHostAvailable, Unavailable):
                pass
        c.shutdown()
        for s in stopped:
            await manager.server_start(s.server_id)
    assert total_found == num_keys

    cql = connect(manager, servers[0])
    cql.execute(f"ALTER KEYSPACE ks WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'dc1' : {num_nodes} }};")
    cql.shutdown()
    await nodetool(manager, random.choice(servers), "repair")
    for node in servers:
        stopped = await stop_others(manager, servers, node)
        c = connect(manager, node, exclusive=True, keyspace="ks")
        res = c.execute(SimpleStatement("SELECT * from ks.tbl", consistency_level=ConsistencyLevel.ONE))
        c.shutdown()
        assert [(r.pk, r.v) for r in sorted(res)] == [(i, i) for i in range(num_keys)]
        for s in stopped:
            await manager.server_start(s.server_id)
