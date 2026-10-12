#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# CQL statements and queries on small clusters, ported from scylla-dtest
# cql_tests.py: keyspace statements across two datacenters, reads with IN
# restrictions on two clustering columns in both orders, range scans and
# filtering without indexes on two nodes, TRUNCATE entries surviving a node
# restart, and large collection warnings. Keyspaces use vnodes, like the
# dtests.

import asyncio
import re
import string
import time
from datetime import timedelta
from random import randint, sample

import pytest
from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy, WhiteListRoundRobinPolicy
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement

from test.cluster.dtest_helpers import DTEST_CONFIG, create_ks as dtest_create_ks, dtest_session, nodetool
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}


def session(manager: ScyllaClusterManager, server, exclusive=False):
    if exclusive:
        return manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    return manager.con_gen([server.ip_addr]).connect()


def rows(cql, query, cl=None):
    if cl is not None:
        query = SimpleStatement(query, consistency_level=cl)
    return [list(r) for r in cql.execute(query)]


def create_ks(cql, rf):
    cql.execute(f"CREATE KEYSPACE ks WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_keyspace(manager: ScyllaClusterManager):
    """CREATE KEYSPACE, USE, ALTER KEYSPACE and DROP KEYSPACE, with nodes in
    two datacenters."""
    cfg = {**VNODES, "endpoint_snitch": "GossipingPropertyFileSnitch"}
    node1 = await manager.server_add(config=cfg, property_file={"dc": "dc0", "rack": "rack1"})
    node2 = await manager.server_add(config=cfg, property_file={"dc": "dc1", "rack": "rack1"})
    cql = session(manager, node1)

    cql.execute("CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")
    cql.execute("USE ks")
    start = time.time()
    while True:
        try:
            cql.execute("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")
            break
        except ConfigurationException:
            # Wait for the snitch to learn the other datacenter.
            assert time.time() - start < 60
            time.sleep(1)
    cql.execute("DROP KEYSPACE ks")
    with pytest.raises(InvalidRequest):
        cql.execute("USE ks")
    # The nodes flush their memtables, as stopping dtest nodes do.
    for s in [node1, node2]:
        await manager.api.flush_all_keyspaces(s.ip_addr)


async def in_restrictions_cluster(manager: ScyllaClusterManager):
    """The dtests' prepare(nodes={"dc1": {"rack1": 2, "rack2": 1, "rack3": 1}},
    rf=3): four nodes on vnodes, a keyspace ks with RF=3, and a session to
    node1 with statements at CL=ONE by default."""
    racks = ["rack1", "rack1", "rack2", "rack3"]
    servers = await manager.servers_add(4, config=DTEST_CONFIG, property_file=[{"dc": "dc1", "rack": r} for r in racks])
    cql = dtest_session(manager.con_gen([servers[0].ip_addr], load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())).connect())
    dtest_create_ks(cql, "ks", 3)
    return servers, cql


async def check_in_restrictions(manager: ScyllaClusterManager, schema: str, values: list[str], in_values: list[str], order_by: str):
    """Writes a row for each of the values to partition 0 of ks.cf, one by
    one, flushes every node, checks the count at QUORUM, then reads the rows
    whose clustering columns are IN the given values at QUORUM, with and
    without BYPASS CACHE (#6171)."""
    servers, cql = await in_restrictions_cluster(manager)
    cql.execute(f"CREATE TABLE ks.cf {schema}")
    text_value = string.ascii_lowercase * 40
    for v in values:
        cql.execute(f"INSERT INTO ks.cf(pk, ck, ck1, v) VALUES (0, {v}, {v}, '{text_value}')" if "ck1" in schema else
                    f"INSERT INTO ks.cf(pk, ck, v) VALUES (0, {v}, '{text_value}')")
    for s in servers:
        await nodetool(manager, s, "flush")

    assert rows(cql, "select count(*) from ks.cf", ConsistencyLevel.QUORUM) == [[len(values)]]
    in_str = ", ".join(in_values)
    restriction = f"ck in ({in_str}) and ck1 in ({in_str})" if "ck1" in schema else f"ck in ({in_str})"
    read_stmt = f"SELECT v FROM ks.cf WHERE pk = 0 and {restriction}{order_by}"
    expected = [[text_value] for _ in in_values]
    # Read without BYPASS CACHE.
    assert rows(cql, read_stmt, ConsistencyLevel.QUORUM) == expected
    # Read with BYPASS CACHE.
    assert rows(cql, f"{read_stmt} BYPASS CACHE", ConsistencyLevel.QUORUM) == expected


# Ported from scylla-dtest cql_tests.py::TestMiscellaneousCQL::test_reverse_query.
# IN on both clustering columns, ORDER BY both DESC.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reverse_query(manager: ScyllaClusterManager):
    await check_in_restrictions(manager, "(pk int, ck int, ck1 int, v text, PRIMARY KEY (pk, ck, ck1))",
                                [str(i) for i in range(10000)], [str(i) for i in range(0, 10000, 1000)],
                                " ORDER BY ck DESC, ck1 DESC")


# Ported from scylla-dtest cql_tests.py::TestMiscellaneousCQL::test_normal_query.
# IN on both clustering columns, in clustering order.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_normal_query(manager: ScyllaClusterManager):
    await check_in_restrictions(manager, "(pk int, ck int, ck1 int, v text, PRIMARY KEY (pk, ck, ck1))",
                                [str(i) for i in range(10000)], [str(i) for i in range(0, 10000, 1000)], "")


# Ported from scylla-dtest cql_tests.py::TestMiscellaneousCQL::test_reverse_query_ck_collect.
# IN on a frozen list clustering column, ORDER BY it DESC.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reverse_query_ck_collect(manager: ScyllaClusterManager):
    all_ascii = list(string.ascii_lowercase)
    await check_in_restrictions(manager, "(pk int, ck frozen<list<text>>, v text, PRIMARY KEY (pk, ck))",
                                [f"['{c}']" for c in all_ascii], [f"['{all_ascii[i]}']" for i in range(0, 26, 10)],
                                " ORDER BY ck DESC")


# Ported from scylla-dtest cql_tests.py::TestMiscellaneousCQL::test_reverse_query_table_desc.
# A table in descending clustering order, ORDER BY both DESC. The dtest's
# range(10000, 0, 1000) is empty: "IN ()" finds nothing.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reverse_query_table_desc(manager: ScyllaClusterManager):
    await check_in_restrictions(manager, "(pk int, ck int, ck1 int, v text, PRIMARY KEY (pk, ck, ck1)) WITH CLUSTERING ORDER BY (ck DESC, ck1 DESC)",
                                [str(i) for i in range(10000)], [str(i) for i in range(10000, 0, 1000)],
                                " ORDER BY ck DESC, ck1 DESC")


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_range_slice(manager: ScyllaClusterManager):
    """A full scan of a two-node cluster returns all the rows. Regression
    test for #1337."""
    servers = await manager.servers_add(2, config=VNODES)
    cql = session(manager, servers[0])
    create_ks(cql, 1)
    cql.execute("""
        CREATE TABLE ks.test (
            k text PRIMARY KEY,
            v int
        );
    """)
    cql.execute("INSERT INTO ks.test (k, v) VALUES ('foo', 0)")
    cql.execute("INSERT INTO ks.test (k, v) VALUES ('bar', 1)")
    assert len(rows(cql, "SELECT * FROM ks.test")) == 2


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_cql_query_filtering_without_indexes(manager: ScyllaClusterManager):
    """
    Filtering without indexes (#2025): equality on regular and key columns,
    a relation on a time column, IN on a clustering column, and LIMIT.
    """
    loop_size = 500
    servers = await manager.servers_add(2, config=VNODES)
    cql = session(manager, servers[0], exclusive=True)
    create_ks(cql, 1)
    cql.execute("""
        CREATE TABLE ks.t1 (
            p int,
            c int,
            v int,
            PRIMARY KEY (p, c)
        );
    """)
    for i in range(loop_size):
        cql.execute(f"INSERT INTO ks.t1 (p, c, v) VALUES ({i},{i},{i + 1}) ")
    n = randint(0, loop_size - 1)
    for query in [f"select * from ks.t1 where c = {n} and v = {n + 1} allow filtering;",
                  f"select * from ks.t1 where p = {n} and v = {n + 1} allow filtering;",
                  f"select * from ks.t1 where p = {n} and c = {n} and v = {n + 1} allow filtering;"]:
        assert rows(cql, query) == [[n, n, n + 1]], query

    cql.execute("""
        CREATE TABLE ks.t2 (
            item_id int,
            item_name text,
            insert_time time,
            PRIMARY KEY (item_id,item_name)
        );
    """)
    inserted_times = []
    selected_items = []
    second_samples = sample(range(int(timedelta(days=1).total_seconds())), loop_size)
    for i in range(loop_size):
        # timedelta is formatted like "1:12:13"
        rand_time_str = str(timedelta(seconds=second_samples[i]))
        inserted_times.append(time.strptime(rand_time_str, "%H:%M:%S"))
        item_name = f"name_{i}"
        if randint(1, 10) == 1 or (i == loop_size - 1 and not selected_items):
            selected_items.append(f"'{item_name}'")
        cql.execute(f"INSERT INTO ks.t2 (item_id, item_name, insert_time) VALUES ({i},'{item_name}','{rand_time_str}')")

    selected_index = 6
    inserted_times.sort()
    count_above = loop_size - selected_index - 1
    selected_time_str = time.strftime("%H:%M:%S", inserted_times[selected_index])

    # A relation (>) on a regular column.
    assert len(rows(cql, f"Select item_id from ks.t2 where insert_time > '{selected_time_str}' allow filtering;")) == count_above
    # IN on a clustering column.
    assert len(rows(cql, f"Select * from ks.t2 where item_name IN ({', '.join(selected_items)})  allow filtering;")) == len(selected_items)
    # LIMIT.
    limit = randint(1, count_above)
    assert len(rows(cql, f"Select item_id from ks.t2 where insert_time >'{selected_time_str}' limit {limit} allow filtering;")) == min(count_above, limit)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_truncate_before_restart(manager: ScyllaClusterManager):
    """
    TRUNCATE leaves an entry in system.truncated, and none in system.local,
    on every node, and the entries are the same after a node restarts.
    auto_snapshot is on, as on dtest nodes, so TRUNCATE snapshots the table.
    """
    servers = await manager.servers_add(3, config={**VNODES, "auto_snapshot": True}, property_file=[{"dc": "datacenter1", "rack": f"rack{i}"} for i in (1, 2, 3)])
    cql = session(manager, servers[0])
    cql.execute("CREATE KEYSPACE ks WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor':3} AND DURABLE_WRITES = true")
    cql.execute("CREATE TABLE ks.test1 (k int PRIMARY KEY, v1 int)")
    data = [[i, i] for i in range(30)]
    for x, y in data:
        cql.execute(SimpleStatement(f"INSERT INTO ks.test1 (k, v1) VALUES ({x}, {y})", consistency_level=ConsistencyLevel.QUORUM))

    select_query = "SELECT * FROM ks.test1"
    assert sorted(rows(cql, select_query, ConsistencyLevel.QUORUM)) == data
    cql.execute("TRUNCATE ks.test1")
    assert rows(cql, select_query, ConsistencyLevel.ALL) == []

    def truncated_entries():
        entries = []
        for s in servers:
            node_cql = session(manager, s, exclusive=True)
            table_id = rows(node_cql, "SELECT id FROM system_schema.tables WHERE keyspace_name='ks' and table_name='test1'")[0][0]
            truncated = rows(node_cql, f"SELECT truncated_at FROM system.truncated WHERE table_uuid={table_id}")
            assert truncated and truncated[0], "Expected a truncated entry in the system.truncated table"
            assert rows(node_cql, "SELECT truncated_at FROM system.local") == [[None]], "Unexpected truncated entry in system.local"
            entries.append(truncated[0])
        return entries

    before = truncated_entries()
    node2 = servers[1]
    await manager.server_stop_gracefully(node2.server_id)
    await manager.server_start(node2.server_id)
    node2_cql = session(manager, node2, exclusive=True)
    assert rows(node2_cql, select_query, ConsistencyLevel.ALL) == []
    assert truncated_entries() == before


async def wait_for_compactions(manager: ScyllaClusterManager, server, quiesce: float = 0.5, timeout: float = 300) -> None:
    """Like ccm's node.wait_for_compactions(): poll "nodetool compactionstats"
    until no compaction is pending or running for `quiesce` seconds."""
    idle_since = None
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = (await nodetool(manager, server, "compactionstats")).stdout
        tasks = sum(int(m.group(1)) for m in re.finditer(r"^\s*- \S+\.\S+: (\d+)$", out, re.MULTILINE))
        tasks += len(re.findall(r"^[0-9a-f]{8}-[0-9a-f]{4}-", out, re.MULTILINE))
        if tasks == 0:
            idle_since = idle_since or time.time()
            if time.time() - idle_since >= quiesce:
                return
        else:
            idle_since = None
        await asyncio.sleep(0.1)
    raise TimeoutError("compactions did not finish")


# Ported from scylla-dtest cql_tests.py::TestMiscellaneousCQL::test_large_collection.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_large_collection(manager: ScyllaClusterManager):
    """A collection is warned about ("Writing large collection") and
    recorded in system.large_cells when, after a flush and a compaction, it
    has more elements than compaction_collection_elements_count_warning_threshold,
    and not when it has exactly that many. The dtest's populate(1):
    SimpleSnitch, datacenter1/rack1."""
    threshold = 1000
    node1 = await manager.server_add(config={**DTEST_CONFIG, "endpoint_snitch": "SimpleSnitch",
                                             "compaction_collection_elements_count_warning_threshold": threshold},
                                     property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = dtest_session(manager.con_gen([node1.ip_addr], load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())).connect())
    dtest_create_ks(cql, "ks", 1)
    cql.execute("""
        CREATE TABLE ks.maps (
            userid text PRIMARY KEY,
            properties map<int, text>
        );
    """)

    # Populate the map up to the warning threshold.
    for i in range(threshold):
        cql.execute("UPDATE ks.maps SET properties[%i] = 'x' WHERE userid = 'user'" % i)
    await nodetool(manager, node1, "flush")
    await nodetool(manager, node1, "compact")
    await wait_for_compactions(manager, node1)

    msg = "Writing large collection"
    log = await manager.server_open_log(node1.server_id)
    res = await log.grep(msg)
    assert not res, f"Found unexpected warnings: {res}"
    mark = await log.mark()
    query = "SELECT * from system.large_cells WHERE keyspace_name='ks' AND table_name='maps' ALLOW FILTERING"
    res = list(cql.execute(query))
    assert not res, f"Found unexpected row in system.large_cells: {res}"

    # One more entry triggers the warning.
    cql.execute(f"UPDATE ks.maps SET properties[{threshold}] = 'x' WHERE userid = 'user'")
    await nodetool(manager, node1, "flush")
    await nodetool(manager, node1, "compact")
    await wait_for_compactions(manager, node1)

    res = await log.grep(msg, from_mark=mark)
    assert res, f"Did not find expected log message: {msg}"
    res = list(cql.execute(query))
    assert res, "Did not find expected row in system.large_cells"
    assert len(res) == 1, f"Found too many rows in system.large_cells: {res}"
