#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Short reads: a read with a LIMIT gets a page from each replica, and when
# reconciling the replicas' answers drops rows deleted on only some of them,
# the result is short and the coordinator must ask the replicas for more.
# The replicas are made to diverge by stopping a node while the others take
# a write; hinted handoff is off so that the stopped node never gets it.

import pytest
from cassandra import ConsistencyLevel  # type: ignore
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test import DEBUG_MODES
from test.cluster.dtest_helpers import DTEST_CONFIG, create_ks, dtest_session, nodetool, topology
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

CONFIG = {"hinted_handoff_enabled": False, "cache_hit_rate_read_balancing": False}

# ccm's set_configuration_options(batch_commitlog=True).
BATCH_COMMITLOG = {"commitlog_sync": "batch", "commitlog_sync_batch_window_in_ms": 5}
# The dtests' cql_timeout(60) in release mode, the only mode these ports run in.
TRUNCATE_TIMEOUT = 60


async def start_cluster(manager: ScyllaClusterManager, nodes: int):
    servers = [await manager.server_add(config={**CONFIG, "tablets_mode_for_new_keyspaces": "disabled"}, property_file={"dc": "dc1", "rack": f"r{i}"})
               for i in range(nodes)]
    return servers, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {nodes}}}"


async def ready_cql(manager: ScyllaClusterManager):
    cql, _ = await manager.get_ready_cql(await manager.running_servers())
    return cql


async def execute(manager: ScyllaClusterManager, query: str, cl=ConsistencyLevel.ALL, **kwargs):
    cql = await ready_cql(manager)
    return list(await cql.run_async(SimpleStatement(query, consistency_level=cl, **kwargs)) or [])


async def execute_while_down(manager: ScyllaClusterManager, server, queries: list[str], cl=ConsistencyLevel.ONE):
    """Run the queries while the given node is stopped, so that it misses them."""
    await manager.server_stop_gracefully(server.server_id)
    for query in queries:
        await execute(manager, query, cl)
    await manager.server_start(server.server_id)


async def start_racks(manager: ScyllaClusterManager, racks: int, config: dict):
    """The dtests' populate(generate_cluster_topology(rack_num=racks)): one
    node in each of rack1..rack<racks> of datacenter1, on vnodes."""
    return await manager.servers_add(racks, config={**DTEST_CONFIG, **config},
                                     property_file=topology({f"rack{i}": 1 for i in range(1, racks + 1)}))


def connect(manager: ScyllaClusterManager, server):
    """The dtests' patient_cql_connection(node): the driver's default
    token-aware policy, statements at CL=ONE by default."""
    return dtest_session(manager.con_gen([server.ip_addr], load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())).connect())


async def stop_node(manager: ScyllaClusterManager, server, wait_other_notice=False):
    """ccm's node.stop(): a graceful stop, optionally waiting until the other
    live nodes see the node down."""
    await manager.server_stop_gracefully(server.server_id)
    if wait_other_notice:
        for other in await manager.running_servers():
            await manager.server_not_sees_other_server(other.ip_addr, server.ip_addr)


async def start_node(manager: ScyllaClusterManager, server, wait_other_notice=False):
    """ccm's node.start(), optionally waiting until the other live nodes see
    the node up."""
    await manager.server_start(server.server_id)
    if wait_other_notice:
        for other in await manager.running_servers():
            if other.server_id != server.server_id:
                await manager.server_sees_other_server(other.ip_addr, server.ip_addr)


def create_cf(cql, name: str):
    """The dtests' create_cf(session, name, read_repair=0.0): a table of
    text rows (key, c) with a text value v; it checks the table exists by
    reading it."""
    cql.execute(f"CREATE COLUMNFAMILY {name} (key varchar, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment='test cf' "
                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {} "
                "AND read_repair_chance=0.000000")
    cql.execute(SimpleStatement(f"SELECT * FROM {name} LIMIT 1", consistency_level=ConsistencyLevel.ONE))


def insert_columns(cql, key, columns_count, consistency=ConsistencyLevel.QUORUM):
    """The dtests' insert_columns(): one batch of updates of columns
    c000000.. of the row k<key>."""
    upds = [f"UPDATE ks.cf SET v='value{i}' WHERE key='k{key}' AND c='c{i:06d}'" for i in range(columns_count)]
    cql.execute(SimpleStatement("BEGIN BATCH %s; APPLY BATCH" % "; ".join(upds), consistency_level=consistency))


async def stop_delete_and_restart(manager: ScyllaClusterManager, servers, node_number, column):
    """The dtests' stop_delete_and_restart(): flush and stop node<node_number>,
    delete a column through another node at QUORUM, start the node again."""
    to_stop = servers[node_number - 1]
    next_node = servers[((node_number + 1) % 3)]
    await nodetool(manager, to_stop, "flush")
    await stop_node(manager, to_stop, wait_other_notice=True)
    cql = connect(manager, next_node)
    query = "BEGIN BATCH "
    query += "DELETE FROM ks.cf WHERE key='k0' AND c='c%06d'; " % column
    query += "DELETE FROM ks.cf WHERE key='k0' AND c='c2'; "
    query += "APPLY BATCH;"
    cql.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM))
    await start_node(manager, to_stop, wait_other_notice=True)


def truncate_cf(cql):
    """The dtests' _truncate_cf(): TRUNCATE at QUORUM with a server timeout
    above the default, and a driver timeout above that."""
    cql.execute(SimpleStatement(f"TRUNCATE ks.cf USING TIMEOUT {TRUNCATE_TIMEOUT}s", consistency_level=ConsistencyLevel.QUORUM),
                timeout=TRUNCATE_TIMEOUT + 30)


async def short_read(manager: ScyllaClusterManager, deleted_columns, query, expected):
    """The dtests' test_short_read and test_short_read_reversed, three times
    over: 9 columns in one row, each of 3 of them deleted while a different
    node is down, then a LIMIT 3 read at QUORUM."""
    servers = await start_racks(manager, 3, {**CONFIG, **BATCH_COMMITLOG, "auto_snapshot": False})
    node1 = servers[0]
    cql = connect(manager, node1)
    create_ks(cql, "ks", 3)
    create_cf(cql, "ks.cf")

    # Repeat 3 times to make a null pointer exception caused by a race
    # (CASSANDRA-9460) easier to spot.
    for _ in range(3):
        insert_columns(cql, 0, 9)
        for node_number, column in enumerate(deleted_columns, start=1):
            await stop_delete_and_restart(manager, servers, node_number, column)

        cql = connect(manager, node1)
        res = list(cql.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)))
        assert len(res) == 3, f"Expecting 3 values, got {len(res)} ({res})"
        for row, value in zip(res, expected):
            assert row[1] == f"value{value}", f"Expecting value{value}, got {row[1]} ({res})"

        truncate_cf(cql)


# Ported from scylla-dtest consistency_test.py::TestConsistency::test_short_read.
# The first 3 columns are deleted, each while a different node is down: the
# LIMIT 3 read returns the next 3 (CASSANDRA-9460).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_short_read(manager: ScyllaClusterManager):
    await short_read(manager, [0, 1, 2], "SELECT c, v FROM ks.cf WHERE key='k0' LIMIT 3", [3, 4, 5])


# Ported from scylla-dtest consistency_test.py::TestConsistency::test_short_read_delete.
# A row deletion the first node missed leaves no column alive (#4000).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_short_read_delete(manager: ScyllaClusterManager):
    servers = await start_racks(manager, 3, {**CONFIG, **BATCH_COMMITLOG})
    node1, node2, _ = servers
    cql = connect(manager, node1)
    create_ks(cql, "ks", 3)
    create_cf(cql, "ks.cf")
    insert_columns(cql, 0, 2)

    # Delete the row while the first node is down.
    await nodetool(manager, node1, "flush")
    await stop_node(manager, node1, wait_other_notice=True)
    cql = connect(manager, node2)
    cql.execute(SimpleStatement("DELETE FROM ks.cf WHERE key='k0'", consistency_level=ConsistencyLevel.ONE))
    await start_node(manager, node1, wait_other_notice=True)

    # Query the first column.
    cql = connect(manager, node1)
    res = list(cql.execute(SimpleStatement("SELECT c, v FROM ks.cf WHERE key='k0' LIMIT 1", consistency_level=ConsistencyLevel.QUORUM)))
    assert len(res) == 0, res


# Ported from scylla-dtest consistency_test.py::TestConsistency::test_short_read_quorum_delete.
# Two rows deleted at QUORUM, each while a different replica was down: a
# QUORUM read of the two replicas that saw one deletion each returns nothing
# (CASSANDRA-8933).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_short_read_quorum_delete(manager: ScyllaClusterManager):
    servers = await start_racks(manager, 3, {**CONFIG, **BATCH_COMMITLOG})
    node1, node2, node3 = servers
    cql = connect(manager, node1)
    create_ks(cql, "ks", 3)
    cql.execute("CREATE TABLE ks.t (id int, v int, PRIMARY KEY(id, v)) WITH read_repair_chance = 0.0")
    # 1 and 2 are written to a partition on all the nodes.
    cql.execute(SimpleStatement("INSERT INTO ks.t (id, v) VALUES (0, 1)", consistency_level=ConsistencyLevel.ALL))
    cql.execute(SimpleStatement("INSERT INTO ks.t (id, v) VALUES (0, 2)", consistency_level=ConsistencyLevel.ALL))

    # 1 is deleted on node1 and node3 only.
    await nodetool(manager, node2, "flush")
    await stop_node(manager, node2, wait_other_notice=True)
    cql.execute(SimpleStatement("DELETE FROM ks.t WHERE id = 0 AND v = 1", consistency_level=ConsistencyLevel.QUORUM))
    await start_node(manager, node2, wait_other_notice=True)

    # 2 is deleted on node2 and node3 only.
    await nodetool(manager, node1, "flush")
    await stop_node(manager, node1, wait_other_notice=True)
    cql = connect(manager, node2)
    cql.execute(SimpleStatement("DELETE FROM ks.t WHERE id = 0 AND v = 2", consistency_level=ConsistencyLevel.QUORUM))
    await start_node(manager, node1, wait_other_notice=True)
    cql = connect(manager, node1)

    # The first row of the partition (LIMIT 1) is read from node1 and node2.
    await nodetool(manager, node3, "flush")
    await stop_node(manager, node3, wait_other_notice=True)
    assert list(cql.execute(SimpleStatement("SELECT * FROM ks.t WHERE id = 0 LIMIT 1", consistency_level=ConsistencyLevel.QUORUM))) == []


# Ported from scylla-dtest consistency_test.py::TestConsistency::test_short_read_reversed.
# The last 3 columns are deleted, each while a different node is down: the
# LIMIT 3 read in reversed order returns the 3 before them (CASSANDRA-9460).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_short_read_reversed(manager: ScyllaClusterManager):
    await short_read(manager, [6, 7, 8], "SELECT c, v FROM ks.cf WHERE key='k0' ORDER BY c DESC LIMIT 3", [5, 4, 3])


# One replica has the first row deleted, the other has a newer value of the
# second row: the result is the newer value even though the first replica's
# data and the second's digest of a LIMIT 1 query cover different rows.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_data_query_digest(manager: ScyllaClusterManager):
    servers, ks_opts = await start_cluster(manager, 2)
    async with new_test_keyspace(manager, ks_opts) as ks:
        await execute(manager, f"CREATE TABLE {ks}.t (p int, c int, r int, PRIMARY KEY (p, c))")
        await execute(manager, f"INSERT INTO {ks}.t (p, c, r) VALUES (0, 1, 1)")
        await execute(manager, f"INSERT INTO {ks}.t (p, c, r) VALUES (0, 2, 1)")
        await execute_while_down(manager, servers[1], [f"DELETE FROM {ks}.t WHERE p = 0 AND c = 1"])
        await execute_while_down(manager, servers[0], [f"INSERT INTO {ks}.t (p, c, r) VALUES (0, 2, 2)"])
        assert [r.r for r in await execute(manager, f"SELECT r FROM {ks}.t WHERE p = 0 LIMIT 1")] == [2]


async def two_nodes(manager: ScyllaClusterManager, config: dict, hinted_handoff_after_start: bool):
    """The dtests' 2-node clusters, in rack1 and rack2 of datacenter1, and a
    keyspace ks with RF=2. Some dtests turn hinted handoff off only after the
    cluster started: it is off from the nodes' next start."""
    servers = await start_racks(manager, 2, config)
    if hinted_handoff_after_start:
        for s in servers:
            await manager.server_update_config(s.server_id, config_options={"hinted_handoff_enabled": False}, reload=False)
    cql = connect(manager, servers[0])
    create_ks(cql, "ks", 2)
    return servers, cql


def execute_at(cql, query, cl, **kwargs):
    return list(cql.execute(SimpleStatement(query, consistency_level=cl, **kwargs)))


# Ported from scylla-dtest consistency_test.py::TestConsistency::test_incomplete_result_test_partition_limit.
# node1 deleted the first partition, node2 updated the second one: a LIMIT 1
# range scan at ALL returns the second partition with its new value.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_incomplete_result_test_partition_limit(manager: ScyllaClusterManager):
    (node1, node2), cql1 = await two_nodes(manager, {"start_rpc": True, "cache_hit_rate_read_balancing": False},
                                           hinted_handoff_after_start=True)
    cql1.execute("create table ks.cf1 (p int, c text, r text, primary key (p, c))")
    execute_at(cql1, "insert into ks.cf1 (p, c, r) values (1, '1', '0')", ConsistencyLevel.ALL)
    execute_at(cql1, "insert into ks.cf1 (p, c, r) values (2, '1', '1')", ConsistencyLevel.ALL)

    # Update node1.
    await stop_node(manager, node2)
    execute_at(cql1, "delete from ks.cf1 where p = 1 and c = '1'", ConsistencyLevel.ONE)

    # Update node2.
    await start_node(manager, node2, wait_other_notice=True)
    await stop_node(manager, node1)
    cql2 = connect(manager, node2)
    execute_at(cql2, "insert into ks.cf1 (p, c, r) values (2, '1', '2')", ConsistencyLevel.ONE)

    # Query the whole cluster.
    await start_node(manager, node1, wait_other_notice=True)
    res = execute_at(cql2, "select r from ks.cf1 limit 1", ConsistencyLevel.ALL)
    assert len(res) == 1, f"Expecting 1 row, got {len(res)} ({res})"
    assert len(res[0]) == 1, f"Expecting 1 cell, got {len(res[0])} ({res[0]})"
    assert res[0][0] == "2", f"Expecting value 2, got {res[0][0]}"


async def empty_reconciled_result_cluster(manager: ScyllaClusterManager, partitions: int):
    """The partitions 1..<partitions> written to both nodes, then partition 1
    deleted on node1 only and partition 2 on node2 only. Returns node2's
    session."""
    (node1, node2), cql1 = await two_nodes(manager, CONFIG, hinted_handoff_after_start=False)
    cql1.execute("create table ks.cf1 (p int, r int, primary key (p))")
    for p in range(1, partitions + 1):
        execute_at(cql1, f"insert into ks.cf1 (p, r) values ({p}, {p})", ConsistencyLevel.ALL)

    # Update node1.
    await stop_node(manager, node2, wait_other_notice=True)
    execute_at(cql1, "delete from ks.cf1 where p = 1", ConsistencyLevel.ONE)

    # Update node2.
    await stop_node(manager, node1)
    await start_node(manager, node2)
    cql2 = connect(manager, node2)
    execute_at(cql2, "delete from ks.cf1 where p = 2", ConsistencyLevel.ONE)

    # Query the whole cluster.
    await start_node(manager, node1, wait_other_notice=True)
    return cql2


# Ported from scylla-dtest consistency_test.py::TestConsistency::test_empty_reconciled_result.
# Each node deleted a different partition: the reconciled first partitions
# are empty, and the unpaged LIMIT 1 query at ALL returns the third one.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_empty_reconciled_result(manager: ScyllaClusterManager):
    cql2 = await empty_reconciled_result_cluster(manager, 3)
    res = execute_at(cql2, "select r from ks.cf1 limit 1", ConsistencyLevel.ALL, fetch_size=0)
    assert len(res) == 1, f"Expecting 1 row, got {len(res)} ({res})"
    assert len(res[0]) == 1, f"Expecting 1 cell, got {len(res[0])} ({res[0]})"
    assert res[0][0] == 3, f"Expecting value 3, got {res[0][0]}"


# Ported from scylla-dtest consistency_test.py::TestConsistency::test_empty_reconciled_result_with_paging.
# The same with 4 partitions and a LIMIT 2 query at ALL paged one row at a
# time: it returns 2 rows in 2 non-empty pages.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_empty_reconciled_result_with_paging(manager: ScyllaClusterManager):
    cql2 = await empty_reconciled_result_cluster(manager, 4)
    result = cql2.execute(SimpleStatement("select r from ks.cf1 limit 2", consistency_level=ConsistencyLevel.ALL, fetch_size=1))
    # Like the dtests' PageFetcher, count the rows of the non-empty pages.
    all_pages = [len(result.current_rows)]
    while result.has_more_pages:
        result.fetch_next_page()
        all_pages.append(len(result.current_rows))
    all_pages = [n for n in all_pages if n]
    assert sum(all_pages) == 2
    assert len(all_pages) == 2


# Ported from scylla-dtest consistency_test.py::TestConsistency::test_short_read_partitions.
# Each node deleted a different partition of 4: a LIMIT 2 range scan at ALL
# returns the other two.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_short_read_partitions(manager: ScyllaClusterManager):
    (node1, node2), cql1 = await two_nodes(manager, {"start_rpc": True, "cache_hit_rate_read_balancing": False},
                                           hinted_handoff_after_start=True)
    cql1.execute("create table ks.cf1 (p int, c text, r text, primary key (p, c))")
    for p in range(1, 5):
        execute_at(cql1, f"insert into ks.cf1 (p, c, r) values ({p}, '1', '{p}')", ConsistencyLevel.ALL)

    # Update node1.
    await stop_node(manager, node2, wait_other_notice=True)
    execute_at(cql1, "delete from ks.cf1 where p = 1", ConsistencyLevel.ONE)

    # Update node2.
    await stop_node(manager, node1)
    await start_node(manager, node2)
    cql2 = connect(manager, node2)
    execute_at(cql2, "delete from ks.cf1 where p = 2", ConsistencyLevel.ONE)

    # Query the whole cluster.
    await start_node(manager, node1, wait_other_notice=True)
    res = execute_at(cql2, "select r from ks.cf1 limit 2", ConsistencyLevel.ALL)
    assert len(res) == 2, f"Expecting 2 rows, got {len(res)} ({res})"
    assert len(res[0]) == 1, f"Expecting 1 cell, got {len(res[0])} ({res[0]})"
    assert res[0][0] == "4", f"Expecting value 4, got {res[0][0]}"
    assert len(res[1]) == 1, f"Expecting 1 cell, got {len(res[1])} ({res[1]})"
    assert res[1][0] == "3", f"Expecting value 3, got {res[1][0]}"


# A replica that missed deletions of all but one of the partitions a
# LIMIT 3 read would see first, and the insertion of newer ones: the retry
# after the short read reaches the end of that replica's data.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_reaching_end_after_retry(manager: ScyllaClusterManager):
    servers, ks_opts = await start_cluster(manager, 2)
    async with new_test_keyspace(manager, ks_opts) as ks:
        await execute(manager, f"CREATE TABLE {ks}.t (p int PRIMARY KEY, r int)")
        for p in range(4):
            await execute(manager, f"INSERT INTO {ks}.t (p, r) VALUES ({p}, {p})")
        await execute_while_down(manager, servers[0], [f"DELETE FROM {ks}.t WHERE p = {p}" for p in range(3)] +
                                 [f"INSERT INTO {ks}.t (p, r) VALUES ({p}, {p})" for p in range(4, 6)])
        assert sorted(r.r for r in await execute(manager, f"SELECT r FROM {ks}.t LIMIT 3")) == [3, 4, 5]
