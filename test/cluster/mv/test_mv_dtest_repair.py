#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Materialized views made consistent by repair, ported from scylla-dtest
# materialized_views_test.py, one test per dtest: a view written while a
# node was down, a base replica whose data was lost, base and view replicas
# written in two disjoint halves of the cluster (two variants), repair of
# the view table alone, a view after a node is decommissioned and another
# joins, and range deletions repaired to a node that lost its data, once or
# in several cycles. Keyspaces use vnodes and nodes are spread over one rack
# per replica, like in the dtests; repairs run "nodetool repair" (and, for
# a repair of every keyspace, "nodetool cluster repair") as ccm's
# node.repair() does.

import re
import subprocess

import pytest
from cassandra import ConsistencyLevel, ReadFailure, ReadTimeout, Unavailable, WriteFailure, WriteTimeout
from cassandra.concurrent import execute_concurrent
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.mv.dtest_mv_util import (add_node, assert_two_queries_equal, delete_sstables, eventually, flush_all_nodes, nodetool,
                                           prepare, repair, session_for, start_nodes, stop_nodes, wait_for_view,
                                           wait_for_view_update_generation)
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


UNAVAILABLE = (Unavailable, WriteTimeout, WriteFailure, ReadTimeout, ReadFailure)


def one(cql, q, cl=ConsistencyLevel.ONE):
    return [list(r) for r in cql.execute(SimpleStatement(q, consistency_level=cl))]


def check_all(cql, queries_and_expected, cl=ConsistencyLevel.ONE):
    """Runs the queries concurrently and checks each result."""
    results = execute_concurrent(cql, [(SimpleStatement(q, consistency_level=cl), ()) for q, _ in queries_and_expected],
                                 concurrency=50, raise_on_first_error=True)
    for (q, expected), (ok, res) in zip(queries_and_expected, results):
        assert [list(r) for r in res] == expected, q


def insert_all(cql, statements, cl=None):
    execute_concurrent(cql, [(SimpleStatement(s, consistency_level=cl) if cl is not None else s, ()) for s in statements],
                       concurrency=50, raise_on_first_error=True)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_simple_repair(manager: ScyllaClusterManager):
    """A view written while a node was down is complete on all replicas
    after a repair."""
    servers, cql = await prepare(manager, nodes=3, rf=3, config={"hinted_handoff_enabled": False})
    node1, node2, _ = servers
    cql.execute("CREATE TABLE ks.t (id int PRIMARY KEY, v int, v2 text, v3 decimal) WITH read_repair_chance = 0.0")
    cql.execute("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM ks.t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id) WITH read_repair_chance = 0.0")
    cql.cluster.control_connection.wait_for_schema_agreement()

    await stop_nodes(manager, [node2])
    for i in range(1000):
        cql.execute(f"INSERT INTO ks.t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0)")
    # With one node down, two base replicas may be updated but only one view
    # replica, so read the view at CL=TWO. #17043
    eventually(lambda: check_all(cql, [(f"SELECT * FROM ks.t_by_v WHERE v = {i}", [[i, i, "a", 3.0]]) for i in range(1000)],
                                 ConsistencyLevel.TWO))
    for i in range(1000):
        try:
            cql.execute(SimpleStatement(f"SELECT * FROM ks.t_by_v WHERE v = {i}", consistency_level=ConsistencyLevel.ALL))
            assert False, "expected the read at ALL to fail"
        except UNAVAILABLE:
            pass

    await start_nodes(manager, [node2])
    await repair(manager, node1)
    check_all(cql, [(f"SELECT * FROM ks.t_by_v WHERE v = {i}", [[i, i, "a", 3.0]]) for i in range(1000)])


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_base_replica_repair(manager: ScyllaClusterManager):
    """A view is consistent again after repairing a base replica that lost
    its data, and then the view."""
    servers, cql = await prepare(manager, nodes=3, rf=3)
    node1, node2, node3 = servers
    cql.execute("CREATE TABLE ks.t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
    cql.execute("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM ks.t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")
    wait_for_view(cql, 3, "ks", "t_by_v")
    cql.cluster.control_connection.wait_for_schema_agreement()

    num_keys = 10000
    insert_all(cql, [f"INSERT INTO ks.t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0)" for i in range(num_keys)])
    view_rows = [(f"SELECT * FROM ks.t_by_v WHERE v = {i}", [[i, i, "a", 3.0]]) for i in range(num_keys)]
    check_all(cql, view_rows, ConsistencyLevel.ALL)

    # node1 loses its data.
    await stop_nodes(manager, [node1])
    await delete_sstables(manager, node1, "ks", ["t_by_v", "t"])
    await manager.server_start(node1.server_id)
    await stop_nodes(manager, [node2, node3])

    cql1 = session_for(manager, node1, exclusive=True)
    check_all(cql1, [(q, []) for q, _ in view_rows])

    await start_nodes(manager, [node2, node3])
    # Repair the base replica only, then the view on every node, as
    # base/view self-pairing is not guaranteed.
    await repair(manager, node1, "ks", ["t"])
    await wait_for_view_update_generation(manager, node1)
    for s in servers:
        await repair(manager, s, "ks", ["t_by_v"], partitioner_range=True)

    check_all(cql1, [(f"SELECT * FROM ks.t WHERE id = {i}", [[i, i, "a", 3.0]]) for i in range(num_keys)])
    check_all(cql1, view_rows)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_complex_repair(manager: ScyllaClusterManager):
    """
    Base and view rows written in two disjoint halves of a five-node RF=5
    cluster: after a repair, the view matches the base on both halves.
    """
    servers, cql = await prepare(manager, nodes=5, rf=5, config={"hinted_handoff_enabled": False, "read_repair_chance": 0.0})
    node1, node2, node3, node4, node5 = servers
    # gc_grace_seconds=5 would let the batchlog expire after 5 seconds.
    cql.execute("CREATE TABLE ks.t (id int PRIMARY KEY, v int, v2 text, v3 decimal)WITH gc_grace_seconds = 5")
    cql.execute("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM ks.t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")
    wait_for_view(cql, 5, "ks", "t_by_v")
    cql.cluster.control_connection.wait_for_schema_agreement()

    rows = 1000

    def verify(s, cl, multiply, none_data=False):
        expected = []
        for i in range(rows):
            v = i * 2 if multiply else i
            expected.append((f"SELECT * FROM ks.t_by_v WHERE v = {v}",
                             [] if none_data else [[v, v, "a", 6.0 if multiply else 3.0]]))
        check_all(s, expected, cl)

    await stop_nodes(manager, [node2, node3])
    # Written to node1, node4 and node5.
    insert_all(cql, [f"INSERT INTO ks.t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0)" for i in range(rows)], ConsistencyLevel.THREE)
    verify(cql, ConsistencyLevel.ONE, False)

    await stop_nodes(manager, [node1, node4, node5])
    await start_nodes(manager, [node2, node3])
    cql2 = session_for(manager, node2)
    verify(cql2, ConsistencyLevel.ONE, False, none_data=True)
    # New data on node2 and node3, overlapping the old: v = i*2.
    insert_all(cql2, [f"INSERT INTO ks.t (id, v, v2, v3) VALUES ({i * 2}, {i * 2}, 'a', 6.0)" for i in range(rows)], ConsistencyLevel.TWO)
    verify(cql2, ConsistencyLevel.ONE, True)

    await start_nodes(manager, [node1, node4, node5])
    await stop_nodes(manager, [node2, node3])
    cql1 = session_for(manager, node1)
    verify(cql1, ConsistencyLevel.QUORUM, False)

    await start_nodes(manager, [node2, node3])
    await repair(manager, node1)
    await stop_nodes(manager, [node2, node3])
    assert_two_queries_equal(cql1, "SELECT * FROM ks.t", cql1, "SELECT * FROM ks.t_by_v", ConsistencyLevel.QUORUM)

    await start_nodes(manager, [node2, node3])
    await stop_nodes(manager, [node1, node4, node5])
    cql2 = session_for(manager, node2)
    assert_two_queries_equal(cql2, "SELECT * FROM ks.t", cql2, "SELECT * FROM ks.t_by_v", ConsistencyLevel.ONE)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_range_tombstone_and_repair_multiple_cycles(manager: ScyllaClusterManager):
    """
    Range deletions on a base table with a view, repaired to a node that
    lost its sstables, in four cycles on the same cluster: the view update
    generator leaves no unclosed permit, and the view matches the base.
    Reproducer for scylla-enterprise#3072.
    """
    servers, cql = await prepare(manager, nodes=2, rf=2)
    node1, node2 = servers
    table, mv = "ks.tombstone_table", "ks.tombstone_mv_by_v2"
    cql.execute(f"CREATE TABLE {table} (id int, ck int, v2 int, v3 text, PRIMARY KEY(id, ck))")
    cql.execute(f"CREATE MATERIALIZED VIEW {mv} AS SELECT * FROM {table} WHERE ck IS NOT NULL  AND v2 is not null PRIMARY KEY (v2, id, ck)")
    wait_for_view(cql, 2, "ks", "tombstone_mv_by_v2")
    cql.cluster.control_connection.wait_for_schema_agreement()
    for k in range(4):
        for i in range(300):
            cql.execute(f"INSERT INTO {table} (id, ck, v2, v3) VALUES ({k}, {i}, {i}, '{10000 * ' '}')")
    await flush_all_nodes(manager, servers)

    for where_clause in ["id = 0 and ck >= 0", "id = 1 and ck > 0 and ck < 300", "id in (2, 3) and ck > 50 and ck < 150", "id in (2, 3) and ck > 150"]:
        cql.execute(f"DELETE FROM {table} where {where_clause}")

        await stop_nodes(manager, [node2])
        await delete_sstables(manager, node2, "ks", ["tombstone_mv_by_v2", "tombstone_table"])
        await start_nodes(manager, [node2])

        log = await manager.server_open_log(node2.server_id)
        mark = await log.mark()
        try:
            await nodetool(manager, node2, "repair", "ks", timeout=60)
        except subprocess.TimeoutExpired:
            pass
        errors = await log.grep(re.escape("permit ks.tombstone_table:view_update_generator: was not closed before destruction"), from_mark=mark)
        assert not errors, f"Found error during repair: {errors}"

        await stop_nodes(manager, [node1])
        cql2 = session_for(manager, node2)
        base = sorted(one(cql2, f"select id, ck, v2, v3 from {table}"))
        view = sorted(one(cql2, f"select id, ck, v2, v3 from {mv}"))
        assert base == view
        assert one(cql2, f"select * from {mv} where {where_clause} ALLOW FILTERING") == []
        await start_nodes(manager, [node1])
        # Like in the dtest, the next cycle's deletion goes through the
        # session to node2.
        cql = cql2


def assert_one(cql, q, expected, cl=None):
    res = [list(r) for r in cql.execute(SimpleStatement(q, consistency_level=cl))]
    assert res == [expected], f"Expected {[expected]} from {q}, but got {res}"


def assert_none(cql, q, cl=None):
    res = [list(r) for r in cql.execute(SimpleStatement(q, consistency_level=cl))]
    assert res == [], f"Expected nothing from {q}, but got {res}"


def assert_all(cql, q, expected, cl=None):
    res = [list(r) for r in cql.execute(SimpleStatement(q, consistency_level=cl))]
    assert res == expected, f"Expected {expected} from {q}, but got {res}"


def count_request_timeout(build_mode):
    """The dtests' count_request_timeout: cql_timeout(400)."""
    return 400 * (3 if build_mode in DEBUG_MODES else 1 if build_mode == "release" else 2)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_repair_mv.
# Repairing only the view fixes a view replica that missed updates while its
# node was down: with the other nodes down, the node has the new view rows,
# and its base rows are still the old ones. The view is keyed by the base's
# partition key (colocated replicas) or not.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("colocated_view_replicas", [True, False], ids=["colocated_view_replicas", "non_colocated_view_replicas"])
async def test_repair_mv(manager: ScyllaClusterManager, build_mode, colocated_view_replicas):
    timeout = count_request_timeout(build_mode)
    servers, cql = await prepare(manager, nodes=3, rf=3, config={"hinted_handoff_enabled": False, "range_request_timeout_in_ms": timeout * 1000},
                                 request_timeout=timeout, fetch_size=100)
    node1, node2, node3 = servers
    cql.execute("CREATE TABLE t (id int, v1 int, v2 int, PRIMARY KEY (id))")
    if colocated_view_replicas:
        cql.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE v1 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (id, v1)")
    else:
        cql.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE v1 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v1, id)")

    prefill = 100
    for i in range(prefill):
        # Symmetric (id, v1) pairs, so that the view's rows equal the base's.
        v1 = i + 1 if i % 2 == 0 else i - 1
        cql.execute(f"INSERT INTO t (id, v1, v2) VALUES ({i}, {v1}, {2})")
    eventually(lambda: assert_two_queries_equal(cql, "SELECT * FROM t", cql, "SELECT * FROM mv"))

    await stop_nodes(manager, [node2])
    for i in range(prefill // 2):
        cql.execute(f"UPDATE t SET v2=3 WHERE id={i}")
    # With a node down, two base replicas may be updated but only one view
    # replica, so read at CL=TWO.
    eventually(lambda: assert_two_queries_equal(cql, "SELECT * FROM t", cql, "SELECT * FROM mv", ConsistencyLevel.TWO))
    await start_nodes(manager, [node2])

    await repair(manager, node1, "ks", ["mv"])
    await stop_nodes(manager, [node1, node3])
    cql2 = session_for(manager, node2, exclusive=True)
    cql2.execute("USE ks")
    assert_one(cql2, "select count(*) from mv where v2=2 ALLOW FILTERING", [prefill - prefill // 2])
    assert_one(cql2, "select count(*) from mv where v2=3 ALLOW FILTERING", [prefill // 2])
    for i in range(prefill):
        assert_one(cql2, f"select v2 from t where id={i}", [2])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_sync_after_remove_add_node.
# After a node is decommissioned and a new one joins its rack, the new node
# alone (nodes 2 and 3 down) reads every view row at CL=ONE: the view went
# through view building and off-strategy compaction on the new node.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_sync_after_remove_add_node(manager: ScyllaClusterManager):
    config = {"hinted_handoff_enabled": False}
    servers, cql = await prepare(manager, nodes={"dc1": {"RAC1": 1, "RAC2": 1, "RAC3": 2}}, rf=3, config=config)
    node4 = servers[-1]
    cql.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal) WITH read_repair_chance = 0.0")
    cql.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id) WITH read_repair_chance = 0.0")
    cql.cluster.control_connection.wait_for_schema_agreement()

    partition_num = 4444
    for i in range(partition_num):
        cql.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0)")

    await manager.decommission_node(node4.server_id)
    for i in range(partition_num):
        assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0], cl=ConsistencyLevel.ONE)

    _, new_node_cql = await add_node(manager, rack="RAC3", config=config)
    wait_for_view(cql, 4, "ks", "t_by_v", cl=ConsistencyLevel.ALL)
    for node in servers[1:3]:
        await stop_nodes(manager, [node])
    for i in range(partition_num):
        assert_one(new_node_cql, f"SELECT * FROM ks.t_by_v WHERE v = {i}", [i, i, "a", 3.0], cl=ConsistencyLevel.ONE)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_really_complex_repair.
# Two disjoint groups of replicas of an RF=5 keyspace write conflicting view
# keys of the same base rows while the other group is down, the second also
# deleting the rows; after a repair of the whole cluster, the first group's
# view rows do not come back.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_really_complex_repair(manager: ScyllaClusterManager):
    servers, cql = await prepare(manager, nodes=5, rf=5, config={"hinted_handoff_enabled": False})
    node1, node2, node3, node4, node5 = servers
    # gc_grace_seconds=1, so that the batchlog would expire soon.
    cql.execute("CREATE TABLE ks.t (id int, v int, v2 text, v3 decimal, PRIMARY KEY(id, v, v2))WITH gc_grace_seconds = 1")
    cql.execute("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL AND v IS NOT NULL AND v2 IS NOT NULL PRIMARY KEY (v2, v, id)")
    wait_for_view(cql, 5, "ks", "t_by_v")
    cql.cluster.control_connection.wait_for_schema_agreement()

    await stop_nodes(manager, [node2, node3])
    # With N=RF, view updates are synchronous: at CL=THREE the view of the
    # three live nodes is up to date when the writes return.
    cql.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'a', 3.0)", consistency_level=ConsistencyLevel.THREE))
    cql.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'a', 3.0)", consistency_level=ConsistencyLevel.THREE))
    assert_all(cql, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", [["a", 1, 1, 3.0], ["a", 2, 2, 3.0]])
    cql.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'b', 3.0)", consistency_level=ConsistencyLevel.THREE))
    cql.execute(SimpleStatement("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'b', 3.0)", consistency_level=ConsistencyLevel.THREE))
    assert_all(cql, "SELECT * FROM ks.t_by_v WHERE v2 = 'b'", [["b", 1, 1, 3.0], ["b", 2, 2, 3.0]])
    cql.shutdown()

    await stop_nodes(manager, [node1, node4, node5])
    await start_nodes(manager, [node2, node3])
    cql2 = session_for(manager, node2, exclusive=True)
    cql2.execute("USE ks")
    assert_none(cql2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'")

    for v2 in ["c", "d"]:
        cql2.execute(SimpleStatement(f"INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, '{v2}', 3.0)", consistency_level=ConsistencyLevel.TWO))
        cql2.execute(SimpleStatement(f"INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, '{v2}', 3.0)", consistency_level=ConsistencyLevel.TWO))
        assert_all(cql2, f"SELECT * FROM ks.t_by_v WHERE v2 = '{v2}'", [[v2, 1, 1, 3.0], [v2, 2, 2, 3.0]])

    # Composite delete of everything.
    cql2.execute(SimpleStatement("DELETE FROM ks.t WHERE id = 1 and v = 1", consistency_level=ConsistencyLevel.TWO))
    cql2.execute(SimpleStatement("DELETE FROM ks.t WHERE id = 2 and v = 2", consistency_level=ConsistencyLevel.TWO))
    assert_none(cql2, "SELECT * FROM ks.t_by_v WHERE v2 = 'c'")
    assert_none(cql2, "SELECT * FROM ks.t_by_v WHERE v2 = 'd'")

    await start_nodes(manager, [node1, node4, node5])
    # Before the repair, the view rows of 'a' may or may not be back.
    res = [list(r) for r in cql2.execute(SimpleStatement("SELECT * FROM ks.t_by_v WHERE v2 = 'a'", consistency_level=ConsistencyLevel.QUORUM))]
    assert res in ([["a", 1, 1, 3.0], ["a", 2, 2, 3.0]], []), f"Expected the 'a' rows or nothing, but got {res}"

    await repair(manager, node1)
    assert_none(cql2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", cl=ConsistencyLevel.QUORUM)


async def prepare_schema_for_range_tombstone_tests(manager: ScyllaClusterManager, servers, cql, table, mv):
    """The dtests' prepare_schema_for_range_tombstone_tests(): the base table
    and its view, and four partitions of 300 rows of 10 KB, flushed."""
    cql.execute(f"CREATE TABLE {table} (id int, ck int, v2 int, v3 text, PRIMARY KEY(id, ck))")
    cql.execute(f"CREATE MATERIALIZED VIEW {mv} AS SELECT * FROM {table} WHERE ck IS NOT NULL  AND v2 is not null PRIMARY KEY (v2, id, ck)")
    wait_for_view(cql, len(servers), "ks", mv)
    cql.cluster.control_connection.wait_for_schema_agreement()
    for k in range(4):
        for i in range(300):
            cql.execute(f"INSERT INTO {table} (id, ck, v2, v3) VALUES ({k}, {i}, {i}, '{10000 * ' '}')")
    await flush_all_nodes(manager, servers)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_range_tombstone_and_repair_test.
# Range deletions, repaired to a node that lost its sstables, are turned into
# view updates split at the view_updating_consumer's buffer limit, which must
# not leave a range tombstone open: the repair leaks no reader permit, and
# the node alone has the same rows in the view as in the base. Reproducer for
# scylla-enterprise#3072, on a new cluster for each set of deletions.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize(
    "where_clauses",
    [["id = 0 and ck >= 0"], ["id = 0 and ck > 0 and ck < 300"], ["id in (0, 1) and ck > 50 and ck < 290"], ["id in (0, 1) and ck > 150"],
     ["id = 0 and ck > 250", "id in (0, 1) and ck > 250", "id in (0, 1) and ck > 200 and ck < 250"]],
    ids=["delete_open_range", "delete_close_range", "delete_close_range_in_few_partitions", "delete_open_range_in_few_partitions", "run_few_delete_queries"],
)
async def test_range_tombstone_and_repair_test(manager: ScyllaClusterManager, where_clauses):
    servers, cql = await prepare(manager, nodes=2, rf=2)
    node1, node2 = servers
    table, mv = "tombstone_table", "tombstone_mv_by_v2"
    await prepare_schema_for_range_tombstone_tests(manager, servers, cql, table, mv)

    for where_clause in where_clauses:
        cql.execute(f"DELETE FROM {table} where {where_clause}")

    await stop_nodes(manager, [node2])
    await delete_sstables(manager, node2, "ks", [mv, table])
    await start_nodes(manager, [node2])

    log = await manager.server_open_log(node2.server_id)
    mark = await log.mark()
    try:
        await repair(manager, node2, "ks", timeout=60)
    except subprocess.TimeoutExpired:
        pass
    expected_error = f"permit ks.{table}:view_update_generator: was not closed before destruction"
    found_error = [e for e in await log.grep_for_errors(distinct_errors=True, from_mark=mark) if expected_error in e]
    assert not found_error, f"Found error during repair: {found_error}"

    await stop_nodes(manager, [node1])
    cql2 = session_for(manager, node2)
    cql2.execute("USE ks")
    base = sorted(one(cql2, f"select id, ck, v2, v3 from {table}"))
    view = sorted(one(cql2, f"select id, ck, v2, v3 from {mv}"))
    assert base == view
    for where_clause in where_clauses:
        assert_none(cql2, f"select * from {mv} where {where_clause} ALLOW FILTERING")
