#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Materialized view basics, ported from scylla-dtest materialized_views_test.py,
# one test per dtest: creating views, querying them through prepared
# statements, truncating the base, dropping views, filtering on views,
# gc_grace_seconds validation, a column added to the base of a view that
# does not select it, LWT writes to the base, adding and dropping base
# columns while the base is written to, and views on a cluster that grows by
# a node or by a datacenter. The clusters are those of the dtests' prepare():
# three nodes with an RF=1 vnodes keyspace "ks" unless the dtest says
# otherwise. As on dtest nodes, auto_snapshot is on.

import re
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, WriteFailure
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.mv.dtest_mv_util import (add_node, assert_row_count, check_errors, create_cf, eventually, flush_all_nodes, nodetool,
                                           prepare, session_for, wait_for_token_ring_and_group0_consistency, wait_for_view)
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


CONFIG = {"auto_snapshot": True}


def rows(cql, q, cl=None):
    stmt = SimpleStatement(q, consistency_level=cl)
    return [list(r) for r in cql.execute(stmt)]


def assert_one(cql, q, expected, cl=None):
    res = rows(cql, q, cl)
    assert res == [expected], f"Expected {[expected]} from {q}, but got {res}"


def assert_none(cql, q, cl=None):
    res = rows(cql, q, cl)
    assert res == [], f"Expected nothing from {q}, but got {res}"


def assert_invalid(cql, q, matching=None, expected=InvalidRequest):
    """The dtests' assert_invalid(): the query fails with the expected
    exception, whose message contains matching."""
    with pytest.raises(expected) as e:
        cql.execute(q)
    if matching:
        assert re.search(re.escape(matching), str(e.value)), f"{str(e.value)!r} does not contain {matching!r}"


def retrying_assert_one(cql, q, expected, num_attempts):
    """retrying(num_attempts=num_attempts, sleep_time=1)(assert_one)."""
    for i in range(num_attempts):
        try:
            return assert_one(cql, q, expected)
        except AssertionError:
            if i == num_attempts - 1:
                raise
            time.sleep(1)


def insert_data(cql):
    """The dtests' _insert_data()."""
    insert_stmt = "INSERT INTO users (username, password, gender, state, birth_year) VALUES "
    cql.execute(insert_stmt + "('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
    cql.execute(insert_stmt + "('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
    cql.execute(insert_stmt + "('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
    cql.execute(insert_stmt + "('user4', 'ch@ngem3d', 'm', 'TX', 1974);")


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_create.
# The view of the users table is in system_schema.views.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_create(manager: ScyllaClusterManager):
    _, cql = await prepare(manager, config=CONFIG, user_table=True)
    result = list(cql.execute("SELECT * FROM system_schema.views WHERE keyspace_name='ks' ALLOW FILTERING"))
    assert len(result) == 1, "Expecting 1 materialized view, got" + str(result)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_gcgs_validation.
# gc_grace_seconds = 0 is refused on the base table of a view, on a view, and
# for a new view on a base table that has it.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_gcgs_validation(manager: ScyllaClusterManager):
    _, cql = await prepare(manager, config=CONFIG, user_table=True)

    assert_invalid(cql, "ALTER TABLE users WITH gc_grace_seconds = 0",
                   "Cannot alter gc_grace_seconds of the base table of a materialized view "
                   "to 0, since this value is used to TTL undelivered updates. Setting "
                   "gc_grace_seconds too low might cause undelivered updates to expire "
                   "before being replayed.")
    cql.execute("ALTER TABLE users WITH gc_grace_seconds = 10")
    assert_invalid(cql, "ALTER MATERIALIZED VIEW users_by_state WITH gc_grace_seconds = 0",
                   "Cannot alter gc_grace_seconds of a materialized view to 0, since this value is used to TTL undelivered updates. "
                   "Setting gc_grace_seconds too low might cause undelivered updates to expire before being replayed.")
    cql.execute("DROP MATERIALIZED VIEW ks.users_by_state;")
    cql.execute("ALTER TABLE users WITH gc_grace_seconds = 0")
    assert_invalid(cql, "CREATE MATERIALIZED VIEW users_by_state AS SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)",
                   "Cannot create materialized view 'users_by_state' for base table 'users' "
                   "with gc_grace_seconds of 0, since this value is used to TTL undelivered "
                   "updates. Setting gc_grace_seconds too low might cause undelivered updates"
                   " to expire before being replayed.")


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_prepared_statement.
# Inserts into the base and queries of the view, prepared.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_prepared_statement(manager: ScyllaClusterManager):
    _, cql = await prepare(manager, config=CONFIG, user_table=True)
    insert = cql.prepare("INSERT INTO users (username, password, gender, state, birth_year) VALUES (?, ?, ?, ?, ?);")
    select = cql.prepare("SELECT state, password, session_token FROM users_by_state WHERE state=?;")
    cql.execute(insert.bind(("user1", "ch@ngem3a", "f", "TX", 1968)))
    cql.execute(insert.bind(("user2", "ch@ngem3b", "m", "CA", 1971)))
    cql.execute(insert.bind(("user3", "ch@ngem3c", "f", "FL", 1978)))
    cql.execute(insert.bind(("user4", "ch@ngem3d", "m", "TX", 1974)))

    result = list(cql.execute("SELECT * FROM users;"))
    assert len(result) == 4, f"Expecting {4} users, got {len(result)}"
    for state, n in [("TX", 2), ("CA", 1), ("MA", 0)]:
        result = list(cql.execute(select.bind([state])))
        assert len(result) == n, f"Expecting {n} users, got {len(result)}"


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_truncate_base.
# Truncating the base, whose data is in sstables and in the cache, empties
# the view too.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_truncate_base(manager: ScyllaClusterManager):
    servers, cql = await prepare(manager, config=CONFIG, user_table=True)
    cql.execute("INSERT INTO users (state, username) VALUES ('TX', 'user1')")
    await flush_all_nodes(manager, servers)
    assert_one(cql, "SELECT * FROM users_by_state", ["TX", "user1", None, None, None, None])
    cql.execute("TRUNCATE table users")
    assert_row_count(cql, "users", 0, ConsistencyLevel.QUORUM)
    assert_row_count(cql, "users_by_state", 0, ConsistencyLevel.QUORUM)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_drop_mv.
# A second view is created and the first one dropped.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_drop_mv(manager: ScyllaClusterManager):
    _, cql = await prepare(manager, config=CONFIG, user_table=True)
    cql.execute("CREATE MATERIALIZED VIEW users_by_birth_year AS SELECT * FROM users WHERE birth_year IS NOT NULL AND username IS NOT NULL PRIMARY KEY (birth_year, username)")
    result = list(cql.execute("SELECT * FROM system_schema.views WHERE keyspace_name='ks'"))
    assert len(result) == 2, f"Expecting {2} materialized view, got {len(result)}"
    cql.execute("DROP MATERIALIZED VIEW ks.users_by_state;")
    result = list(cql.execute("SELECT * FROM system_schema.views WHERE keyspace_name='ks'"))
    assert len(result) == 1, f"Expecting {1} materialized view, got {len(result)}"


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_allow_filtering.
# Queries on views filter on non-key columns with ALLOW FILTERING only.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_allow_filtering(manager: ScyllaClusterManager, build_mode):
    _, cql = await prepare(manager, config=CONFIG)
    cql.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
    cql.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")
    cql.execute("CREATE MATERIALIZED VIEW t_by_v2 AS SELECT * FROM t WHERE v2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v2, id)")
    for i in range(1000):
        cql.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0)")

    num_attempts = 75 if build_mode not in DEBUG_MODES else 300
    for i in range(1000):
        retrying_assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0], num_attempts)

    def check_t_by_v2():
        n = len(list(cql.execute("SELECT * FROM t_by_v2 WHERE v2 = 'a'")))
        assert n == 1000, f"Expected 1000 rows but got {n}"
    for attempt in range(num_attempts):
        try:
            check_t_by_v2()
            break
        except AssertionError:
            if attempt == num_attempts - 1:
                raise
            time.sleep(1)

    assert_invalid(cql, "SELECT * FROM t_by_v WHERE v = 1 AND v2 = 'a'", expected=Exception)
    assert_invalid(cql, "SELECT * FROM t_by_v2 WHERE v2 = 'a' AND v = 1", expected=Exception)
    for i in range(1000):
        assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {i} AND v3 = 3.0 ALLOW FILTERING", [i, i, "a", 3.0])
        assert_one(cql, f"SELECT * FROM t_by_v2 WHERE v2 = 'a' AND v = {i} ALLOW FILTERING", ["a", i, i, 3.0])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_query_new_column.
# A column added to the base of a view that selects only some columns does
# not show up in the view.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_query_new_column(manager: ScyllaClusterManager):
    _, cql = await prepare(manager, config=CONFIG, user_table=True)
    cql.execute("CREATE MATERIALIZED VIEW users_by_state2 AS SELECT username FROM users WHERE STATE IS NOT NULL AND USERNAME IS NOT NULL PRIMARY KEY (state, username)")
    insert_data(cql)
    assert_one(cql, "SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'", ["TX", "user1"])
    cql.execute("ALTER TABLE users ADD first_name varchar;")
    results = list(cql.execute("SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'"))
    assert len(results) == 1
    assert not hasattr(results[0], "first_name"), 'Column "first_name" found in view'
    assert_one(cql, "SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'", ["TX", "user1"])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_lwt.
# Conditional inserts, updates and deletes of the base change the view only
# when they are applied.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_lwt(manager: ScyllaClusterManager):
    _, cql = await prepare(manager, config=CONFIG)
    cql.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
    cql.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")

    for i in range(1000):
        cql.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {i}, 'a', 3.0) IF NOT EXISTS")
    for i in range(1000):
        assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0])

    for i in range(1000):
        cql.execute(f"INSERT INTO t (id, v, v2, v3) VALUES ({i}, {i * 2}, 'a', 3.0) IF NOT EXISTS")
    for i in range(1000):
        assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0])

    for i in range(1000):
        cql.execute(f"UPDATE t SET v={i + 2000} WHERE id = {i} IF v < 10")
    results = list(cql.execute("SELECT * FROM t_by_v;"))
    assert len(results) == 1000
    for i in range(1000):
        v = i + 2000 if i < 10 else i
        assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {v}", [v, i, "a", 3.0])

    for i in range(1000):
        cql.execute(f"DELETE FROM t WHERE id = {i} IF v = {i + 2000} ")
    results = list(cql.execute("SELECT * FROM t_by_v;"))
    assert len(results) == 990
    for i in range(10, 1000):
        assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {i}", [i, i, "a", 3.0])


async def prepare_cluster_for_drop(manager: ScyllaClusterManager):
    """The dtests' _prepare_cluster_for_drop(): three nodes, RF=3, the table
    cf (key int, v0 int, v1 int) with the view mv_v1_view, and one row."""
    servers, cql = await prepare(manager, nodes=3, rf=3, config=CONFIG)
    create_cf(cql, "cf", columns={"v0": "int", "v1": "int"}, key_type="int")
    cql.execute("CREATE MATERIALIZED VIEW mv_v1_view AS SELECT v1, key FROM cf WHERE v1 IS NOT NULL and key IS NOT NULL PRIMARY KEY (v1, key)")
    wait_for_view(cql, 3, "ks", "mv_v1_view")
    cql.execute("INSERT INTO cf (key, v0, v1) VALUES(0, 0, 0)")
    return servers, cql


def run_insert(cql, columns, range_start, range_end, repeat):
    for _ in range(repeat):
        try:
            for i in range(range_start, range_end):
                cql.execute(f"INSERT INTO cf ({columns}) VALUES({','.join([str(i) for _ in columns.split(',')])})")
        except WriteFailure as wf:
            assert False, f"Insert request failed unexpectedly with exception {wf}"


async def mark_logs(manager: ScyllaClusterManager, servers):
    marks = {}
    for s in servers:
        marks[s.server_id] = await (await manager.server_open_log(s.server_id)).mark()
    return marks


async def search_for_apply_mutation_error(manager: ScyllaClusterManager, servers, marks, update_mark_logs=True):
    """The dtests' _search_for_apply_mutation_error(): no node logs "Failed
    to apply mutation" since its mark, within 5 seconds. (The dtest's bare
    except swallows its own assertion, so it never fails; this checks what
    it meant to.)"""
    for s in servers:
        log = await manager.server_open_log(s.server_id)
        deadline = time.time() + 5
        while True:
            found = await log.grep("Failed to apply mutation", from_mark=marks[s.server_id])
            assert not found, f"'Failed to apply mutation' error found in the log of {s.ip_addr} unexpectedly"
            if time.time() > deadline:
                break
            time.sleep(1)
        if update_mark_logs:
            marks[s.server_id] = await log.mark()


def rows_validation(cql, n):
    """The dtests' rows_validation(), retried 3 times a second apart."""
    for attempt in range(3):
        try:
            assert_row_count(cql, "cf", n, ConsistencyLevel.QUORUM)
            assert_row_count(cql, "mv_v1_view", n, ConsistencyLevel.QUORUM)
            return
        except AssertionError:
            if attempt == 2:
                raise
            time.sleep(1)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_add_drop_column.
# Columns added to the base after the view was created, and then dropped,
# while rows are written: no write fails and no node fails to apply a
# mutation. scylla-enterprise#1467, scylladb#7061.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_add_drop_column(manager: ScyllaClusterManager):
    servers, cql = await prepare_cluster_for_drop(manager)
    marks = await mark_logs(manager, servers)

    with ThreadPoolExecutor(max_workers=1) as tp:
        thread = tp.submit(run_insert, cql, "key, v0, v1", 0, 20, 20)
        for i in range(2, 7):
            cql.execute(f"ALTER TABLE cf ADD v{i} int")
        thread.result(timeout=60)
    await search_for_apply_mutation_error(manager, servers, marks)
    rows_validation(cql, 20)

    with ThreadPoolExecutor(max_workers=1) as tp:
        thread = tp.submit(run_insert, cql, "key, v0, v1", 20, 40, 20)
        for i in range(2, 7):
            cql.execute(f"ALTER TABLE cf DROP v{i}")
        thread.result(timeout=60)
    await search_for_apply_mutation_error(manager, servers, marks, update_mark_logs=False)
    rows_validation(cql, 40)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_drop_existing_column.
# A base column that existed before the view was created, and is not
# selected by it, is dropped while rows are written: no write fails and no
# node fails to apply a mutation. scylla-enterprise#1467, scylladb#7061.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_drop_existing_column(manager: ScyllaClusterManager):
    servers, cql = await prepare_cluster_for_drop(manager)
    marks = await mark_logs(manager, servers)

    with ThreadPoolExecutor(max_workers=1) as tp:
        thread = tp.submit(run_insert, cql, "key,v1", 0, 20, 20)
        cql.execute("ALTER TABLE cf DROP v0")
        thread.result(timeout=60)
    await search_for_apply_mutation_error(manager, servers, marks, update_mark_logs=False)
    rows_validation(cql, 20)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_add_node_after_mv.
# A view keeps working after a node joins: the new node serves the old
# rows, and new rows reach the view. CASSANDRA-10978.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_add_node_after_mv(manager: ScyllaClusterManager, build_mode):
    _, cql = await prepare(manager, nodes=3, rf=1, config=CONFIG)
    cql.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
    cql.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")
    for i in range(1000):
        cql.execute(f"INSERT INTO t (id, v) VALUES ({i}, {-i})")
    num_attempts = 75 if build_mode not in DEBUG_MODES else 300
    for i in range(1000):
        retrying_assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {-i}", [-i, i], num_attempts)

    _, cql2 = await add_node(manager, config=CONFIG)
    # Like the dtest, wait on every node until token ring and group 0 agree,
    # reading group 0 with cqlsh.
    for s in await manager.running_servers():
        await wait_for_token_ring_and_group0_consistency(manager, s, 4)
    for i in range(1000):
        assert_one(cql2, f"SELECT * FROM ks.t_by_v WHERE v = {-i}", [-i, i])
    for i in range(1000, 1100):
        cql.execute(f"INSERT INTO t (id, v) VALUES ({i}, {-i})")
    for i in range(1000, 1100):
        retrying_assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {-i}", [-i, i], num_attempts)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_add_node_after_wide_mv_with_range_deletions.
# A wide view with range deletions keeps its contents after a node joins.
# CASSANDRA-11670.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_add_node_after_wide_mv_with_range_deletions(manager: ScyllaClusterManager):
    _, cql = await prepare(manager, nodes=3, rf=1, config=CONFIG)
    cql.execute("CREATE TABLE t (id int, v int, PRIMARY KEY (id, v)) WITH compaction = { 'class': 'SizeTieredCompactionStrategy', 'enabled': 'false' }")
    cql.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")
    for i in range(10):
        for j in range(100):
            cql.execute(f"INSERT INTO t (id, v) VALUES ({i}, {j})")
    for i in range(10):
        for j in range(100):
            assert_one(cql, f"SELECT * FROM t WHERE id = {i} and v = {j}", [i, j])
            assert_one(cql, f"SELECT * FROM t_by_v WHERE id = {i} and v = {j}", [j, i])
    for i in range(10):
        for j in range(100):
            if j % 10 == 0:
                cql.execute(f"DELETE FROM t WHERE id = {i} AND v >= {j} and v < {j + 2}")

    def check(s, upto, ks=""):
        for i in range(10):
            for j in range(upto):
                if j < 100 and (j % 10 == 0 or (j - 1) % 10 == 0):
                    assert_none(s, f"SELECT * FROM {ks}t WHERE id = {i} and v = {j}")
                    assert_none(s, f"SELECT * FROM {ks}t_by_v WHERE id = {i} and v = {j}")
                else:
                    assert_one(s, f"SELECT * FROM {ks}t WHERE id = {i} and v = {j}", [i, j])
                    assert_one(s, f"SELECT * FROM {ks}t_by_v WHERE id = {i} and v = {j}", [j, i])

    check(cql, 100)
    _, cql2 = await add_node(manager, config=CONFIG)
    check(cql2, 100, "ks.")
    for i in range(10):
        for j in range(100, 110):
            cql.execute(f"INSERT INTO t (id, v) VALUES ({i}, {j})")
    check(cql2, 110, "ks.")


async def add_dc_after_mv(manager: ScyllaClusterManager, rf, nodes=3):
    """
    The dtests' _add_dc_after_mv_test(): a view keeps working when a
    datacenter is added: a dc2 node bootstraps, the keyspace is extended to
    dc2, the node is rebuilt from dc1, a second dc2 node joins, and the dc2
    node serves the view and writes new rows. CASSANDRA-10978.
    """
    servers, cql = await prepare(manager, nodes=nodes, rf=rf, config=CONFIG)
    cql.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
    cql.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")
    for i in range(1000):
        cql.execute(f"INSERT INTO t (id, v) VALUES ({i}, {-i})")
    for i in range(1000):
        eventually(lambda: assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {-i}", [-i, i]))
    for i in range(1000):
        assert_one(cql, f"SELECT * FROM t WHERE id = {i}", [i, -i])

    node4, _ = await add_node(manager, dc="dc2", config=CONFIG)
    cql.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};", timeout=200)
    await nodetool(manager, node4, "rebuild", "--", "dc1")
    await add_node(manager, dc="dc2", config=CONFIG)

    cql2 = session_for(manager, node4, exclusive=True)
    cql2.execute("USE ks")
    for i in range(1000):
        eventually(lambda: assert_one(cql2, f"SELECT * FROM ks.t_by_v WHERE v = {-i}", [-i, i], ConsistencyLevel.LOCAL_ONE))
    for i in range(1000, 1100):
        cql2.execute(f"INSERT INTO t (id, v) VALUES ({i}, {-i})")
    for i in range(1000, 1100):
        eventually(lambda: assert_one(cql, f"SELECT * FROM t_by_v WHERE v = {-i}", [-i, i], ConsistencyLevel.LOCAL_ONE))

    await check_errors(manager, servers[0], exclude_errors=[re.escape("migration_task - Cant send migration request")])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_add_dc_after_mv_simple_replication.
# A datacenter is added to a cluster whose keyspace has replication_factor 1.
# CASSANDRA-10634.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_add_dc_after_mv_simple_replication(manager: ScyllaClusterManager):
    await add_dc_after_mv(manager, 1)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_add_dc_after_mv_network_replication.
# A datacenter is added to a cluster whose keyspace is replicated to dc1
# only. CASSANDRA-10634.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_add_dc_after_mv_network_replication(manager: ScyllaClusterManager):
    await add_dc_after_mv(manager, {"dc1": 1}, nodes=[3])
