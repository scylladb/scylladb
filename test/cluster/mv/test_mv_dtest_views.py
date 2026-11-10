#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Materialized views on two- to four-node clusters with vnodes keyspaces,
# ported from scylla-dtest materialized_views_test.py: views with
# restrictions in their primary key, synchronous views emptied by a base
# truncation or read right after base updates, "nodetool viewbuildstatus"
# while a view builds and for a name that is not a view, views switched to
# synchronous updates, view updates of nodes that were down delivered by
# hinted handoff or read repair, views of tables with a default TTL, and
# view building with injected errors. viewbuildstatus uses the dtest's
# debug-mode size of 10000 rows; the synchronous updates test uses its full
# size of 100000 rows and 50 views.

import asyncio
import random
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.mv.dtest_mv_util import (VNODES, TableManager, assert_row_count, compact_all_nodes, create_cf, eventually, flush_all_nodes,
                                           insert_c1c2, nodetool, prepare, session_for, start_nodes, stop_nodes, sync_hinted_handoff,
                                           wait_for_view)
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


QUORUM = ConsistencyLevel.QUORUM


def rows(cql, q, cl=ConsistencyLevel.ONE):
    return [list(r) for r in cql.execute(SimpleStatement(q, consistency_level=cl))]


def assert_all(cql, q, expected, num_attempts=1):
    for attempt in range(num_attempts):
        res = sorted(rows(cql, q, QUORUM))
        if res == sorted(expected) or attempt == num_attempts - 1:
            assert res == sorted(expected), q
            return
        time.sleep(1)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_complex_mv_select_statements(manager: ScyllaClusterManager):
    """
    Views restricted to a = 1 AND c = 1, for each kind of primary key, follow
    inserts, updates and deletions of matching and non-matching base rows.
    CASSANDRA-9664. As on dtest nodes, auto_snapshot is on.
    """
    servers, _ = await prepare(manager, nodes=3, rf=3, config={"auto_snapshot": True})
    cql = session_for(manager, servers[0])
    cql.execute("CREATE KEYSPACE mvtest WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}")
    cql.execute("USE mvtest")

    for mv_primary_key in ["((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"]:
        cql.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY (a, b, c))")
        insert = cql.prepare("INSERT INTO test (a, b, c, d) VALUES (?, ?, ?, ?)")
        update = cql.prepare("UPDATE test SET d = ? WHERE a = ? AND b = ? AND c = ?")
        delete1 = cql.prepare("DELETE FROM test WHERE a = ? AND b = ? AND c = ?")
        delete2 = cql.prepare("DELETE FROM test WHERE a = ?")
        cql.cluster.control_connection.wait_for_schema_agreement()

        for row in [(0, 0, 0, 0), (0, 0, 1, 0), (0, 1, 0, 0), (0, 1, 1, 0), (1, 0, 0, 0), (1, 0, 1, 0), (1, 1, -1, 0), (1, 1, 0, 0), (1, 1, 1, 0)]:
            cql.execute(insert, row)

        cql.execute(f"CREATE MATERIALIZED VIEW mv AS SELECT * FROM test WHERE a = 1 AND b IS NOT NULL AND c = 1 PRIMARY KEY {mv_primary_key}")
        wait_for_view(cql, 3, "mvtest", "mv")
        select = "SELECT a, b, c, d FROM mv"
        assert_all(cql, select, [[1, 0, 1, 0], [1, 1, 1, 0]])

        # Inserts of rows that do not match the filter.
        cql.execute(insert, (0, 0, 1, 0))
        cql.execute(insert, (1, 1, 0, 0))
        assert_all(cql, select, [[1, 0, 1, 0], [1, 1, 1, 0]], 20)
        # An insert of a row that matches.
        cql.execute(insert, (1, 2, 1, 0))
        assert_all(cql, select, [[1, 0, 1, 0], [1, 1, 1, 0], [1, 2, 1, 0]], 20)
        # Updates of rows that do not match.
        cql.execute(update, (1, 1, -1, 0))
        cql.execute(update, (0, 1, 1, 0))
        assert_all(cql, select, [[1, 0, 1, 0], [1, 1, 1, 0], [1, 2, 1, 0]], 20)
        # An update of a row that matches.
        cql.execute(update, (2, 1, 1, 1))
        assert_all(cql, select, [[1, 0, 1, 0], [1, 1, 1, 2], [1, 2, 1, 0]], 20)
        # Deletions of rows that do not match.
        cql.execute(delete1, (1, 1, -1))
        cql.execute(delete1, (2, 0, 1))
        cql.execute(delete2, (0,))
        assert_all(cql, select, [[1, 0, 1, 0], [1, 1, 1, 2], [1, 2, 1, 0]], 20)
        # Deletion of a row that matches.
        cql.execute(delete1, (1, 1, 1))
        assert_all(cql, select, [[1, 0, 1, 0], [1, 2, 1, 0]], 20)
        # Deletion of a partition that matches.
        cql.execute(delete2, (1,))
        assert_all(cql, select, [], 20)

        cql.execute("DROP MATERIALIZED VIEW mv")
        cql.execute("DROP TABLE test")


def create_few_mv(cql, nodes, mvs_count, table, synchronous_updates, rows_expected, prefix="mv_cf_view"):
    """The dtests' create_few_mv(): views keyed by (c1, key), waited for and
    counted."""
    for i in range(mvs_count):
        cql.execute(f"CREATE MATERIALIZED VIEW ks.{prefix}_{i} AS SELECT * FROM ks.{table} WHERE c1 IS NOT NULL and key IS NOT NULL PRIMARY KEY (c1, key) "
                    f"{' WITH synchronous_updates = true' if synchronous_updates else ''}")
    for i in range(mvs_count):
        wait_for_view(cql, nodes, "ks", f"{prefix}_{i}")
        assert_row_count(cql, f"ks.{prefix}_{i}", rows_expected, QUORUM)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_mv_consistency_after_truncate_and_insert(manager: ScyllaClusterManager):
    """Synchronous views of a base table are emptied by its truncation and
    filled again by new inserts. auto_snapshot is off, like in the dtest."""
    servers, cql = await prepare(manager, nodes=3, rf=3, config={"auto_snapshot": False}, consistency_level=QUORUM)
    create_cf(cql, "ks.truncate_test", columns={"c1": "text", "c2": "text"})
    n = 100
    values = ([f"c1 value {i}" for i in range(n)], [f"c2 value {i}" for i in range(n)])
    insert_c1c2(cql, n, cf="truncate_test", c1_values=values[0], c2_values=values[1])
    assert_row_count(cql, "ks.truncate_test", n, QUORUM)
    create_few_mv(cql, 3, 5, "truncate_test", True, n)
    cql.execute("TRUNCATE ks.truncate_test")
    for i in range(5):
        assert_row_count(cql, f"ks.mv_cf_view_{i}", 0, QUORUM)
    insert_c1c2(cql, n, cf="truncate_test", c1_values=values[0], c2_values=values[1])
    assert_row_count(cql, "ks.truncate_test", n, QUORUM)
    for i in range(5):
        assert_row_count(cql, f"ks.mv_cf_view_{i}", n, QUORUM)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_viewbuildstatus_progress_success_flow(manager: ScyllaClusterManager):
    """
    "nodetool viewbuildstatus" from random nodes while a view builds: until
    it is built it exits with an error and lists every node as STARTED or
    SUCCESS, then it reports the view built.
    """
    servers, cql = await prepare(manager, nodes=3, rf=3)
    tm = TableManager(cql, 3, columns={"int": (20, 1, 100)})
    tm.create_table()
    tm.prefill_table(10000)
    await flush_all_nodes(manager, servers)
    mv = tm.create_mv(tm.column_names_list[1], wait=False, select="*")

    in_progress, success = "has not finished building; node status is below.", "has finished building"
    output = in_progress
    retry = 0
    while retry < 60 and in_progress in output:
        retry += 1
        proc = await nodetool(manager, random.choice(servers), "viewbuildstatus", "ks", mv, check=False)
        output = proc.stdout
        if proc.returncode == 0:
            assert success in output, f"unexpected viewbuildstatus output: {output}"
        else:
            # Exit status 1 while the view builds.
            hosts = [line for line in output.splitlines()[3:] if line.strip()]
            assert len(hosts) == len(servers), f"wrong viewbuildstatus output: {output}"
            for line in hosts:
                host_ip, host_status = line.split()
                assert host_status in ("SUCCESS", "STARTED"), f"host {host_ip} state is {host_status}"
        time.sleep(1)
    assert success in output, f"viewbuildstatus did not report {success!r} in {retry} retries"


def update_one_row_and_assert_view(cql, row_index, mvs_count):
    """Updates a row's c2 and reads it from a random view at QUORUM; returns
    the failure, if the view has the old value."""
    new_value = f"c2 value {row_index * random.randint(10, 100000)}"
    cql.execute(f"INSERT INTO ks.cf (key, c1, c2) VALUES ('k{row_index}', 'c1 value {row_index}', '{new_value}')")
    mv = f"mv_cf_view_{random.randint(0, mvs_count - 1)}"
    res = rows(cql, f"select c2 from ks.{mv} where key='k{row_index}' and c1 = 'c1 value {row_index}'", QUORUM)
    return {} if res == [[new_value]] else {"mv name": mv, "result": res}


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_alter_with_synchronous_updates(manager: ScyllaClusterManager, build_mode):
    """
    Views updated asynchronously may show stale rows right after an update;
    once altered to synchronous_updates = true, they never do.
    """
    servers, cql = await prepare(manager, nodes=4, rf=3, consistency_level=QUORUM)
    create_cf(cql, "ks.cf", columns={"c1": "text", "c2": "text"})
    # The dtest's sizes, smaller in debug mode as there.
    n = 10000 if build_mode in DEBUG_MODES else 100000
    insert_c1c2(cql, n, c1_values=[f"c1 value {i}" for i in range(n)], c2_values=[f"c2 value {i}" for i in range(n)])
    assert_row_count(cql, "ks.cf", n, QUORUM)
    mvs_count = 10 if build_mode in DEBUG_MODES else 50
    create_few_mv(cql, 4, mvs_count, "cf", False, n)

    # Asynchronous: a stale row may be seen.
    for row_index in range(n):
        if update_one_row_and_assert_view(cql, row_index, mvs_count):
            break
    for i in range(mvs_count):
        cql.execute(f"ALTER MATERIALIZED VIEW ks.mv_cf_view_{i} WITH synchronous_updates = true")
    for row_index in range(n):
        failed = update_one_row_and_assert_view(cql, row_index, mvs_count)
        assert not failed, f"Unexpectedly found not updated rows in views: {failed}"


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_create_with_synchronous_updates.
# 50 views created with synchronous_updates = true on a populated table:
# a view read right after a base update never returns the old value.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_create_with_synchronous_updates(manager: ScyllaClusterManager):
    servers, cql = await prepare(manager, nodes=4, rf=3, consistency_level=QUORUM)
    create_cf(cql, "ks.cf", columns={"c1": "text", "c2": "text"})
    n = 1000
    insert_c1c2(cql, n, c1_values=[f"c1 value {i}" for i in range(n)], c2_values=[f"c2 value {i}" for i in range(n)])
    assert_row_count(cql, "ks.cf", n, QUORUM)
    mvs_count = 50
    create_few_mv(cql, 4, mvs_count, "cf", True, n)

    for _ in range(20000):
        row_index = random.randint(0, n)
        failed = update_one_row_and_assert_view(cql, row_index, mvs_count)
        assert not failed, f"Unexpectedly found not updated rows in views: {failed}"


async def write_to_hinted_handoff_for_views(manager: ScyllaClusterManager, double_failure):
    """The dtests' _write_to_hinted_handoff_for_views(): base updates
    written at CL=ANY while one or two of three replicas are down reach
    the view through hinted handoff: the view ends with all the updated
    rows, read at CL=ALL."""
    servers, cql = await prepare(manager, nodes=3, rf=3, user_table=True, config={"hinted_handoff_enabled": True})
    _, node2, node3 = servers
    for i in range(500):
        cql.execute(SimpleStatement(f"INSERT INTO users (username, password, gender, birth_year) VALUES('Jane{i}', 'Doe', 'F', 1980)",
                                    consistency_level=ConsistencyLevel.ALL))
    await flush_all_nodes(manager, servers)

    stopped = [node2] + ([node3] if double_failure else [])
    await stop_nodes(manager, stopped)
    num_updates = 500
    for i in range(num_updates):
        cql.execute(SimpleStatement(f"UPDATE users SET state = 'CA{i}' WHERE username = 'Jane{1500 - 2 * i}'",
                                    consistency_level=ConsistencyLevel.ANY))
    await start_nodes(manager, stopped)
    # Waits until the view is built, not until the hints are delivered.
    wait_for_view(cql, 3, "ks", "users_by_state")
    returned_rows = []
    for _ in range(60 // 5):
        returned_rows = list(cql.execute(SimpleStatement("SELECT * FROM users_by_state", consistency_level=ConsistencyLevel.ALL)))
        if len(returned_rows) == num_updates:
            break
        await asyncio.sleep(5)
    assert len(returned_rows) == num_updates
    for row in returned_rows:
        assert int(row.username[4:]) == 1500 - 2 * int(row.state[2:])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_write_to_hinted_handoff_for_views.
# One of three replicas is down while the base is updated.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_write_to_hinted_handoff_for_views(manager: ScyllaClusterManager):
    await write_to_hinted_handoff_for_views(manager, double_failure=False)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_write_to_hinted_handoff_for_views_double_failure.
# Two of three replicas are down while the base is updated.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_write_to_hinted_handoff_for_views_double_failure(manager: ScyllaClusterManager):
    await write_to_hinted_handoff_for_views(manager, double_failure=True)


async def update_view(manager: ScyllaClusterManager, cql, query, flush, compact=False):
    """The dtests' update_view(): run the statement, then flush (and
    compact) every running node."""
    cql.execute(query)
    if flush:
        await flush_all_nodes(manager, await manager.running_servers())
    if compact:
        await compact_all_nodes(manager)


def assert_one(cql, query, expected, cl=None):
    stmt = SimpleStatement(query, consistency_level=cl) if cl is not None else query
    res = [list(r) for r in cql.execute(stmt)]
    assert res == [expected], f"Expected {[expected]} from {query}, but got {res}"


def assert_none(cql, query, cl=None):
    stmt = SimpleStatement(query, consistency_level=cl) if cl is not None else query
    res = [list(r) for r in cql.execute(stmt)]
    assert res == [], f"Expected nothing from {query}, but got {res}"


def eventually_assert_one(cql, query, expected, cl=None):
    eventually(lambda: assert_one(cql, query, expected, cl))


def eventually_assert_none(cql, query, cl=None):
    eventually(lambda: assert_none(cql, query, cl))


async def mv_with_default_ttl(manager: ScyllaClusterManager, flush):
    """
    The dtests' _test_mv_with_default_ttl(): views of tables with
    default_time_to_live, with unselected columns or an extra key column,
    and user-provided TTLs: view rows are deleted by expired liveness
    info. CASSANDRA-14071.
    """
    _, cql = await prepare(manager, nodes=3, rf=3, config={"hinted_handoff_enabled": False}, consistency_level=QUORUM)
    cql.execute("USE ks")

    # View with the same key and unselected columns.
    cql.execute("CREATE TABLE t2 (k int, a int, b int, c int, primary key(k, a)) with default_time_to_live=600")
    cql.execute("CREATE MATERIALIZED VIEW mv2 AS SELECT k,a,b FROM t2 WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)")
    cql.cluster.control_connection.wait_for_schema_agreement()

    await update_view(manager, cql, "UPDATE t2 SET c=1 WHERE k=1 AND a=1;", flush)
    eventually_assert_one(cql, "SELECT k,a,b,c FROM t2", [1, 1, None, 1])
    eventually_assert_one(cql, "SELECT k,a,b FROM mv2", [1, 1, None])

    await update_view(manager, cql, "UPDATE t2 SET c=null WHERE k=1 AND a=1;", flush)
    eventually_assert_none(cql, "SELECT k,a,b,c FROM t2")
    eventually_assert_none(cql, "SELECT k,a,b FROM mv2")

    await update_view(manager, cql, "UPDATE t2 SET c=2 WHERE k=1 AND a=1;", flush)
    eventually_assert_one(cql, "SELECT k,a,b,c FROM t2", [1, 1, None, 2])
    eventually_assert_one(cql, "SELECT k,a,b FROM mv2", [1, 1, None])

    await update_view(manager, cql, "DELETE c FROM t2 WHERE k=1 AND a=1;", flush)
    eventually_assert_none(cql, "SELECT k,a,b,c FROM t2")
    eventually_assert_none(cql, "SELECT k,a,b FROM mv2")

    if flush:
        await compact_all_nodes(manager)
        assert_none(cql, "SELECT * FROM t2")
        assert_none(cql, "SELECT * FROM mv2")

    # With user-provided TTLs.
    await update_view(manager, cql, "INSERT INTO t2(k,a,b,c) VALUES(2,2,2,2) USING TTL 5", flush)
    await update_view(manager, cql, "UPDATE t2 USING TTL 100 SET c=1 WHERE k=2 AND a=2;", flush)
    await update_view(manager, cql, "UPDATE t2 USING TTL 50 SET c=2 WHERE k=2 AND a=2;", flush)
    await update_view(manager, cql, "DELETE c FROM t2 WHERE k=2 AND a=2;", flush)

    await asyncio.sleep(6)

    eventually_assert_none(cql, "SELECT k,a,b,c FROM t2")
    eventually_assert_none(cql, "SELECT k,a,b FROM mv2")

    if flush:
        await compact_all_nodes(manager)
        assert_none(cql, "SELECT * FROM t2")
        assert_none(cql, "SELECT * FROM mv2")

    # View with an extra key column.
    cql.execute("CREATE TABLE t (k int PRIMARY KEY, a int, b int) with default_time_to_live=600")
    cql.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)")
    cql.cluster.control_connection.wait_for_schema_agreement()

    for a in [1, 2, 3]:
        await update_view(manager, cql, f"INSERT INTO t (k, a, b) VALUES (1, {a}, 1);", flush)
        eventually_assert_one(cql, "SELECT * FROM t", [1, a, 1])
        eventually_assert_one(cql, "SELECT * FROM mv", [1, a, 1])

    if flush:
        await compact_all_nodes(manager)
        assert_one(cql, "SELECT * FROM t", [1, 3, 1])
        assert_one(cql, "SELECT * FROM mv", [1, 3, 1])

    # User-provided TTLs.
    await update_view(manager, cql, "UPDATE t USING TTL 30 SET a = 4 WHERE k = 1", flush)
    eventually_assert_one(cql, "SELECT * FROM t", [1, 4, 1])
    eventually_assert_one(cql, "SELECT * FROM mv", [1, 4, 1])

    await update_view(manager, cql, "UPDATE t USING TTL 20 SET a = 5 WHERE k = 1", flush)
    eventually_assert_one(cql, "SELECT * FROM t", [1, 5, 1])
    eventually_assert_one(cql, "SELECT * FROM mv", [1, 5, 1])

    last_update = time.time()
    await update_view(manager, cql, "UPDATE t USING TTL 10 SET a = 6 WHERE k = 1", flush)
    eventually_assert_one(cql, "SELECT * FROM t", [1, 6, 1])
    eventually_assert_one(cql, "SELECT * FROM mv", [1, 6, 1])

    if flush:
        await compact_all_nodes(manager)
        now = time.time()
        if now - last_update < 10:
            await asyncio.sleep(10 - (now - last_update))
        eventually_assert_one(cql, "SELECT * FROM t", [1, None, 1])
        eventually_assert_none(cql, "SELECT * FROM mv")


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_with_default_ttl_with_flush.
# Every write is flushed on all nodes, and the nodes compact at checkpoints.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_with_default_ttl_with_flush(manager: ScyllaClusterManager):
    await mv_with_default_ttl(manager, flush=True)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_with_default_ttl_without_flush.
# The data stays in memtables.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_with_default_ttl_without_flush(manager: ScyllaClusterManager):
    await mv_with_default_ttl(manager, flush=False)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_viewbuildstatus_progress_unknown_flow.
# "nodetool viewbuildstatus" of the base table, which is not a view, lists
# every node as UNKNOWN.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_viewbuildstatus_progress_unknown_flow(manager: ScyllaClusterManager):
    num_of_rows = 10
    servers, cql = await prepare(manager, nodes=3, rf=3, fetch_size=num_of_rows * 2)
    tm = TableManager(cql, 3, columns={"int": (20, 1, 100)})
    tm.create_table()
    tm.prefill_table(num_of_rows)
    await flush_all_nodes(manager, servers)
    tm.create_mv(tm.column_names_list[1], wait=False, select="*")

    proc = await nodetool(manager, random.choice(servers), "viewbuildstatus", tm.keyspace, tm.table_name, check=False)
    if proc.returncode:
        assert proc.stdout.count("UNKNOWN") == len(servers), f"wrong number of UNKNOWN host states in viewbuildstatus output: {proc.stdout}"


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_base_column_in_view_pk_complex_timestamp.
# A view row is shadowed and re-inserted when the base column in the view
# key has a timestamp larger or smaller than the row's other columns; with
# two of three nodes down, writes at CL=ONE reach their view replicas by
# read repair at CL=ALL once they are back. CASSANDRA-11500.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("flush", [True, False], ids=["with_flush", "without_flush"])
async def test_base_column_in_view_pk_complex_timestamp(manager: ScyllaClusterManager, flush):
    servers, cql = await prepare(manager, nodes=3, rf=3, config={"hinted_handoff_enabled": False}, consistency_level=QUORUM)
    _, node2, node3 = servers

    cql.execute("USE ks")
    cql.execute("CREATE TABLE t (k int PRIMARY KEY, a int, b int)")
    cql.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)")
    cql.cluster.control_connection.wait_for_schema_agreement()

    async def update(query, compact=False):
        await update_view(manager, cql, query, flush, compact)

    # Initial values, TS=1.
    await update("INSERT INTO t (k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 1;")
    eventually_assert_one(cql, "SELECT * FROM t", [1, 1, 1])
    eventually_assert_one(cql, "SELECT * FROM mv", [1, 1, 1])

    # b's timestamp goes up to 10.
    await update("UPDATE t USING TIMESTAMP 10 SET b = 2 WHERE k = 1;")
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])

    # Switch entries: shadow a = 1, insert a = 2.
    await update("UPDATE t USING TIMESTAMP 2 SET a = 2 WHERE k = 1;")
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM t", [1, 2, 2, 10])
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 2, 10])

    # Switch entries: shadow a = 2, insert a = 1.
    await update("UPDATE t USING TIMESTAMP 3 SET a = 1 WHERE k = 1;")
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])

    # Switch entries: shadow a = 1, insert a = 2.
    await update("UPDATE t USING TIMESTAMP 4 SET a = 2 WHERE k = 1;", compact=True)
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM t", [1, 2, 2, 10])
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 2, 10])

    # The view row is shadowed even if the timestamp of the base column in
    # the view key is smaller than the row's: row TS = 20, a@6, b@20.
    await update("DELETE FROM t USING TIMESTAMP 5 where k = 1;")
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM t", [1, None, 2, 10])
    eventually_assert_none(cql, "SELECT k,a,b,writetime(b) FROM mv")
    await update("INSERT INTO t (k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 6;")
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])
    await update("INSERT INTO t (k, b) VALUES (1, 1) USING TIMESTAMP 20;")
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 1, 20])
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 1, 20])

    # Switch entries: shadow a = 1, insert a = 2.
    await update("UPDATE t USING TIMESTAMP 7 SET a = 2 WHERE k = 1;")
    eventually_assert_one(cql, "SELECT k,a,b,writetime(a),writetime(b) FROM t", [1, 2, 1, 7, 20])
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 1, 20])

    # Switch entries: shadow a = 2, insert a = 1.
    await update("UPDATE t USING TIMESTAMP 8 SET a = 1 WHERE k = 1;")
    eventually_assert_one(cql, "SELECT k,a,b,writetime(a),writetime(b) FROM t", [1, 1, 1, 8, 20])
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 1, 20])

    # Another view row.
    await update("INSERT INTO t (k, a, b) VALUES (2, 2, 2);")
    eventually_assert_one(cql, "SELECT k,a,b FROM t WHERE k = 2", [2, 2, 2])
    eventually_assert_one(cql, "SELECT k,a,b FROM mv WHERE k = 2", [2, 2, 2])

    await stop_nodes(manager, [node2, node3])
    # Shadow a = 1, create a = 2.
    await update(SimpleStatement("UPDATE t USING TIMESTAMP 9 SET a = 2 WHERE k = 1", consistency_level=ConsistencyLevel.ONE))
    # Shadow (a=2, k=2) after 3 seconds.
    await update(SimpleStatement("UPDATE t USING TTL 3 SET a = 2 WHERE k = 2", consistency_level=ConsistencyLevel.ONE))
    await start_nodes(manager, [node2, node3])

    def check_query_rows_size(query, expected_rows_size):
        assert len(cql.execute(query, trace=True).current_rows) == expected_rows_size

    # k = 1 & a = 1: a digest mismatch of tombstones, repaired.
    query = SimpleStatement("SELECT * FROM mv WHERE k = 1 AND a = 1", consistency_level=ConsistencyLevel.ALL)
    eventually(lambda: check_query_rows_size(query, 0))
    # k = 1 & a = 2: a digest mismatch of data, repaired.
    query2 = SimpleStatement("SELECT * FROM mv WHERE k = 1 AND a = 2", consistency_level=ConsistencyLevel.ALL)
    eventually(lambda: check_query_rows_size(query2, 1))
    eventually_assert_one(cql, "SELECT k,a,b,writetime(b) FROM mv WHERE k = 1", [1, 2, 1, 20])

    await asyncio.sleep(3)
    # k = 2 & a = 2: a digest mismatch of expired data, repaired.
    query3 = SimpleStatement("SELECT * FROM mv WHERE k = 2 AND a = 2", consistency_level=ConsistencyLevel.ALL)
    eventually(lambda: check_query_rows_size(query3, 0))


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_view_tombstone.
# A view key change written at CL=ONE while two of three nodes are down
# tombstones the old view row on every replica after read repair at
# CL=ALL. CASSANDRA-10261, CASSANDRA-10910.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_view_tombstone(manager: ScyllaClusterManager):
    servers, _ = await prepare(manager, nodes=3, rf=3, config={"hinted_handoff_enabled": False, "cache_hit_rate_read_balancing": False})
    node1, node2, node3 = servers

    cql = session_for(manager, node1, exclusive=True)
    cql.execute("USE ks")
    cql.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
    cql.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v,id) "
                "WITH read_repair_chance = 0.0 AND dclocal_read_repair_chance = 0.0 AND speculative_retry = 'none'")
    cql.cluster.control_connection.wait_for_schema_agreement()

    # Initial values TS=0.
    cql.execute(SimpleStatement("INSERT INTO t (id, v, v2, v3) VALUES (1, 2, 'a', 3.0) USING TIMESTAMP 0", consistency_level=ConsistencyLevel.ALL))
    assert_one(cql, "SELECT * FROM t_by_v WHERE v = 2", [2, 1, "a", 3.0])
    cql.execute(SimpleStatement("INSERT INTO t (id, v2) VALUES (1, 'b') USING TIMESTAMP 1", consistency_level=ConsistencyLevel.ALL))
    assert_one(cql, "SELECT * FROM t_by_v WHERE v = 2", [2, 1, "b", 3.0])

    # v changes at TS=3: the v=2 view row gets a tombstone and v=0 is added.
    cql.execute(SimpleStatement("UPDATE t USING TIMESTAMP 3 SET v = 0 WHERE id = 1", consistency_level=ConsistencyLevel.ALL))
    assert_none(cql, "SELECT * FROM t_by_v WHERE v = 2")

    await stop_nodes(manager, [node2, node3])
    cql.execute(SimpleStatement("UPDATE t USING TIMESTAMP 4 SET v = 2 WHERE id = 1", consistency_level=ConsistencyLevel.ONE))
    await start_nodes(manager, [node2, node3])

    cql2 = session_for(manager, node2, exclusive=True)
    cql2.execute("USE ks")

    await sync_hinted_handoff(manager, node1)

    # A digest mismatch, and the data is repaired.
    assert_one(cql, "SELECT * FROM t_by_v WHERE v = 2", [2, 1, "b", 3.0], cl=ConsistencyLevel.ALL)
    assert_none(cql, "SELECT * FROM t_by_v WHERE v = 0", cl=ConsistencyLevel.ALL)
    assert_none(cql, "SELECT * FROM t_by_v WHERE v = 1", cl=ConsistencyLevel.ALL)
    assert_one(cql2, "SELECT * FROM t_by_v WHERE v = 2", [2, 1, "b", 3.0], cl=ConsistencyLevel.ONE)


INJECTION_POINTS = [
    "table_push_view_replica_updates_stale_time_point",
    "table_push_view_replica_updates_timeout",
    "view_builder_load_views",
    "view_builder_check_for_built_views",
    "view_builder_consume_new_partition",
    "view_builder_consume_tombstone",
    "view_builder_consume_static_row",
    "view_builder_consume_clustering_row",
    "view_builder_consume_range_tombstone",
    "view_builder_flush_fragments",
    "view_builder_consume_end_of_partition",
    "view_builder_consume_end_of_stream",
    "view_builder_mark_view_as_built",
    "view_update_generator_consume_staging_sstable",
    "view_update_generator_collect_consumed_sstables",
    "view_update_generator_move_staging_sstable",
    "view_update_generator_registering_staging_sstable",
]


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_injected_noncritical_errors.
# Errors injected once into the view update and view building paths, each
# on one of two nodes, are not critical: the view built from rows written
# before it was created, and updated by rows written after, gets all 20 rows.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_injected_noncritical_errors(manager: ScyllaClusterManager):
    # The dtest's populate({"dc1": generate_rack_topology_based_rf(nodes=2, rf=2), "dc2": 0}).
    servers = await manager.servers_add(2, config=VNODES, property_file=[{"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack2"}])
    node1, node2 = servers
    cql = session_for(manager, node1)
    cql2 = session_for(manager, node2)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':2}")
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute("USE ks")
    cql.execute("CREATE TABLE tab (a INT, b INT, c INT,PRIMARY KEY (a));")
    cql.cluster.control_connection.wait_for_schema_agreement()
    # The dtests' disable_errors(node) on each node.
    for s in servers:
        await manager.api.client.delete("/v2/error_injection/injection", host=s.ip_addr)
    # The dtests' enable_error(name, i % 2, one_shot=True).
    for i, injection_point in enumerate(INJECTION_POINTS):
        await manager.api.client.post(f"/v2/error_injection/injection/{injection_point}", host=servers[i % 2].ip_addr, params={"one_shot": "True"})

    for i in range(10):
        cql.execute(SimpleStatement(f"INSERT INTO tab (a, b, c) VALUES({i}, {2 * i}, {-i})", consistency_level=ConsistencyLevel.ALL))
    cql.execute("CREATE MATERIALIZED VIEW mv AS SELECT a,b FROM tab WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)")
    for i in range(5, 20):
        cql2.execute(SimpleStatement(f"INSERT INTO ks.tab (a, b, c) VALUES({i}, {2 * i}, {-i})", consistency_level=ConsistencyLevel.ALL))

    def assert_row_count_in_select(session, expected):
        res = list(session.execute(SimpleStatement("SELECT * FROM ks.mv", consistency_level=ConsistencyLevel.ALL)))
        assert len(res) == expected, f"Expected {expected} rows, got {len(res)}"

    eventually(lambda: assert_row_count_in_select(cql, 20))
    eventually(lambda: assert_row_count_in_select(cql2, 20))
