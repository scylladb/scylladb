#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Views built from existing base data, ported from scylla-dtest
# materialized_views_test.py, one test per dtest: views created on a populated
# table, created while the table is written to, updated or deleted from, or
# while a node joins, leaves, stops or restarts, views restricted to one value
# of a key column, views on several tables of several keyspaces at once,
# views updated concurrently by roles with service levels, concurrent updates
# and deletions of a row, a view dropped while its table is written to, a view
# dropped and created again, and view key updates while a node joins. The
# clusters have three or four nodes with vnodes keyspaces, and the base
# tables are those of the dtests' TableManager (which flushes every node
# after a prefill, unless told not to), with the dtests' data sizes, scaled
# down in debug mode as the dtests did.

import asyncio
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.query import SimpleStatement

from test.cluster.mv.dtest_mv_util import (TableManager, add_node, assert_row_count, assert_two_queries_equal, check_errors, eventually,
                                           flush_all_nodes, grouped, prepare, query, row_count, session_for, start_nodes, stop_nodes,
                                           validate_data_in_mvs, wait_for_view)
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


QUORUM, ALL = ConsistencyLevel.QUORUM, ConsistencyLevel.ALL


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mvs_populating_from_existing_data.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mvs_populating_from_existing_data(manager: ScyllaClusterManager):
    """Ten views created on a populated table get all of its rows."""
    servers, cql = await prepare(manager, nodes=4, rf=3)
    tm = TableManager(cql, 4, columns={"int": (10, 1, 100)})
    tm.create_table()
    tm.prefill_table(1000)
    await flush_all_nodes(manager, servers)
    tm.create_mvs_by_one_column(10, wait=True)
    validate_data_in_mvs(cql, tm, 1000, 1000, ALL)


def set_consistency_level(node_action, total_rf, nodes_in_cluster):
    """The dtests' set_consistency_level(): QUORUM after a node stop,
    restart or decommission, or when the RF exceeds the nodes, else ALL."""
    if node_action in ["stop", "restart", "decommission"] or total_rf > nodes_in_cluster:
        return QUORUM
    return ALL


async def mv_populating_from_existing_data_during_changes(manager: ScyllaClusterManager, change_type, build_mode, nodes=4, rf=3):
    """
    The dtests' _mv_populating_from_existing_data_during_changes_test():
    views, one per int column, created on a prefilled (and flushed) table
    in parallel with a change - inserts, updates, deletes, a node joining,
    being removed, stopped, decommissioned (and a new node added) or
    restarted - get the table's rows.
    """
    servers, cql = await prepare(manager, nodes=nodes, rf=rf)
    node_action = change_type.split(" ")[0]
    if node_action in ["decommission", "restart", "remove", "stop"]:
        cql.shutdown()
        cql = session_for(manager, servers[0], exclusive=True)
        cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = QUORUM
        cql.execute("USE ks")

    prefill, max_delete, mvs = (10000, 2000, 2) if build_mode == "debug" else (40000, 6000, 10)
    tm = TableManager(cql, nodes, columns={"int": (mvs, 1, 100)})
    rows_after_test = prefill
    nodes_in_cluster = nodes
    node2 = servers[1]

    async def change_func():
        nonlocal nodes_in_cluster
        if change_type == "insert":
            await asyncio.to_thread(tm.prefill_table, prefill // 2, start_id_from=prefill + 1, delay=1)
            await flush_all_nodes(manager, servers)
        elif change_type == "update":
            await asyncio.to_thread(tm.multiple_int_updates_by_id, [-100, -1], same_id=False, delay=1)
        elif change_type == "delete":
            await asyncio.to_thread(tm.multiple_deletes, range(1000, max_delete), delay=1)
        elif change_type == "add node":
            await asyncio.sleep(1)
            await add_node(manager)
            nodes_in_cluster += 1
        elif change_type == "decommission":
            await asyncio.sleep(2)
            await manager.decommission_node(node2.server_id)
            await add_node(manager)
        elif change_type == "restart node":
            await asyncio.sleep(1)
            await stop_nodes(manager, [node2])
            await asyncio.sleep(1)
            await start_nodes(manager, [node2])
        elif change_type == "remove node":
            # tools/misc remove_node(): the node is killed, then removed.
            await asyncio.sleep(1)
            await manager.server_stop(node2.server_id, convict=False)
            await manager.others_not_see_server(node2.ip_addr)
            await manager.remove_node(servers[0].server_id, node2.server_id)
            nodes_in_cluster -= 1
        elif change_type == "stop node":
            await asyncio.sleep(1)
            await stop_nodes(manager, [node2])

    if change_type == "insert":
        rows_after_test = int(prefill * 1.5)
    elif change_type == "delete":
        rows_after_test = max(0, prefill - (max_delete - 1000))

    tm.create_table()
    tm.prefill_table(prefill)
    await flush_all_nodes(manager, servers)
    cql.cluster.max_schema_agreement_wait = 0

    await asyncio.gather(change_func(), asyncio.to_thread(tm.create_mvs_by_one_column, mvs))
    if node_action == "stop":
        await start_nodes(manager, [node2])

    alive = len(await manager.running_servers())
    for mv in tm.materialized_views:
        wait_for_view(cql, alive, tm.keyspace, mv)
    validate_data_in_mvs(cql, tm, rows_after_test, rows_after_test, set_consistency_level(node_action, rf, nodes_in_cluster))


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_populating_from_existing_data_during_inserts.
# Views created while more rows are inserted into the table get all rows.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_populating_from_existing_data_during_inserts(manager: ScyllaClusterManager, build_mode):
    await mv_populating_from_existing_data_during_changes(manager, "insert", build_mode)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_populating_from_existing_data_during_updates.
# Views created while rows of the table are updated match the table.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_populating_from_existing_data_during_updates(manager: ScyllaClusterManager, build_mode):
    await mv_populating_from_existing_data_during_changes(manager, "update", build_mode)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_populating_from_existing_data_during_deletes.
# Views created while rows of the table are deleted do not keep the deleted rows.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_populating_from_existing_data_during_deletes(manager: ScyllaClusterManager, build_mode):
    await mv_populating_from_existing_data_during_changes(manager, "delete", build_mode)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_populating_from_existing_data_during_extend.
# Views created while a node joins get all rows.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_populating_from_existing_data_during_extend(manager: ScyllaClusterManager, build_mode):
    await mv_populating_from_existing_data_during_changes(manager, "add node", build_mode)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_populating_from_existing_data_during_node_remove.
# Views created while a node is killed and removed get all rows.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_populating_from_existing_data_during_node_remove(manager: ScyllaClusterManager, build_mode):
    await mv_populating_from_existing_data_during_changes(manager, "remove node", build_mode)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_populating_from_existing_data_during_node_stop.
# Views created while a node is stopped get all rows once it is back.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest runs it nightly in release mode only")
async def test_mv_populating_from_existing_data_during_node_stop(manager: ScyllaClusterManager, build_mode):
    await mv_populating_from_existing_data_during_changes(manager, "stop node", build_mode)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_populating_from_existing_data_during_node_decommission.
# Views created while a node is decommissioned, and a new one added, get all rows.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_populating_from_existing_data_during_node_decommission(manager: ScyllaClusterManager, build_mode):
    await mv_populating_from_existing_data_during_changes(manager, "decommission", build_mode)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_populating_from_existing_data_during_node_restart.
# Views created while a node restarts get all rows.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest runs it nightly in release mode only")
async def test_mv_populating_from_existing_data_during_node_restart(manager: ScyllaClusterManager, build_mode):
    await mv_populating_from_existing_data_during_changes(manager, "restart node", build_mode)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_mv_populating_from_existing_data_with_restriction.
# Views created on a populated table, each restricted to one value of the
# table's second partition key column, hold exactly the table's rows with
# that value.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_mv_populating_from_existing_data_with_restriction(manager: ScyllaClusterManager):
    servers, cql = await prepare(manager, nodes=4, rf=3)
    mvs = 10
    # TableManager's default partition key: (id, clmn_int0).
    tm = TableManager(cql, 4, columns={"int": (mvs + 1, 1, 100)}, int_pk_columns=1)
    tm.create_table()
    data = [2, 5, 12, 45, 53, 78, 36, 85, 98, 100]
    tm.prefill_table(10000, data={"int": data})
    await flush_all_nodes(manager, servers)

    restricted = tm.pk_list[1]
    views = {}
    for i in range(2, mvs + 1):
        # MaterializedViewManager.create_materialized_view(mv_columns={"int": {"names": [column]}},
        # mv_pk_column={"names": [column]}, mv_where_restriction={"names": {clmn_int0: {"operator": "=", ...}}}).
        column = tm.column_names_list[i]
        mv_name = f"{tm.table_name}_mv_{len(views)}"
        cql.execute(f"CREATE MATERIALIZED VIEW ks.{mv_name} AS SELECT {column} FROM ks.{tm.table_name} "
                    f"WHERE id IS NOT NULL and {column} IS NOT NULL and {restricted} = {data[i - 1]} PRIMARY KEY (id, {restricted}, {column})")
        wait_for_view(cql, 4, "ks", mv_name)
        views[mv_name] = (column, data[i - 1])

    for mv_name, (column, value) in views.items():
        q = "select id, clmn_int0, {clmn} from {tbl}"
        eventually(lambda: assert_two_queries_equal(cql, q.format(clmn=column, tbl=tm.table_name), cql, q.format(clmn=column, tbl=mv_name), QUORUM,
                                                    column, column, restrict_column1=restricted, restrict_values1=[value]))


def multi_mvs_on_different_base_tables(cql, keyspace, tables, mvs, prefill_start, increase_rows, populated_table):
    """The dtests' _multi_mvs_on_different_base_tables(): the tables are
    prefilled without a flush."""
    base_tables = []
    for i in range(tables):
        tm = TableManager(cql, 4, columns={"int": (mvs // 2, 1, 100), "text": (mvs // 2, 1, 10)}, keyspace=keyspace, table_name=f"tm_table{i}")
        tm.create_table()
        base_tables.append(tm)

    def prefill_base_tables():
        for n, tm in enumerate(base_tables):
            tm.prefill_table(prefill_start + n * increase_rows)

    def create_mvs():
        for tm in base_tables:
            tm.create_mvs_by_one_column(mvs, wait=True)

    for func in [prefill_base_tables, create_mvs] if populated_table else [create_mvs, prefill_base_tables]:
        func()
    for n, tm in enumerate(base_tables):
        validate_data_in_mvs(cql, tm, prefill_start + n * increase_rows, prefill_start + n * increase_rows, ALL)


async def multi_mvs_on_different_base_tables_multi_ks(manager: ScyllaClusterManager, populated_table, rf=3, tables=3, mvs=4, prefill_start=10000, increase_rows=10):
    """The dtests' _multi_mvs_on_different_base_tables_multi_ks(): three
    keyspaces, written through three nodes at once."""
    servers, cql = await prepare(manager, nodes=4, rf=rf, config={"hinted_handoff_enabled": False, "read_repair_chance": 0.0})
    cql2, cql3 = session_for(manager, servers[1]), session_for(manager, servers[2])
    for s, ks in [(cql2, "multi1"), (cql3, "multi2")]:
        s.execute(f"CREATE KEYSPACE {ks} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
        s.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
        s.execute(f"USE {ks}")
    await asyncio.gather(*[asyncio.to_thread(multi_mvs_on_different_base_tables, s, ks, tables, mvs, prefill_start, increase_rows, populated_table)
                           for s, ks in [(cql, "ks"), (cql2, "multi1"), (cql3, "multi2")]])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_multi_mvs_on_different_base_tables.
# Three keyspaces, each with three tables of four views, the views created
# on the empty tables. The dtest did not run in debug mode, where it timed out.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.skip_mode(mode="debug", reason="too slow in debug mode, the dtest did not run in it either")
async def test_multi_mvs_on_different_base_tables(manager: ScyllaClusterManager):
    await multi_mvs_on_different_base_tables_multi_ks(manager, populated_table=False)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_multi_mvs_on_different_populated_base_tables.
# Three keyspaces, each with three tables of four views, the views created
# on the populated tables.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_multi_mvs_on_different_populated_base_tables(manager: ScyllaClusterManager):
    await multi_mvs_on_different_base_tables_multi_ks(manager, populated_table=True)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_concurrent_updates_deletes.
# Updates of the view key column of a row, racing with deletions of the
# same row, leave the view matching the table.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_concurrent_updates_deletes(manager: ScyllaClusterManager):
    prefill = 2
    servers, cql = await prepare(manager, nodes=4, rf=3, fetch_size=prefill * 2)
    mvs = 1
    tm = TableManager(cql, 4, columns={"int": (mvs, 1, 1000)})
    tm.create_table()
    tm.create_mvs_by_one_column(mvs, wait=True)

    start_data = [2, 5, 12, 45, 63, 78, 36, 85, 98, 100]
    tm.prefill_table(prefill, data={"int": start_data})
    await flush_all_nodes(manager, servers)

    await asyncio.gather(
        asyncio.to_thread(tm.multiple_int_updates_by_id, [-100, -1], same_id=False, ids=[0 for _ in range(1001)], updates=1000),
        asyncio.to_thread(tm.multiple_deletes, [0 for _ in range(1001)]))

    mv, column = next(iter(tm.materialized_views.items()))
    eventually(lambda: assert_two_queries_equal(cql, f"select * from {tm.table_name}", cql, f"select * from {mv}", QUORUM, column, column))


def cql_timeout(seconds, build_mode):
    """The dtests' cql_timeout(): longer in debug and dev modes."""
    return seconds * (3 if build_mode == "debug" else 1 if build_mode == "release" else 2)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_drop_mv_during_base_table_writes.
# A view created on an empty table is dropped 40 seconds into a prefill of
# 1000000 rows: it is gone from the schema, and the table has every row.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest runs it nightly in release mode only")
async def test_drop_mv_during_base_table_writes(manager: ScyllaClusterManager, build_mode):
    prefill = 1000000
    if build_mode == "debug":
        prefill //= 10
    timeout = cql_timeout(300, build_mode)
    # The dtest does not check the pre-drop snapshot, and flushing the view
    # for it under the prefill load makes the drop outlast the client timeout.
    servers, cql = await prepare(manager, nodes=4, rf=3, config={"range_request_timeout_in_ms": timeout * 1000, "auto_snapshot": False})
    tm = TableManager(cql, 4, columns={"int": (1, 1, 100)})
    tm.create_table()
    mv = tm.create_mv(tm.column_names_list[1])

    def add_mv_records():
        # The dtests' add_mv_records(tm, inserts=prefill): ids from 0 on,
        # without a flush.
        _id = tm.get_max_id()
        tm.prefill_table(prefill, start_id_from=_id + 1 if _id else _id)

    def drop_mv(delay):
        time.sleep(delay)
        cql.execute(f"drop materialized view {mv}", timeout=timeout)
        del tm.materialized_views[mv]

    await asyncio.gather(asyncio.to_thread(add_mv_records), asyncio.to_thread(drop_mv, 40))

    assert query(cql, "select * from system_schema.views", ALL) == []
    count = row_count(cql, tm.table_name, QUORUM, timeout=timeout)
    assert count == prefill, f"Expected a row count of {prefill} in table '{tm.table_name}', but got {count}"
    await check_errors(manager, servers[0], ["mutation_write_timeout_exception", "no_such_column_family"])


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_fetch_mv_after_recreate.
# A view dropped and created again with the same name can be read, and has
# the table's rows.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_fetch_mv_after_recreate(manager: ScyllaClusterManager):
    # Scylla's default, as on the dtest nodes; test.py turns it off.
    servers, cql = await prepare(manager, nodes=3, rf=3, config={"auto_snapshot": True})
    # TableManager's default partition key: (id, clmn_int0).
    tm = TableManager(cql, 3, columns={"int": (2, 1, 100)}, int_pk_columns=1)
    tm.create_table()

    def create_mv():
        # MaterializedViewManager.create_materialized_view(mv_pk_column={"type": "int"}):
        # all columns, keyed by the table's key and its first other int column.
        cql.execute(f"CREATE MATERIALIZED VIEW ks.{tm.table_name}_mv_0 AS SELECT * FROM ks.{tm.table_name} "
                    f"WHERE id IS NOT NULL and clmn_int0 IS NOT NULL and clmn_int1 IS NOT NULL PRIMARY KEY (id, clmn_int0, clmn_int1)")
        wait_for_view(cql, 3, "ks", f"{tm.table_name}_mv_0")
        return f"{tm.table_name}_mv_0"

    def assert_equal_ignore_order(mv):
        expected = [list(r) for r in cql.execute(f"select * from {tm.table_name}")]
        assert sorted(expected) == sorted(list(r) for r in query(cql, f"select * from {mv}", ALL))

    mv = create_mv()
    tm.prefill_table(100)
    await flush_all_nodes(manager, servers)
    assert_equal_ignore_order(mv)
    cql.execute(f"drop materialized view {mv}")
    mv = create_mv()
    assert_equal_ignore_order(mv)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_add_node_during_base_table_update.
# Updates of the view key column of existing rows go on while a node joins,
# and the view, read through the new node, ends up matching the table.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("rf", [
    pytest.param(1, marks=pytest.mark.skip_bug(link="https://github.com/scylladb/scylladb/issues/26981",
                                               reason="view updates are lost during topology changes (also scylladb/scylladb#26984)")),
    pytest.param(3),
])
async def test_add_node_during_base_table_update(manager: ScyllaClusterManager, rf):
    servers, cql = await prepare(manager, nodes=3, rf=rf)
    tm = TableManager(cql, 3, columns={"int": (1, 1, 10000), "text": (1, 1, 100)})
    tm.create_table()
    # MaterializedViewManager.create_materialized_view(mv_columns=None, mv_pk_column={"type": "int"}).
    mv = f"{tm.table_name}_mv_0"
    cql.execute(f"CREATE MATERIALIZED VIEW ks.{mv} AS SELECT * FROM ks.{tm.table_name} "
                "WHERE id IS NOT NULL and clmn_int0 IS NOT NULL PRIMARY KEY (id, clmn_int0)")
    wait_for_view(cql, 3, "ks", mv)

    prefill = 5000
    tm.prefill_table(prefill)
    await flush_all_nodes(manager, servers)

    def assert_count_table_mv():
        assert_row_count(cql, tm.table_name, prefill, QUORUM)
        assert_row_count(cql, mv, prefill, QUORUM)

    eventually(assert_count_table_mv)

    def update_table(start):
        # TableManager.update_table({"by type": {"int": 5000}}, {"by name": {"id": {"operator": "in", ...}}}, delay=5).
        time.sleep(5)
        cql.execute(f"UPDATE ks.{tm.table_name} SET clmn_int0 = 5000 WHERE id in ({', '.join(str(i) for i in range(start, start + 100))})")

    results = await asyncio.gather(add_node(manager), *[asyncio.to_thread(update_table, start) for start in range(100, 4900, 100)])
    _, new_node_cql = results[0]
    new_node_cql.execute("USE ks")

    def base_and_view_match():
        exp = grouped(cql, f"select clmn_int0 from ks.{tm.table_name}", QUORUM, "clmn_int0")
        act = grouped(new_node_cql, f"select clmn_int0 from ks.{mv}", QUORUM, "clmn_int0")
        assert exp == act, f"Expected {exp}, but got {act}"

    eventually(base_and_view_match)


# Ported from scylla-dtest materialized_views_test.py::TestMaterializedViews::test_small_concurrent.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_small_concurrent(manager: ScyllaClusterManager):
    """
    Ten views of one table, prefilled with 2000 rows; then roles with their
    own service levels insert 2000 more rows, update rows and read the views
    concurrently. The base and the views end with the same rows.
    """
    auth = ("cassandra", "cassandra")
    servers, cql = await prepare(manager, nodes=3, rf=3, config={"authenticator": "PasswordAuthenticator", "authorizer": "CassandraAuthorizer"}, auth=auth)
    records, mvs_amount = 2000, 10
    tm = TableManager(cql, 3, columns={"int": (mvs_amount, 1, 100)})
    tm.create_table()
    for column in tm.column_names_list[1:]:
        tm.create_mv(column, wait=True)

    start_data = [2, 5, 12, 45, 63, 78, 36, 85, 98, 100]
    tm.prefill_table(records, data={"int": start_data})
    await flush_all_nodes(manager, servers)

    def assert_rows_count(expected):
        for name in [tm.table_name, *tm.materialized_views]:
            assert len(list(cql.execute(SimpleStatement(f"SELECT * FROM ks.{name}", consistency_level=QUORUM)))) == expected, name

    assert_rows_count(records)

    def role_tm(name):
        cql.execute(f'CREATE SERVICE_LEVEL IF NOT EXISTS "{name}_sl" WITH shares = 100')
        cql.execute(f"CREATE ROLE {name}_role WITH password = 'password' AND login = True AND superuser = True")
        cql.execute(f'ATTACH SERVICE_LEVEL "{name}_sl" TO {name}_role')
        role_cql = session_for(manager, servers[0], auth_provider=PlainTextAuthProvider(username=f"{name}_role", password="password"))
        role_cql.set_keyspace("ks")
        rtm = TableManager(role_cql, 3, columns={"int": (mvs_amount, 1, 100)})
        rtm.create_table()
        rtm.materialized_views = tm.materialized_views
        return rtm

    prefill_tm, update_tm, select_tm = role_tm("prefill"), role_tm("update"), role_tm("select")
    new_data = [-2, -5, -12, -45, -63, -78, -36, -85, -98, -100]
    num_updates = records // 10
    await asyncio.gather(
        asyncio.to_thread(prefill_tm.prefill_table, records, data={"int": new_data}, start_id_from=records + 1),
        asyncio.to_thread(update_tm.multiple_int_updates_by_id, [200, 300], filter_values=start_data + new_data, updates=num_updates // 2, same_id=False),
        asyncio.to_thread(update_tm.multiple_int_updates_by_id, [300, 400], filter_values=start_data, updates=num_updates // 2),
        asyncio.to_thread(select_tm.select_all_mvs, 2000, True),
    )
    await flush_all_nodes(manager, servers)

    eventually(lambda: assert_rows_count(records * 2))
    for mv, column in tm.materialized_views.items():
        eventually(lambda: assert_two_queries_equal(cql, f"select {column} from {tm.name}", cql, f"select {column} from ks.{mv}", QUORUM, column, column))
