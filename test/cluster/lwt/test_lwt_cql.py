#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
CQL semantics of LWT statements on a three-node cluster: conditions on
static columns of missing partitions and null values, alone and in batches
(CASSANDRA-9842, #6273), and compare-and-set at QUORUM with RF=3.

Ported from scylla-dtest's cql_additional_tests.py (TestLWTWithCQL and
TestCQL::test_cas_simple). Like the dtests, the keyspaces use vnodes, by
the nodes' default.
"""

import logging

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.dtest_helpers import nodetool
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
import pytest

logger = logging.getLogger(__name__)


# Like the dtests' nodes, new keyspaces use vnodes by default.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled'}


def keyspace_opts(rf: int) -> str:
    return f"WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"


async def rows(cql, stmt) -> list[tuple]:
    return [tuple(r) for r in await cql.run_async(stmt)]


async def validate_non_existing_or_null_values(cql, table: str) -> None:
    assert await rows(cql, f"UPDATE {table} SET s = 1 WHERE a = 1 IF s = NULL") == [(True, None)]
    assert await rows(cql, f"SELECT a, s, d FROM {table} WHERE a = 1") == [(1, 1, None)]
    assert await rows(cql, f"UPDATE {table} SET s = 2 WHERE a = 2 IF s IN (10,20,NULL)") == [(True, None)]
    assert await rows(cql, f"SELECT a, s, d FROM {table} WHERE a = 2") == [(2, 2, None)]
    assert await rows(cql, f"UPDATE {table} SET s = 4 WHERE a = 4 IF s != 4") == [(True, None)]
    assert await rows(cql, f"SELECT a, s, d FROM {table} WHERE a = 4") == [(4, 4, None)]


# Conditions on a static column of partitions without rows, applied by
# UPDATE, IF EXISTS, INSERT IF NOT EXISTS and in batches with an INSERT.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_lwt_with_static_columns(manager: ScyllaClusterManager):
    await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.lwt_with_static"
        await cql.run_async(f"CREATE TABLE {table} (a int, b int, s int static, d text, PRIMARY KEY (a, b))")
        assert await rows(cql, f"UPDATE {table} SET s = 1 WHERE a = 1 IF s = NULL") == [(True, None)]
        assert await rows(cql, f"SELECT * FROM {table}") == [(1, None, 1, None)]
        assert await rows(cql, f"UPDATE {table} SET s = 2 WHERE a = 2 IF EXISTS") == [(False, None, None, None, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 1") == [(1, None, 1, None)]
        assert await rows(cql, f"INSERT INTO {table} (a, s) VALUES (2, 2) IF NOT EXISTS") == [(True, None, None, None, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 2") == [(2, None, 2, None)]
        for a in [3, 4]:
            batch = f"BEGIN BATCH\nINSERT INTO {table} (a, b, d) values ({a}, {a}, 'a');\nUPDATE {table} SET s = {a} WHERE a = {a} IF s = null;\nAPPLY BATCH;"
            assert await rows(cql, batch) == [(True, None, None, None), (True, None, None, None)]
            assert await rows(cql, f"SELECT * FROM {table} WHERE a = {a}") == [(a, a, a, "a")]


# Conditions on a null static column of partitions with rows.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_conditional_updates_on_null_static_column(manager: ScyllaClusterManager):
    await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.conditional_updates_on_static_columns_with_null"
        await cql.run_async(f"CREATE TABLE {table} (a int, b int, s int static, d text, PRIMARY KEY (a, b))")
        for i in range(1, 6):
            await cql.run_async(f"INSERT INTO {table} (a, b) VALUES ({i}, {i})")
        await validate_non_existing_or_null_values(cql, table)
        assert await rows(cql, f"UPDATE {table} SET s = 30 WHERE a = 3 IF s IN (10,20,30)") == [(False, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 3") == [(3, 3, None, None)]
        for operator in [">", "<", ">=", "<=", "="]:
            assert await rows(cql, f"UPDATE {table} SET s = 50 WHERE a = 5 IF s {operator} 3") == [(False, None)]
            assert await rows(cql, f"SELECT * FROM {table} WHERE a = 5") == [(5, 5, None, None)]


# Conditions on the static column of missing partitions.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_conditional_updates_on_missing_static_column(manager: ScyllaClusterManager):
    await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.conditional_updates_on_static_columns_with_ne"
        await cql.run_async(f"CREATE TABLE {table} (a int, b int, s int static, d text, PRIMARY KEY (a, b))")
        await validate_non_existing_or_null_values(cql, table)
        assert await rows(cql, f"UPDATE {table} SET s = 30 WHERE a = 3 IF s IN (10,20,30)") == [(False, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 3") == []
        for operator in [">", "<", ">=", "<=", "="]:
            assert await rows(cql, f"UPDATE {table} SET s = 50 WHERE a = 5 IF s {operator} 3") == [(False, None)]
            assert await rows(cql, f"SELECT * FROM {table} WHERE a = 5") == []


# Batches of an INSERT and an UPDATE conditioned on a null static column.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_conditional_updates_on_null_static_column_batch(manager: ScyllaClusterManager):
    await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.lwt_on_static_columns_with_null_batch"
        await cql.run_async(f"CREATE TABLE {table} (a int, b int, s int static, d text, PRIMARY KEY (a, b))")
        for i in range(1, 7):
            await cql.run_async(f"INSERT INTO {table} (a, b) VALUES ({i}, {i})")

        async def batch(insert, update):
            return await rows(cql, f"BEGIN BATCH\n{insert}\n{update}\nAPPLY BATCH")
        assert await batch(f"INSERT INTO {table} (a, b, d) values (2, 2, 'a');", f"UPDATE {table} SET s = 2 WHERE a = 2 IF s = null;") == [
            (True, 2, 2, None), (True, 2, None, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 2") == [(2, 2, 2, "a")]
        assert await batch(f"INSERT INTO {table} (a, b, s, d) values (4, 4, 4, 'a')", f"UPDATE {table} SET s = 5 WHERE a = 4 IF s = null;") == [
            (True, 4, 4, None), (True, 4, None, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 4") == [(4, 4, 5, "a")]
        assert await batch(f"INSERT INTO {table} (a, b, s, d) values (5, 5, 5, 'a')", f"UPDATE {table} SET s = 6 WHERE a = 5 IF s IN (1,2,null)") == [
            (True, 5, 5, None), (True, 5, None, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 5") == [(5, 5, 6, "a")]
        assert await batch(f"INSERT INTO {table} (a, b, s, d) values (7, 7, 7, 'a')", f"UPDATE {table} SET s = 8 WHERE a = 7 IF s != 7;") == [
            (True, None, None, None), (True, None, None, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 7") == [(7, 7, 8, "a")]
        for operator in [">", "<", ">=", "<=", "="]:
            assert await batch(f"INSERT INTO {table} (a, b, s, d) values (3, 3, 40, 'a')", f"UPDATE {table} SET s = 30 WHERE a = 3 IF s {operator} 5;") == [
                (False, 3, 3, None), (False, 3, None, None)]
            assert await rows(cql, f"SELECT * FROM {table} WHERE a = 3") == [(3, 3, None, None)]
        assert await batch(f"INSERT INTO {table} (a, b, s, d) values (6, 6, 70, 'a')", f"UPDATE {table} SET s = 60 WHERE a = 6 IF s IN (1,2,3)") == [
            (False, 6, 6, None), (False, 6, None, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 6") == [(6, 6, None, None)]


# DELETE of a static column conditioned on another, null, static column.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_conditional_deletes_on_null_static_column(manager: ScyllaClusterManager):
    await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.conditional_deletes_on_static_with_null"
        await cql.run_async(f"CREATE TABLE {table} (a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b))")
        for i in range(1, 6):
            await cql.run_async(f"INSERT INTO {table} (a, b, s1, s2, v) VALUES ({i}, {i}, {i}, null, {i})")
        assert await rows(cql, f"DELETE s1 FROM {table} WHERE a = 1 IF s2 = null") == [(True, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 1") == [(1, 1, None, None, 1)]
        assert await rows(cql, f"DELETE s1 FROM {table} WHERE a = 2 IF s2 IN (10,20,30)") == [(False, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 2") == [(2, 2, 2, None, 2)]
        assert await rows(cql, f"DELETE s1 FROM {table} WHERE a = 3 IF s2 IN (null,20,30)") == [(True, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 3") == [(3, 3, None, None, 3)]
        assert await rows(cql, f"DELETE s1 FROM {table} WHERE a = 4 IF s2 != 4") == [(True, None)]
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 4") == [(4, 4, None, None, 4)]
        for operator in [">", "<", ">=", "<=", "="]:
            assert await rows(cql, f"DELETE s1 FROM {table} WHERE a = 5 IF s2 {operator} 3") == [(False, None)]
            assert await rows(cql, f"SELECT * FROM {table} WHERE a = 5") == [(5, 5, 5, None, 5)]


# Batches of an INSERT and a DELETE of a static column conditioned on a
# static column of a partition that does not exist yet.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_conditional_deletes_on_null_static_column_batch(manager: ScyllaClusterManager):
    await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.conditional_deletes_on_static_with_null_batch"
        await cql.run_async(f"CREATE TABLE {table} (a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b))")

        async def batch(a, condition):
            return await rows(cql, f"BEGIN BATCH\nINSERT INTO {table} (a, b, s1, v) values ({a}, {a}, {a}, {a});\n"
                                   f"DELETE s1 FROM {table} WHERE a = {a} IF {condition};\nAPPLY BATCH")
        applied = [(True, None, None, None), (True, None, None, None)]
        not_applied = [(False, None, None, None), (False, None, None, None)]
        assert await batch(2, "s2 = null") == applied
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 2") == [(2, 2, None, None, 2)]
        for operator in [">", "<", ">=", "<=", "="]:
            assert await batch(3, f"s2 {operator} 5") == not_applied
            assert await rows(cql, f"SELECT * FROM {table} WHERE a = 3") == []
        assert await batch(6, "s2 IN (1,2,3)") == not_applied
        assert await rows(cql, f"SELECT * FROM {table} WHERE a = 6") == []
        for a, condition in [(4, "s2 = null"), (5, "s1 IN (1,2,null)"), (7, "s2 != 7")]:
            assert await batch(a, condition) == applied
            assert await rows(cql, f"SELECT * FROM {table} WHERE a = {a}") == [(a, a, None, None, a)]


# A batch of a conditional DELETE of a row and an INSERT of the same row:
# the DELETE wins (#6273).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_batch_conditional_delete_and_insert_same_row(manager: ScyllaClusterManager):
    await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.cf"
        await cql.run_async(f"CREATE COLUMNFAMILY {table} (key bigint, ck int, cv set<text>, PRIMARY KEY ((key), ck))")
        assert await rows(cql, f"INSERT INTO {table} (key, ck, cv) VALUES (1, 0, {{'a', 'b'}}) if not exists;") == [(True, None, None, None)]
        assert await rows(cql, f"""BEGIN BATCH
                DELETE FROM {table} WHERE key=1 and ck=0 if exists;
                INSERT INTO {table} (key, ck, cv) VALUES (1, 0, {{'b', 'c'}});
            APPLY BATCH;""") == [(True, 1, 0, {"a", "b"}), (True, 1, 0, {"a", "b"})]
        assert await rows(cql, f"SELECT * FROM {table}") == []


# A batch of a conditional UPDATE setting a collection to null and an INSERT
# of the same row, the workaround for #6273: the INSERT's value is kept.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_batch_conditional_update_and_insert_same_row(manager: ScyllaClusterManager):
    await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.cf"
        await cql.run_async(f"CREATE COLUMNFAMILY {table} (key bigint, ck int, cv set<text>, PRIMARY KEY ((key), ck))")
        assert await rows(cql, f"INSERT INTO {table} (key, ck, cv) VALUES (1, 0, {{'a', 'b'}}) if not exists;") == [(True, None, None, None)]
        assert await rows(cql, f"""BEGIN BATCH
                UPDATE {table} SET cv=null WHERE key=1 and ck=0 if exists;
                INSERT INTO {table} (key, ck, cv) VALUES (1, 0, {{'b', 'c'}});
            APPLY BATCH;""") == [(True, 1, 0, {"a", "b"}), (True, 1, 0, {"a", "b"})]
        assert await rows(cql, f"SELECT * FROM {table}") == [(1, 0, {"b", "c"})]


# With RF=3, a row inserted at QUORUM is consumed by the first of two
# identical conditional UPDATEs at QUORUM; the second sees it consumed.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_cas_at_quorum(manager: ScyllaClusterManager):
    await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(3)) as ks:
        table = f"{ks}.tkns"
        await cql.run_async(f"CREATE TABLE {table} (tkn int, consumed boolean, PRIMARY KEY (tkn));")
        for i in range(1, 10):
            await cql.run_async(SimpleStatement(f"INSERT INTO {table} (tkn, consumed) VALUES ({i},FALSE);", consistency_level=ConsistencyLevel.QUORUM))
            update = SimpleStatement(f"UPDATE {table} SET consumed = TRUE WHERE tkn = {i} IF consumed = FALSE;", consistency_level=ConsistencyLevel.QUORUM)
            assert await rows(cql, update) == [(True, False)]
            assert await rows(cql, update) == [(False, True)]


# The dtests' populate(3): SimpleSnitch, every node in datacenter1/rack1, vnodes.
POPULATE_CONFIG = {**CONFIG, "endpoint_snitch": "SimpleSnitch"}
POPULATE_PROPERTY_FILE = {"dc": "datacenter1", "rack": "rack1"}


async def prepare(manager: ScyllaClusterManager):
    """The dtests' prepare(): populate(3), then get_lwttester_session(): a
    session to node1, at consistency level ONE like dtest sessions, using
    the keyspace "ks" (RF=1) it creates. Returns the servers and the session."""
    servers = await manager.servers_add(3, config=POPULATE_CONFIG, property_file=POPULATE_PROPERTY_FILE)
    cql = manager.con_gen([servers[0].ip_addr]).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    cql.execute("""CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION={'class':'NetworkTopologyStrategy',
            'replication_factor':1}""")
    cql.execute("USE ks")
    return servers, cql


# Ported from scylla-dtest cql_additional_tests.py::TestLWTWithCQL::test_lwt_with_empty_resultset.
# An LWT condition on a row created by an UPDATE (so without a row marker)
# and flushed on every node: "IF v2 = null" applies (CASSANDRA-12694).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_lwt_with_empty_resultset(manager: ScyllaClusterManager):
    servers, cql = await prepare(manager)
    try:
        cql.execute("""
            CREATE TABLE test (pk text, v1 int, v2 text, PRIMARY KEY (pk));
        """)
        cql.execute("update test set v1 = 100 where pk = 'test1';")
        # ccm's cluster.flush(): nodetool flush on each node.
        for server in servers:
            await nodetool(manager, server, "flush")
        assert [tuple(r) for r in cql.execute("UPDATE test SET v1 = 100 WHERE pk = 'test1' IF v2 = null;")] == [(True, None)]
    finally:
        cql.shutdown()


# Ported from scylla-dtest cql_additional_tests.py::TestLWTWithCQL::test_batch_insert_delete_same_row.
# A batch of an INSERT of a row and a conditional DELETE of the same row:
# the DELETE wins and the row is gone (#6273).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_batch_insert_delete_same_row(manager: ScyllaClusterManager):
    _, cql = await prepare(manager)
    try:
        table_name = "cf"
        cql.execute(f"""
                        CREATE COLUMNFAMILY {table_name} (key bigint, ck int, cv set<text>, PRIMARY KEY ((key), ck))
                        """)
        assert [tuple(r) for r in cql.execute(f"""INSERT INTO {table_name} (key, ck, cv) VALUES (1, 0, {{'a', 'b'}}) if not exists;""")] == [
            (True, None, None, None)]
        assert [tuple(r) for r in cql.execute(f"""BEGIN BATCH
                        INSERT INTO {table_name} (key, ck, cv) VALUES (1, 0, {{'b', 'c'}});
                        DELETE FROM {table_name} WHERE key=1 and ck=0 if exists;
                      APPLY BATCH;""")] == [(True, 1, 0, {"a", "b"}), (True, 1, 0, {"a", "b"})]
        assert list(cql.execute(f"SELECT * FROM {table_name}")) == []
    finally:
        cql.shutdown()


# Ported from scylla-dtest cql_additional_tests.py::TestLWTWithCQL::test_batch_insert_new_delete_old_row.
# A batch inserting a new row IF NOT EXISTS and deleting another row of the
# same partition IF EXISTS: both apply, only the new row is left.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_batch_insert_new_delete_old_row(manager: ScyllaClusterManager):
    _, cql = await prepare(manager)
    try:
        table_name = "cf"
        cql.execute(f"""
                        CREATE COLUMNFAMILY {table_name} (key bigint, ck int, cv set<text>, PRIMARY KEY ((key), ck))
                        """)
        assert [tuple(r) for r in cql.execute(f"""INSERT INTO {table_name} (key, ck, cv) VALUES (1, 0, {{'a', 'b'}}) if not exists;""")] == [
            (True, None, None, None)]
        assert [tuple(r) for r in cql.execute(f"""BEGIN BATCH
                        INSERT INTO {table_name} (key, ck, cv) VALUES (1, 1, {{'b', 'c'}}) IF NOT EXISTS;
                        DELETE FROM {table_name} WHERE key=1 and ck=0 if exists;
                      APPLY BATCH;""")] == [(True, None, None, None), (True, 1, 0, {"a", "b"})]
        assert [tuple(r) for r in cql.execute(f"SELECT * FROM {table_name}")] == [(1, 1, {"b", "c"})]
    finally:
        cql.shutdown()
