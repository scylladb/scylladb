# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Each kind of CQL statement executed at least once, to go through its path
# in the storage proxy: table, index, type, user and batch statements.
# Ported from scylla-dtest cql_tests.py (TestStorageProxyCQL). Each test
# uses an RF=1 vnodes keyspace of its own like the dtests, and ends with a
# flush of all tables, as a dtest node flushes its memtables when it stops.

import pytest
from cassandra import ConsistencyLevel
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement

from .nodetool import flush_all
from .util import config_value_context, new_test_keyspace, unique_name

KS_OPTS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}"


@pytest.fixture(autouse=True)
def auto_snapshot(cql):
    """auto_snapshot is on, as on dtest nodes: TRUNCATE and DROP snapshot the table."""
    with config_value_context(cql, 'auto_snapshot', 'true'):
        yield


def rows(cql, query):
    return [list(r) for r in cql.execute(query)]


# CREATE TABLE, ALTER TABLE, TRUNCATE TABLE, DROP TABLE.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_table(cql, compact_storage):
    with new_test_keyspace(cql, KS_OPTS) as ks:
        cql.execute(f"CREATE TABLE {ks}.test1 (k int PRIMARY KEY, v1 int)")
        cql.execute(f"CREATE TABLE {ks}.test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE")
        cql.execute(f"ALTER TABLE {ks}.test1 ADD v2 int")
        for i in range(10):
            cql.execute(f"INSERT INTO {ks}.test1 (k, v1, v2) VALUES ({i}, {i}, {i})")
            cql.execute(f"INSERT INTO {ks}.test2 (k, c1, v1) VALUES ({i}, {i}, {i})")
        for t in ["test1", "test2"]:
            assert sorted(rows(cql, f"SELECT * FROM {ks}.{t}")) == [[i, i, i] for i in range(10)]
        for t in ["test1", "test2"]:
            cql.execute(f"TRUNCATE {ks}.{t}")
        for t in ["test1", "test2"]:
            assert rows(cql, f"SELECT * FROM {ks}.{t}") == []
        for t in ["test1", "test2"]:
            cql.execute(f"DROP TABLE {ks}.{t}")
        for t in ["test1", "test2"]:
            with pytest.raises(InvalidRequest):
                cql.execute(f"SELECT * FROM {ks}.{t}")
        flush_all(cql)


# CREATE INDEX, DROP INDEX.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_index(cql):
    with new_test_keyspace(cql, KS_OPTS) as ks:
        cql.execute(f"CREATE TABLE {ks}.test3 (k int PRIMARY KEY, v1 int, v2 int)")
        cql.execute(f"CREATE INDEX testidx ON {ks}.test3 (v1)")
        for i in range(10):
            cql.execute(f"INSERT INTO {ks}.test3 (k, v1, v2) VALUES ({i}, {i}, {i})")
        assert rows(cql, f"SELECT * FROM {ks}.test3 WHERE v1 = 0") == [[0, 0, 0]]
        cql.execute(f"DROP INDEX {ks}.testidx")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT * FROM {ks}.test3 where v1 = 0")
        flush_all(cql)


# CREATE TYPE, ALTER TYPE, DROP TYPE.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_type(cql):
    with new_test_keyspace(cql, KS_OPTS) as ks:
        cql.execute(f"CREATE TYPE {ks}.address_t (street text, city text, zip_code int)")
        cql.execute(f"CREATE TABLE {ks}.test4 (id int PRIMARY KEY, address frozen<address_t>)")
        cql.execute(f"ALTER TYPE {ks}.address_t ADD phones set<text>")
        cql.execute(f"CREATE TABLE {ks}.test5 (id int PRIMARY KEY, address frozen<address_t>)")
        cql.execute(f"DROP TABLE {ks}.test4")
        cql.execute(f"DROP TABLE {ks}.test5")
        cql.execute(f"DROP TYPE {ks}.address_t")
        with pytest.raises(InvalidRequest):
            cql.execute(f"CREATE TABLE {ks}.test6 (id int PRIMARY KEY, address frozen<address_t>)")
        flush_all(cql)


# CREATE USER, ALTER USER, DROP USER, by the superuser.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_user(cql):
    with new_test_keyspace(cql, KS_OPTS):
        user = unique_name()
        cql.execute(f"CREATE USER {user} WITH PASSWORD 'secret'")
        cql.execute(f"ALTER USER {user} WITH PASSWORD 'secret^2'")
        cql.execute(f"DROP USER {user}")
        flush_all(cql)


# A BATCH of inserts, an update and a cell deletion, at QUORUM.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_batch(cql):
    with new_test_keyspace(cql, KS_OPTS) as ks:
        cql.execute(f"""
            CREATE TABLE {ks}.test8 (
                userid text PRIMARY KEY,
                name text,
                password text
            )
        """)
        cql.execute(SimpleStatement(f"""
            BEGIN BATCH
                INSERT INTO {ks}.test8 (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                UPDATE {ks}.test8 SET password = 'ps22dhds' WHERE userid = 'user3';
                INSERT INTO {ks}.test8 (userid, password) VALUES ('user4', 'ch@ngem3c');
                DELETE name FROM {ks}.test8 WHERE userid = 'user1';
            APPLY BATCH;
        """, consistency_level=ConsistencyLevel.QUORUM))
        flush_all(cql)


# Ported from scylla-dtest cql_tests.py::TestStorageProxyCQL::test_statements.
# INSERT, UPDATE, SELECT, SELECT COUNT and DELETE statements.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_statements(cql):
    with new_test_keyspace(cql, KS_OPTS) as ks:
        cql.execute(f"CREATE TABLE {ks}.test7 (kind text, time int, v1 int, v2 int, PRIMARY KEY(kind, time) )")

        for i in range(10):
            cql.execute(f"INSERT INTO {ks}.test7 (kind, time, v1, v2) VALUES ('ev1', {i}, {i}, {i})")
            cql.execute(f"INSERT INTO {ks}.test7 (kind, time, v1, v2) VALUES ('ev2', {i}, {i}, {i})")

        assert rows(cql, f"SELECT COUNT(*) FROM {ks}.test7 WHERE kind = 'ev1'") == [[10]]
        assert rows(cql, f"SELECT COUNT(*) FROM {ks}.test7 WHERE kind IN ('ev1', 'ev2')") == [[20]]
        assert rows(cql, f"SELECT COUNT(*) FROM {ks}.test7 WHERE kind IN ('ev1', 'ev2') AND time=0") == [[2]]
        assert rows(cql, f"SELECT * FROM {ks}.test7 WHERE kind = 'ev1'") == [["ev1", i, i, i] for i in range(10)]
        assert rows(cql, f"SELECT * FROM {ks}.test7 WHERE kind = 'ev2'") == [["ev2", i, i, i] for i in range(10)]

        for i in range(10):
            cql.execute(f"UPDATE {ks}.test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time={i}")
        assert rows(cql, f"SELECT * FROM {ks}.test7 WHERE kind = 'ev1'") == [["ev1", i, 0, 0] for i in range(10)]

        cql.execute(f"DELETE FROM {ks}.test7 WHERE kind = 'ev1'")
        assert rows(cql, f"SELECT * FROM {ks}.test7 WHERE kind = 'ev1'") == []
        assert rows(cql, f"SELECT COUNT(*) FROM {ks}.test7 WHERE kind = 'ev1'") == [[0]]
        flush_all(cql)
