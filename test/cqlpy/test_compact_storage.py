# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for tables WITH COMPACT STORAGE. More such tests, translated from
# Cassandra's unit tests, are in cassandra_tests/.../compact_storage_test.py.
#############################################################################

from test import DEBUG_MODES
from uuid import UUID

from cassandra.protocol import InvalidRequest

from .util import new_test_table, new_named_table
from .nodetool import flush
import pytest

# In a COMPACT STORAGE table with two clustering columns, a row can be
# written with only the first clustering column (c2 is then null). Such a
# "prefix" row sorts before all the rows with the same c1, so it is included
# by an upper bound on c2 and excluded by a lower bound on c2. Check single-
# and multi-column slices across such rows.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_compact_storage_slices_with_prefix_rows(compact_storage, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)", "WITH COMPACT STORAGE") as table:
        for i in range(3):
            cql.execute(f"INSERT INTO {table} (k, c1, v) VALUES (0, {i}, 1)")
            for j in range(3):
                cql.execute(f"INSERT INTO {table} (k, c1, c2, v) VALUES (0, {i}, {j}, 0)")
        def select(where):
            return list(cql.execute(f"SELECT c1, c2, v FROM {table} WHERE k = 0 {where}"))
        all_rows = [(i, j, v) for i in range(3) for (j, v) in [(None, 1), (0, 0), (1, 0), (2, 0)]]
        assert select("") == all_rows
        assert select("AND c1 = 0 AND c2 >= 1 AND c2 < 2") == [(0, 1, 0)]
        assert select("AND c1 = 0 AND c2 > 1") == [(0, 2, 0)]
        assert select("AND c1 = 0 AND c2 >= 1") == [(0, 1, 0), (0, 2, 0)]
        assert select("AND c1 = 0 AND c2 < 2") == [(0, None, 1), (0, 0, 0), (0, 1, 0)]
        assert select("AND c1 = 0 AND c2 <= 2") == [(0, None, 1), (0, 0, 0), (0, 1, 0), (0, 2, 0)]
        assert select("AND c1 = 0 AND c2 < 1 AND c2 > 2") == []
        assert select("AND c1 = 0 AND c2 < 1 AND c2 >= 2") == []
        assert select("AND c1 = 0 AND c2 < 2 AND c2 > 1") == []
        assert select("AND c1 = 0 AND c2 < 3 AND c2 > 1") == [(0, 2, 0)]
        assert select("AND (c1, c2) < (0, 1) AND (c1, c2) > (0, 2)") == []
        assert select("AND (c1, c2) >= (0, 1) AND (c1) < (0)") == []
        assert select("AND (c1, c2) >= (0, 1) AND (c1) <= (0)") == [(0, 1, 0), (0, 2, 0)]
        assert select("AND (c1, c2) < (0, 1) AND (c1, c2) >= (0, 2)") == []
        assert select("AND (c1, c2) < (0, 2) AND (c1, c2) > (0, 1)") == []
        assert select("AND (c1, c2) < (0, 3) AND (c1, c2) > (0, 1)") == [(0, 2, 0)]
        assert select("AND c1 >= 0") == all_rows
        assert select("AND c1 < 1") == all_rows[:4]
        assert select("AND c1 < 1 AND c1 > 1") == []

# A non-composite "dynamic" COMPACT STORAGE table: one clustering column and
# one value column, so every row is a single cell named by the clustering
# key. Rows are written with INSERT and UPDATE (also with a partition key IN)
# and read by partition and by full scan. An empty clustering key is rejected,
# as it would be the cell's whole name (CASSANDRA-6152).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_compact_storage_dynamic_table(cql, test_keyspace_vnodes, compact_storage):
    schema = "userid uuid, url text, time bigint, PRIMARY KEY (userid, url)"
    with new_named_table(cql, test_keyspace_vnodes, "clicks", schema, "WITH COMPACT STORAGE") as table:
        cql.execute(f"INSERT INTO {table} (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo.bar', 42)")
        cql.execute(f"INSERT INTO {table} (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo-2.bar', 24)")
        cql.execute(f"INSERT INTO {table} (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://bar.bar', 128)")
        cql.execute(f"UPDATE {table} SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 and url = 'http://bar.foo'")
        cql.execute(f"UPDATE {table} SET time = 12 WHERE userid IN (f47ac10b-58cc-4372-a567-0e02b2c3d479, 550e8400-e29b-41d4-a716-446655440000) and url = 'http://foo-3'")

        assert list(cql.execute(f"SELECT url, time FROM {table} WHERE userid = 550e8400-e29b-41d4-a716-446655440000")) == [
            ("http://bar.bar", 128), ("http://foo-2.bar", 24), ("http://foo-3", 12), ("http://foo.bar", 42)]
        u = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")
        assert list(cql.execute(f"SELECT * FROM {table} WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")) == [
            (u, "http://bar.foo", 24), (u, "http://foo-3", 12)]
        # The partition f47ac10b-... comes first in token order.
        assert list(cql.execute(f"SELECT time FROM {table}")) == [(24,), (12,), (128,), (24,), (12,), (42,)]

        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {table} (userid, url, time) VALUES (810e8500-e29b-41d4-a716-446655440000, '', 42)")

# A composite "dense" COMPACT STORAGE table (two clustering columns, one
# value column). A row can be written with only a prefix of the clustering
# key (CASSANDRA-7990), and slices on the first clustering column, deletion
# of the value cell, of a whole partition and of a prefix row work, also
# after a flush.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_compact_storage_dense_table(cql, test_keyspace_vnodes, compact_storage):
    schema = "userid uuid, ip text, port int, time bigint, PRIMARY KEY (userid, ip, port)"
    with new_named_table(cql, test_keyspace_vnodes, "connections", schema, "WITH COMPACT STORAGE") as table:
        cql.execute(f"INSERT INTO {table} (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.1', 80, 42)")
        cql.execute(f"INSERT INTO {table} (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 80, 24)")
        cql.execute(f"INSERT INTO {table} (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 90, 42)")
        cql.execute(f"UPDATE {table} SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.2' AND port = 80")
        # Not all of the clustering columns have to be given.
        cql.execute(f"INSERT INTO {table} (userid, ip, time) VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, '192.168.0.3', 42)")
        cql.execute(f"UPDATE {table} SET time = 42 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.4'")

        def select(where):
            return list(cql.execute(f"SELECT ip, port, time FROM {table} WHERE {where}"))
        u1 = "userid = 550e8400-e29b-41d4-a716-446655440000"
        u2 = "userid = f47ac10b-58cc-4372-a567-0e02b2c3d479"
        assert select(u1) == [("192.168.0.1", 80, 42), ("192.168.0.2", 80, 24), ("192.168.0.2", 90, 42)]
        assert select(f"{u1} and ip >= '192.168.0.2'") == [("192.168.0.2", 80, 24), ("192.168.0.2", 90, 42)]
        assert select(f"{u1} and ip = '192.168.0.2'") == [("192.168.0.2", 80, 24), ("192.168.0.2", 90, 42)]
        assert select(f"{u1} and ip > '192.168.0.2'") == []
        assert select(f"{u2} AND ip = '192.168.0.3'") == [("192.168.0.3", None, 42)]
        assert select(f"{u2} AND ip = '192.168.0.4'") == [("192.168.0.4", None, 42)]

        cql.execute(f"DELETE time FROM {table} WHERE {u1} AND ip = '192.168.0.2' AND port = 80")
        assert len(list(cql.execute(f"SELECT * FROM {table} WHERE {u1}"))) == 2
        cql.execute(f"DELETE FROM {table} WHERE {u1}")
        assert list(cql.execute(f"SELECT * FROM {table} WHERE {u1}")) == []
        cql.execute(f"DELETE FROM {table} WHERE {u2} AND ip = '192.168.0.3'")
        assert list(cql.execute(f"SELECT * FROM {table} WHERE {u2} AND ip = '192.168.0.3'")) == []
        # The rows, prefix rows and deletions written to an sstable read the same.
        flush(cql, table)
        assert select(u1) == []
        assert select(u2) == [("192.168.0.2", 80, 24), ("192.168.0.4", None, 42)]

# A COMPACT STORAGE table cannot have a collection column.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_compact_storage_collection_rejected(cql, test_keyspace_vnodes, compact_storage):
    with pytest.raises(InvalidRequest):
        cql.execute(f"CREATE TABLE {test_keyspace_vnodes}.test (user ascii PRIMARY KEY, mails list<text>) WITH COMPACT STORAGE")

# A COMPACT STORAGE table with only a partition key and one regular column
# returns both in SELECT * (CASSANDRA-5189), also after a flush.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_compact_storage_static_table_metadata(cql, test_keyspace_vnodes, compact_storage):
    with new_named_table(cql, test_keyspace_vnodes, "bar", "id int primary key, i int", "WITH COMPACT STORAGE") as table:
        cql.execute(f"INSERT INTO {table} (id, i) VALUES (1, 2);")
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(1, 2)]
        flush(cql, table)
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(1, 2)]

# Secondary indexes on the primary key columns of a COMPACT STORAGE table,
# and on the value column of one with clustering columns, are rejected.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_compact_storage_invalid_indexes(cql, test_keyspace_vnodes, compact_storage):
    with new_named_table(cql, test_keyspace_vnodes, "test1", "a int, b int, c int, d int, PRIMARY KEY ((a, b))", "WITH COMPACT STORAGE") as table:
        for col in ["a", "b"]:
            with pytest.raises(InvalidRequest):
                cql.execute(f"CREATE INDEX ON {table}({col})")
    with new_named_table(cql, test_keyspace_vnodes, "test2", "a int, b int, c int, PRIMARY KEY (a, b)", "WITH COMPACT STORAGE") as table:
        for col in ["a", "b", "c"]:
            with pytest.raises(InvalidRequest):
                cql.execute(f"CREATE INDEX ON {table}({col})")

# Slices on the clustering column of a COMPACT STORAGE table, with
# CLUSTERING ORDER BY DESC and without it, in default, ASC and DESC order
# (CASSANDRA-4716).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_compact_storage_reversed_slices(cql, test_keyspace_vnodes, compact_storage):
    for name, extra, desc in [("test1", " AND CLUSTERING ORDER BY (c DESC)", True), ("test2", "", False)]:
        with new_named_table(cql, test_keyspace_vnodes, name, "k text, c int, v int, PRIMARY KEY (k, c)", "WITH COMPACT STORAGE" + extra) as table:
            for i in range(10):
                cql.execute(f"INSERT INTO {table}(k, c, v) VALUES ('foo', {i}, {i})")
            def select(where, order_by=""):
                return [r.c for r in cql.execute(f"SELECT c FROM {table} WHERE {where} AND k = 'foo' {order_by}")]
            exclusive, inclusive = [3, 4, 5], [2, 3, 4, 5, 6]
            assert select("c > 2 AND c < 6") == (exclusive[::-1] if desc else exclusive)
            assert select("c >= 2 AND c <= 6") == (inclusive[::-1] if desc else inclusive)
            assert select("c > 2 AND c < 6", "ORDER BY c ASC") == exclusive
            assert select("c >= 2 AND c <= 6", "ORDER BY c ASC") == inclusive
            assert select("c > 2 AND c < 6", "ORDER BY c DESC") == exclusive[::-1]
            assert select("c >= 2 AND c <= 6", "ORDER BY c DESC") == inclusive[::-1]

# Equality and slice restrictions on the first of two DESC clustering columns
# of a COMPACT STORAGE table, in default, ASC and DESC order (CASSANDRA-4760,
# CASSANDRA-4759).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_compact_storage_reversed_multikey(cql, test_keyspace_vnodes, compact_storage):
    with new_named_table(cql, test_keyspace_vnodes, "test", "key text, c1 int, c2 int, value text, PRIMARY KEY(key, c1, c2)",
                         "WITH COMPACT STORAGE AND CLUSTERING ORDER BY(c1 DESC, c2 DESC)") as table:
        for i in range(3):
            for j in range(3):
                cql.execute(f"INSERT INTO {table}(key, c1, c2, value) VALUES ('foo', {i}, {j}, 'bar');")
        def select(where, order_by=""):
            return [tuple(r) for r in cql.execute(f"SELECT c1, c2 FROM {table} WHERE key='foo' AND {where} {order_by}")]
        def rows(c1s):
            return [(i, j) for i in c1s for j in range(3)]
        asc_all, desc_all = "ORDER BY c1 ASC, c2 ASC", "ORDER BY c1 DESC, c2 DESC"
        for where, c1s in [("c1 = 1", [1]), ("c1 > 1", [2]), ("c1 >= 1", [1, 2]), ("c1 < 1", [0]), ("c1 <= 1", [0, 1])]:
            assert select(where) == rows(c1s)[::-1]
            assert select(where, asc_all) == rows(c1s)
            assert select(where, desc_all) == rows(c1s)[::-1]
            if len(c1s) > 1:
                assert select(where, "ORDER BY c1 ASC") == rows(c1s)
