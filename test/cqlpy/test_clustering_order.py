# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for clustering key ordering, namely the WITH CLUSTERING ORDER BY
# setting in the table schema, and ORDER BY in select.
#
# We have many other tests for this feature - in C++ tests, in translated
# unit tests from Cassandra (cassandra_tests), and its interaction with
# other features (filtering, secondary indexes, etc.) in other test files.

from test import DEBUG_MODES

import pytest

from .util import new_test_table, unique_key_int, sleep_till_whole_second, new_named_table
from . import nodetool
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement

@pytest.fixture(scope="module")
def table_int_desc(cql, test_keyspace):
    schema="k INT, c INT, PRIMARY KEY (k, c)"
    order="WITH CLUSTERING ORDER BY (c DESC)"
    with new_test_table(cql, test_keyspace, schema, order) as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    schema="p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)"
    order="WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)"
    with new_test_table(cql, test_keyspace, schema, order) as table:
        yield table

# Verify that if a table is created with descending order for its
# clustering key, the default ordering of SELECT is changed to descending
# order. This was contrary to our documentation which used to suggest
# that SELECT always defaults to ascending order.
def test_select_default_order(cql, table_int_desc):
    k = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table_int_desc} (k, c) VALUES ({k}, ?)')
    numbers = range(5)
    for i in numbers:
        cql.execute(stmt, [i])
    # In a table created with descending sort order, the default select
    # order is descending:
    rows = [(i,) for i in numbers]
    reverse_rows = [(i,) for i in reversed(numbers)]
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k}'))
    # Confirm that when specifying the order explicitly, both work:
    assert rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c ASC'))
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c DESC'))
    # Repeat the same three assertions as above, adding a "limit" of N=3:
    N=3
    rows = rows[0:N]
    reverse_rows = reverse_rows[0:N]
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} LIMIT {N}'))
    assert rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c ASC LIMIT {N}'))
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c DESC LIMIT {N}'))

# Reproduce issue #65: Test SELECT with a compound clustering key c1,c2
# and a multi-column relation (c1, c2) >= (1,1) when one of the clustering
# column has reversed order. We need to uphold the correct inequality (not
# reversed), just return results in a the reversed order.
def test_multi_column_relation_desc(cql, table2):
    k = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table2} (p, c1, c2) VALUES (?, ?, ?)')
    cql.execute(stmt, [k, 1, 0])
    cql.execute(stmt, [k, 1, 1])
    cql.execute(stmt, [k, 1, 2])
    assert [(1, 2), (1, 1)] == list(cql.execute(f'SELECT c1,c2 FROM {table2} WHERE p = {k} AND (c1, c2) >= (1, 1)'))

@pytest.fixture(scope="module")
def table_all_asc(cql, test_keyspace):
    schema="p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)"
    with new_test_table(cql, test_keyspace, schema) as table:
        yield table

@pytest.fixture(scope="module")
def table_all_desc(cql, test_keyspace):
    schema="p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)"
    order="WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)"
    with new_test_table(cql, test_keyspace, schema, order) as table:
        yield table

# A multi-column equality names a single row, and UPDATE and DELETE are
# allowed to use it. The clustering bounds of a table whose clustering
# columns are all descending are computed in the natural column order and
# then reversed, and it is easy for the reversal to lose the fact that the
# bounds name a single row rather than a range of them - which is all these
# statements have to go by when they decide to accept the WHERE clause.
def test_multi_column_eq_write_all_desc(cql, table_all_desc):
    p = unique_key_int()
    cql.execute(f'UPDATE {table_all_desc} SET v = 1 WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [(1, 2, 1)] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_desc} WHERE p = {p}'))
    cql.execute(f'DELETE FROM {table_all_desc} WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_desc} WHERE p = {p}'))

# Same as test_multi_column_eq_write_all_desc(), for a multi-column IN
# naming several single rows.
def test_multi_column_in_write_all_desc(cql, table_all_desc):
    p = unique_key_int()
    cql.execute(f'UPDATE {table_all_desc} SET v = 1 WHERE p = {p} AND (c1, c2) IN ((1, 2), (3, 4))')
    assert [(3, 4, 1), (1, 2, 1)] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_desc} WHERE p = {p}'))
    cql.execute(f'DELETE FROM {table_all_desc} WHERE p = {p} AND (c1, c2) IN ((1, 2), (3, 4))')
    assert [] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_desc} WHERE p = {p}'))

# Same as test_multi_column_eq_write_all_desc(), for a table that mixes
# ascending and descending clustering columns, and so computes its bounds
# in yet another way.
def test_multi_column_eq_write_mixed_order(cql, table2):
    p = unique_key_int()
    cql.execute(f'UPDATE {table2} SET v = 1 WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [(1, 2, 1)] == list(cql.execute(f'SELECT c1, c2, v FROM {table2} WHERE p = {p}'))
    cql.execute(f'DELETE FROM {table2} WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [] == list(cql.execute(f'SELECT c1, c2, v FROM {table2} WHERE p = {p}'))

# Same as test_multi_column_eq_write_all_desc(), for a table whose
# clustering columns are all ascending, so that no reordering of the
# bounds is needed at all.
def test_multi_column_eq_write_all_asc(cql, table_all_asc):
    p = unique_key_int()
    cql.execute(f'UPDATE {table_all_asc} SET v = 1 WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [(1, 2, 1)] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_asc} WHERE p = {p}'))
    cql.execute(f'DELETE FROM {table_all_asc} WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_asc} WHERE p = {p}'))

# Test a reversed query (ORDER BY DESC) with LIMIT that reads rows and a
# range tombstone deleting some of them, where the range tombstone is already
# old enough to be garbage-collected. The query should not return the deleted
# rows, and neither should later reads of the same partition. Scylla's row
# cache may drop such a purgeable tombstone when it is read, but it must then
# drop the rows that the tombstone deleted as well.
# Reproduces SCYLLADB-5187 (reversed read with LIMIT drops a purgeable range
# tombstone from the row cache, resurrecting the deleted rows).
def test_reversed_read_purgeable_range_tombstone(cql, test_keyspace):
    schema = "p int, c int, PRIMARY KEY (p, c)"
    with new_test_table(cql, test_keyspace, schema, "WITH gc_grace_seconds = 0") as table:
        for c in range(5):
            cql.execute(f"INSERT INTO {table} (p, c) VALUES (0, {c})")
        # Flush the rows before deleting some of them, so that the deleted rows
        # aren't simply dropped when the deletion is flushed. In Scylla, both
        # flushes also add their data to the row cache, so the cache holds
        # both the rows and the range tombstone deleting some of them.
        nodetool.flush(cql, table)
        cql.execute(f"DELETE FROM {table} WHERE p = 0 AND c >= 1 AND c <= 3")
        nodetool.flush(cql, table)
        # A tombstone's deletion time has a resolution of one second, so the
        # tombstone becomes purgeable (with gc_grace_seconds = 0, or Scylla's
        # tombstone_gc "repair" mode with RF=1) only in the next second.
        sleep_till_whole_second()
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = 0 AND c <= 2 ORDER BY c DESC LIMIT 1")) == [(0,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = 0")) == [(0,), (4,)]


# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_order_by.
# ORDER BY a single clustering column in both directions, then on a table
# with two clustering columns: ORDER BY an unknown column, the second
# clustering column alone or the partition key is rejected, ORDER BY the
# first clustering column works in both directions.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_order_by_non_clustering_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int, c int, v int, PRIMARY KEY (k, c)") as table:
        for x in range(10):
            cql.execute(f"INSERT INTO {table} (k, c, v) VALUES (0, {x}, {x})")
        assert list(cql.execute(f"SELECT v FROM {table} WHERE k = 0 ORDER BY c DESC")) == [(x,) for x in range(9, -1, -1)]
    with new_test_table(cql, test_keyspace, "k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)") as table:
        for x in range(4):
            for y in range(2):
                cql.execute(f"INSERT INTO {table} (k, c1, c2, v) VALUES (0, {x}, {y}, {x * 2 + y})")
        for col in ['c', 'c2', 'k']:
            with pytest.raises(InvalidRequest):
                cql.execute(f"SELECT v FROM {table} WHERE k = 0 ORDER BY {col} DESC")
        assert list(cql.execute(f"SELECT v FROM {table} WHERE k = 0 ORDER BY c1 DESC")) == [(x,) for x in range(7, -1, -1)]
        assert list(cql.execute(f"SELECT v FROM {table} WHERE k = 0 ORDER BY c1")) == [(x,) for x in range(8)]

# A multi-column slice (b, c, d, e) > (...) on clustering columns with mixed
# ASC and DESC order returns exactly the rows after the given tuple, in the
# table's clustering order (CASSANDRA-7281).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_tuple_slice_mixed_order_columns(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "foo", "a int, b int, c int, d int , e int, PRIMARY KEY (a, b, c, d, e)",
                         "WITH CLUSTERING ORDER BY (b DESC, c ASC, d DESC, e ASC)") as table:
        for row in [(0, 2, 0, 0, 0), (0, 1, 0, 0, 0), (0, 0, 0, 0, 0), (0, 0, 1, 2, -1), (0, 0, 1, 1, -1), (0, 0, 1, 1, 0),
                    (0, 0, 1, 1, 1), (0, 0, 1, 0, 2), (0, 0, 2, 1, -3), (0, 0, 2, 0, 3), (0, -1, 2, 2, 2)]:
            cql.execute(f"INSERT INTO {table} (a, b, c, d, e) VALUES {row};")
        assert list(cql.execute(f"SELECT * FROM {table} WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")) == [
            (0, 2, 0, 0, 0), (0, 1, 0, 0, 0), (0, 0, 1, 2, -1), (0, 0, 1, 1, 1), (0, 0, 2, 1, -3), (0, 0, 2, 0, 3)]

# ORDER BY a clustering column is rejected in a scan without a partition key
# restriction (CASSANDRA-4246).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_order_by_without_partition_key(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k1 int, k2 int, v int, PRIMARY KEY (k1, k2)") as table:
        for i in range(3):
            cql.execute(f"INSERT INTO {table} (k1, k2, v) VALUES ({i}, {i}, {i})")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT * FROM {table} ORDER BY k2")

# ORDER BY on a table with a DESC clustering column, and on one with
# (c1 ASC, c2 DESC): the clustering order and its exact reverse are allowed,
# any other combination of directions or of columns is rejected.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_order_by_reversed_comparator(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int, c int, v int, PRIMARY KEY (k, c)",
                         "WITH CLUSTERING ORDER BY (c DESC)") as table:
        for x in range(10):
            cql.execute(f"INSERT INTO {table} (k, c, v) VALUES (0, {x}, {x})")
        assert list(cql.execute(f"SELECT c, v FROM {table} WHERE k = 0 ORDER BY c ASC")) == [(x, x) for x in range(10)]
        assert list(cql.execute(f"SELECT c, v FROM {table} WHERE k = 0 ORDER BY c DESC")) == [(x, x) for x in range(9, -1, -1)]
    with new_named_table(cql, test_keyspace_vnodes, "test2", "k int, c1 int, c2 int, v text, PRIMARY KEY (k, c1, c2)",
                         "WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)") as table:
        for x in range(10):
            for y in range(10):
                cql.execute(f"INSERT INTO {table} (k, c1, c2, v) VALUES (0, {x}, {y}, '{x}{y}')")
        select = f"SELECT c1, c2, v FROM {table} WHERE k = 0"
        for order_by in ["c1 ASC, c2 ASC", "c1 DESC, c2 DESC"]:
            with pytest.raises(InvalidRequest):
                cql.execute(f"{select} ORDER BY {order_by}")
        expected = [(x, y, f"{x}{y}") for x in range(10) for y in range(9, -1, -1)]
        assert list(cql.execute(f"{select} ORDER BY c1 ASC")) == expected
        assert list(cql.execute(f"{select} ORDER BY c1 ASC, c2 DESC")) == expected
        assert list(cql.execute(f"{select} ORDER BY c1 DESC, c2 ASC")) == [(x, y, f"{x}{y}") for x in range(9, -1, -1) for y in range(10)]
        with pytest.raises(InvalidRequest):
            cql.execute(f"{select} ORDER BY c2 DESC, c1 ASC")

# A text partition key with clustering order (c1 ASC, c2 DESC): the default
# order, ORDER BY the clustering order and its reverse; ORDER BY only the
# second column, or with directions not matching the clustering order, is
# rejected.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_order_by_multiordering(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k text, c1 int, c2 int, PRIMARY KEY (k, c1, c2)",
                         "WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)") as table:
        for i in range(2):
            for j in range(2):
                cql.execute(f"INSERT INTO {table}(k, c1, c2) VALUES ('foo', {i}, {j})")
        select = f"SELECT c1, c2 FROM {table} WHERE k = 'foo'"
        assert list(cql.execute(select)) == [(0, 1), (0, 0), (1, 1), (1, 0)]
        assert list(cql.execute(f"{select} ORDER BY c1 ASC, c2 DESC")) == [(0, 1), (0, 0), (1, 1), (1, 0)]
        assert list(cql.execute(f"{select} ORDER BY c1 DESC, c2 ASC")) == [(1, 0), (1, 1), (0, 0), (0, 1)]
        for order_by in ["c2 DESC", "c2 ASC", "c1 ASC, c2 ASC"]:
            with pytest.raises(InvalidRequest):
                cql.execute(f"{select} ORDER BY {order_by}")

# LIMIT 1 on a partition with clustering order (c1 ASC, c2 DESC) returns
# the first row in that order (CASSANDRA-4882).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_limit_with_mixed_clustering_order(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)",
                         "WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)") as table:
        for row in [(0, 0, 0, 0), (0, 1, 1, 1), (0, 0, 2, 2), (0, 1, 3, 3)]:
            cql.execute(f"INSERT INTO {table} (k, c1, c2, v) VALUES {row};")
        assert list(cql.execute(f"select * from {table} where k = 0 limit 1;")) == [(0, 0, 2, 2)]

# With an IN restriction, ORDER BY does not need the ordering columns to be
# selected (CASSANDRA-4911), also with IN on the partition key, and with
# writetime() selected (CASSANDRA-8286). Without ORDER BY, IN on the
# partition key returns the partitions sorted by key value. Paging is
# disabled, as ORDER BY with IN on the partition key needs it.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_in_order_by_without_selecting(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)") as table:
        for row in [(0, 0, 0, 0), (0, 0, 1, 1), (0, 0, 2, 2), (1, 1, 0, 3), (1, 1, 1, 4), (1, 1, 2, 5)]:
            cql.execute(f"INSERT INTO {table}(k, c1, c2, v) VALUES {row}")
        def select(what, where):
            return [tuple(r) for r in cql.execute(SimpleStatement(f"SELECT {what} FROM {table} WHERE {where}", fetch_size=None))]
        assert select("*", "k=0 AND c1 = 0 AND c2 IN (2, 0)") == [(0, 0, 0, 0), (0, 0, 2, 2)]
        assert select("*", "k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC, c2 ASC") == [(0, 0, 0, 0), (0, 0, 2, 2)]
        assert select("v", "k=0 AND c1 = 0 AND c2 IN (2, 0)") == [(0,), (2,)]
        assert select("v", "k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC") == [(0,), (2,)]
        assert select("v", "k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC") == [(2,), (0,)]
        assert select("v", "k IN (1, 0)") == [(0,), (1,), (2,), (3,), (4,), (5,)]
        assert select("v", "k IN (1, 0) ORDER BY c1 ASC") == [(0,), (1,), (2,), (3,), (4,), (5,)]
        results = select("writetime(v)", "k IN (1, 0) ORDER BY c1 ASC")
        assert results == sorted(results)

# IN on the last clustering column with ORDER BY DESC of the first one
# returns the rows in descending order.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_in_with_desc_order(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)") as table:
        for c2 in range(3):
            cql.execute(f"INSERT INTO {table}(k, c1, c2) VALUES (0, 0, {c2})")
        assert list(cql.execute(f"SELECT * FROM {table} WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC")) == [(0, 0, 2), (0, 0, 0)]

# IN on a DESC clustering column of a table with a composite partition key,
# with one value and with a value that does not exist (CASSANDRA-7105).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_in_on_desc_clustering_column(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "a int, b int, c int, PRIMARY KEY ((a, b), c)",
                         "with clustering order by (c desc)") as table:
        cql.execute(f"INSERT INTO {table} (a, b, c) VALUES (1, 2, 3)")
        cql.execute(f"INSERT INTO {table} (a, b, c) VALUES (4, 5, 6)")
        assert list(cql.execute(f"SELECT * FROM {table} WHERE a=1 AND b=2 AND c IN (3)")) == [(1, 2, 3)]
        assert list(cql.execute(f"SELECT * FROM {table} WHERE a=1 AND b=2 AND c IN (3, 4)")) == [(1, 2, 3)]

# now() can be written into a DESC timeuuid clustering column
# (CASSANDRA-5386).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_now_into_desc_clustering_column(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int, c timeuuid, v int, PRIMARY KEY (k, c)",
                         "WITH CLUSTERING ORDER BY (c DESC)") as table:
        cql.execute(f"INSERT INTO {table} (k, c, v) VALUES (0, now(), 0);")
        assert len(list(cql.execute(f"SELECT * FROM {table} WHERE k = 0"))) == 1

# dateOf() of a DESC timeuuid clustering column can be selected in a full
# scan.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_function_on_desc_clustering_column(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int, t timeuuid, PRIMARY KEY (k, t)",
                         "WITH CLUSTERING ORDER BY (t DESC)") as table:
        for i in range(5):
            cql.execute(f"INSERT INTO {table} (k, t) VALUES ({i}, now())")
        assert len(list(cql.execute(f"SELECT dateOf(t) FROM {table}"))) == 5
