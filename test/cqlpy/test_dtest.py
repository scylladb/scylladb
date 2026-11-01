# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests translated from scylla-dtest's single-node tests.
#
# Many of scylla-dtest's tests started a cluster of just one node and ran
# CQL requests on it, so didn't need to be in dtest at all. Such tests that
# were not already covered by other tests in test/cqlpy were moved here.
# Running them here is much faster, and also lets us run them against
# Cassandra to check their assumptions.
#############################################################################

import re

import pytest
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.protocol import AlreadyExists, InvalidRequest
from cassandra.query import UNSET_VALUE

from .util import new_test_table, new_type, unique_name


# Test that a SELECT can list the same column more than once, and returns
# that column's value in each of these positions in the result.
# Reproduces issue #1367, where such a SELECT crashed Scylla on an assertion
# failure in cql3::result_set::add_row(), because the number of values in
# the row did not match the number of columns in the result's metadata.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_select_duplicate_column.
def test_select_duplicate_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c text, v bigint, PRIMARY KEY (p, c)') as table:
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 'a', 42)")
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 'b', 24)")
        # Check that the partition key column can be selected twice
        assert list(cql.execute(f'SELECT p, p FROM {table}')) == [(1, 1), (1, 1)]
        # Check that a regular column can be selected three times, mixed
        # with other columns
        assert list(cql.execute(f'SELECT v, c, v, v FROM {table}')) == [(42, 'a', 42, 42), (24, 'b', 24, 24)]

# Test LIMIT on a range scan - a SELECT whose WHERE restricts only the
# token of the partition key - when partitions have more than one row.
# LIMIT counts rows, not partitions, so "LIMIT 1" returns just the first row
# of the first partition in the range, and a larger LIMIT can stop in the
# middle of a partition. The test also checks that "token(p) >= token(2)"
# includes partition 2, while "token(p) > token(2)" starts at the next
# partition in token order. That next partition is 45, because both Scylla
# and Cassandra use the Murmur3 partitioner by default.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_limit_ranges.
def test_limit_ranges(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c text, v bigint, PRIMARY KEY (p, c)') as table:
        stmt = cql.prepare(f'INSERT INTO {table} (p, c, v) VALUES (?, ?, 42)')
        args = [(p, f'http://foo.{tld}') for p in range(100) for tld in ['com', 'org', 'net']]
        execute_concurrent_with_args(cql, stmt, args, concurrency=100, raise_on_first_error=True)
        # Check that the range starting at partition 2 (inclusive) returns
        # only partition 2's first row
        assert list(cql.execute(f'SELECT * FROM {table} WHERE token(p) >= token(2) LIMIT 1')) == [(2, 'http://foo.com', 42)]
        # Check that the range after partition 2 (exclusive) returns only
        # the first row of the next partition in token order, 45
        assert list(cql.execute(f'SELECT * FROM {table} WHERE token(p) > token(2) LIMIT 1')) == [(45, 'http://foo.com', 42)]
        # Check that a LIMIT larger than a partition stops in the middle of
        # the next partition
        assert list(cql.execute(f'SELECT * FROM {table} WHERE token(p) >= token(2) LIMIT 4')) == [
            (2, 'http://foo.com', 42), (2, 'http://foo.net', 42), (2, 'http://foo.org', 42), (45, 'http://foo.com', 42)]

# Test LIMIT on a SELECT that reads several partitions with an IN
# restriction on the partition key, when partitions have more than one row.
# LIMIT counts rows, not partitions, so "LIMIT 1" returns just one row.
# The partitions are returned in a fixed order, not in the order in which
# the IN lists them, so partition 2 comes first although the IN lists 48
# first. Cassandra before version 2.2 did return them in the IN's order.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_limit_multiget.
def test_limit_multiget(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c text, v bigint, PRIMARY KEY (p, c)') as table:
        stmt = cql.prepare(f'INSERT INTO {table} (p, c, v) VALUES (?, ?, 42)')
        args = [(p, f'http://foo.{tld}') for p in range(100) for tld in ['com', 'org', 'net']]
        execute_concurrent_with_args(cql, stmt, args, concurrency=100, raise_on_first_error=True)
        # Check that LIMIT 1 returns only the first row of partition 2
        assert list(cql.execute(f'SELECT * FROM {table} WHERE p IN (48, 2) LIMIT 1')) == [(2, 'http://foo.com', 42)]
        # Check that a LIMIT larger than a partition stops in the middle of
        # the second partition
        assert list(cql.execute(f'SELECT * FROM {table} WHERE p IN (48, 2) LIMIT 4')) == [
            (2, 'http://foo.com', 42), (2, 'http://foo.net', 42), (2, 'http://foo.org', 42), (48, 'http://foo.com', 42)]

# Test a multi-column restriction on clustering columns, like
# "(b, c, d, e) > (0, 1, 1, 0)", on tables whose clustering columns have
# different mixes of ASC and DESC clustering order. Such a restriction
# compares the tuples of values, regardless of the clustering order of each
# column, while the matching rows are returned in the table's clustering
# order. Reproduces CASSANDRA-7281, where a mix of ASC and DESC columns
# made such SELECTs return wrong results.
# The expected results are computed in Python: filter the rows by comparing
# tuples, and sort them by the clustering order.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_tuple_query_mixed_order_columns
# and the eight similar tuple_query_mixed_order_columns_test2 through 9,
# which never ran because their names do not begin with "test". Those
# nine tests used the seven clustering orders below, each with ">", "<=",
# or both, and this test checks both for every one of them.
@pytest.mark.parametrize('order', [
    ('DESC', 'ASC', 'DESC', 'ASC'),
    ('DESC', 'DESC', 'DESC', 'ASC'),
    ('ASC', 'DESC', 'DESC', 'ASC'),
    ('DESC', 'ASC', 'ASC', 'DESC'),
    ('DESC', 'ASC', 'DESC', 'DESC'),
    ('DESC', 'DESC', 'DESC', 'DESC'),
    ('ASC', 'ASC', 'ASC', 'ASC')], ids='-'.join)
def test_tuple_query_mixed_order_columns(cql, test_keyspace, order):
    clustering_order = ', '.join(f'{col} {o}' for col, o in zip('bcde', order))
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d, e)',
                        f'WITH CLUSTERING ORDER BY ({clustering_order})') as table:
        rows = [(2, 0, 0, 0), (1, 0, 0, 0), (0, 0, 0, 0), (0, 1, 2, -1), (0, 1, 1, -1), (0, 1, 1, 0),
                (0, 1, 1, 1), (0, 1, 0, 2), (0, 2, 1, -3), (0, 2, 0, 3), (-1, 2, 2, 2)]
        stmt = cql.prepare(f'INSERT INTO {table} (a, b, c, d, e) VALUES (0, ?, ?, ?, ?)')
        for row in rows:
            cql.execute(stmt, row)
        def clustering_key(row):
            return tuple(v if o == 'ASC' else -v for v, o in zip(row, order))
        # Check that ">" returns the rows whose (b, c, d, e) is greater than
        # (0, 1, 1, 0), in clustering order
        expected = [(0, *row) for row in sorted(rows, key=clustering_key) if row > (0, 1, 1, 0)]
        assert list(cql.execute(f'SELECT * FROM {table} WHERE a = 0 AND (b, c, d, e) > (0, 1, 1, 0)')) == expected
        # Check that "<=" returns the rest of the rows, in clustering order
        expected = [(0, *row) for row in sorted(rows, key=clustering_key) if row <= (0, 1, 1, 0)]
        assert list(cql.execute(f'SELECT * FROM {table} WHERE a = 0 AND (b, c, d, e) <= (0, 1, 1, 0)')) == expected

# Test filtering (with ALLOW FILTERING) on counter columns: with "=", ">",
# ">=", "<" and "IN" restrictions, restrictions on two counter columns
# together, and counter restrictions combined with restrictions on the
# entire primary key. Also check that such a query is rejected without
# ALLOW FILTERING, and that filtering works correctly on a counter column
# added with ALTER TABLE, and on a counter cell that was deleted - in both
# cases the counter is missing (null), and a missing counter does not match
# any restriction, not even "= 0".
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_filter_by_counter.
def test_filter_by_counter(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, c1 counter, c2 counter, PRIMARY KEY (pk, ck)') as table:
        for i in range(5):
            cql.execute(f'UPDATE {table} SET c1 = c1 + {i} WHERE pk = 0 AND ck = {i}')
            cql.execute(f'UPDATE {table} SET c1 = c1 + {i}, c2 = c2 + {i} WHERE pk = 1 AND ck = {i}')
        # Partition 1 comes before partition 0 in token order. Partition 0
        # has no c2 counter, so its c2 is null.
        # Check filtering by one counter column with "="
        assert list(cql.execute(f'SELECT * FROM {table} WHERE c1 = 2 ALLOW FILTERING')) == [(1, 2, 2, 2), (0, 2, 2, None)]
        # Check filtering by one counter column with ">"
        assert list(cql.execute(f'SELECT * FROM {table} WHERE c1 > 2 ALLOW FILTERING')) == [
            (1, 3, 3, 3), (1, 4, 4, 4), (0, 3, 3, None), (0, 4, 4, None)]
        # Check filtering by two counter columns, with ">" and "="
        assert list(cql.execute(f'SELECT * FROM {table} WHERE c1 > 3 AND c2 = 4 ALLOW FILTERING')) == [(1, 4, 4, 4)]
        # Check filtering by two counter columns, with "IN" and ">="
        assert list(cql.execute(f'SELECT * FROM {table} WHERE c1 IN (0, 2) AND c2 >= 2 ALLOW FILTERING')) == [(1, 2, 2, 2)]
        # Check filtering by counter columns combined with the entire primary key
        assert list(cql.execute(f'SELECT * FROM {table} WHERE pk = 1 AND ck = 4 AND c1 < 3 AND c2 = 0 ALLOW FILTERING')) == []
        # Check that filtering by counter columns requires ALLOW FILTERING,
        # even when the entire primary key is also restricted
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f'SELECT * FROM {table} WHERE pk = 1 AND ck = 4 AND c1 > 3 AND c2 = 0')
        # Check filtering by a counter column added with ALTER TABLE. All its
        # values are null, so nothing matches "c3 = 0". After one row's c3
        # is decremented, that row matches "c3 = -1".
        cql.execute(f'ALTER TABLE {table} ADD c3 counter')
        assert list(cql.execute(f'SELECT * FROM {table} WHERE pk = 1 AND ck = 1')) == [(1, 1, 1, 1, None)]
        assert list(cql.execute(f'SELECT count(*) FROM {table} WHERE c3 = 0 ALLOW FILTERING')) == [(0,)]
        cql.execute(f'UPDATE {table} SET c3 = c3 - 1 WHERE pk = 0 AND ck = 1')
        assert list(cql.execute(f'SELECT * FROM {table} WHERE c3 = -1 ALLOW FILTERING')) == [(0, 1, 1, None, -1)]
        # Check that after a counter cell is deleted, filtering no longer
        # matches its old value
        cql.execute(f'DELETE c1 FROM {table} WHERE pk = 1 AND ck = 1')
        assert list(cql.execute(f'SELECT * FROM {table} WHERE c1 = 1 ALLOW FILTERING')) == [(0, 1, 1, None, -1)]
        assert list(cql.execute(f'SELECT * FROM {table} WHERE c1 = 0 ALLOW FILTERING')) == [
            (1, 0, 0, 0, None), (0, 0, 0, None, None)]

# Test basic increments and decrements of a counter column, written with
# different spacing: "total + 1", "total - 4", "total+1" and "total -2".
# The last one is interesting, because "-2" is parsed as a single negative
# integer literal, so the CQL grammar needs a special case to understand
# "total -2" as "total - 2".
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_counters, which originally came
# from Cassandra's old Python cql_tests.py (counters_test). Cassandra's Java
# version of that test, translated in cassandra_tests as testCounters, uses
# a COMPACT STORAGE table and also checks the "+=" syntax.
def test_counters(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'userid int, url text, total counter, PRIMARY KEY (userid, url)') as table:
        where = "WHERE userid = 1 AND url = 'http://foo.com'"
        cql.execute(f'UPDATE {table} SET total = total + 1 {where}')
        assert list(cql.execute(f'SELECT total FROM {table} {where}')) == [(1,)]
        cql.execute(f'UPDATE {table} SET total = total - 4 {where}')
        assert list(cql.execute(f'SELECT total FROM {table} {where}')) == [(-3,)]
        cql.execute(f'UPDATE {table} SET total = total+1 {where}')
        assert list(cql.execute(f'SELECT total FROM {table} {where}')) == [(-2,)]
        cql.execute(f'UPDATE {table} SET total = total -2 {where}')
        assert list(cql.execute(f'SELECT total FROM {table} {where}')) == [(-4,)]

# Test that SELECT respects inclusive and exclusive bounds of a slice on
# the clustering key: each of the four combinations of ">" or ">=" with "<"
# or "<=", and also with LIMIT, and with ORDER BY DESC and LIMIT.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_exclusive_slice, which originally
# came from Cassandra's old Python cql_tests.py (exclusive_slice_test).
# Cassandra's Java version of that test, translated in cassandra_tests as
# testSelectBounds, uses a COMPACT STORAGE table; this one uses a regular
# table.
def test_exclusive_slice(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int, c int, v int, PRIMARY KEY (k, c)') as table:
        stmt = cql.prepare(f'INSERT INTO {table} (k, c, v) VALUES (0, ?, ?)')
        for i in range(10):
            cql.execute(stmt, [i, i])
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0')) == [(i,) for i in range(10)]
        # Check the four combinations of inclusive and exclusive bounds
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND c >= 2 AND c <= 6')) == [(i,) for i in range(2, 7)]
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND c > 2 AND c <= 6')) == [(i,) for i in range(3, 7)]
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND c >= 2 AND c < 6')) == [(i,) for i in range(2, 6)]
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND c > 2 AND c < 6')) == [(i,) for i in range(3, 6)]
        # Check bounds with LIMIT, and with ORDER BY DESC and LIMIT
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND c > 2 AND c <= 6 LIMIT 2')) == [(3,), (4,)]
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND c >= 2 AND c < 6 ORDER BY c DESC LIMIT 2')) == [(5,), (4,)]

# Test IN restrictions on clustering columns. With a single clustering
# column, "c IN (5, 2, 8)" returns the rows in clustering order, not in the
# order of the IN list. With two clustering columns, IN is allowed on the
# second one when the first is restricted by "=", and also on the first one
# when the second is restricted by "=".
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_in_clause_wide_rows, which
# originally came from Cassandra's old Python cql_tests.py
# (in_clause_wide_rows_test). Cassandra's Java version of that test,
# translated in cassandra_tests as testSelectKeyInForWideRows, uses COMPACT
# STORAGE tables; this one uses regular tables.
def test_in_clause_wide_rows(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int, c int, v int, PRIMARY KEY (k, c)') as table:
        stmt = cql.prepare(f'INSERT INTO {table} (k, c, v) VALUES (0, ?, ?)')
        for i in range(10):
            cql.execute(stmt, [i, i])
        # Check that the rows are returned in clustering order
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND c IN (5, 2, 8)')) == [(2,), (5,), (8,)]
    with new_test_table(cql, test_keyspace, 'k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)') as table:
        stmt = cql.prepare(f'INSERT INTO {table} (k, c1, c2, v) VALUES (0, 0, ?, ?)')
        for i in range(10):
            cql.execute(stmt, [i, i])
        # Check IN on the first clustering column, with "=" on the second.
        # All rows have c1 = 0, so nothing matches.
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3')) == []
        # Check IN on the second clustering column, with "=" on the first
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND c1 = 0 AND c2 IN (5, 2, 8)')) == [(2,), (5,), (8,)]

# Test that "= null" restrictions in SELECT match nothing - like in SQL,
# where nothing is equal to null, not even null: Neither "k = null" on the
# partition key, nor "v1 = null" on a regular column, even when v1 is null
# (unset) in every row. The "v1 = null" restriction is tested both with
# ALLOW FILTERING, and with a secondary index on v1 instead.
# Cassandra refuses "= null" restrictions, so allowing them is a deliberate
# Scylla extension and this is a scylla_only test - see more in
# test_null.py::test_filtering_eq_null.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_null_support and
# test_null_support_index. The rest of those dtests, writing nulls and
# reading them back, is the same as Cassandra's null_support_test, which
# is already translated in cassandra_tests as testNullSupport.
@pytest.mark.parametrize('use_index', [False, True], ids=['filtering', 'index'])
def test_eq_null_matches_nothing(cql, test_keyspace, use_index, scylla_only):
    with new_test_table(cql, test_keyspace, 'k int, c int, v1 int, PRIMARY KEY (k, c)') as table:
        if use_index:
            cql.execute(f'CREATE INDEX ON {table}(v1)')
            allow_filtering = ''
        else:
            allow_filtering = 'ALLOW FILTERING'
        cql.execute(f'INSERT INTO {table} (k, c) VALUES (0, 0)')
        cql.execute(f'INSERT INTO {table} (k, c) VALUES (0, 1)')
        # Check that "k = null" matches nothing
        assert list(cql.execute(f'SELECT * FROM {table} WHERE k = null')) == []
        # Check that "v1 = null" matches nothing, with or without a
        # partition-key restriction
        assert list(cql.execute(f'SELECT * FROM {table} WHERE k = 0 AND v1 = null {allow_filtering}')) == []
        assert list(cql.execute(f'SELECT * FROM {table} WHERE v1 = null {allow_filtering}')) == []

# Test repeated equality restrictions on the same column in SELECT:
# "k = 0 AND k = 0" is the same as "k = 0", also together with a
# clustering-key restriction, and "k = 0 AND k = 1" matches nothing.
# Cassandra does not allow restricting a column with an equality more than
# once, so this is a scylla_only test - see more in test_restrictions.py.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_null_support.
def test_repeated_eq_on_partition_key(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'k int, c int, v int, PRIMARY KEY (k, c)') as table:
        cql.execute(f'INSERT INTO {table} (k, c, v) VALUES (0, 0, 0)')
        cql.execute(f'INSERT INTO {table} (k, c, v) VALUES (0, 1, 1)')
        assert list(cql.execute(f'SELECT * FROM {table} WHERE k = 0 AND k = 0')) == [(0, 0, 0), (0, 1, 1)]
        assert list(cql.execute(f'SELECT * FROM {table} WHERE k = 0 AND k = 0 AND c = 1')) == [(0, 1, 1)]
        assert list(cql.execute(f'SELECT * FROM {table} WHERE k = 0 AND k = 1')) == []

# Test that binding UNSET_VALUE to an assignment in a prepared UPDATE skips
# that assignment, leaving the column's old value - for columns of every
# kind: int, list, set, map, tuple and frozen user-defined type. Also check
# that in the same UPDATE, assignments with real values are still done.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_unset_value_support.
def test_unset_value_support(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(number int)') as simple_type, \
         new_test_table(cql, test_keyspace, f'key int PRIMARY KEY, i int, l list<int>, s set<int>, m map<int, int>, '
                                            f't tuple<int, int>, u frozen<{simple_type}>') as table:
        cql.execute(f'INSERT INTO {table} (key, i, l, s, m, t, u) VALUES (0, 1, [1, 2, 3], {{1, 2, 3}}, {{1: 2}}, (1, 2), {{number: 1}})')
        assert list(cql.execute(f'SELECT key, i, l, s, m, t, u FROM {table}')) == [(0, 1, [1, 2, 3], {1, 2, 3}, {1: 2}, (1, 2), (1,))]
        stmt = cql.prepare(f'UPDATE {table} SET i = ?, l = ?, s = ?, m = ?, t = ?, u = ? WHERE key = ?')
        # Check that UNSET_VALUE in all assignments leaves all columns unchanged
        cql.execute(stmt, [UNSET_VALUE] * 6 + [0])
        assert list(cql.execute(f'SELECT key, i, l, s, m, t, u FROM {table}')) == [(0, 1, [1, 2, 3], {1, 2, 3}, {1: 2}, (1, 2), (1,))]
        # Check that a real value in one assignment is still written, while
        # the columns bound to UNSET_VALUE are left unchanged
        cql.execute(stmt, [2] + [UNSET_VALUE] * 5 + [0])
        assert list(cql.execute(f'SELECT key, i, l, s, m, t, u FROM {table}')) == [(0, 2, [1, 2, 3], {1, 2, 3}, {1: 2}, (1, 2), (1,))]

# Test ORDER BY in a single-partition SELECT: with one clustering column,
# ORDER BY c DESC reverses the rows; with two clustering columns, ORDER BY
# on just the first one is allowed, in both directions. Also check that
# ORDER BY is rejected on a column which does not exist, on the second
# clustering column without the first, and on the partition key.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_order_by, which originally came
# from Cassandra's old Python cql_tests.py (order_by_test).
def test_order_by(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int, c int, v int, PRIMARY KEY (k, c)') as table:
        stmt = cql.prepare(f'INSERT INTO {table} (k, c, v) VALUES (0, ?, ?)')
        for i in range(10):
            cql.execute(stmt, [i, i])
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 ORDER BY c DESC')) == [(i,) for i in range(9, -1, -1)]
    with new_test_table(cql, test_keyspace, 'k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)') as table:
        stmt = cql.prepare(f'INSERT INTO {table} (k, c1, c2, v) VALUES (0, ?, ?, ?)')
        for i in range(4):
            for j in range(2):
                cql.execute(stmt, [i, j, i * 2 + j])
        # Check that ORDER BY is rejected on a column that doesn't exist, on
        # the second clustering column alone, and on the partition key
        for order in ['c DESC', 'c2 DESC', 'k DESC']:
            with pytest.raises(InvalidRequest):
                cql.execute(f'SELECT v FROM {table} WHERE k = 0 ORDER BY {order}')
        # Check ORDER BY on just the first clustering column
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 ORDER BY c1 DESC')) == [(i,) for i in range(7, -1, -1)]
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 ORDER BY c1')) == [(i,) for i in range(8)]

# Test slices on the clustering key (with "<", "<=", ">" or ">=") together
# with ORDER BY ASC or DESC, and check that the right rows are returned in
# the right order. Reproduces CASSANDRA-4160.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_more_order_by, which originally
# came from Cassandra's old Python cql_tests.py (more_order_by_test).
def test_more_order_by(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'row text, number int, string text, PRIMARY KEY (row, number)') as table:
        for number, string in [(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')]:
            cql.execute(f"INSERT INTO {table} (row, number, string) VALUES ('row', {number}, '{string}')")
        def numbers(where_and_order):
            return [r.number for r in cql.execute(f"SELECT number FROM {table} WHERE row = 'row' AND {where_and_order}")]
        assert numbers('number < 3 ORDER BY number ASC') == [1, 2]
        assert numbers('number >= 3 ORDER BY number ASC') == [3, 4]
        assert numbers('number < 3 ORDER BY number DESC') == [2, 1]
        assert numbers('number >= 3 ORDER BY number DESC') == [4, 3]
        assert numbers('number > 3 ORDER BY number DESC') == [4]
        assert numbers('number <= 3 ORDER BY number DESC') == [3, 2, 1]

# Test basic CREATE TABLE, DROP TABLE and TRUNCATE operations: creating
# tables (including a COMPACT STORAGE table, and a column list with a
# trailing comma), that creating a table which already exists fails with
# AlreadyExists, that a repeated column or a COMPACT STORAGE table with
# more than one regular column is rejected, truncating the COMPACT STORAGE
# table, and dropping a table and creating it again with a new schema.
# Translated from scylla-dtest's cql_additional_tests.py::TestCQL::test_table,
# which originally came from Cassandra's old Python cql_tests.py
# (table_test). That test had a typo in its COMPACT STORAGE check - "name,
# int" instead of "name int" - which turned it into a check of a syntax
# error, so here it is fixed.
def test_table(cql, test_keyspace, compact_storage):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, c int') as table1, \
         new_test_table(cql, test_keyspace, 'k int, name int, value int, PRIMARY KEY (k, name)', 'WITH COMPACT STORAGE') as table2, \
         new_test_table(cql, test_keyspace, 'k int, c int, PRIMARY KEY (k),') as table3:
        # Check that creating a table which already exists fails
        with pytest.raises(AlreadyExists, match=re.escape(table3)):
            cql.execute(f'CREATE TABLE {table3} (k int PRIMARY KEY, c int)')
        table4 = f'{test_keyspace}.{unique_name()}'
        # Check that a repeated column is rejected
        with pytest.raises(InvalidRequest):
            cql.execute(f'CREATE TABLE {table4} (k int PRIMARY KEY, c int, k text)')
        # Check that a COMPACT STORAGE table with a clustering key may not
        # have more than one regular column
        with pytest.raises(InvalidRequest, match='COMPACT STORAGE'):
            cql.execute(f'CREATE TABLE {table4} (k int, name int, c1 int, c2 int, PRIMARY KEY (k, name)) WITH COMPACT STORAGE')
        # Check TRUNCATE of the COMPACT STORAGE table
        cql.execute(f'INSERT INTO {table2} (k, name, value) VALUES (1, 2, 3)')
        cql.execute(f'TRUNCATE {table2}')
        assert list(cql.execute(f'SELECT * FROM {table2}')) == []
        # Check that a dropped table can be created again with a new schema
        cql.execute(f'DROP TABLE {table1}')
        cql.execute(f'CREATE TABLE {table1} (k int PRIMARY KEY, c1 int, c2 int,)')
        cql.execute(f'INSERT INTO {table1} (k, c1, c2) VALUES (1, 2, 3)')
        assert list(cql.execute(f'SELECT * FROM {table1}')) == [(1, 2, 3)]
