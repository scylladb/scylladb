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
from cassandra.query import SimpleStatement, UNSET_VALUE

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

# Read the given table's row from system_schema.tables.
def table_schema(cql, table):
    ks, cf = table.split('.')
    return cql.execute(f"SELECT * FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{cf}'").one()

# Test setting table options in CREATE TABLE and then changing them with
# ALTER TABLE: comment, gc_grace_seconds, bloom_filter_fp_chance, and
# compaction strategy with its options, and that the new values appear in
# system_schema.tables.
# Translated from scylla-dtest's cql_additional_tests.py::TestCQL::test_table_options,
# which originally came from Cassandra's old Python cql_tests.py
# (table_options_test). That test also set some options using old syntax
# which Cassandra no longer accepts - this part is tested separately below,
# in test_table_options_old_syntax.
def test_table_options(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, c int',
                        "WITH comment = 'My comment' AND gc_grace_seconds = 4 AND bloom_filter_fp_chance = 0.01 "
                        "AND compaction = { 'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 10 }") as table:
        schema = table_schema(cql, table)
        assert schema.comment == 'My comment'
        assert schema.gc_grace_seconds == 4
        assert schema.bloom_filter_fp_chance == 0.01
        # Cassandra writes the full Java class name and also adds default
        # options, so we check only the class name's end and our option.
        assert schema.compaction['class'].endswith('LeveledCompactionStrategy')
        assert schema.compaction['sstable_size_in_mb'] == '10'
        cql.execute(f"ALTER TABLE {table} WITH comment = 'other comment' AND gc_grace_seconds = 100 "
                    "AND bloom_filter_fp_chance = 0.1 "
                    "AND compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_sstable_size' : 42 }")
        schema = table_schema(cql, table)
        assert schema.comment == 'other comment'
        assert schema.gc_grace_seconds == 100
        assert schema.bloom_filter_fp_chance == 0.1
        assert schema.compaction['class'].endswith('SizeTieredCompactionStrategy')
        assert schema.compaction['min_sstable_size'] == '42'

# Test that Scylla still accepts some table options in old syntax which
# Cassandra no longer accepts: the read_repair_chance and
# dclocal_read_repair_chance options (removed in Cassandra 4.0, and ignored
# by Scylla), compression options given as "sstable_compression" instead
# of "class", and caching options given as a JSON string instead of a map.
# Accepting these old forms is a deliberate Scylla decision, for backward
# compatibility, so this is a scylla_only test.
# Translated from scylla-dtest's cql_additional_tests.py::TestCQL::test_table_options.
def test_table_options_old_syntax(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, c int',
                        "WITH read_repair_chance = 0.5 AND dclocal_read_repair_chance = 0.5 "
                        "AND compression = { 'sstable_compression' : '' } "
                        """AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'""") as table:
        schema = table_schema(cql, table)
        assert dict(schema.caching) == {'keys': 'ALL', 'rows_per_partition': 'ALL'}
        cql.execute(f"ALTER TABLE {table} WITH read_repair_chance = 0.3 AND dclocal_read_repair_chance = 0.3 "
                    "AND compression = { 'sstable_compression' : 'SnappyCompressor' } "
                    """AND caching = '{"keys":"NONE","rows_per_partition":"ALL"}'""")
        schema = table_schema(cql, table)
        assert dict(schema.caching) == {'keys': 'NONE', 'rows_per_partition': 'ALL'}
        assert schema.compression['sstable_compression'].endswith('SnappyCompressor')

# Test removing elements from a static list column and from a regular list
# column in the same table, and setting an element of the static list.
# Removing elements from a list ("l = l - [...]") and setting an element by
# its position need to read the list first, and with both a static list and
# a regular list in the table, Scylla needs to read the right one.
# Reproduces issue #903.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_list_prefetch_with_static_column.
def test_list_prefetch_with_static_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'fn text, ln text, static_tags list<text> static, tags list<text>, PRIMARY KEY (fn, ln)') as table:
        where_row = "WHERE fn = 'Tom' AND ln = 'Bombadil'"
        where_partition = "WHERE fn = 'Tom'"
        cql.execute(f"UPDATE {table} SET tags = tags + ['a', 'b', 'c', 'b'] {where_row}")
        cql.execute(f"UPDATE {table} SET static_tags = static_tags + ['a', 'b', 'c', 'b'] {where_partition}")
        # Check removing elements from the regular list doesn't touch the
        # static list
        cql.execute(f"UPDATE {table} SET tags = tags - ['b'] {where_row}")
        assert list(cql.execute(f'SELECT tags FROM {table} {where_row}')) == [(['a', 'c'],)]
        assert list(cql.execute(f'SELECT static_tags FROM {table} {where_partition}')) == [(['a', 'b', 'c', 'b'],)]
        # Check removing elements from the static list
        cql.execute(f"UPDATE {table} SET static_tags = static_tags - ['b'] {where_partition}")
        assert list(cql.execute(f'SELECT static_tags FROM {table} {where_partition}')) == [(['a', 'c'],)]
        # Check setting an element of the static list by its position
        cql.execute(f"UPDATE {table} SET static_tags[1] = 'b' {where_partition}")
        assert list(cql.execute(f'SELECT static_tags FROM {table} {where_partition}')) == [(['a', 'b'],)]

# Test changing the type of a column with ALTER TABLE ALTER ... TYPE, from
# text to blob, which is a compatible type (every text value is a valid
# blob), for a regular column, a partition-key column, a clustering-key
# column, and a set's elements. The existing data is not rewritten, but is
# now read as the new type: the text's UTF-8 bytes. The test uses a non-ASCII
# character, so the UTF-8 encoding is visibly different from the string.
# Cassandra dropped support for changing a column's type in versions 3.0.11
# and 3.10 (CASSANDRA-12443) - this test passes on Cassandra 3.0.10 and 3.9,
# but later versions fail with "Altering of types is not allowed" or, since
# 4.0, "Altering column types is no longer supported". Scylla still supports
# changing a column to a compatible type, as documented in docs/cql/ddl.rst,
# so this is a scylla_only test.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_update_type, which originally came
# from Cassandra's old Python cql_tests.py (update_type_test, for
# CASSANDRA-4041).
def test_update_type(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'k text, c text, s set<text>, v text, PRIMARY KEY (k, c)') as table:
        phi = '\u0278'
        phi_utf8 = phi.encode()
        cql.execute(f"INSERT INTO {table} (k, c, v, s) VALUES ('{phi}', '{phi}', '{phi}', {{'{phi}'}})")
        assert list(cql.execute(f'SELECT * FROM {table}')) == [(phi, phi, {phi}, phi)]
        # Check changing the type of a regular column
        cql.execute(f'ALTER TABLE {table} ALTER v TYPE blob')
        assert list(cql.execute(f'SELECT * FROM {table}')) == [(phi, phi, {phi}, phi_utf8)]
        # Check changing the type of the partition key
        cql.execute(f'ALTER TABLE {table} ALTER k TYPE blob')
        assert list(cql.execute(f'SELECT * FROM {table}')) == [(phi_utf8, phi, {phi}, phi_utf8)]
        # Check changing the type of the clustering key
        cql.execute(f'ALTER TABLE {table} ALTER c TYPE blob')
        assert list(cql.execute(f'SELECT * FROM {table}')) == [(phi_utf8, phi_utf8, {phi}, phi_utf8)]
        # Check changing the type of a set's elements
        cql.execute(f'ALTER TABLE {table} ALTER s TYPE set<blob>')
        assert list(cql.execute(f'SELECT * FROM {table}')) == [(phi_utf8, phi_utf8, {phi_utf8}, phi_utf8)]

# Test LIMIT on a full-table scan, with one row in each partition: for every
# LIMIT, the result must be the first rows of the full scan, which returns
# partitions in token order. This is checked both for a table with a
# clustering key and for one without. With the Murmur3 partitioner, the
# default in both Scylla and Cassandra, the token order of partitions 1 to 4
# is 1, 2, 4, 3. Reproduces CASSANDRA-4579, where LIMIT returned fewer rows
# than it should have.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_limit_bugs, which originally came
# from Cassandra's old Python cql_tests.py (limit_bugs_test). Cassandra's
# Java version of that test was not translated in cassandra_tests, because
# it used the ByteOrderedPartitioner.
@pytest.mark.parametrize('schema,columns', [
    ('a int, b int, c int, d int, e int, PRIMARY KEY (a, b)', 'a, b, c, d, e'),
    ('a int PRIMARY KEY, b int, c int', 'a, b, c')], ids=['clustering', 'no_clustering'])
def test_limit_bugs(cql, test_keyspace, schema, columns):
    with new_test_table(cql, test_keyspace, schema) as table:
        n = len(columns.split(', '))
        for i in [1, 2, 3, 4]:
            cql.execute(f'INSERT INTO {table} ({columns}) VALUES ({", ".join([str(i)] * n)})')
        all_rows = [tuple([i] * n) for i in [1, 2, 4, 3]]
        assert list(cql.execute(f'SELECT * FROM {table}')) == all_rows
        for limit in range(1, 6):
            assert list(cql.execute(f'SELECT * FROM {table} LIMIT {limit}')) == all_rows[:limit]

# Test that WRITETIME() and TTL() cannot be selected on an entire unfrozen
# set column. Like an unfrozen map, an unfrozen set is stored as multiple
# cells which may each have a different timestamp and TTL, so there is no
# single value to return. Cassandra allows this, and returns a list of
# timestamps, which we consider a Cassandra bug - see the explanation in
# test_select_collection_element.py::test_writetime_ttl_whole_collection_forbidden,
# which checks the same for a map column.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_collection_function, which
# originally came from Cassandra's old Python cql_tests.py
# (collection_function_test).
def test_collection_function(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, l set<int>') as table:
        with pytest.raises(InvalidRequest):
            cql.execute(f'SELECT ttl(l) FROM {table} WHERE k = 0')
        with pytest.raises(InvalidRequest):
            cql.execute(f'SELECT writetime(l) FROM {table} WHERE k = 0')

# Test a SELECT with an IN restriction on the clustering key with many
# values, most of which match no row. Reproduces CASSANDRA-8410.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_large_clustering_in, which
# originally came from Cassandra's old Python cql_tests.py
# (large_clustering_in_test). That test inserted 1000 random rows; here we
# insert every 7th row, so the test is deterministic. Cassandra's Java
# version of this test, translated in cassandra_tests as
# testLargeClusteringINValues, uses 10,000 IN values, which is more than
# Scylla's max_clustering_key_restrictions_per_query allows, so it is
# marked xfail (issue #10577); this test uses only 100 values.
def test_large_clustering_in(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int, c int, v int, PRIMARY KEY (k, c)') as table:
        insert = cql.prepare(f'INSERT INTO {table} (k, c, v) VALUES (?, ?, ?)')
        select = cql.prepare(f'SELECT * FROM {table} WHERE k = ? AND c IN ?')
        in_values = list(range(100))
        # Check fetching one existing row and 99 nonexistent ones
        cql.execute(insert, [0, 0, 0])
        assert list(cql.execute(select, [0, in_values])) == [(0, 0, 0)]
        # Insert more rows, some inside the range of the IN values and many
        # outside it, and check that exactly the ones inside it are returned
        clustering_values = list(range(0, 1000, 7))
        execute_concurrent_with_args(cql, insert, [(0, c, c) for c in clustering_values], concurrency=100, raise_on_first_error=True)
        assert list(cql.execute(select, [0, in_values])) == [(0, c, c) for c in clustering_values if c in in_values]

# Test the range of the tinyint type: the smallest and largest values,
# -128 and 127, can be written and read back, and one beyond each end is
# rejected.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_cql_tinyint_type.
def test_cql_tinyint_type(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 't tinyint PRIMARY KEY') as table:
        for v in [-129, 128]:
            with pytest.raises(InvalidRequest):
                cql.execute(f'INSERT INTO {table} (t) VALUES ({v})')
        for v in [-128, 127]:
            cql.execute(f'INSERT INTO {table} (t) VALUES ({v})')
        assert {r.t for r in cql.execute(f'SELECT * FROM {table}')} == {-128, 127}

# Test the range of the smallint type: the smallest and largest values,
# -32768 and 32767, can be written and read back, and one beyond each end
# is rejected.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_cql_smallint_type.
def test_cql_smallint_type(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 't smallint PRIMARY KEY') as table:
        for v in [-32769, 32768]:
            with pytest.raises(InvalidRequest):
                cql.execute(f'INSERT INTO {table} (t) VALUES ({v})')
        for v in [-32768, 32767]:
            cql.execute(f'INSERT INTO {table} (t) VALUES ({v})')
        assert {r.t for r in cql.execute(f'SELECT * FROM {table}')} == {-32768, 32767}

# Test the range of the date type at its low end, and the epoch: a date is
# stored as a 32-bit number of days, so the earliest date is -5877641-06-23,
# and the day before it is rejected. The Python driver can't represent dates
# outside Python's datetime.date range, so it returns such a date as the
# number of days since the epoch - for the earliest date, -2^31.
# The latest date, 5881580-07-11 (2^31-1 days after the epoch), is checked
# in the following tests, because Scylla and Cassandra disagree on how to
# write it.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_cql_date_type.
def test_cql_date_type(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 't date PRIMARY KEY') as table:
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {table} (t) VALUES ('-5877641-06-22')")
        for v in ['-5877641-06-23', '1970-01-01']:
            cql.execute(f"INSERT INTO {table} (t) VALUES ('{v}')")
        assert {str(r.t) for r in cql.execute(f'SELECT * FROM {table}')} == {'-2147483648', '1970-01-01'}

# Test that a date literal with a year beyond 9999 may be written with a "+"
# sign, as ISO 8601 requires, up to the latest date, +5881580-07-11. This
# is how Cassandra 4.0 and above require such dates to be written, and also
# how they print them. Cassandra 3.x accepted both this form and the form
# without a sign.
# Reproduces SCYLLADB-5919, which this test helped discover.
@pytest.mark.xfail(reason='SCYLLADB-5919')
def test_date_literal_plus_sign(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 't date PRIMARY KEY') as table:
        for v in ['+10000-01-01', '+5881580-07-11']:
            cql.execute(f"INSERT INTO {table} (t) VALUES ('{v}')")
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {table} (t) VALUES ('+5881580-07-12')")
        # The Python driver returns dates beyond the year 9999 as the number
        # of days since the epoch
        assert {str(r.t) for r in cql.execute(f'SELECT * FROM {table}')} == {'2932897', '2147483647'}

# Test that a date literal with a year beyond 9999 may also be written
# without a sign, up to the latest date, 5881580-07-11, and that one day
# beyond it is rejected. Cassandra 3.x accepted this form, and Scylla
# implemented it in 2016, but Cassandra 4.0 stopped accepting it when it
# replaced its date parser in CASSANDRA-15257 - Cassandra 4.0 and above
# require a "+" sign for such years (see test_date_literal_plus_sign above).
# Scylla keeps accepting this form, for backward compatibility, so this
# test is scylla_only.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_cql_date_type.
def test_date_literal_large_year_no_sign(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 't date PRIMARY KEY') as table:
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {table} (t) VALUES ('5881580-07-12')")
        for v in ['10000-01-01', '5881580-07-11']:
            cql.execute(f"INSERT INTO {table} (t) VALUES ('{v}')")
        # The Python driver returns dates beyond the year 9999 as the number
        # of days since the epoch
        assert {str(r.t) for r in cql.execute(f'SELECT * FROM {table}')} == {'2932897', '2147483647'}

# Test parsing of time literals: seconds are required, and up to nine
# digits of fractions of a second (nanoseconds) are allowed, but not ten.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_cql_time_type.
def test_cql_time_type(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 't time PRIMARY KEY') as table:
        for v in ['14:53', '14:53:12.1234567890']:
            with pytest.raises(InvalidRequest):
                cql.execute(f"INSERT INTO {table} (t) VALUES ('{v}')")
        for v in ['14:53:12', '14:53:12.1234', '14:53:12.123456789']:
            cql.execute(f"INSERT INTO {table} (t) VALUES ('{v}')")
        assert {str(r.t) for r in cql.execute(f'SELECT * FROM {table}')} == {'14:53:12.000000000', '14:53:12.123400000', '14:53:12.123456789'}

# Test that the deprecated functions dateOf() and unixTimestampOf() return
# null when their argument, a timeuuid column, is null. The same check for
# their replacements, toTimestamp() and toUnixTimestamp(), is in
# cassandra_tests/validation/miscellaneous/overflow_test.py::testFunctionWithNull,
# but Scylla implements dateOf() and unixTimestampOf() separately, so they
# need their own test.
# Cassandra removed these deprecated functions in version 5.0 (this test
# passes on Cassandra 4.0), but Scylla still supports them, for backward
# compatibility, so this test is scylla_only.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_function_with_null, which
# originally came from Cassandra's old Python cql_tests.py
# (function_with_null_test). The dtest checked only dateOf(), and this test
# also checks unixTimestampOf().
def test_function_with_null(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, t timeuuid') as table:
        cql.execute(f'INSERT INTO {table} (k) VALUES (0)')
        assert list(cql.execute(f'SELECT dateOf(t), unixTimestampOf(t) FROM {table} WHERE k = 0')) == [(None, None)]

# Test that SELECT COUNT(*) on a whole table counts all the rows, even when
# the table has more rows than one page. Although COUNT(*) returns a single
# row, the aggregation reads the table internally in pages, and when the
# request has a page size (fetch_size), that page size is used for this
# internal paging. The test checks counts just below, at, and just above
# the page size, and of one and a half pages.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_large_count, which originally came
# from Cassandra's old Python cql_tests.py (large_count_test). The dtest
# used a page size of 10,000 and wrote 15,000 rows, which was slow. This test
# checks the same page boundaries with a page size of 10 and 15 rows.
def test_count_across_pages(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, v int') as table:
        insert = cql.prepare(f'INSERT INTO {table} (k) VALUES (?)')
        count = SimpleStatement(f'SELECT COUNT(*) FROM {table}', fetch_size=10)
        k = 0
        for n in [9, 10, 11, 15]:
            while k < n:
                k += 1
                cql.execute(insert, [k])
            assert list(cql.execute(count)) == [(n,)]

# Test SELECT DISTINCT of the partition key and static columns, with paging,
# on tables with several rows in each partition. Every partition should be
# returned exactly once, with its static columns' values, whatever the page
# size. The test checks a whole-table scan, a SELECT with "k IN (...)", which
# returns the partitions in the order of the IN list, and a LIMIT. The second
# table has 25 rows in each partition, so some of the page sizes are just
# below, at, and just above the number of rows in a partition.
# CASSANDRA-8108 and CASSANDRA-8087 were bugs in paging such DISTINCT queries
# on static columns.
# Translated from the paging parts of scylla-dtest's
# cql_additional_tests.py::TestCQL::test_static_columns_with_distinct, which
# originally came from Cassandra's old Python cql_tests.py
# (static_columns_with_distinct_test). The rest of that test is in
# cassandra_tests/validation/entities/static_columns_test.py::testStaticColumnsWithDistinct,
# but that translation does not page. The dtest paged the first table only
# with page size 7, and this test tries the second table's page sizes on both
# tables, except page size 1000, which on these small tables is no different
# from the default page size.
def test_static_distinct_paging(cql, test_keyspace):
    fetch_sizes = [None, 2, 5, 7, 10, 24, 25, 26]
    keys = ', '.join(str(i) for i in range(10))
    with new_test_table(cql, test_keyspace, 'k int, p int, s int static, PRIMARY KEY (k, p)') as table:
        insert = cql.prepare(f'INSERT INTO {table} (k, p, s) VALUES (?, ?, ?)')
        execute_concurrent_with_args(cql, insert, [(i, j, i) for i in range(10) for j in range(10)])
        expected = [(i, i) for i in range(10)]
        for fetch_size in fetch_sizes:
            stmt = SimpleStatement(f'SELECT DISTINCT k, s FROM {table}', fetch_size=fetch_size)
            assert sorted(cql.execute(stmt)) == expected
            stmt = SimpleStatement(f'SELECT DISTINCT k, s FROM {table} WHERE k IN ({keys})', fetch_size=fetch_size)
            assert list(cql.execute(stmt)) == expected
    with new_test_table(cql, test_keyspace, 'k int, c1 int, c2 int, s1 int static, s2 int static, PRIMARY KEY (k, c1, c2)') as table:
        insert = cql.prepare(f'INSERT INTO {table} (k, c1, c2, s1, s2) VALUES (?, ?, ?, ?, ?)')
        execute_concurrent_with_args(cql, insert, [(i, j, k, i, i + 1) for i in range(10) for j in range(5) for k in range(5)])
        expected_s1 = [(i, i) for i in range(10)]
        expected_s2 = [(i, i + 1) for i in range(10)]
        def select(columns, rest, fetch_size):
            return list(cql.execute(SimpleStatement(f'SELECT DISTINCT {columns} FROM {table} {rest}', fetch_size=fetch_size)))
        for fetch_size in fetch_sizes:
            assert sorted(select('k, s1', '', fetch_size)) == expected_s1
            assert sorted(select('k, s2', '', fetch_size)) == expected_s2
            assert sorted(select('k, s1', 'LIMIT 10', fetch_size)) == expected_s1
            assert select('k, s1', f'WHERE k IN ({keys})', fetch_size) == expected_s1
            assert select('k, s2', f'WHERE k IN ({keys})', fetch_size) == expected_s2
            assert sorted(select('k, s1', f'WHERE k IN ({keys}) LIMIT 10', fetch_size)) == expected_s1
