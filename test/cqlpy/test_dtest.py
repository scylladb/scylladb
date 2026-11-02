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

import math
import re
import sys
import time
from contextlib import contextmanager, nullcontext
from datetime import datetime

import pytest
import requests
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.protocol import AlreadyExists, InvalidRequest
from cassandra.query import SimpleStatement, UNSET_VALUE

from . import nodetool
from .util import ScyllaMetrics, config_value_context, new_materialized_view, new_test_keyspace, new_test_table, new_type, new_user, unique_name


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

# Test slicing - restricting the clustering columns to a range - in a
# partition of a COMPACT STORAGE table which also has rows with only a
# prefix of the clustering key. In a COMPACT STORAGE table with two
# clustering columns, an INSERT may leave out the last clustering column c2,
# and such a row sorts before all the rows with the same c1 and a non-null
# c2. The test checks single-column and multi-column slices on c1 and c2,
# including empty and contradictory ranges, and that a slice on c2 includes
# the prefix row only when it has no lower bound on c2.
# Reproduces issue #1669, where Scylla returned no rows for some slices with
# only a lower or only an upper bound on the second clustering column, such
# as "c1 = 0 AND c2 > 1".
# Translated from scylla-dtest's cql_additional_tests.py::TestCQL::test_slicing.
def test_slicing_with_clustering_prefix_rows(cql, test_keyspace, compact_storage):
    with new_test_table(cql, test_keyspace, 'k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)', 'WITH COMPACT STORAGE') as table:
        for i in range(3):
            cql.execute(f'INSERT INTO {table} (k, c1, v) VALUES (0, {i}, 1)')
            for j in range(3):
                cql.execute(f'INSERT INTO {table} (k, c1, c2, v) VALUES (0, {i}, {j}, 0)')
        def select(where):
            return list(cql.execute(f'SELECT c1, c2, v FROM {table} WHERE k = 0 {where}'))
        all_rows = [(i, j, 0) if j is not None else (i, None, 1) for i in range(3) for j in [None, 0, 1, 2]]
        assert select('') == all_rows
        assert select('AND c1 >= 0') == all_rows
        assert select('AND c1 < 1') == all_rows[:4]
        assert select('AND c1 < 1 AND c1 > 1') == []

        assert select('AND c1 = 0 AND c2 >= 1 AND c2 < 2') == [(0, 1, 0)]
        assert select('AND c1 = 0 AND c2 > 1') == [(0, 2, 0)]
        assert select('AND c1 = 0 AND c2 >= 1') == [(0, 1, 0), (0, 2, 0)]
        assert select('AND c1 = 0 AND c2 < 2') == [(0, None, 1), (0, 0, 0), (0, 1, 0)]
        assert select('AND c1 = 0 AND c2 <= 2') == [(0, None, 1), (0, 0, 0), (0, 1, 0), (0, 2, 0)]
        assert select('AND c1 = 0 AND c2 < 1 AND c2 > 2') == []
        assert select('AND c1 = 0 AND c2 < 1 AND c2 >= 2') == []
        assert select('AND c1 = 0 AND c2 < 2 AND c2 > 1') == []
        assert select('AND c1 = 0 AND c2 < 3 AND c2 > 1') == [(0, 2, 0)]

        assert select('AND (c1, c2) < (0, 1) AND (c1, c2) > (0, 2)') == []
        assert select('AND (c1, c2) >= (0, 1) AND (c1) < (0)') == []
        assert select('AND (c1, c2) >= (0, 1) AND (c1) <= (0)') == [(0, 1, 0), (0, 2, 0)]
        assert select('AND (c1, c2) < (0, 1) AND (c1, c2) >= (0, 2)') == []
        assert select('AND (c1, c2) < (0, 2) AND (c1, c2) > (0, 1)') == []
        assert select('AND (c1, c2) < (0, 3) AND (c1, c2) > (0, 1)') == [(0, 2, 0)]

# Test that in an LWT condition, a list element with a null or negative
# index, such as l[null] or l[-2], is null - like an index beyond the end of
# the list - instead of being an error. Scylla deliberately allows such
# indexes (since commit 8e972b52c5), for consistency with subscripts outside
# LWT conditions, while Cassandra rejects them with "Invalid null value for
# list element access" or "Invalid negative list index", so this test is
# scylla_only. The Cassandra versions of these checks are commented out in
# cassandra_tests/validation/operations/insert_update_if_condition_collections_test.py.
# Translated from the Scylla-specific checks in scylla-dtest's
# cql_additional_tests.py::TestCQL tests test_list_item_conditional and
# test_expanded_list_item_conditional; the rest of these tests are in that
# cassandra_tests file. The dtests didn't check "l[-2] = null", which this
# test adds.
@pytest.mark.parametrize('frozen', [False, True], ids=['unfrozen', 'frozen'])
def test_lwt_list_null_or_negative_index(cql, test_keyspace, scylla_only, frozen):
    list_type = 'frozen<list<text>>' if frozen else 'list<text>'
    with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, l {list_type}') as table:
        l = ['foo', 'bar', 'foobar']
        cql.execute(f"INSERT INTO {table} (k, l) VALUES (0, ['foo', 'bar', 'foobar'])")
        # Check that l[null] and l[-2] are not equal to 'foobar', so the
        # conditional delete does not apply
        for index in ['null', '-2']:
            assert list(cql.execute(f"DELETE FROM {table} WHERE k = 0 IF l[{index}] = 'foobar'")) == [(False, l)]
            assert list(cql.execute(f'SELECT * FROM {table}')) == [(0, l)]
        # Check that l[null] and l[-2] are null, so the conditional update
        # applies
        for index in ['null', '-2']:
            assert list(cql.execute(f"UPDATE {table} SET l = ['foo', 'bar', 'foobar'] WHERE k = 0 IF l[{index}] = null")) == [(True, l)]
            assert list(cql.execute(f'SELECT * FROM {table}')) == [(0, l)]

# Test that in an LWT condition, a map element with a null key, m[null], is
# null - like the element of a key missing from the map - instead of being an
# error. So "m[null] = 'foo'" is false, and "m[null] = null" is true. Scylla
# deliberately allows a null subscript (and since commit 433b778a4d, evaluates
# "m[null] = null" as true instead of short-circuiting the whole condition to
# false), while Cassandra rejects it with "Invalid null value for map element
# access", so this test is scylla_only. The Cassandra versions of these checks
# are commented out in
# cassandra_tests/validation/operations/insert_update_if_condition_collections_test.py.
# Translated from the Scylla-specific checks in scylla-dtest's
# cql_additional_tests.py::TestCQL tests test_map_item_conditional and
# test_expanded_map_item_conditional; the rest of these tests are in that
# cassandra_tests file.
@pytest.mark.parametrize('frozen', [False, True], ids=['unfrozen', 'frozen'])
def test_lwt_map_null_key(cql, test_keyspace, scylla_only, frozen):
    map_type = 'frozen<map<text, text>>' if frozen else 'map<text, text>'
    with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, m {map_type}') as table:
        m = {'foo': 'bar'}
        cql.execute(f"INSERT INTO {table} (k, m) VALUES (0, {{'foo': 'bar'}})")
        # Check that m[null] is not equal to 'foo', so the conditional delete
        # does not apply
        assert list(cql.execute(f"DELETE FROM {table} WHERE k = 0 IF m[null] = 'foo'")) == [(False, m)]
        assert list(cql.execute(f'SELECT * FROM {table}')) == [(0, m)]
        # Check that m[null] is null, so the conditional update applies
        assert list(cql.execute(f"UPDATE {table} SET m = {{'foo': 'bar'}} WHERE k = 0 IF m[null] = null")) == [(True, m)]
        assert list(cql.execute(f'SELECT * FROM {table}')) == [(0, m)]

# Test that in an LWT condition, a list value may contain a null element, as
# in "l = [null]" - which is never true, because a stored list cannot contain
# a null. Scylla deliberately allows lists with nulls as transient values
# (pull request #12411), because a list value such as the one bound to
# "IF v IN ?" may need to contain a null, while Cassandra rejects such a
# condition. So this test is scylla_only. The Cassandra version of this check
# is commented out in testWholeList in
# cassandra_tests/validation/operations/insert_update_if_condition_collections_test.py.
# The test also checks that a list with a null element still cannot be stored.
# Translated from the Scylla-specific check in scylla-dtest's
# cql_additional_tests.py::TestCQL::test_whole_list_conditional; the rest of
# that test is testWholeList in that cassandra_tests file. The check that a
# list with a null cannot be stored is new.
@pytest.mark.parametrize('frozen', [False, True], ids=['unfrozen', 'frozen'])
def test_lwt_list_with_null_element(cql, test_keyspace, scylla_only, frozen):
    list_type = 'frozen<list<text>>' if frozen else 'list<text>'
    with new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, l {list_type}') as table:
        l = ['foo', 'bar', 'foobar']
        cql.execute(f"INSERT INTO {table} (k, l) VALUES (0, ['foo', 'bar', 'foobar'])")
        # Check that "l = [null]" is allowed, but false
        assert list(cql.execute(f"UPDATE {table} SET l = ['foo', 'bar', 'foobar'] WHERE k = 0 IF l = [null]")) == [(False, l)]
        assert list(cql.execute(f'SELECT * FROM {table}')) == [(0, l)]
        # Check that a list with a null element cannot be stored, even when
        # the condition, which allows nulls, is true
        with pytest.raises(InvalidRequest, match='NULL'):
            cql.execute(f"UPDATE {table} SET l = ['foo', null] WHERE k = 0 IF l != [null]")
        assert list(cql.execute(f'SELECT * FROM {table}')) == [(0, l)]

# Test LIMIT on a slice of the clustering key - in one partition and in
# several partitions selected with IN - in a COMPACT STORAGE table whose only
# columns are the partition key and one clustering column, so it has no
# regular columns. The slices' lower bounds exclude some rows (v > 0) or none
# (v > -1), and their upper bound (v <= 4) is beyond the last row of each
# partition. LIMIT counts rows across partitions, so a LIMIT smaller than the
# number of matching rows stops in the middle of a partition.
# The original test was a regression test for CASSANDRA-7052.
# Translated from scylla-dtest's cql_additional_tests.py::TestCQL::limit_compact_table
# (which never ran, because its name does not start with "test"), which
# originally came from Cassandra's old Python cql_tests.py
# (limit_compact_table_test). That test also expected the filtering query
# "v > 1 AND v <= 3 ... ALLOW FILTERING" to be rejected (CASSANDRA-7059), but
# both Scylla and Cassandra now allow it, so this test checks its results
# instead.
def test_limit_compact_table(cql, test_keyspace, compact_storage):
    with new_test_table(cql, test_keyspace, 'k int, v int, PRIMARY KEY (k, v)', 'WITH COMPACT STORAGE') as table:
        insert = cql.prepare(f'INSERT INTO {table} (k, v) VALUES (?, ?)')
        for i in range(4):
            for j in range(4):
                cql.execute(insert, [i, j])
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND v > 0 AND v <= 4 LIMIT 2')) == [(1,), (2,)]
        assert list(cql.execute(f'SELECT v FROM {table} WHERE k = 0 AND v > -1 AND v <= 4 LIMIT 2')) == [(0,), (1,)]
        assert list(cql.execute(f'SELECT * FROM {table} WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 2')) == [(0, 1), (0, 2)]
        assert list(cql.execute(f'SELECT * FROM {table} WHERE k IN (0, 1, 2) AND v > -1 AND v <= 4 LIMIT 2')) == [(0, 0), (0, 1)]
        assert list(cql.execute(f'SELECT * FROM {table} WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 6')) == [(0, 1), (0, 2), (0, 3), (1, 1), (1, 2), (1, 3)]
        # Without a partition-key restriction, rows are returned in token
        # order, so with LIMIT we expect the first rows of the same query
        # without a LIMIT.
        all_rows = list(cql.execute(f'SELECT * FROM {table} WHERE v > 1 AND v <= 3 ALLOW FILTERING'))
        assert sorted(all_rows) == [(i, j) for i in range(4) for j in [2, 3]]
        assert list(cql.execute(f'SELECT * FROM {table} WHERE v > 1 AND v <= 3 LIMIT 6 ALLOW FILTERING')) == all_rows[:6]

# Test that ALTER TABLE ADD can add several columns at once, given as a
# parenthesized list, and that the new columns can then be written and read.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_alter_with_multiple_columns. The
# dtest only wrote to the new columns, and this test also reads them back.
def test_alter_table_add_multiple_columns(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY') as table:
        cql.execute(f'ALTER TABLE {table} ADD (c text, d int)')
        cql.execute(f"INSERT INTO {table} (k, c, d) VALUES (1, 'hello', 100)")
        assert list(cql.execute(f'SELECT k, c, d FROM {table}')) == [(1, 'hello', 100)]

# Test that INSERT statements containing non-ASCII string literals fail with
# an ordinary InvalidRequest error when they are invalid: when the table
# doesn't exist, when a column doesn't exist, and when the non-ASCII string is
# inserted into an "ascii" column.
# CASSANDRA-8101 was about such errors being reported as protocol errors.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_invalid_string_literals, which
# originally came from Cassandra's old Python cql_tests.py
# (invalid_string_literals_test).
def test_invalid_string_literals(cql, test_keyspace):
    s = '\u038e\u0394\u03b4\u03e0'
    # Check that inserting into a nonexistent table fails with InvalidRequest
    with pytest.raises(InvalidRequest):
        cql.execute(f"INSERT INTO {test_keyspace}.{unique_name()} (k, a) VALUES (0, '{s}')")
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, a ascii, b text') as table:
        # Check that inserting into a nonexistent column fails with
        # InvalidRequest
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {table} (k, c) VALUES (0, '{s}')")
        # Check that inserting a non-ASCII string into an ascii column fails
        # with InvalidRequest
        with pytest.raises(InvalidRequest, match='Invalid ASCII character|Value not compatible'):
            cql.execute(f"INSERT INTO {table} (k, a) VALUES (0, '\xe0\x80\x80')")

# Test that a full-table scan returns the partitions in the token order of
# their keys. With the Murmur3 partitioner, the default in both Scylla and
# Cassandra, this order of text keys is not their alphabetical order. The
# test checks the exact order of the rows, and that their tokens increase.
# Translated from scylla-dtest's cql_additional_tests.py::TestCQL::test_bop_order.
# Despite its name, which refers to the byte-ordered partitioner ("BOP"), that
# test used the default Murmur3 partitioner, and checked the Murmur3 order.
# This test also checks that the tokens increase.
def test_scan_text_keys_in_token_order(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k text PRIMARY KEY, v int') as table:
        insert = cql.prepare(f'INSERT INTO {table} (k, v) VALUES (?, ?)')
        for v, k in enumerate(['c1', 'a1', 'b1', 'z', 'g1', '1', '1000', '2']):
            cql.execute(insert, [k, v])
        rows = list(cql.execute(f'SELECT token(k), v FROM {table}'))
        assert [row[1] for row in rows] == [3, 0, 2, 6, 4, 7, 1, 5]
        tokens = [row[0] for row in rows]
        assert tokens == sorted(tokens)

# Test that a SELECT with ALLOW FILTERING can restrict a materialized view's
# clustering column without restricting the view's partition key, and that
# COUNT(*) on the base table and on the view, with and without filtering,
# counts the right rows.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_filtering_with_mv.
def test_view_filtering_and_count(cql, test_keyspace):
    schema = 'username text, password text, gender text, state text, birth_year bigint, PRIMARY KEY (username)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with new_materialized_view(cql, table, '*', 'state, username', 'state IS NOT NULL AND username IS NOT NULL') as view:
            insert = cql.prepare(f'INSERT INTO {table} (username, password, gender, state, birth_year) VALUES (?, ?, ?, ?, ?)')
            cql.execute(insert, ['user1', 'ch@ngem3a', 'f', 'TX', 1968])
            cql.execute(insert, ['user2', 'ch@ngem3b', 'm', 'CA', 1971])
            cql.execute(insert, ['user3', 'ch@ngem3c', 'f', 'FL', 1978])
            cql.execute(insert, ['user4', 'ch@ngem3d', 'm', 'TX', 1974])
            assert list(cql.execute(f"SELECT count(*) FROM {table} WHERE username = 'user1'")) == [(1,)]
            assert list(cql.execute(f"SELECT count(*) FROM {view} WHERE username = 'user1' ALLOW FILTERING")) == [(1,)]
            assert list(cql.execute(f"SELECT count(*) FROM {view} WHERE state = 'TX' AND username = 'user1'")) == [(1,)]
            assert list(cql.execute(f"SELECT count(*) FROM {view} WHERE state = 'TX'")) == [(2,)]
            assert list(cql.execute(f"SELECT count(*) FROM {view}")) == [(4,)]

# Test filtering with ALLOW FILTERING on the components of a composite
# partition key: an equality or a range on just one component, ranges on
# both, and restrictions on the partition key together with the clustering
# key, with SELECT * and with COUNT(*). The test also checks that such
# queries are rejected without ALLOW FILTERING.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestCQL::test_partition_key_allow_filtering
# (CASSANDRA-11031).
def test_filtering_on_partition_key_components(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k1 int, k2 int, ck1 int, v int, PRIMARY KEY ((k1, k2), ck1)') as table:
        insert = cql.prepare(f'INSERT INTO {table} (k1, k2, ck1, v) VALUES (?, ?, ?, 0)')
        for k1 in [0, 1]:
            for k2 in [0, 1]:
                for ck1 in range(4):
                    cql.execute(insert, [k1, k2, ck1])
        def select(where):
            return sorted(cql.execute(f'SELECT * FROM {table} WHERE {where}'))
        def rows(k1s, k2s, ck1s=range(4)):
            return [(k1, k2, ck1, 0) for k1 in k1s for k2 in k2s for ck1 in ck1s]
        assert select('k1 = 0 ALLOW FILTERING') == rows([0], [0, 1])
        assert select('k1 <= 1 AND k2 >= 1 ALLOW FILTERING') == rows([0, 1], [1])
        assert select('k1 = 2 ALLOW FILTERING') == []
        assert select('k1 <= 0 AND k2 > 1 ALLOW FILTERING') == []
        assert select('k2 <= 0 ALLOW FILTERING') == rows([0, 1], [0])
        assert select('k1 <= 0 AND k2 = 0 ALLOW FILTERING') == rows([0], [0])
        assert select('k2 = 1 ALLOW FILTERING') == rows([0, 1], [1])
        assert select('k2 = 2 ALLOW FILTERING') == []
        assert select('k1 = 0 AND ck1 = 0 ALLOW FILTERING') == rows([0], [0, 1], [0])
        assert select('k1 = 0 AND k2 = 1 AND ck1 = 0') == rows([0], [1], [0])
        for k2, count in [(0, 8), (1, 8), (2, 0)]:
            assert list(cql.execute(f'SELECT count(*) FROM {table} WHERE k2 = {k2} ALLOW FILTERING')) == [(count,)]
        # Check that these queries need ALLOW FILTERING
        for where in ['k1 = 0', 'k1 = 0 AND k2 > 0', 'k1 >= 0 AND k2 IN (0, 1, 2)', 'k2 > 0']:
            with pytest.raises(InvalidRequest, match=re.compile('allow filtering', re.IGNORECASE)):
                cql.execute(f'SELECT * FROM {table} WHERE {where}')

# Test UPDATE statements, with bound values, that set some or all of a row's
# regular columns to null. An UPDATE doesn't create a row marker, so an UPDATE
# that sets all the regular columns to null doesn't create a row, while one
# that sets only some of them to null does.
# Translated from scylla-dtest's
# cql_additional_tests.py::TestsCQLAdditional::test_simple_null_value. The
# dtest only checked the number of rows, and this test also checks their
# contents.
def test_update_with_null_values(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'key text PRIMARY KEY, val1 text, val2 float') as table:
        update = cql.prepare(f'UPDATE {table} SET val1 = ?, val2 = ? WHERE key = ?')
        cql.execute(update, ['ccc', 1.0, 'java1'])
        cql.execute(update, [None, 1.0, 'java2'])
        cql.execute(update, [None, None, 'java3'])
        cql.execute(update, ['ddd', None, 'java4'])
        assert sorted(cql.execute(f'SELECT * FROM {table}')) == [
            ('java1', 'ccc', 1.0), ('java2', None, 1.0), ('java4', 'ddd', None)]

# Test GRANT, REVOKE and LIST PERMISSIONS on a few kinds of resources:
# granting a permission on all keyspaces, revoking the same permission on one
# table - which was never granted on that table, so it does nothing - and
# listing the permissions on a table, which include those granted on all
# keyspaces.
# Translated from scylla-dtest's cql_additional_tests.py::TestsCQLAdditional
# tests test_grant, test_revoke and test_list, which were written when
# Scylla did not implement these statements yet, so they also accepted a
# "Not implemented" error, and didn't check what LIST returns. This test
# checks that the statements succeed, and what LIST returns.
def test_grant_revoke_list_permissions(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY') as table, new_user(cql) as role:
        cql.execute(f'GRANT SELECT ON ALL KEYSPACES TO {role}')
        try:
            cql.execute(f'REVOKE SELECT ON {table} FROM {role}')
            perms = [(r.role, r.resource, r.permission) for r in cql.execute(f'LIST ALL PERMISSIONS ON {table}') if r.role == role]
            assert perms == [(role, '<all keyspaces>', 'SELECT')]
        finally:
            cql.execute(f'REVOKE SELECT ON ALL KEYSPACES FROM {role}')

# Test timestamps given as strings over a wide range of years - from 1901,
# before the UNIX epoch, up to 9999, the largest four-digit year - in the
# clustering key, in both clustering orders: that they are read back
# correctly, and that SELECT with and without LIMIT returns the right number
# of rows.
# Translated from scylla-dtest's cql_additional_tests.py::TestsCQLAdditional
# tests test_limit_date_value_out_of_range (issue #1694, a client-side
# "OverflowError: date value out of range" in cqlsh) and
# test_limit_date_value_out_of_range_upper_limit. Those tests also ran the
# same SELECTs in cqlsh, which test/cqlpy doesn't run. They wrote 100 and
# 8000 rows, one per year; this test writes the same 100 years before 2000,
# but only every 100th year after it (and 9999), to be fast.
@pytest.mark.parametrize('order', ['ASC', 'DESC'])
def test_timestamp_year_range(cql, test_keyspace, order):
    schema = 'test_id int, partition_key text, time timestamp, value double, PRIMARY KEY ((test_id, partition_key), time)'
    with new_test_table(cql, test_keyspace, schema, f'WITH CLUSTERING ORDER BY (time {order})') as table:
        assert list(cql.execute(f'SELECT * FROM {table} LIMIT 1')) == []
        years = list(range(1901, 2001)) + list(range(2100, 10000, 100)) + [9999]
        for i, year in enumerate(years):
            cql.execute(f"INSERT INTO {table} (test_id, partition_key, time, value) VALUES ({i}, '{i}', '{year:04}-02-03 04:05+0000', {i * 1.0})")
        assert len(list(cql.execute(f'SELECT * FROM {table} LIMIT 1'))) == 1
        assert len(list(cql.execute(f'SELECT * FROM {table} LIMIT 10'))) == 10
        rows = list(cql.execute(f'SELECT test_id, time FROM {table}'))
        assert sorted(rows) == [(i, datetime(year, 2, 3, 4, 5)) for i, year in enumerate(years)]

# Test timestamps given as strings with years before 1900, back to year 1.
# Cassandra accepts them, but Scylla fails to parse such dates.
# Reproduces issue #2251.
# Translated from scylla-dtest's cql_additional_tests.py::TestsCQLAdditional::
# test_limit_date_value_out_of_range_lower_limit, which was skipped until
# issue #2251 is fixed. That test wrote 2000 rows, one per year from 1 to
# 2000, and also ran its SELECTs in cqlsh; this test checks a few years.
@pytest.mark.xfail(reason='issue #2251')
def test_timestamp_ancient_year(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, t timestamp') as table:
        years = [1, 100, 1399, 1899]
        for year in years:
            cql.execute(f"INSERT INTO {table} (k, t) VALUES ({year}, '{year:04}-02-03 04:05+0000')")
        assert sorted(cql.execute(f'SELECT k, t FROM {table}')) == [(year, datetime(year, 2, 3, 4, 5)) for year in years]

# Test the restrict_future_timestamp configuration option: when it is
# enabled, a USING TIMESTAMP more than 3 days into the future is rejected in
# INSERT, UPDATE and DELETE, and when it is disabled, it is allowed. A
# timestamp up to 3 days into the future is always allowed.
# test_using_timestamp.py::test_futuristic_timestamp checks a very futuristic
# timestamp in INSERT, in whichever mode the server runs; this test sets the
# option to each value, so it is scylla_only.
# Translated from scylla-dtest's cql_additional_tests.py::TestsCQLAdditional::
# test_restricted_future_timestamp.
@pytest.mark.parametrize('restrict', [False, True], ids=['unrestricted', 'restricted'])
def test_restrict_future_timestamp(cql, test_keyspace, scylla_only, restrict):
    day = 24 * 3600 * 1000000
    with config_value_context(cql, 'restrict_future_timestamp', str(restrict).lower()), \
         new_test_table(cql, test_keyspace, 'key bigint PRIMARY KEY, val1 text, val2 float') as table:
        def check_allowed(timestamp):
            cql.execute(f"INSERT INTO {table} (key, val1, val2) VALUES (1, 'java1', 1.0) USING TIMESTAMP {timestamp}")
            assert list(cql.execute(f'SELECT writetime(val1) FROM {table} WHERE key = 1')) == [(timestamp,)]
            cql.execute(f"UPDATE {table} USING TIMESTAMP {timestamp + 100} SET val1 = 'test' WHERE key = 1")
            assert list(cql.execute(f'SELECT writetime(val1) FROM {table} WHERE key = 1')) == [(timestamp + 100,)]
            cql.execute(f'DELETE val1 FROM {table} USING TIMESTAMP {timestamp + 1000} WHERE key = 1')
            assert list(cql.execute(f'SELECT val1 FROM {table} WHERE key = 1')) == [(None,)]
        now = int(time.time() * 1000000)
        # Check that a timestamp up to 3 days into the future is always allowed
        for days in [0, 1, 2]:
            check_allowed(now + days * day)
        # Check that a timestamp further into the future is allowed only if
        # restrict_future_timestamp is disabled
        for days in [5, 10, 100, 1000]:
            timestamp = now + days * day
            if restrict:
                with pytest.raises(InvalidRequest, match='into the future'):
                    cql.execute(f"INSERT INTO {table} (key, val1, val2) VALUES (1, 'java1', 1.0) USING TIMESTAMP {timestamp}")
                with pytest.raises(InvalidRequest, match='into the future'):
                    cql.execute(f"UPDATE {table} USING TIMESTAMP {timestamp} SET val1 = 'test' WHERE key = 1")
                with pytest.raises(InvalidRequest, match='into the future'):
                    cql.execute(f'DELETE val1 FROM {table} USING TIMESTAMP {timestamp} WHERE key = 1')
            else:
                check_allowed(timestamp)

# Test a single-partition read with a clustering-key restriction, in a table
# using TimeWindowCompactionStrategy, when the row cache is empty and the only
# sstable holding the partition doesn't match the clustering restriction.
# For such reads, TWCS skips sstables whose clustering-key range doesn't
# match, so no sstable is left to read - but the reader must still return
# the start of the partition, because the row cache expects it for any
# partition that exists. In issue #8447, it returned nothing, so the row
# cache remembered that partition 1 doesn't exist, and a later read of that
# partition's existing row returned nothing.
# The table is in a keyspace with a single tablet, so both of its sstables
# are in the same set of sstables - which is why the test needs tablets. The test needs Scylla's REST API to empty
# the row cache, and Scylla's metrics to check that the read took the TWCS
# code path described above, so it is scylla_only.
# Translated from scylla-dtest's cql_additional_tests.py::TestsCQLAdditional::
# test_twcs_ck_filtering_with_cache, which restarted the node to empty the
# row cache; this test uses the REST API's drop_sstable_caches instead, which
# also empties the row cache. The dtest's comment said that, thanks to
# bloom_filter_fp_chance = 1, both sstables pass the partition-key filter.
# But that filter also checks each sstable's range of keys, so the sstable of
# partition 0 doesn't pass it - this didn't matter for reproducing the bug.
def test_twcs_ck_filtering_with_cache(cql, skip_without_tablets):
    ksdef = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}"
    with new_test_keyspace(cql, ksdef) as keyspace, \
         new_test_table(cql, keyspace, 'pk int, ck int, PRIMARY KEY (pk, ck)', "WITH compaction = {'class': 'TimeWindowCompactionStrategy'} AND bloom_filter_fp_chance = 1") as table:
        cql.execute(f'INSERT INTO {table} (pk, ck) VALUES (1, 0)')
        nodetool.flush(cql, table)
        cql.execute(f'INSERT INTO {table} (pk, ck) VALUES (0, 1)')
        nodetool.flush(cql, table)
        requests.post(f'{nodetool.rest_api_url(cql)}/system/drop_sstable_caches').raise_for_status()
        def clustering_filter_metrics():
            metrics = ScyllaMetrics.query(cql)
            return [metrics.get(f'scylla_database_clustering_filter_{name}') or 0
                    for name in ['count', 'sstables_checked', 'surviving_sstables']]
        before = clustering_filter_metrics()
        assert list(cql.execute(f'SELECT * FROM {table} WHERE pk = 1 AND ck = 1')) == []
        # Check that this read took the TWCS single-partition read path that
        # had the bug: it was not served by the row cache, the sstable of
        # partition 1 passed the partition-key filter, and the clustering-key
        # filter then left no sstables. If this check fails, the test no
        # longer reproduces the situation of #8447 and needs to be fixed.
        after = clustering_filter_metrics()
        assert [a - b for a, b in zip(after, before)] == [1, 1, 0]
        # If the previous read made the cache remember that partition 1
        # doesn't exist, this read would return nothing.
        assert list(cql.execute(f'SELECT * FROM {table} WHERE pk = 1 AND ck = 0')) == [(1, 0)]

#############################################################################
# The following tests check filtering with ALLOW FILTERING on several regular
# columns of different types, sometimes together with restrictions on the
# primary key, a secondary index or a materialized view.
# Translated from scylla-dtest's cql_additional_tests.py class
# TestsMultiColumnRestrictionSimple.
#############################################################################

UUID1 = 'de5cba0d-41a2-4f39-8834-35130d8b5d86'
UUID2 = 'fa80080c-a4c5-46d6-afe4-5e184fec35ae'
# The rows to insert: the columns, and the values as CQL literals. The
# timestamps are given as strings of milliseconds since the epoch.
MULTI_COLUMN_INSERTS = [
    ('key, clmn_int, clmn_text, clmn_timestamp, clmn_bool, clmn_ascii, clmn_uuid, clmn_blob',
     f"0, 0, 'text1', '12345674987', True, '045asciitext', {UUID1}, textAsBlob('cccccccccc')"),
    ('key, clmn_int, clmn_text, clmn_timestamp, clmn_bool, clmn_ascii, clmn_uuid, clmn_blob',
     f"1, 0, 'text2', '63873478378', False, 'abcdefj', {UUID2}, textAsBlob('bbbbbbbbbb')"),
    ('key, clmn_int, clmn_text, clmn_timestamp, clmn_bool, clmn_ascii, clmn_uuid, clmn_blob',
     f"2, 2, 'text3', '398793781719', True, '354dsfsd', {UUID1}, textAsBlob('bbbbbbbbbb')"),
    ('key, clmn_int, clmn_text, clmn_timestamp, clmn_bool, clmn_ascii, clmn_uuid, clmn_blob',
     f"3, 3, 'text4', '398793781719', False, '897dfjka9', {UUID2}, textAsBlob('aaaaaaaaaa')"),
    ('key, clmn_int', '4, 4'),
]
# The rows as read by multi_column_select(), which reads the timestamp and
# uuid columns cast to text.
MULTI_COLUMN_ROWS = [
    (0, 0, 'text1', '1970-05-23T21:21:14.987Z', True, '045asciitext', UUID1, b'c' * 10),
    (1, 0, 'text2', '1972-01-10T06:37:58.378Z', False, 'abcdefj', UUID2, b'b' * 10),
    (2, 2, 'text3', '1982-08-21T16:03:01.719Z', True, '354dsfsd', UUID1, b'b' * 10),
    (3, 3, 'text4', '1982-08-21T16:03:01.719Z', False, '897dfjka9', UUID2, b'a' * 10),
    (4, 4, None, None, None, None, None, None),
]

# Create a table with the columns of MULTI_COLUMN_INSERTS, with clmn_int as a
# clustering key if with_ck, and with an optional index or materialized
# view created by the "prepare" function, before inserting the first
# "nrows" rows of MULTI_COLUMN_INSERTS.
@contextmanager
def multi_column_table(cql, keyspace, with_ck, prepare=None, nrows=len(MULTI_COLUMN_INSERTS)):
    schema = 'key int, clmn_int int, clmn_text text, clmn_timestamp timestamp, clmn_bool boolean, clmn_ascii ascii, clmn_uuid uuid, clmn_blob blob, '
    schema += 'PRIMARY KEY (key, clmn_int)' if with_ck else 'PRIMARY KEY (key)'
    with new_test_table(cql, keyspace, schema) as table:
        with prepare(table) if prepare else nullcontext() as extra:
            for columns, values in MULTI_COLUMN_INSERTS[:nrows]:
                cql.execute(f'INSERT INTO {table} ({columns}) VALUES ({values})')
            yield table, extra

def multi_column_select(cql, table, where):
    columns = 'key, clmn_int, clmn_text, cast(clmn_timestamp as text), clmn_bool, clmn_ascii, cast(clmn_uuid as text), clmn_blob'
    return sorted(cql.execute(f'SELECT {columns} FROM {table} WHERE {where} ALLOW FILTERING'))

def multi_column_rows(*keys):
    return [MULTI_COLUMN_ROWS[k] for k in keys]

# Test filtering on one regular column, for columns of several types.
def test_filter_by_one_non_indexed_column(cql, test_keyspace):
    with multi_column_table(cql, test_keyspace, with_ck=False) as (table, _):
        assert multi_column_select(cql, table, 'clmn_int = 0') == multi_column_rows(0, 1)
        assert multi_column_select(cql, table, "clmn_text = 'text3'") == multi_column_rows(2)
        assert multi_column_select(cql, table, 'clmn_timestamp = 398793781719') == multi_column_rows(2, 3)
        assert multi_column_select(cql, table, 'clmn_bool = True') == multi_column_rows(0, 2)
        assert multi_column_select(cql, table, "clmn_ascii = 'abcdefj'") == multi_column_rows(1)
        assert multi_column_select(cql, table, f'clmn_uuid = {UUID1}') == multi_column_rows(0, 2)
        assert multi_column_select(cql, table, "clmn_blob = textAsBlob('bbbbbbbbbb')") == multi_column_rows(1, 2)

# Test filtering on three regular columns at once.
def test_filter_by_three_non_indexed_columns(cql, test_keyspace):
    with multi_column_table(cql, test_keyspace, with_ck=False) as (table, _):
        assert multi_column_select(cql, table, f'clmn_int = 2 AND clmn_uuid = {UUID1} AND clmn_timestamp = 398793781719') == multi_column_rows(2)
        assert multi_column_select(cql, table, "clmn_ascii = '897dfjka9' AND clmn_text = 'text4' AND clmn_blob = textAsBlob('aaaaaaaaaa')") == multi_column_rows(3)

# Test filtering on regular columns together with a restriction on the
# partition key, with and without one on the clustering key.
def test_filter_by_pk_ck_and_non_indexed_columns(cql, test_keyspace):
    with multi_column_table(cql, test_keyspace, with_ck=True) as (table, _):
        assert multi_column_select(cql, table, 'key = 0 AND clmn_timestamp = 12345674987') == multi_column_rows(0)
        assert multi_column_select(cql, table, 'key = 0 AND clmn_int = 0 AND clmn_timestamp = 12345674987') == multi_column_rows(0)
        assert multi_column_select(cql, table, 'key = 0 AND clmn_timestamp = 12345674987 AND clmn_bool = True') == multi_column_rows(0)
        assert multi_column_select(cql, table, f'key = 0 AND clmn_int = 0 AND clmn_timestamp = 12345674987 AND clmn_uuid = {UUID1}') == multi_column_rows(0)

# The checks of the following two tests, with a global and with a local
# secondary index on clmn_text: filtering on regular columns together with
# restrictions on the partition key, the clustering key and the indexed
# column.
def check_filter_by_pk_ck_si_and_non_indexed_columns(cql, table):
    assert multi_column_select(cql, table, "key = 1 AND clmn_text = 'text2' AND clmn_timestamp = 63873478378") == multi_column_rows(1)
    assert multi_column_select(cql, table, "key = 1 AND clmn_int = 0 AND clmn_text = 'text2' AND clmn_timestamp = 63873478378") == multi_column_rows(1)
    assert multi_column_select(cql, table, f"key = 1 AND clmn_text = 'text2' AND clmn_uuid = {UUID2} AND clmn_timestamp = 63873478378") == multi_column_rows(1)
    assert multi_column_select(cql, table, "key = 1 AND clmn_int = 0 AND clmn_text = 'text2' AND clmn_bool = False AND clmn_timestamp = 63873478378") == multi_column_rows(1)

def global_index_on_clmn_text(cql):
    @contextmanager
    def prepare(table):
        cql.execute(f'CREATE INDEX ON {table} (clmn_text)')
        yield
    return prepare

def local_index_on_clmn_text(cql):
    @contextmanager
    def prepare(table):
        cql.execute(f'CREATE INDEX ON {table} ((key), clmn_text)')
        yield
    return prepare

def test_filter_by_pk_ck_global_si_and_non_indexed_columns(cql, test_keyspace):
    with multi_column_table(cql, test_keyspace, with_ck=True, prepare=global_index_on_clmn_text(cql)) as (table, _):
        check_filter_by_pk_ck_si_and_non_indexed_columns(cql, table)

# Local secondary indexes are a Scylla-only feature.
def test_filter_by_pk_ck_local_si_and_non_indexed_columns(cql, test_keyspace, scylla_only):
    with multi_column_table(cql, test_keyspace, with_ck=True, prepare=local_index_on_clmn_text(cql)) as (table, _):
        check_filter_by_pk_ck_si_and_non_indexed_columns(cql, table)

# Test filtering on two regular columns with "<" and "<=" operators.
def test_filter_by_two_non_indexed_columns_with_operator(cql, test_keyspace):
    with multi_column_table(cql, test_keyspace, with_ck=False) as (table, _):
        assert multi_column_select(cql, table, f'clmn_int < 2 AND clmn_uuid <= {UUID2}') == multi_column_rows(0, 1)

# The checks of the following two tests, with a global and with a local
# secondary index on clmn_text: filtering with range operators on the
# partition key, the clustering key, the indexed column and a regular column.
def check_filter_by_pk_ck_si_and_non_indexed_columns_with_operator(cql, table):
    assert multi_column_select(cql, table, "key > 1 AND clmn_int < 5 AND clmn_text >= 'text2' AND clmn_timestamp > 63873478378") == multi_column_rows(2, 3)

def test_filter_by_pk_ck_global_si_and_non_indexed_columns_with_operator(cql, test_keyspace):
    with multi_column_table(cql, test_keyspace, with_ck=True, prepare=global_index_on_clmn_text(cql), nrows=4) as (table, _):
        check_filter_by_pk_ck_si_and_non_indexed_columns_with_operator(cql, table)

# Local secondary indexes are a Scylla-only feature.
def test_filter_by_pk_ck_local_si_and_non_indexed_columns_with_operator(cql, test_keyspace, scylla_only):
    with multi_column_table(cql, test_keyspace, with_ck=True, prepare=local_index_on_clmn_text(cql), nrows=4) as (table, _):
        check_filter_by_pk_ck_si_and_non_indexed_columns_with_operator(cql, table)

def view_by_clmn_text(cql):
    def prepare(table):
        return new_materialized_view(cql, table, '*', 'key, clmn_int, clmn_text',
            'key IS NOT NULL AND clmn_int IS NOT NULL AND clmn_text IS NOT NULL')
    return prepare

# Test filtering on regular columns of a materialized view, whose primary
# key adds a regular column of the base table (clmn_text). The test also
# checks that "clmn_text = ''" matches no rows, in the view and in the base
# table: row 4's clmn_text is null, not an empty string, so it isn't in the
# view at all, and in the base table an empty string doesn't match it. The
# dtest had this check commented out, expecting it to find row 4, citing
# issue #4776 - but that issue, asking for such matching of null values, was
# closed as "won't fix", and Cassandra doesn't match row 4 either.
def test_filter_by_non_indexed_columns_from_mv(cql, test_keyspace):
    with multi_column_table(cql, test_keyspace, with_ck=True, prepare=view_by_clmn_text(cql)) as (table, view):
        assert multi_column_select(cql, view, "clmn_text = 'text1'") == multi_column_rows(0)
        assert multi_column_select(cql, view, "clmn_text = ''") == []
        assert multi_column_select(cql, table, "clmn_text = ''") == []
        assert multi_column_select(cql, view, f"key > 1 AND clmn_int < 5 AND clmn_uuid = {UUID2} AND clmn_blob = textAsBlob('aaaaaaaaaa')") == multi_column_rows(3)

# Test filtering which matches no rows, on a base table and on its
# materialized view.
def test_filter_by_multiple_columns_empty_result(cql, test_keyspace):
    with multi_column_table(cql, test_keyspace, with_ck=True, prepare=view_by_clmn_text(cql), nrows=4) as (table, view):
        assert multi_column_select(cql, table, "key > 1 AND clmn_int < 5 AND clmn_text <= 'text2' AND clmn_timestamp < 63873478378") == []
        assert multi_column_select(cql, table, f"key > 1 AND clmn_int < 5 AND clmn_uuid = {UUID2} AND clmn_blob = textAsBlob('bbbbbbbbbb')") == []
        assert multi_column_select(cql, view, "key > 1 AND clmn_int < 5 AND clmn_text <= 'text2' AND clmn_timestamp < 63873478378") == []

#############################################################################
# The following tests check filtering with ALLOW FILTERING on collection
# columns - frozen and unfrozen lists, sets and maps - with CONTAINS,
# CONTAINS KEY and equality.
# Translated from scylla-dtest's cql_additional_tests.py class
# TestsMultiColumnRestrictionCollection.
#############################################################################

COLLECTIONS_SCHEMA = ('id int PRIMARY KEY, list_int list<int>, list_blob list<blob>, '
    'set_uuid set<uuid>, set_text set<text>, map_bool map<text, boolean>, '
    'map_uuid map<text, uuid>, f_list_int frozen<list<int>>, f_list_text frozen<list<text>>, '
    'f_set_int frozen<set<int>>, f_set_text frozen<set<text>>, f_map_text frozen<map<text, text>>, '
    'f_map_int frozen<map<text, int>>')

COLLECTIONS_ROWS = [
    "0, [0, 1, 2], [textAsBlob('t1'), textAsBlob('t2')], "
    "{de5cba0d-41a2-4f39-8834-35130d8b5d86, fa80080c-a4c5-46d6-afe4-5e184fec35ae}, {'t3', 't4', 't5'}, "
    "{'a': True, 'b': True, 'c': False}, "
    "{'a': f34f6a76-b383-11e9-a2a3-2a2ae2dbcce4, 'c': f34f6cec-b383-11e9-a2a3-2a2ae2dbcce4}, "
    "[5, 6], ['f1', 'f2'], {7, 9}, {'f3', 'f4', 'f5'}, {'fa': 'b', 'fc': 'd'}, {'fa': 1, 'fb': 2, 'fc': 3}",
    "1, [3, 4, 5], [textAsBlob('t3'), textAsBlob('t4')], "
    "{8e4fe826-b383-11e9-a2a3-2a2ae2dbcce4, 8e4fea9c-b383-11e9-a2a3-2a2ae2dbcce4}, {'t5', 't6', 't7'}, "
    "{'a1': False, 'c1': False}, "
    "{'a1': 26a8e352-b384-11e9-a2a3-2a2ae2dbcce4, 'b1': 26a8e5be-b384-11e9-a2a3-2a2ae2dbcce4, 'c1': 26a8e712-b384-11e9-a2a3-2a2ae2dbcce4}, "
    "[7, 8], ['f3', 'f4'], {9, 10}, {'f6', 'f7', 'f8'}, {'f1': 'c', 'f2': 'e'}, {'f1': 1, 'f2': 2, 'f3': 3}",
]

@pytest.fixture(scope='module')
def collections_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, COLLECTIONS_SCHEMA) as table:
        for row in COLLECTIONS_ROWS:
            cql.execute(f'INSERT INTO {table} (id, list_int, list_blob, set_uuid, set_text, map_bool, map_uuid, '
                        f'f_list_int, f_list_text, f_set_int, f_set_text, f_map_text, f_map_int) VALUES ({row})')
        yield table

def collections_select_ids(cql, table, where):
    return sorted(row.id for row in cql.execute(f'SELECT id FROM {table} WHERE {where} ALLOW FILTERING'))

# Test filtering on one collection column, for frozen and unfrozen lists,
# sets and maps of several element types.
def test_filter_by_one_non_indexed_collection_column(cql, collections_table):
    table = collections_table
    assert collections_select_ids(cql, table, 'list_int CONTAINS 4') == [1]
    assert collections_select_ids(cql, table, "list_blob CONTAINS textAsBlob('t1')") == [0]
    assert collections_select_ids(cql, table, 'set_uuid CONTAINS 8e4fe826-b383-11e9-a2a3-2a2ae2dbcce4') == [1]
    assert collections_select_ids(cql, table, "set_text CONTAINS 't5'") == [0, 1]
    assert collections_select_ids(cql, table, "map_bool CONTAINS False AND map_bool CONTAINS KEY 'a1'") == [1]
    assert collections_select_ids(cql, table, 'map_uuid CONTAINS f34f6a76-b383-11e9-a2a3-2a2ae2dbcce4') == [0]
    assert collections_select_ids(cql, table, "map_uuid CONTAINS KEY 'a1'") == [1]
    assert collections_select_ids(cql, table, 'f_list_int CONTAINS 8') == [1]
    assert collections_select_ids(cql, table, "f_list_text CONTAINS 'f4'") == [1]
    assert collections_select_ids(cql, table, 'f_set_int = {9, 7}') == [0]
    assert collections_select_ids(cql, table, 'f_set_int CONTAINS 9 AND f_set_int CONTAINS 10') == [1]
    assert collections_select_ids(cql, table, "f_set_text CONTAINS 'f6'") == [1]
    assert collections_select_ids(cql, table, "f_map_text = {'f2': 'e', 'f1': 'c'}") == [1]
    assert collections_select_ids(cql, table, "f_map_text CONTAINS 'c'") == [1]
    assert collections_select_ids(cql, table, "f_map_text CONTAINS KEY 'f2'") == [1]
    assert collections_select_ids(cql, table, 'f_map_int CONTAINS 2') == [0, 1]

# Test filtering on two collection columns together with a restriction on
# the partition key, including one that matches no rows.
def test_filter_by_pk_and_two_non_indexed_collection_columns(cql, collections_table):
    table = collections_table
    assert collections_select_ids(cql, table, 'id = 0 AND map_uuid CONTAINS f34f6a76-b383-11e9-a2a3-2a2ae2dbcce4 AND f_set_int CONTAINS 9') == [0]
    assert collections_select_ids(cql, table, "id = 1 AND set_uuid CONTAINS 8e4fe826-b383-11e9-a2a3-2a2ae2dbcce4 AND f_map_int = {'f1': 1, 'f2': 2, 'f3': 3}") == [1]
    # This map_uuid value differs from row 0's value in one digit
    assert collections_select_ids(cql, table, 'id = 0 AND map_uuid CONTAINS f54f6a76-b383-11e9-a2a3-2a2ae2dbcce4 AND f_set_int CONTAINS 9') == []

#############################################################################
# The following tests check the results of CAST between many pairs of types,
# for specific values, and of CAST combined with the aggregate functions
# avg(), sum(), min() and max(): casting a column and aggregating the casts,
# casting an aggregate, and casting a cast. Casts from fields of a
# user-defined type are also tested.
#
# These tests were translated from scylla-dtest's cql_cast_test.py. That
# test ran its queries in cqlsh, and its expected results - kept here as
# they were, in POSITIVE_VALUES - are the values as cqlsh printed them. So
# the tests here format each result the same way before comparing it: in
# particular, cqlsh printed float and double values with only 5 significant
# digits, so these tests compare them to that precision.
#############################################################################

# Format a result value as cqlsh printed it.
def format_cqlsh_result(value):
    if value is None:
        return 'null'
    if isinstance(value, float):
        # cqlsh's format_floating_point_type(), with cqlsh's default
        # precision of 5 for both float and double
        if math.isnan(value):
            return 'NaN'
        if math.isinf(value):
            return 'Infinity' if value > 0 else '-Infinity'
        precision = 5
        exponent = int(math.log10(abs(value))) if abs(value) > sys.float_info.epsilon else -sys.maxsize - 1
        if -4 <= exponent < precision:
            precision += exponent + 1
        return '%.*g' % (precision, value)
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S.%f') + '+0000'
    return str(value)

# The expression to SELECT for each kind of check.
def cast_select_expression(kind, column, to_type):
    if kind == 'cast':
        return f'cast({column} as {to_type})'
    if kind in ['avg', 'sum', 'min', 'max']:
        return f'{kind}(cast({column} as {to_type}))'
    if kind in ['cast_avg', 'cast_sum', 'cast_min', 'cast_max']:
        return f'cast({kind[5:]}({column}) as {to_type})'
    if kind == 'cast_cast':
        first, second = to_type.split('/')
        return f'cast(cast({column} as {first}) as {second})'
    raise ValueError(kind)

# Check the results expected in "rows" - a list in the format of a
# POSITIVE_VALUES entry - for "column" of type "from_type" in "table", whose
# row k holds the value of rows[k]. The "cast" and "cast_cast" checks read
# one row, and the other checks aggregate over all rows.
# Expected results that are None aren't checked, and neither are checks or
# target types whose name begins with "#". The checks of casts to varchar
# are also skipped, because varchar is just another name for text.
# If "include" is given, only the checks for which include(kind, to_type)
# is true are done.
def check_casts(cql, table, column, from_type, rows, include=None):
    for k, (_, checks) in enumerate(rows):
        for kind, results in checks.items():
            if kind.startswith('#'):
                continue
            for to_type, expected in results.items():
                if to_type.startswith('#') or expected is None or to_type == 'varchar':
                    continue
                if include and not include(kind, to_type):
                    continue
                where = f' WHERE k = {k}' if kind in ['cast', 'cast_cast'] else ''
                query = f'SELECT {cast_select_expression(kind, column, to_type)} FROM {table}{where}'
                [[value]] = cql.execute(query)
                actual = format_cqlsh_result(value)
                expected = str(expected)
                if to_type == 'text' and from_type == 'timestamp':
                    # Scylla omits zero milliseconds
                    actual = actual.removesuffix('.000Z')
                    expected = expected.removesuffix('.000Z')
                if to_type in ['decimal', 'text']:
                    # Cassandra's decimal omits ".0" (issue #3109)
                    actual = actual.removesuffix('.0')
                    expected = expected.removesuffix('.0')
                if to_type == 'decimal' and from_type in ['float', 'double']:
                    assert float(actual) == pytest.approx(float(expected), rel=1e-6), query
                else:
                    assert actual == expected, query

# Values to cast and the expected results, as cqlsh printed them, from
# scylla-dtest's cql_cast_test.py. See check_casts() for the format.
POSITIVE_VALUES = {
    "bigint": [
        [
            7657139128428828644,
            {
                "cast": {
                    "tinyint": -28,
                    "smallint": -26652,
                    "int": -776038428,
                    "float": "7.6571e+18",
                    "double": "7.6571e+18",
                    "decimal": "7657139128428828644",
                    "varint": 7657139128428828644,
                    "text": "7657139128428828644",
                    "varchar": "7657139128428828644",
                },
                "avg": {"tinyint": 10, "smallint": -10741, "int": -258681333, "float": "2.5524e+18", "double": "2.5524e+18", "decimal": "2552379709476274357", "varint": 2552379709476274357, "text": None, "varchar": None},
                "sum": {"tinyint": 31, "smallint": -32225, "int": -776044001, "float": "7.6571e+18", "double": "7.6571e+18", "decimal": "7657139128428823071", "varint": 7657139128428823071, "text": None, "varchar": None},
                "min": {"tinyint": -66, "smallint": -26652, "int": -776038428, "float": -5698, "double": -5698, "decimal": -5698.0, "varint": -5698, "text": "-5698", "varchar": "-5698"},
                "max": {
                    "tinyint": 125,
                    "smallint": 125,
                    "int": 125,
                    "float": "7.6571e+18",
                    "double": "7.6571e+18",
                    "decimal": "7657139128428828644",
                    "varint": 7657139128428828644,
                    "text": "7657139128428828644",
                    "varchar": "7657139128428828644",
                },
            },
        ],
        [125, {"cast": {"tinyint": 125, "smallint": 125, "int": 125, "float": 125, "double": 125, "decimal": 125.0, "varint": 125, "text": "125", "varchar": "125"}}],
        [-5698, {"cast": {"tinyint": -66, "smallint": -5698, "int": -5698, "double": -5698, "decimal": -5698.0, "varint": -5698, "text": "-5698", "varchar": "-5698"}}],
    ],
    "boolean": [[True, {"cast": {"text": "true", "varchar": "true"}}], [False, {"cast": {"text": "false", "varchar": "false"}}]],
    "decimal": [
        [
            76571.345,
            {
                "cast": {"tinyint": 27, "smallint": 11035, "int": 76571, "float": 76571.34375, "double": 76571.345, "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                "avg": {"tinyint": 14, "smallint": 5390, "int": 38158, "float": 38158.54297, "double": 38158.54401, "bigint": 38158, "varint": 38158, "text": None, "varchar": None},
                "sum": {"tinyint": 29, "smallint": 10781, "int": 76317, "float": 76317.08594, "double": 76317.08801, "bigint": 76317, "varint": 76317, "text": None, "varchar": None},
                "min": {"tinyint": 2, "smallint": -254, "int": -254, "float": -254.25699, "double": -254.25699, "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"},
                "max": {"tinyint": 27, "smallint": 11035, "int": 76571, "float": 76571.34375, "double": 76571.345, "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                "cast_avg": {"tinyint": 14, "smallint": -27378, "int": 38158, "float": "38158.54297", "double": "38158.54401", "bigint": 38158, "varint": 38158, "text": None, "varchar": None},
                "cast_sum": {"tinyint": 29, "smallint": 10781, "int": 76317, "float": "76317.08594", "double": "76317.08801", "bigint": 76317, "varint": 76317, "text": None, "varchar": None},
                "cast_min": {"tinyint": 2, "smallint": -254, "int": -254, "float": "-254.25699", "double": "-254.25699", "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"},
                "cast_max": {"tinyint": 27, "smallint": 11035, "int": 76571, "float": 76571.34375, "double": "76571.345", "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                "cast_cast": {"double/bigint": "76571"},
            },
        ],
        [-254.256987, {"cast": {"tinyint": 2, "smallint": -254, "int": -254, "float": -254.25699, "double": -254.25699, "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"}}],
    ],
    "double": [
        [
            76571.345,
            {
                "cast": {"tinyint": 27, "smallint": 11035, "int": 76571, "float": 76571.34375, "decimal": "76571.345000000001", "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                "avg": {"tinyint": 14, "smallint": 5390, "int": 38158, "float": 38158.54297, "decimal": "38158.54400650000050", "bigint": 38158, "varint": 38158, "text": None, "varchar": None},
                "sum": {"tinyint": 29, "smallint": 10781, "int": 76317, "float": 76317.08594, "decimal": "76317.08801300000099", "bigint": 76317, "varint": 76317, "text": None, "varchar": None},
                "min": {"tinyint": 2, "smallint": -254, "int": -254, "float": -254.25699, "decimal": "-254.25698700000001", "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"},
                "max": {"tinyint": 27, "smallint": 11035, "int": 76571, "float": 76571.34375, "decimal": "76571.345000000001", "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
            },
        ],
        [-254.256987, {"cast": {"tinyint": 2, "smallint": -254, "int": -254, "float": -254.25699, "decimal": "-254.25698700000001", "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"}}],
    ],
    "float": [
        [
            -564,
            {
                "cast": {"tinyint": -52, "smallint": -564, "int": -564, "double": -564, "decimal": "-564.0", "bigint": -564, "varint": -564, "text": "-564.0", "varchar": "-564.0"},
                "avg": {"tinyint": 10, "smallint": 1034, "int": 1034, "double": "1034.23883", "decimal": "1034.238824", "bigint": 1034, "varint": 1034, "text": None, "varchar": None},
                "sum": {"tinyint": 30, "smallint": 3102, "int": 3102, "double": "3102.71648", "decimal": "3102.716473", "bigint": 3102, "varint": 3102, "text": None, "varchar": None},
                "min": {"tinyint": -52, "smallint": -564, "int": -564, "double": -564, "decimal": "-564.0", "bigint": -564, "varint": -564, "text": "-564.0", "varchar": "-564.0"},
                "max": {"tinyint": 123, "smallint": 3543, "int": 3543, "double": "3543.45654", "decimal": "3543.45654", "bigint": 3543, "varint": 3543, "text": "3543.4565", "varchar": "3543.4565"},
            },
        ],
        [3543.4565429688, {"cast": {"tinyint": -41, "smallint": 3543, "int": 3543, "double": 3543.45654, "decimal": "3543.45654", "bigint": 3543, "varint": 3543, "text": "3543.4565", "varchar": "3543.4565"}}],
        ["123.25993624886543132316548678", {"cast": {"tinyint": 123, "smallint": 123, "int": 123, "double": "123.25993", "decimal": "123.259933", "bigint": 123, "varint": 123, "text": "123.25993", "varchar": "123.25993"}}],
    ],
    "int": [
        [
            0,
            {
                "cast": {"tinyint": 0, "smallint": 0, "float": 0, "double": 0, "decimal": "0.0", "bigint": 0, "varint": 0, "text": "0", "varchar": "0"},
                "avg": {"tinyint": 3, "smallint": -2044, "float": -2044.40002, "double": -2044.4, "decimal": "-2044.4", "bigint": -2044, "varint": -2044, "text": None, "varchar": None},
                "sum": {"tinyint": 18, "smallint": -10222, "float": -10222, "double": -10222, "decimal": "-10222.0", "bigint": -10222, "varint": -10222, "text": None, "varchar": None},
                "min": {"tinyint": -76, "smallint": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-12876", "varchar": "-12876"},
                "max": {"tinyint": 94, "smallint": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"},
            },
        ],
        [32767, {"cast": {"tinyint": -1, "smallint": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"}}],
        [-32767, {"cast": {"tinyint": 1, "smallint": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-32767", "varchar": "-32767"}}],
        [-12876, {}],
        [2654, {}],
    ],
    "smallint": [
        [
            32767,
            {
                "cast": {"tinyint": -1, "int": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"},
                "avg": {"tinyint": 4, "int": -2555, "float": -2555.5, "double": -2555.5, "decimal": "-2555.5", "bigint": -2555, "varint": -2555, "text": None, "varchar": None},
                "sum": {"tinyint": 18, "int": -10222, "float": -10222, "double": -10222, "decimal": "-10222.0", "bigint": -10222, "varint": -10222, "text": None, "varchar": None},
                "min": {"tinyint": -76, "int": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-12876", "varchar": "-12876"},
                "max": {"tinyint": 94, "int": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"},
            },
        ],
        [-32767, {"cast": {"tinyint": 1, "int": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-32767", "varchar": "-32767"}}],
        [-12876, {"cast": {"tinyint": -76, "int": -12876, "float": -12876, "double": -12876, "decimal": "-12876.0", "bigint": -12876, "varint": -12876, "text": "-12876", "varchar": "-12876"}}],
        [2654, {"cast": {"tinyint": 94, "int": 2654, "float": 2654, "double": 2654, "decimal": "2654.0", "bigint": 2654, "varint": 2654, "text": "2654", "varchar": "2654"}}],
    ],
    "tinyint": [
        [
            127,
            {
                "cast": {"smallint": 127, "int": 127, "float": 127, "double": 127, "decimal": "127.0", "bigint": 127, "varint": 127, "text": "127", "varchar": "127"},
                "avg": {"smallint": 11, "int": 11, "float": 11.33333, "double": 11.33333, "decimal": "11.3", "bigint": 11, "varint": 11, "text": None, "varchar": None},
                "sum": {"smallint": 34, "int": 34, "float": 34, "double": 34, "decimal": "34.0", "bigint": 34, "varint": 34, "text": None, "varchar": None},
                "min": {"smallint": -128, "int": -128, "float": -128, "double": -128, "decimal": "-128.0", "bigint": -128, "varint": -128, "text": "-128", "varchar": "-128"},
                "max": {"smallint": 127, "int": 127, "float": 127, "double": 127, "decimal": "127.0", "bigint": 127, "varint": 127, "text": "35", "varchar": "35"},
            },
        ],
        [-128, {"cast": {"smallint": -128, "int": -128, "float": -128, "double": -128, "decimal": "-128.0", "bigint": -128, "varint": -128, "text": "-128", "varchar": "-128"}}],
        [35, {"cast": {"smallint": 35, "int": 35, "float": 35, "double": 35, "decimal": "35.0", "bigint": 35, "varint": 35, "text": "35", "varchar": "35"}}],
    ],
    "varint": [
        [
            32767456456456456456545678943512357658768763546575675,
            {
                "cast": {
                    "tinyint": 59,
                    "smallint": 9019,
                    "int": -1446698181,
                    "float": "Infinity",
                    "double": 3.2767e52,
                    "decimal": "32767456456456456456545678943512357658768763546575675",
                    "bigint": -4832518953173179589,
                    "text": "32767456456456456456545678943512357658768763546575675",
                    "varchar": "32767456456456456456545678943512357658768763546575675",
                },
                "avg": {
                    "tinyint": -9,
                    "smallint": 8011,
                    "int": -678398815,
                    "float": "NaN",
                    "double": "1419.33333",
                    "#decimal": "-2110054581",  # commented cause cassandra is rounding the result
                    "bigint": -2110054580,
                    "text": None,
                    "varchar": None,
                },
                "sum": {"tinyint": -29, "smallint": 24035, "int": -2035196445, "float": "NaN", "double": 4258, "decimal": "-6330163741", "bigint": -6330163741, "text": None, "varchar": None},
                "min": {
                    "tinyint": -94,
                    "smallint": 4258,
                    "int": -1446698181,
                    "float": "-Infinity",
                    "double": -3.2767e52,
                    "decimal": "-32767456456456456456545678943512357658768769876743674",
                    "bigint": -4832518953173179589,
                    "text": "-32767456456456456456545678943512357658768769876743674",
                    "varchar": "-32767456456456456456545678943512357658768769876743674",
                },
                "max": {
                    "tinyint": 59,
                    "smallint": 10758,
                    "int": 4258,
                    "float": "Infinity",
                    "double": 3.2767e52,
                    "decimal": "32767456456456456456545678943512357658768763546575675",
                    "bigint": 4832518946843011590,
                    "text": "4258",
                    "varchar": "4258",
                },
                "cast_avg": {"tinyint": 76, "smallint": 8012, "int": -2110054580, "float": "-2.1101e+09", "double": "-2.1101e+09", "decimal": "-2110054580", "bigint": -2110054580, "text": None, "varchar": None},
                "cast_sum": {"tinyint": -29, "smallint": 24035, "int": -2035196445, "float": "-6.3302e+09", "double": "-6.3302e+09", "decimal": "-6330163741", "bigint": -6330163741, "text": None, "varchar": None},
                "cast_min": {
                    "tinyint": 6,
                    "smallint": 10758,
                    "int": -588502522,
                    "float": "-Infinity",
                    "double": "-3.2767e+52",
                    "decimal": "-32767456456456456456545678943512357658768769876743674",
                    "bigint": 4832518946843011590,
                    "text": "-32767456456456456456545678943512357658768769876743674",
                    "varchar": "-32767456456456456456545678943512357658768769876743674",
                },
                "cast_max": {
                    "tinyint": 59,
                    "smallint": 9019,
                    "int": -1446698181,
                    "float": "Infinity",
                    "double": "3.2767e+52",
                    "decimal": "32767456456456456456545678943512357658768763546575675",
                    "bigint": -4832518953173179589,
                    "text": "32767456456456456456545678943512357658768763546575675",
                    "varchar": "32767456456456456456545678943512357658768763546575675",
                },
            },
        ],
        [
            -32767456456456456456545678943512357658768769876743674,
            {
                "cast": {
                    "tinyint": 6,
                    "smallint": 10758,
                    "int": -588502522,
                    "float": "-Infinity",
                    "double": -3.2767e52,
                    "decimal": "-32767456456456456456545678943512357658768769876743674",
                    "bigint": 4832518946843011590,
                    "text": "-32767456456456456456545678943512357658768769876743674",
                    "varchar": "-32767456456456456456545678943512357658768769876743674",
                }
            },
        ],
        [4258, {"cast": {"tinyint": -94, "smallint": 4258, "int": 4258, "float": 4258, "double": 4258, "decimal": "4258.0", "bigint": 4258, "text": "4258", "varchar": "4258"}}],
    ],
    "ascii": [
        ["'045asciitext'", {"cast": {"text": "045asciitext", "varchar": "045asciitext"}, "min": {"text": "045asciitext", "varchar": "045asciitext"}, "max": {"text": "abcdefj", "varchar": "abcdefj"}}],
        ["'abcdefj'", {"cast": {"text": "abcdefj", "varchar": "abcdefj"}}],
    ],
    "date": [
        ["'2017-11-25'", {"cast": {"timestamp": "2017-11-25 00:00:00.000000+0000"}, "min": {"timestamp": "1900-01-01 00:00:00.000000+0000"}, "max": {"timestamp": "2458-12-13 00:00:00.000000+0000"}}],
        ["'1900-01-01'", {"cast": {"timestamp": "1900-01-01 00:00:00.000000+0000"}}],
        ["'2458-12-13'", {"cast": {"timestamp": "2458-12-13 00:00:00.000000+0000"}}],
    ],
    "time": [
        [
            "'13:07:45.089'",
            {"cast": {"text": "13:07:45.089000000", "varchar": "13:07:45.089000000"}, "min": {"text": "00:45:25.123000000", "varchar": "00:45:25.123000000"}, "max": {"text": "13:07:45.089000000", "varchar": "13:07:45.089000000"}},
        ],
        ["'00:45:25.123'", {"cast": {"text": "00:45:25.123000000", "varchar": "00:45:25.123000000"}}],
        ["'10:14:36.0002312'", {"cast": {"text": "10:14:36.000231200", "varchar": "10:14:36.000231200"}}],
    ],
    "timestamp": [
        [
            "'2017-12-27 11:57:42+0000'",
            {
                "cast": {"date": "2017-12-27", "text": "2017-12-27T11:57:42.000Z", "varchar": "2017-12-27T11:57:42.000Z"},
                "min": {"date": "1900-01-01", "text": "1900-01-01T00:00:00.000Z", "varchar": "1900-01-01T00:00:00.000Z"},
                "max": {"date": "2018-02-01", "text": "2018-02-01T01:00:42.000Z", "varchar": "2018-02-01T01:00:42.000Z"},
            },
        ],
        [
            "'2017-12-27 11:57:42+0100'",  # timezone offset
            {
                "cast": {"date": "2017-12-27", "text": "2017-12-27T10:57:42.000Z", "varchar": "2017-12-27T10:57:42.000Z"},
            },
        ],
        [
            "'2017-12-27 11:57:42.500+0100'",  # with milliseconds, scylla is returning with 6 digit vs. cassandra with 3 digits
            {
                "cast": {"date": "2017-12-27", "text": "2017-12-27T10:57:42.500Z", "varchar": "2017-12-27T10:57:42.500Z"},
            },
        ],
        ["'2018-02-01 01:00:42+0000'", {"cast": {"date": "2018-02-01", "text": "2018-02-01T01:00:42.000Z", "varchar": "2018-02-01T01:00:42.000Z"}}],
        ["'1900-01-01 00:00:00+0000'", {"cast": {"date": "1900-01-01", "text": "1900-01-01T00:00:00.000Z", "varchar": "1900-01-01T00:00:00.000Z"}}],
    ],
    "timeuuid": [
        [
            "minTimeuuid('2018-01-01 17:01:07+0000')",
            {
                "cast": {"timestamp": "2018-01-01 17:01:07.000000+0000", "date": "2018-01-01", "text": "5bb48b80-ef15-11e7-8080-808080808080", "varchar": "5bb48b80-ef15-11e7-8080-808080808080"},
                "min": {"timestamp": "1900-01-01 00:00:00.000000+0000", "date": "1900-01-01", "text": "3c230000-a32f-1163-8080-808080808080", "varchar": "3c230000-a32f-1163-8080-808080808080"},
                "max": {"timestamp": "2018-01-01 17:01:07.000000+0000", "date": "2018-01-01", "text": "7a73e600-eb01-11e7-8080-808080808080", "varchar": "7a73e600-eb01-11e7-8080-808080808080"},
            },
        ],
        ["minTimeuuid('2017-12-27 12:28:44+0000')", {"cast": {"timestamp": "2017-12-27 12:28:44.000000+0000", "date": "2017-12-27", "text": "7a73e600-eb01-11e7-8080-808080808080", "varchar": "7a73e600-eb01-11e7-8080-808080808080"}}],
        ["minTimeuuid('1900-01-01 00:00:00+0000')", {"cast": {"timestamp": "1900-01-01 00:00:00.000000+0000", "date": "1900-01-01", "text": "3c230000-a32f-1163-8080-808080808080", "varchar": "3c230000-a32f-1163-8080-808080808080"}}],
    ],
    "uuid": [
        [
            "de5cba0d-41a2-4f39-8834-35130d8b5d86",
            {
                "cast": {"text": "de5cba0d-41a2-4f39-8834-35130d8b5d86", "varchar": "de5cba0d-41a2-4f39-8834-35130d8b5d86"},
                "min": {"text": "de5cba0d-41a2-4f39-8834-35130d8b5d86", "varchar": "de5cba0d-41a2-4f39-8834-35130d8b5d86"},
                "max": {"text": "fa80080c-a4c5-46d6-afe4-5e184fec35ae", "varchar": "fa80080c-a4c5-46d6-afe4-5e184fec35ae"},
            },
        ],
        ["f5f576e7-efb6-41b5-804e-fa3205548137", {"cast": {"text": "f5f576e7-efb6-41b5-804e-fa3205548137", "varchar": "f5f576e7-efb6-41b5-804e-fa3205548137"}}],
        ["fa80080c-a4c5-46d6-afe4-5e184fec35ae", {"cast": {"text": "fa80080c-a4c5-46d6-afe4-5e184fec35ae", "varchar": "fa80080c-a4c5-46d6-afe4-5e184fec35ae"}}],
    ],
    "inet": [
        ["'205.118.5.3'", {"cast": {"text": "205.118.5.3", "varchar": "205.118.5.3"}, "min": {"text": "192.168.122.255", "varchar": "192.168.122.255"}, "max": {"text": "205.118.5.3", "varchar": "205.118.5.3"}}],
        ["'192.168.2.1'", {"cast": {"text": "192.168.2.1", "varchar": "192.168.2.1"}}],
        ["'192.168.122.255'", {"cast": {"text": "192.168.122.255", "varchar": "192.168.122.255"}}],
    ],
    "udt": {
        "bigint": [
            [
                4561313648948,
                {
                    "cast": {"decimal": "4561313648948.0", "text": "4561313648948"},
                    "avg": {"decimal": "2280656551235.0", "text": None},
                    "sum": {"decimal": "4561313102470.0", "text": None},
                    "min": {"decimal": "-546478.0", "text": "-546478"},
                    "max": {"decimal": "4561313648948.0", "text": "4561313648948"},
                    "cast_avg": {"decimal": "2280656551235.0", "text": "2280656551235"},
                    "cast_sum": {"decimal": "4561313102470.0", "text": "4561313102470"},
                    "cast_min": {"decimal": "-546478.0", "text": "-546478"},
                    "cast_max": {"decimal": "4561313648948.0", "text": "4561313648948"},
                    "cast_cast": {"decimal/float": "4.5613e+12", "smallint/text": "-11980"},
                },
            ],
            [-546478, {"cast": {"decimal": "-546478.0", "text": "-546478"}}],
        ],
        "float": [[55646.234556984, {"cast": {"int": 55646, "double": "55646.23438"}}]],
        "timestamp": [["'2017-12-27 11:57:42+0000'", {"cast": {"date": "2017-12-27", "text": "2017-12-27T11:57:42.000Z"}}]],
    },
}


# Test casts from columns of each type in POSITIVE_VALUES.
# Translated from scylla-dtest's cql_cast_test.py::TestCQLCast tests
# test_cast_{varint,tinyint,smallint,int,bigint,float,double,decimal,date,
# time,timestamp,uuid,boolean,ascii,inet,timeuuid}.
# The checks of avg() of casts to decimal of columns of these types are done
# in test_cast_avg_to_decimal() below instead.
AVG_TO_DECIMAL_TYPES = ['int', 'smallint', 'tinyint']

def is_avg_to_decimal(kind, to_type):
    return kind == 'avg' and to_type == 'decimal'

def create_cast_table(cql, keyspace, from_type):
    return new_test_table(cql, keyspace, f'k int PRIMARY KEY, {from_type}_clmn {from_type}')

def insert_cast_values(cql, table, from_type):
    for k, (value, _) in enumerate(POSITIVE_VALUES[from_type]):
        cql.execute(f'INSERT INTO {table} (k, {from_type}_clmn) VALUES ({k}, {value})')

@pytest.mark.parametrize('from_type', [t for t in POSITIVE_VALUES if t != 'udt'])
def test_cast_from(cql, test_keyspace, from_type):
    include = None
    if from_type in AVG_TO_DECIMAL_TYPES:
        include = lambda kind, to_type: not is_avg_to_decimal(kind, to_type)
    with create_cast_table(cql, test_keyspace, from_type) as table:
        insert_cast_values(cql, table, from_type)
        check_casts(cql, table, f'{from_type}_clmn', from_type, POSITIVE_VALUES[from_type], include)

# Test avg() of casts to decimal of integer columns. The average of these
# decimals, whose values are integers, is not necessarily an integer: for
# example, the average of the int values is exactly -2044.4, which is what
# Scylla returns. Cassandra returns -2044 - and for the tinyint values,
# whose average is 11.333..., it returns 12, which isn't even the nearest
# integer. This is CASSANDRA-18470: Cassandra updates a running average whose
# precision is that of the values, instead of dividing the sum by the count.
# test_aggregate.py::test_avg_decimal and test_avg_decimal_2 also mark this
# Cassandra behavior as a bug, so this test is marked cassandra_bug.
# Note that Scylla's precision for such averages is also questionable (issue
# #13601): it returns the tinyint values' average as 11.3, and that is the
# value this test expects.
# This check was part of the tests test_cast_{int,smallint,tinyint} in
# scylla-dtest's cql_cast_test.py::TestCQLCast.
@pytest.mark.parametrize('from_type', AVG_TO_DECIMAL_TYPES)
def test_cast_avg_to_decimal(cql, test_keyspace, from_type, cassandra_bug):
    with create_cast_table(cql, test_keyspace, from_type) as table:
        insert_cast_values(cql, table, from_type)
        check_casts(cql, table, f'{from_type}_clmn', from_type, POSITIVE_VALUES[from_type], is_avg_to_decimal)

# Test casts from fields of a user-defined type, with the field types and
# values in POSITIVE_VALUES['udt'].
# Translated from scylla-dtest's cql_cast_test.py::TestCQLCast::test_cast_udt.
def test_cast_from_udt_fields(cql, test_keyspace):
    fields = POSITIVE_VALUES['udt']
    with new_type(cql, test_keyspace, '(' + ', '.join(f'{t}_clmn {t}' for t in fields) + ')') as typ, \
         new_test_table(cql, test_keyspace, f'k int PRIMARY KEY, udt_clmn frozen<{typ}>') as table:
        for k in range(max(len(rows) for rows in fields.values())):
            values = ', '.join(f'{t}_clmn: {rows[k][0]}' for t, rows in fields.items() if len(rows) > k)
            cql.execute(f'INSERT INTO {table} (k, udt_clmn) VALUES ({k}, {{{values}}})')
        for from_type, rows in fields.items():
            check_casts(cql, table, f'udt_clmn.{from_type}_clmn', from_type, rows)

# Test that the error messages for invalid casts - from text to int, uuid,
# ascii and inet, also inside aggregates, and from date to int - don't
# expose internal details such as Cassandra's Java class names: Scylla
# reports, for example, "org.apache.cassandra.db.marshal.UTF8Type cannot be
# cast to org.apache.cassandra.db.marshal.Int32Type".
# Reproduces issue #3108.
# Translated from scylla-dtest's cql_cast_test.py::TestCQLCast::test_cast_negative,
# which was skipped until issue #3108 is fixed. It expected Cassandra's exact
# error messages, which list the signatures of a function in an order that
# changes between Cassandra versions, so this test only checks that the
# messages don't contain Java class names. The dtest couldn't have passed even
# if #3108 were fixed: its text value wasn't quoted, so it was never inserted,
# and its date checks were malformed.
@pytest.mark.xfail(reason='issue #3108')
def test_cast_invalid_error_message(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, text_clmn text, date_clmn date') as table:
        cql.execute(f"INSERT INTO {table} (k, text_clmn, date_clmn) VALUES (0, 'jhkhkjh', '2015-12-23')")
        for expression in ['cast(text_clmn as int)', 'cast(text_clmn as uuid)', 'cast(text_clmn as ascii)',
                           'avg(cast(text_clmn as int))', 'sum(cast(text_clmn as float))',
                           'min(cast(text_clmn as inet))', 'max(cast(text_clmn as uuid))',
                           'cast(date_clmn as int)']:
            with pytest.raises(InvalidRequest) as e:
                cql.execute(f'SELECT {expression} FROM {table}')
            assert 'org.apache.cassandra' not in str(e.value), expression
