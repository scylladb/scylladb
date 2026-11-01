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

import pytest
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.protocol import InvalidRequest

from .util import new_test_table


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
