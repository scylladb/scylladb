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

from cassandra.concurrent import execute_concurrent_with_args

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
