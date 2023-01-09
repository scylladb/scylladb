# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the COUNT() aggregation function
#############################################################################

import pytest
from util import new_test_table, unique_key_int

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        yield table

# When there is no row matching the selection, the count should be 0.
# First check a "=" expression matching no row:
def test_count_empty_eq(cql, table1):
    p = unique_key_int()
    assert [(0,)] == list(cql.execute(f"select count(*) from {table1} where p = {p}"))
    # The aggregation "0" for no results is true for count(), but not all
    # aggregators - min() returns null for no results:
    assert [(None,)] == list(cql.execute(f"select min(v) from {table1} where p = {p}"))

# The query "p = null" also matches no row so should have a count 0, but
# it's a special case where no partition belongs to the query range so the
# aggregator doesn't need to send the query to any node. This reproduces
# issue #12475.
# This test is scylla_only because Cassandra doesn't support "p = null".
# it does support "p IN ()" with the same effect, though, so that will
# be the next test.
def test_count_empty_eq_null(cql, table1, scylla_only):
    assert [(0,)] == list(cql.execute(f"select count(*) from {table1} where p = null"))
    # A more complex list of aggregators, some return zero and some null
    # for an empty result set:
    assert [(0,0,None)] == list(cql.execute(f"select count(*), count(v), min(v) from {table1} where p = null"))

# Another special case of a query which matches no partition - an IN with
# an empty list. Reproduces #12475.
def test_count_empty_in(cql, table1):
    assert [(0,)] == list(cql.execute(f"select count(*) from {table1} where p in ()"))
    assert [(0,0,None)] == list(cql.execute(f"select count(*), count(v), min(v) from {table1} where p in ()"))

# Simple test of counting the number of rows in a single partition
def test_count_in_partition(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, 2])
    cql.execute(stmt, [p, 3, 3])
    assert [(3,)] == list(cql.execute(f"select count(*) from {table1} where p = {p}"))

# Using count(v) instead of count(*) allows counting only rows with a set
# value in v
def test_count_specific_column(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, 2])
    cql.execute(stmt, [p, 3, 3])
    cql.execute(stmt, [p, 4, None])
    assert [(4,)] == list(cql.execute(f"select count(*) from {table1} where p = {p}"))
    assert [(3,)] == list(cql.execute(f"select count(v) from {table1} where p = {p}"))

# COUNT can be combined with GROUP BY to count separately for each partition
# or row.
def test_count_and_group_by_row(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, 2])
    cql.execute(stmt, [p, 3, 3])
    cql.execute(stmt, [p, 4, None])
    assert [(p, 1, 1), (p, 2, 1), (p, 3, 1), (p, 4, 0)] == list(cql.execute(f"select p, c, count(v) from {table1} where p = {p} group by p,c"))

def test_count_and_group_by_partition(cql, table1):
    p1 = unique_key_int()
    p2 = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p1, 1, 1])
    cql.execute(stmt, [p1, 2, 2])
    cql.execute(stmt, [p2, 3, 3])
    cql.execute(stmt, [p2, 4, None])
    assert [(p1, 2), (p2, 1)] == list(cql.execute(f"select p, count(v) from {table1} where p in ({p1},{p2}) group by p"))

# In the above tests we looked for per-row or per-partition counts and got
# back more than one count. But if our query matches no row, we should get
# back no count.
@pytest.mark.xfail(reason="issue #12477")
def test_count_and_group_by_row_none(cql, table1):
    p = unique_key_int()
    assert [] == list(cql.execute(f"select p, c, count(v) from {table1} where p = {p} group by p,c"))

@pytest.mark.xfail(reason="issue #12477")
def test_count_and_group_by_partition_none(cql, table1):
    p = unique_key_int()
    assert [] == list(cql.execute(f"select p, count(v) from {table1} where p = {p} group by p"))
