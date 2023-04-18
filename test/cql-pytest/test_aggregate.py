# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests aggregation functions COUNT(), MIN(), MAX(), SUM()
#############################################################################

import pytest
import math
from util import new_test_table, unique_key_int, project
from cassandra.util import Date

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v int, d double, PRIMARY KEY (p, c)") as table:
        yield table

# When there is no row matching the selection, the count should be 0.
# First check a "=" expression matching no row:
def test_count_empty_eq(cql, table1):
    p = unique_key_int()
    assert [(0,)] == list(cql.execute(f"select count(*) from {table1} where p = {p}"))
    # The aggregation "0" for no results is true for count(), but not all
    # aggregators - min() returns null for no results, while sum() and
    # avg() return 0 for no results (see discussion in issue #13027):
    assert [(None,)] == list(cql.execute(f"select min(v) from {table1} where p = {p}"))
    assert [(0,)] == list(cql.execute(f"select sum(v) from {table1} where p = {p}"))
    assert [(0,)] == list(cql.execute(f"select avg(v) from {table1} where p = {p}"))

# Above in test_count_empty_eq() we tested that aggregates return some value -
# sometimes 0 and sometimes null - when aggregating no data. If we select
# not just the aggregate but also something else like p, the result is even
# more bizarre: we get a null for p. One might argue that if there are no
# rows we should have just got back nothing in return - just like Cassandra
# does for GROUP BY (see below tests for #12477), but Cassandra doesn't do
# this, and this test accepts Cassandra's (and Scylla's) behavior here as
# being correct.
# See issue #13027.
def test_count_and_p_empty_eq(cql, table1):
    p = unique_key_int()
    assert [(None,0)] == list(cql.execute(f"select p, count(*) from {table1} where p = {p}"))
    assert [(None,None)] == list(cql.execute(f"select p, min(v) from {table1} where p = {p}"))
    assert [(None,0)] == list(cql.execute(f"select p, sum(v) from {table1} where p = {p}"))

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
    assert [(0,0,None,0)] == list(cql.execute(f"select count(*), count(v), min(v), sum(v) from {table1} where p = null"))

# Another special case of a query which matches no partition - an IN with
# an empty list. Reproduces #12475.
def test_count_empty_in(cql, table1):
    assert [(0,)] == list(cql.execute(f"select count(*) from {table1} where p in ()"))
    assert [(0,0,None,0)] == list(cql.execute(f"select count(*), count(v), min(v), sum(v) from {table1} where p in ()"))

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

# Regression-test for #7729.
def test_timeuuid(cql, test_keyspace):
    schema = "a int, b timeuuid, primary key (a,b)"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f'insert into {table} (a, b) values (0, 13814000-1dd2-11ff-8080-808080808080)')
        cql.execute(f'insert into {table} (a, b) values (0, 6b1b3620-33fd-11eb-8080-808080808080)')
        assert project('system_todate_system_min_b',
                       cql.execute(f'select todate(min(b)) from {table} where a = 0')) == [Date('2020-12-01')]
        assert project('system_todate_system_max_b',
                       cql.execute(f'select todate(max(b)) from {table} where a = 0')) == [Date('2038-09-06')]

# Check floating-point aggregations with non-finite results - inf and nan.
# Reproduces issue #13551: sum of +inf and -inf produced an error instead
# of a NaN as in Cassandra (and in older Scylla).
def test_aggregation_inf_nan(cql, table1):
    p = unique_key_int()
    # Add a single infinity value, see we can read it back as infinity,
    # and its sum() is also infinity of course.
    cql.execute(f"insert into {table1} (p, c, d) values ({p}, 1, infinity)")
    assert [(math.inf,)] == list(cql.execute(f"select d from {table1} where p = {p} and c = 1"))
    assert [(math.inf,)] == list(cql.execute(f"select sum(d) from {table1} where p = {p}"))
    # Add a second value, a negative infinity. See we can read it back, and
    # now the sum of +Inf and -Inf should be a NaN.
    # Note that we have to use isnan() to check that the result is a NaN,
    # because equality check doesn't work on NaN:
    cql.execute(f"insert into {table1} (p, c, d) values ({p}, 2, -infinity)")
    assert [(-math.inf,)] == list(cql.execute(f"select d from {table1} where p = {p} and c = 2"))
    assert math.isnan(list(cql.execute(f"select sum(d) from {table1} where p = {p}"))[0][0])
