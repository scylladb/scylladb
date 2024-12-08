# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests aggregation functions COUNT(), MIN(), MAX(), SUM()
#############################################################################

import pytest
import math
from decimal import Decimal
from util import new_test_table, unique_key_int, project, new_type
from cassandra.util import Date
from cassandra.protocol import SyntaxException
from cassandra_tests.porting import assert_invalid_message

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v int, d double, dc decimal, PRIMARY KEY (p, c)") as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, somecolumn int, "SomeColumn" int, "OtherColumn" int, PRIMARY KEY (p, c)') as table:
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

# Using count(v) instead of count(*) allows counting only rows with a non-NULL
# value in v
def test_count_specific_column(cql, test_keyspace, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, 2])
    cql.execute(stmt, [p, 3, 3])
    cql.execute(stmt, [p, 4, None])
    assert [(4,)] == list(cql.execute(f"select count(*) from {table1} where p = {p}"))
    assert [(3,)] == list(cql.execute(f"select count(v) from {table1} where p = {p}"))
    # Check with non-scalar types too, reproduces #14198
    with new_type(cql, test_keyspace, '(i int, t text)') as udt:
        with new_test_table(cql, test_keyspace, f'p int, c int, fli frozen<list<int>>, tup tuple<int, bigint>, udt {udt}, PRIMARY KEY (p, c)') as table2:
            cql.execute(f'INSERT INTO {table2}(p, c, fli, tup, udt) VALUES({p}, 5, [1, 2], (3, 4), {{i: 3, t: \'text\'}})')
            cql.execute(f'INSERT INTO {table2}(p, c) VALUES({p}, 6)')

            assert [(1,)] == list(cql.execute(f"select count(fli) from {table2} where p = {p}"))
            assert [(1,)] == list(cql.execute(f"select count(tup) from {table2} where p = {p}"))
            assert [(1,)] == list(cql.execute(f"select count(udt) from {table2} where p = {p}"))
            assert [(2,)] == list(cql.execute(f"select count(*) from {table2} where p = {p}"))

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
#
# Reproduces #12477
def test_count_and_group_by_row_none(cql, table1):
    p = unique_key_int()
    assert [] == list(cql.execute(f"select p, c, count(v) from {table1} where p = {p} group by p,c"))

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

# When averaging "decimal" (arbitrary-precision floating point) values,
# which precision should the output use? The average of 0, 0, 1 is
# 0.3333333333..., but with how many threes?
# The correct answer isn't clear. As explained in #13601 and CASSANDRA-18470,
# both Scylla and Cassandra took the number of significant digits in the
# average from the number of significant digits in the sum. So for example,
# the average of 1 and 2 is 2 (in Scylla, or 1 in Cassandra), but the average
# of 1.0 and 2.0 is 1.5. Similarly the average of 1.1 and 1.2 is 1.2 (or 1.1),
# not 1.15. This is surprising.
#
# Given that there is no way for the user to configure the desired precision,
# I don't know what would be a "correct" solution. I just feel that the
# scenarios tested in this test - averaging 1 and 2 or 1.1 and 1.2 -
# don't do something that any reasonable user might expect, and will need
# to be fixed.
@pytest.mark.xfail(reason="issue #13601")
def test_avg_decimal(cql, table1, cassandra_bug):
    p = unique_key_int()
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 1, 1.0)")
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 2, 2.0)")
    assert [(Decimal('1.5'),)] == list(cql.execute(f"select avg(dc) from {table1} where p = {p}"))
    p = unique_key_int()
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 1, 1)")
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 2, 2.0)")
    assert [(Decimal('1.5'),)] == list(cql.execute(f"select avg(dc) from {table1} where p = {p}"))
    p = unique_key_int()
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 1, 1.0)")
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 2, 2)")
    assert [(Decimal('1.5'),)] == list(cql.execute(f"select avg(dc) from {table1} where p = {p}"))
    p = unique_key_int()
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 1, 1)")
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 2, 2)")
    # Reproduces #13601: the average of 1 and 2 was rounded up to the
    # integer 2 instead of returning 1.5:
    assert [(Decimal('1.5'),)] == list(cql.execute(f"select avg(dc) from {table1} where p = {p}"))
    # Similarly, average of 1.1 and 1.2 was rounded up to 1.2 instead of
    # returning 1.15.
    p = unique_key_int()
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 1, 1.1)")
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 2, 1.2)")
    assert [(Decimal('1.15'),)] == list(cql.execute(f"select avg(dc) from {table1} where p = {p}"))

# Another test for average of "decimal" values: the average of 1, 2, 2, 3.
# In this case the average is an integer, 2, but Cassandra doesn't even
# get this right (see CASSANDRA-18470) because instead of of keeping the
# integer sum and count separately, it tries to update the average and does
# it in a too-low precision. Scylla passes this test correctly.
def test_avg_decimal_2(cql, table1, cassandra_bug):
    p = unique_key_int()
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 1, 1)")
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 2, 2)")
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 3, 2)")
    cql.execute(f"insert into {table1} (p, c, dc) values ({p}, 4, 3)")
    # Reproduces CASSANDRA-18470:
    assert [(Decimal('2'),)] == list(cql.execute(f"select avg(dc) from {table1} where p = {p}"))

def test_reject_aggregates_in_where_clause(cql, table1):
    assert_invalid_message(cql, table1, 'Aggregation',
                           f'SELECT * FROM {table1} WHERE p = sum((int)4)')

# Reproduces #13265
# Aggregates must handle case-sensitive column names correctly.
def test_sum_case_sensitive_column(cql, table2):
    p = unique_key_int()
    cql.execute(f'insert into {table2} (p, c, somecolumn, "SomeColumn", "OtherColumn") VALUES ({p}, 1, 1, 10, 100)')
    cql.execute(f'insert into {table2} (p, c, somecolumn, "SomeColumn", "OtherColumn") VALUES ({p}, 2, 2, 20, 200)')
    cql.execute(f'insert into {table2} (p, c, somecolumn, "SomeColumn", "OtherColumn") VALUES ({p}, 3, 3, 30, 300)')

    assert cql.execute(f'select sum(somecolumn) from {table2} where p = {p}').one()[0] == 6
    assert cql.execute(f'select sum("somecolumn") from {table2} where p = {p}').one()[0] == 6
    assert cql.execute(f'select sum("SomeColumn") from {table2} where p = {p}').one()[0] == 60
    assert cql.execute(f'select sum(SomeColumn) from {table2} where p = {p}').one()[0] == 6
    assert cql.execute(f'select sum("OtherColumn") from {table2} where p = {p}').one()[0] == 600

    assert_invalid_message(cql, table2, 'someColumn',
                           f'select sum("someColumn") from {table2} where p = {p}')
    assert_invalid_message(cql, table2, 'othercolumn',
                           f'select sum(OtherColumn) from {table2} where p = {p}')

# Check that a simple case of summing up a specific int column in a partition
# works correctly
def test_sum_in_partition(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 4])
    cql.execute(stmt, [p, 2, 5])
    cql.execute(stmt, [p, 3, 6])
    assert [(15,)] == list(cql.execute(f"select sum(v) from {table1} where p = {p}"))

# Check that you *can't* do "sum(*)" expecting (perhaps) sums of all the
# columns. It's a syntax error: The grammar allows "count(*)" because it has
# a separate rule for # "count", but other functions and aggregators (like
# "sum") don't allow "*" and expect a real column name as a parameter.
def test_sum_star(cql, table1):
    p = unique_key_int()
    with pytest.raises(SyntaxException):
        cql.execute(f"select sum(*) from {table1} where p = {p}")
