# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests aggregation functions COUNT(), MIN(), MAX(), SUM()
#############################################################################

import pytest
import math
from decimal import Decimal
from contextlib import contextmanager
from .util import new_named_table, new_test_table, unique_key_int, project, new_type, new_cql, unique_name
from cassandra.query import SimpleStatement
from cassandra.util import Date
from cassandra.protocol import InvalidRequest, SyntaxException
from .cassandra_tests.porting import assert_invalid_message


# The dtests' sessions USE their keyspace and create user types by their
# unqualified name; this creates one so and yields its qualified name.
@contextmanager
def new_type_after_use(cql, keyspace, name, fields):
    with new_cql(cql) as session:
        session.execute(f"USE {keyspace}")
        session.execute(f"CREATE TYPE {name} {fields}")
    try:
        yield f"{keyspace}.{name}"
    finally:
        cql.execute(f"DROP TYPE {keyspace}.{name}")

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

# count(1) counts rows like count(*), and its result column is named "count"
# like count(*)'s: Cassandra prepares count(<literal>) as count(*). Scylla once named it
# "system.count(1)", which broke clients reading the column by name
# (lwt_banking_load_test in dtest). The same holds for any non-null constant.
def test_count_1_column_name(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, None])
    for q in ['count(*)', 'count(1)', 'count(0)', "count('abc')", 'count(true)']:
        row = cql.execute(f"select {q} from {table1} where p = {p}").one()
        assert row._fields == ('count',)
        assert row.count == 2

# SELECT JSON names the key like the result column, so count(1)'s key is "count".
def test_count_1_json_key(cql, table1):
    p = unique_key_int()
    cql.execute(f"insert into {table1} (p, c, v) values ({p}, 1, 1)")
    assert cql.execute(f"select json count(1) from {table1} where p = {p}").one()[0] == '{"count": 1}'

# A count over the whole table takes a different (parallelized) path in Scylla
# than a count in one partition. Its result column is named "count" too.
def test_count_1_column_name_whole_table(cql, table1):
    cql.execute(f"insert into {table1} (p, c, v) values ({unique_key_int()}, 1, 1)")
    row = cql.execute(f"select count(1) from {table1}").one()
    assert row._fields == ('count',)
    assert row.count >= 1

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

# An aggregate function has nothing to aggregate over in an INSERT, so the
# statement must be rejected when it is prepared, rather than fail when the
# aggregate is evaluated. This is as true of a primary-key column as it is of
# a regular one. We deliberately call prepare(), and not execute(), so that
# the test can only pass if it is the preparation that fails.
def test_reject_aggregates_in_insert(cql, table1):
    for insert in [f'INSERT INTO {table1} (p, c) VALUES (sum((int)4), 1)',
                   f'INSERT INTO {table1} (p, c) VALUES (1, sum((int)4))',
                   f'INSERT INTO {table1} (p, c, v) VALUES (1, 1, sum((int)4))']:
        with pytest.raises(InvalidRequest, match='Aggregation'):
            cql.prepare(insert)

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

# An aggregate's argument may use a bind variable, e.g. max(intasblob(?)).
# The argument is evaluated for each row aggregated, and Scylla used to crash
# (null pointer dereference) doing it, with any value bound - so an
# unprivileged user could crash a node with one prepared statement.
# Reproduces SCYLLADB-5622.
def test_aggregate_of_bind_variable(cql, table1):
    p = unique_key_int()
    cql.execute(f"insert into {table1} (p, c, v) values ({p}, 1, 1)")
    cql.execute(f"insert into {table1} (p, c, v) values ({p}, 2, 2)")
    stmt = cql.prepare(f"select max(intasblob(?)), count(intasblob(?)) from {table1} where p = ?")
    assert [(b'\x00\x00\x00\x07', 2)] == list(cql.execute(stmt, [7, 7, p]))
    # With GROUP BY, each group's result is computed when the group ends,
    # not when the query ends.
    stmt = cql.prepare(f"select c, max(intasblob(?)) from {table1} where p = ? group by c")
    assert [(1, b'\x00\x00\x00\x07'), (2, b'\x00\x00\x00\x07')] == list(cql.execute(stmt, [7, p]))

# The result of an aggregate may also be used with a bind variable, e.g.
# max(m)[?], which is evaluated once for each result row, after aggregating
# its rows. Scylla used to crash doing it too.
# Reproduces SCYLLADB-5622.
def test_bind_variable_on_aggregate(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, m frozen<map<int, int>>, PRIMARY KEY (p, c)") as table:
        p = unique_key_int()
        cql.execute(f"insert into {table} (p, c, m) values ({p}, 1, {{1: 10, 2: 20}})")
        cql.execute(f"insert into {table} (p, c, m) values ({p}, 2, {{1: 11}})")
        stmt = cql.prepare(f"select max(m)[?] from {table} where p = ?")
        assert [(11,)] == list(cql.execute(stmt, [1, p]))
        # With GROUP BY, there is a result row for each group.
        stmt = cql.prepare(f"select c, max(m)[?] from {table} where p = ? group by c")
        assert [(1, 10), (2, 11)] == list(cql.execute(stmt, [1, p]))

# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_min_and_max_on_sets_and_udt.
# min() and max() on a set<ascii> column, on a UDT column, and on set<int>
# and set<blob> columns compare values lexicographically. Reproduces #6768.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_min_max_set_ascii_and_udt(cql, test_keyspace):
    with new_type_after_use(cql, test_keyspace, unique_name(), "(first text, second int, third int)") as typ, \
         new_test_table(cql, test_keyspace, f"id int PRIMARY KEY, s1 set<int>, s2 set<blob>, my_asc set<ascii>, my_type {typ}") as table:
        cql.execute(f"INSERT INTO {table} (id, my_asc) VALUES (1, {{'hi', 'im', 'ascii'}})")
        cql.execute(f"INSERT INTO {table} (id, my_asc) VALUES (2, {{'im', 'ascii', 'too'}})")
        assert list(cql.execute(f"select max(my_asc) FROM {table}"))[0][0] == {'ascii', 'im', 'too'}
        assert list(cql.execute(f"select min(my_asc) FROM {table}"))[0][0] == {'ascii', 'hi', 'im'}

        cql.execute(f"INSERT INTO {table} (id, s1, s2, my_type) VALUES (1, {{-1, 1}}, {{0xff, 0x01}}, {{first:'a', second: 2, third: 3}})")
        cql.execute(f"INSERT INTO {table} (id, s1, s2, my_type) VALUES (2, {{-2, 2}}, {{0xfe, 0x02}}, {{first:'b', second: 5, third: 6}})")
        mx = list(cql.execute(f"select max(my_type) FROM {table}"))[0][0]
        mn = list(cql.execute(f"select min(my_type) FROM {table}"))[0][0]
        assert (mx.first, mx.second, mx.third) == ('b', 5, 6)
        assert (mn.first, mn.second, mn.third) == ('a', 2, 3)

        assert [tuple(r) for r in cql.execute(f"select max(s1), max(s2) FROM {table}")] == [({-1, 1}, {b"\x02", b"\xfe"})]

# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_large_count.
# COUNT(*) must add up the rows of all pages when the rows span several
# pages: with a 10000-row page size, count just below, at and just above
# the page size, and a bit after that.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_count_paged(cql, test_keyspace):
    def count(table):
        return list(cql.execute(SimpleStatement(f"SELECT COUNT(*) FROM {table}", fetch_size=10000)))
    with new_test_table(cql, test_keyspace, "k int, v int, PRIMARY KEY (k)") as table:
        for k in range(1, 10000):
            cql.execute(f"INSERT INTO {table}(k) VALUES ({k})")
        assert count(table) == [(9999,)]
        cql.execute(f"INSERT INTO {table}(k) VALUES (10000)")
        assert count(table) == [(10000,)]
        cql.execute(f"INSERT INTO {table}(k) VALUES (10001)")
        assert count(table) == [(10001,)]
        for k in range(10002, 15001):
            cql.execute(f"INSERT INTO {table}(k) VALUES ({k})")
        assert count(table) == [(15000,)]


# Tests ported from scylla-dtest's cql_additional_tests.py. Like the dtests,
# they run in a keyspace with vnodes, and keep the dtests' table names.

# COUNT(*) of a partition counts rows also when a column is null, and
# COUNT(1) works with IN on the partition key.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_count_rows_with_nulls_and_in(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "events", "kind text, time int, value1 int, value2 int, PRIMARY KEY(kind, time)") as table:
        full = f"INSERT INTO {table} (kind, time, value1, value2) VALUES ('ev1', {{}}, {{}}, {{}})"
        no_v2 = f"INSERT INTO {table} (kind, time, value1) VALUES ('ev1', {{}}, {{}})"
        cql.execute(full.format(0, 0, 0))
        cql.execute(full.format(1, 1, 1))
        cql.execute(no_v2.format(2, 2))
        cql.execute(full.format(3, 3, 3))
        cql.execute(no_v2.format(4, 4))
        cql.execute(f"INSERT INTO {table} (kind, time, value1, value2) VALUES ('ev2', 0, 0, 0)")
        assert list(cql.execute(f"SELECT COUNT(*) FROM {table} WHERE kind = 'ev1'")) == [(5,)]
        assert list(cql.execute(f"SELECT COUNT(1) FROM {table} WHERE kind IN ('ev1', 'ev2') AND time=0")) == [(2,)]

# Aggregates selected together with a plain column, which takes its value
# from the first row, with filtering, and on one partition.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_aggregates_with_plain_column(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "together", "a int, b int, c int, PRIMARY KEY ((a), c)") as table:
        for row in [(1, 2, 3), (2, 4, 6), (3, 6, 9), (3, 8, 10)]:
            cql.execute(f"INSERT INTO {table} (a, b, c) VALUES {row}")
        assert list(cql.execute(f"SELECT sum(c), avg(b), min(c), a  FROM {table} WHERE b>2 ALLOW FILTERING")) == [(25, 6, 6, 2)]
        assert list(cql.execute(f"SELECT count(c), max(b)  FROM {table} WHERE a = 3 ")) == [(2, 8)]
