# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the SELECT requests with various filtering expressions.
# We have a separate test file test_allow_filtering.py, for tests that
# focus on whether the string "ALLOW FILTERING" is needed or not needed
# for a query. In the tests in this file we focus more on the correctness
# of various filtering expressions - regardless of whether ALLOW FILTERING
# is or isn't necessary.

import pytest
import re
import time
from contextlib import contextmanager
from .util import new_named_table, new_test_table, new_type, user_type
from cassandra import ConsistencyLevel
from cassandra.protocol import InvalidRequest
from cassandra.query import UNSET_VALUE, SimpleStatement

# When filtering for "x > 0" or "x < 0", rows with an unset value for x
# should not match the filter.
# Reproduces issue #6295 and its duplicate #8122.
def test_filter_on_unset(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
        cql.execute(f"INSERT INTO {table} (a) VALUES (1)")
        cql.execute(f"INSERT INTO {table} (a, b) VALUES (2, 2)")
        cql.execute(f"INSERT INTO {table} (a, b) VALUES (3, -1)")
        assert list(cql.execute(f"SELECT a FROM {table} WHERE b>0 ALLOW FILTERING")) == [(2,)]
        assert list(cql.execute(f"SELECT a FROM {table} WHERE b<0 ALLOW FILTERING")) == [(3,)]
        cql.execute(f"ALTER TABLE {table} ADD c int")
        cql.execute(f"INSERT INTO {table} (a, b,c ) VALUES (4, 5, 6)")
        assert list(cql.execute(f"SELECT a FROM {table} WHERE c<0 ALLOW FILTERING")) == []
        assert list(cql.execute(f"SELECT a FROM {table} WHERE c>0 ALLOW FILTERING")) == [(4,)]

# Reproducers for issue #8203, which test a scan (whole-table or single-
# partition) with filtering which keeps just the last row, after a long list
# of non-matching rows.
# As usual, the scan is done with paging, and since most rows do not match
# the filter, several empty pages should be returned until finally we get
# the expected matching row. If we allow the Python driver to iterate over
# all results, it should read all these pages and give us the one result.
# The bug is that the iteration stops prematurely (it seems after the second
# empty page) and an empty result set is returned.
# It turns out that this was a bug in the Python driver, not in Scylla,
# which was fixed by
# https://github.com/datastax/python-driver/commit/1d9077d3f4c937929acc14f45c7693e76dde39a9
# So below we check if the driver version is recent enough (the Datastax
# and Scylla versions have different version numbers), and if not, we skip
# this test.

# Reproducer for issue #8203, partition-range (whole-table) scan case
def test_filtering_contiguous_nonmatching_partition_range(cql, test_keyspace, driver_bug_1):
    # The bug depends on the amount of data being scanned passing some
    # page size limit, so it doesn't matter if the reproducer has a lot of
    # small rows or fewer long rows - and inserting fewer long rows is
    # significantly faster.
    count = 100
    long='x'*60000
    with new_test_table(cql, test_keyspace,
            "p int, c text, v int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, v) VALUES (?, '{long}', ?)")
        for i in range(count):
            cql.execute(stmt, [i, i])
        # We want the filter to match only at the end the scan - but we don't
        # know the partition order (we don't want the test to depend on the
        # partitioner). So we first figure out a partition near the end (at
        # some high token), and use that in the filter.
        p, v = list(cql.execute(f"SELECT p, v FROM {table} WHERE TOKEN(p) > 8000000000000000000 LIMIT 1"))[0]
        assert list(cql.execute(f"SELECT p FROM {table} WHERE v={v} ALLOW FILTERING")) == [(p,)]

# Reproducer for issue #8203, single-partition scan case
def test_filtering_contiguous_nonmatching_single_partition(cql, test_keyspace, driver_bug_1):
    count = 100
    long='x'*60000
    with new_test_table(cql, test_keyspace,
            "p int, c int, s text, v int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, v, s) VALUES (1, ?, ?, '{long}')")
        for i in range(count):
            cql.execute(stmt, [i, i])
        # To fail this test, we must select s here. If s is not selected,
        # Scylla won't count its size as part of the 1MB limit, and will not
        # return empty pages - the first page will contain the result.
        assert list(cql.execute(f"SELECT c, s FROM {table} WHERE p=1 AND v={count-1} ALLOW FILTERING")) == [(count-1, long)]

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
        yield table

# Cassandra 6 added support for the "!=" operator in WHERE clauses, for
# filtering (CASSANDRA-18584). Before that, neither Cassandra nor Scylla
# supported "!=" in WHERE (although it exists in the parser, and is allowed
# in other places such as LWT conditions).
# A "!=" restriction always requires filtering, even on a partition key
# column. A row in which the column is null doesn't match "!=".
# Reproduces #12911.
@pytest.mark.xfail(reason="#12911")
def test_operator_ne(cql, test_keyspace, new_to_cassandra_6):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
        cql.execute(f'INSERT INTO {table} (a, b) VALUES (1, 1)')
        cql.execute(f'INSERT INTO {table} (a, b) VALUES (2, 2)')
        cql.execute(f'INSERT INTO {table} (a) VALUES (3)')
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f'SELECT a FROM {table} WHERE b != 0')
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f'SELECT a FROM {table} WHERE a != 0')
        assert sorted(cql.execute(f'SELECT a FROM {table} WHERE b != 0 ALLOW FILTERING')) == [(1,), (2,)]
        assert sorted(cql.execute(f'SELECT a FROM {table} WHERE b != 1 ALLOW FILTERING')) == [(2,)]
        assert sorted(cql.execute(f'SELECT a FROM {table} WHERE a != 1 ALLOW FILTERING')) == [(2,), (3,)]

# Even in Cassandra 6, which supports the "!=" operator in WHERE (see
# test_operator_ne above), it is not supported on the token.
def test_operator_ne_token_not_supported(cql, table1):
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE token(a) != 0')
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE token(a) != 0 ALLOW FILTERING')

# Test that LIKE operator works fine as a filter when the filtered column
# has descending order. Regression test for issue #10183, when it was
# incorrectly rejected as a "non-string" column.
#
# Currently, Cassandra only allows LIKE on a SASI index, *not* when doing
# filtering, so this test will fail on Cassandra. We mark the test with
# "cassandra_bug" because they have an open issue to fix it (CASSANDRA-17198).
# When Cassandra fixes this issue, this mark should be removed.
def test_filter_like_on_desc_column(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, "a int, b text, primary key(a, b)",
            extra="with clustering order by (b desc)") as table:
        cql.execute(f"INSERT INTO {table} (a, b) VALUES (1, 'one')")
        res = cql.execute(f"SELECT b FROM {table} WHERE b LIKE '%%%' ALLOW FILTERING")
        assert res.one().b == "one"

# Test that the fact that a column is indexed does not cause us to fetch
# incorrect results from a filtering query (issue #10300).
def test_index_with_in_relation(scylla_only, cql, test_keyspace):
    schema = 'p int, c int, v boolean, primary key (p,c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"create index on {table}(v)")
        for p, c, v in [(0,0,True),(0,1,False),(0,2,True),(0,3,False),
                (1,0,True),(1,1,False),(1,2,True),(1,3,False),
                (2,0,True),(2,1,False),(2,2,True),(2,3,False)]:
            cql.execute(f"insert into {table} (p,c,v) values ({p}, {c}, {v})")
        res = cql.execute(f"select * from {table} where p in (0,1) and v = False ALLOW FILTERING")
        assert set(res) == set([(0,1,False),(0,3,False),(1,1,False), (1,3,False)])

# Test that IN restrictions are supported with filtering and return the
# correct results.
# We mark this test "cassandra_bug" because Cassandra could support this
# feature but doesn't yet: It reports "IN predicates on non-primary-key
# columns (v) is not yet supported" when v is a regular column, or "IN
# restrictions are not supported when the query involves filtering" on
# partition-key columns p1 or p2. By the way, it does support IN restrictions
# on a clustering-key column.
def test_filtering_with_in_relation(cql, test_keyspace, cassandra_bug):
    schema = 'p1 int, p2 int, c int, v int, primary key ((p1, p2),c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (1, 2, 3, 4)")
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (2, 3, 4, 5)")
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (3, 4, 5, 6)")
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (4, 5, 6, 7)")
        res = cql.execute(f"select * from {table} where p1 in (2,4) ALLOW FILTERING")
        assert set(res) == set([(2,3,4,5), (4,5,6,7)])
        res = cql.execute(f"select * from {table} where p2 in (2,4) ALLOW FILTERING")
        assert set(res) == set([(1,2,3,4), (3,4,5,6)])
        res = cql.execute(f"select * from {table} where c in (3,5) ALLOW FILTERING")
        assert set(res) == set([(1,2,3,4), (3,4,5,6)])
        res = cql.execute(f"select * from {table} where v in (5,7) ALLOW FILTERING")
        assert set(res) == set([(2,3,4,5), (4,5,6,7)])

# Test that NOT IN restrictions are supported with filtering and return the
# correct results.
def test_filtering_with_not_in_relation(cql, test_keyspace):
    schema = 'p1 int, p2 int, c int, v int, primary key ((p1, p2),c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (1, 2, 3, 4)")
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (2, 3, 4, 5)")
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (3, 4, 5, 6)")
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (4, 5, 6, 7)")
        cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES (5, 6, 7, NULL)")
        res = cql.execute(f"select * from {table} where p1 NOT IN (2,4) ALLOW FILTERING")
        assert set(res) == set([(1,2,3,4), (3,4,5,6), (5,6,7,None)])
        res = cql.execute(f"select * from {table} where p2 NOT IN (2,4) ALLOW FILTERING")
        assert set(res) == set([(2,3,4,5), (4,5,6,7), (5,6,7,None)])
        res = cql.execute(f"select * from {table} where c NOT IN (3,5) ALLOW FILTERING")
        assert set(res) == set([(2,3,4,5), (4,5,6,7), (5,6,7,None)])
        res = cql.execute(f"select * from {table} where v NOT IN (5,7) ALLOW FILTERING")
        assert set(res) == set([(1,2,3,4), (3,4,5,6)])
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f"select * from {table} where p1 NOT IN (2,4)")
        # Since NULL is unknown, it could have matched p1, so NOT IN returns NULL too.
        res = cql.execute(f"select * from {table} where v NOT IN (5,NULL) ALLOW FILTERING")
        assert set(res) == set()


# Test that subscripts in expressions work as expected. They should only work
# on map columns, and must have the correct type. Test that they also work
# as expected for null or unset subscripts.
# Cassandra considers the null subscript 'm[null]' to be an invalid request.
# In Scylla we decided to it differently (we think better): m[null] is simply
# a null, so the filter 'WHERE m[null] = 2' is not an error - it just doesn't
# match anything. This is more consistent with our usual null handling
# (null[2] and null < 2 are both defined as returning null), and will also
# allow us in the future to support non-constant subscript - for example m[a]
# where the column a can be null for some rows and non-null for other rows.
# Because we decided that our behavior is better than Cassandra's, this test
# fails on Cassandra and is marked with cassandra_bug.
# This test is a superset of test test_null.py::test_map_subscript_null which
# tests only the special case of a null subscript.
# Reproduces #10361
#
# Also test list subscripts. These also don't work in Cassandra.
def test_filtering_with_subscript(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace,
            "p int, m1 map<int, int>, m2 map<text, text>, s set<int>, l list<text>, PRIMARY KEY (p)") as table:
        # Check for *errors* in subscript expressions - such as wrong type or
        # null - with an empty table. This will force the implementation to
        # check for these errors before actually evaluating the filter
        # expression - because there will be no rows to filter.

        # A subscript is not allowed on a non-map column (in this case, a set)
        with pytest.raises(InvalidRequest, match='cannot be subscripted'):
            cql.execute(f"SELECT p FROM {table} WHERE s[2] = 3 ALLOW FILTERING")
        # A wrong type is passed for the subscript is not allowed
        with pytest.raises(InvalidRequest, match=re.escape('key(m1)')):
            cql.execute(f"select p from {table} where m1['black'] = 2 ALLOW FILTERING")
        with pytest.raises(InvalidRequest, match=re.escape('key(m2)')):
            cql.execute(f"select p from {table} where m2[1] = 2 ALLOW FILTERING")
        # See discussion of m1[null] above. Reproduces #10361, and fails
        # on Cassandra (Cassandra deliberately returns an error here -
        # an InvalidRequest with "Unsupported null map key for column m1"
        assert list(cql.execute(f"select p from {table} where m1[null] = 2 ALLOW FILTERING")) == []
        assert list(cql.execute(f"select p from {table} where m2[null] = 'hi' ALLOW FILTERING")) == []
        # Similar to above checks, but using a prepared statement. We can't
        # cause the driver to send the wrong type to a bound variable, so we
        # can't check that case unfortunately, but we have a new UNSET_VALUE
        # case.
        stmt = cql.prepare(f"select p from {table} where m1[?] = 2 ALLOW FILTERING")
        assert list(cql.execute(stmt, [None])) == []
        # The expression m1[UNSET_VALUE] should be an error, but because the
        # table is empty, we do not actually need to evaluate the expression
        # and the error might might not be caught. So this test is commented
        # out. We'll do it below, after we add some data to ensure that the
        # expression does need to be evaluated.
        #with pytest.raises(InvalidRequest, match='Unsupported unset map key for column m1'):
        #    cql.execute(stmt, [UNSET_VALUE])

        # Finally, check for successful filtering with subscripts. For that we
        # need to add some data:
        cql.execute("INSERT INTO "+table+" (p, m1, m2) VALUES (1, {1:2, 3:4}, {'dog':'cat', 'hi':'hello'})")
        cql.execute("INSERT INTO "+table+" (p, m1, m2) VALUES (2, {2:3, 4:5}, {'man':'woman', 'black':'white'})")
        res = cql.execute(f"select p from {table} where m1[1] = 2 ALLOW FILTERING")
        assert list(res) == [(1,)]
        res = cql.execute(f"select p from {table} where m2['black'] = 'white' ALLOW FILTERING")
        assert list(res) == [(2,)]
        res = cql.execute(stmt, [1])
        assert list(res) == [(1,)]

        # Try again the null-key request (reproduces #10361) that we did
        # earlier when there was no data in the table. Now there is, and
        # the scan brings up several rows, it may exercise different code
        # paths.
        assert list(cql.execute(f"select p from {table} where m1[null] = 2 ALLOW FILTERING")) == []
        with pytest.raises(InvalidRequest, match='unset'):
            cql.execute(stmt, [UNSET_VALUE])

        # check subscripted list filtering
        cql.execute(f"INSERT INTO {table} (p, l) VALUES (11, ['zero', 'one', 'two'])")
        res = cql.execute(f"SELECT p FROM {table} WHERE l[1] = 'one' ALLOW FILTERING")
        assert list(res) == [(11,)]
        res = cql.execute(f"SELECT p FROM {table} WHERE l[1] = 'eight' ALLOW FILTERING")
        assert list(res) == []
        res = cql.execute(f"SELECT p FROM {table} WHERE l[-5] = 'minus five' ALLOW FILTERING")
        assert list(res) == []
        res = cql.execute(f"SELECT p FROM {table} WHERE l[3] = 'three' ALLOW FILTERING")
        assert list(res) == []

        # check list filtering type checking
        with pytest.raises(InvalidRequest, match=re.escape('Invalid STRING constant')):
            cql.execute(f"select * from {table} where l['hi'] = 'hi' ALLOW FILTERING")
        with pytest.raises(InvalidRequest, match=re.escape('Invalid INTEGER constant')):
            cql.execute(f"select * from {table} where l[3] = 3 ALLOW FILTERING")

        # check that NULL lists or NULL indexes don't confuse the system
        cql.execute(f"INSERT INTO {table} (p) VALUES (12)")
        res = cql.execute(f"SELECT p FROM {table} WHERE l[1] = 'one' ALLOW FILTERING")
        assert list(res) == [(11,)]
        res = cql.execute(f"SELECT p FROM {table} WHERE l[NULL] = 'one' ALLOW FILTERING")
        assert list(res) == []

# Beyond the tests of map subscript expressions above, also test what happens
# when the expression is fine (e.g., m[2] = 3) but the *data* itself is null.
# We used to have a bug there where we attempted to incorrectly deserialize
# this null and get marshaling errors or even crashes - see issue #10417.
# This test reproduces #10417, but not always - run with "--count" to
# reproduce failures.
def test_filtering_null_map_with_subscript(cql, test_keyspace):
    schema = 'p text primary key, m map<int, int>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (p) VALUES ('dog')")
        assert list(cql.execute(f"SELECT p FROM {table} WHERE m[2] = 3 ALLOW FILTERING")) == []

# Test whether it is allowed to specify more than on restriction on the same
# column. For example, "c=0 and c>0".
# Cassandra does *not* allow such expressions, giving errors such as
# "Column "c" cannot be restricted by both an equality and an inequality
# relation", "More than one restriction was found for the start bound on
# c", "c cannot be restricted by more than one relation if it includes a
# IN", and so on.
#
# Scylla chose to *allow* such expressions. In that case, we need to verify
# that it at least gives the correct results - if the two restrictions
# conflict, they result list should be happy, but if they overlap without
# conflicting the result list can be non-empty.
# This test is marked scylla_only because the support for such expressions
# is a Scylla extension.
def test_multiple_restrictions_on_same_column(cql, test_keyspace, scylla_only):
    schema = 'p int, c int, primary key (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        p = 1
        cql.execute(f"INSERT INTO {table} (p,c) VALUES ({p}, 0)")
        cql.execute(f"INSERT INTO {table} (p,c) VALUES ({p}, 1)")
        cql.execute(f"INSERT INTO {table} (p,c) VALUES ({p}, 2)")
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c = 0 and c > 0")) == []
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c > 0 and c = 0")) == []
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c = 1 and c > 0")) == [(1,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c > 0 and c = 1")) == [(1,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c = 1 and c = 1")) == [(1,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c = 0 and c = 1")) == []
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c > 0 and c > 1")) == [(2,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c > 1 and c > 0")) == [(2,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c = 1 and c >= 1")) == [(1,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c >= 1 and c = 1")) == [(1,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c > 1 and c < 1")) == []
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c >= 1 and c <= 1")) == [(1,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c >= 1 and c <= 2")) == [(1,),(2,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c = 1 and c in (2, 3)")) == []
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} and c = 2 and c in (2, 3)")) == [(2,)]

# Cassandra does not allow IN restrictions on non-primary-key columns,
# even when doing filtering (ALLOW FILTERING). Scylla does support this
# case, but in this test let's check that it returns correct results.
# Cassandra consider this a bug (the message say "not yet supported")
# so we mark this test cassandra_bug (maybe one day it will start working
# on Cassandra).
def test_filter_in_restriction(cql, test_keyspace, cassandra_bug):
    schema = 'pk int, ck int, x int, PRIMARY KEY (pk, ck)'
    with new_test_table(cql, test_keyspace, schema) as table:
        stmt = cql.prepare(f'INSERT INTO {table} (pk, ck, x) VALUES (?, ?, ?)')
        for i in range(3):
            cql.execute(stmt, [1, i, i*2])
        assert [(1,), (2,)] == list(cql.execute(f'SELECT ck FROM {table} WHERE x IN (2, 4) ALLOW FILTERING'))
        assert [(1,)] == list(cql.execute(f'SELECT ck FROM {table} WHERE x IN (2, 7) ALLOW FILTERING'))
        assert [] == list(cql.execute(f'SELECT ck FROM {table} WHERE x IN (3, 7) ALLOW FILTERING'))

        prepared_select1 = cql.prepare(f'SELECT ck FROM {table} WHERE x IN (?, 4) ALLOW FILTERING')
        assert [(1,), (2,)] == list(cql.execute(prepared_select1, [2]))

        prepared_select2 = cql.prepare(f'SELECT ck FROM {table} WHERE x IN ? ALLOW FILTERING')
        assert [(1,), (2,)] == list(cql.execute(prepared_select2, [[2, 4]]))


# Both Cassandra and Scylla allow filtering restrictions on frozen UDTs,
# and the "frozen=True" case of the following test verifies their behavior
# is the same in this case.
# Non-frozen UDTs could also theoretically behave the same - but they are
# currently not allowed in Cassandra (this was decided in CASSANDRA-13247).
# Scylla, however, does allow filtering on non-frozen UDTs so the
# "frozen=False" case of the following test verifies that they behave just
# like frozen ones.
# The non-frozen ("frozen=False") case is expected to fail on Cassandra
# but the frozen case is expected to pass.
def test_filter_UDT_restriction_frozen(cql, test_keyspace):
    do_test_filter_UDT_restriction(cql, test_keyspace, frozen=True)
def test_filter_UDT_restriction_nonfrozen(cql, test_keyspace, cassandra_bug):
    do_test_filter_UDT_restriction(cql, test_keyspace, frozen=False)
def do_test_filter_UDT_restriction(cql, test_keyspace, frozen):
    # Single-integer UDT, should be comparable like a normal integer:
    with new_type(cql, test_keyspace, "(a int)") as typ:
        ftyp = f"frozen<{typ}>" if frozen else typ
        schema = f"pk int, ck int, x {ftyp}, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema) as table:
            stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, x) VALUES (?, ?, ?)")
            for i in range(5):
                cql.execute(stmt, [1, i, user_type("a", i*2)])
            stmt = cql.prepare(f"SELECT ck FROM {table} WHERE x = ? ALLOW FILTERING")
            assert [(2,)] == list(cql.execute(stmt, [user_type("a", 4)]))
            assert [] == list(cql.execute(stmt, [user_type("a", 3)]))
            stmt = cql.prepare(f"SELECT ck FROM {table} WHERE x < ? ALLOW FILTERING")
            assert [(0,), (1,)] == list(cql.execute(stmt, [user_type("a", 4)]))
            assert [] == list(cql.execute(stmt, [user_type("a", -1)]))
    # UDT with two integers. EQ operator is obvious, LT is lexicographical
    with new_type(cql, test_keyspace, "(a int, b int)") as typ:
        ftyp = f"frozen<{typ}>" if frozen else typ
        schema = f"pk int, ck int, x {ftyp}, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema) as table:
            stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, x) VALUES (?, ?, ?)")
            for i in range(5):
                cql.execute(stmt, [1, i, user_type("a", i*2, "b", i*3)])
            stmt = cql.prepare(f"SELECT ck FROM {table} WHERE x = ? ALLOW FILTERING")
            assert [(2,)] == list(cql.execute(stmt, [user_type("a", 4, "b", 6)]))
            assert [] == list(cql.execute(stmt, [user_type("a", 4, "b", 5)]))
            stmt = cql.prepare(f"SELECT ck FROM {table} WHERE x < ? ALLOW FILTERING")
            assert [(0,), (1,)] == list(cql.execute(stmt, [user_type("a", 4, "b", 6)]))
            assert [(0,), (1,), (2,)] == list(cql.execute(stmt, [user_type("a", 4, "b", 7)]))
            assert [] == list(cql.execute(stmt, [user_type("a", -1, "b", 7)]))

# Reproducer for issue #12102, checking the meaning of fetch_size (page size
# in paging) during a scan with filtering. We check both scanning a regular
# table, and a secondary index.
# The question to be tested is whether fetch_size counts the number of rows
# before filtering, after filtering, or something else. As noted in #12102,
# in Cassandra the behavior is the former - and this is what this test
# expects. In Scylla it's currently the latter, causing this test to fail.
@pytest.mark.parametrize("use_index", [
        pytest.param(True, marks=pytest.mark.xfail(reason="#12102")),
        pytest.param(False, marks=pytest.mark.xfail(reason="#12102"))])
def test_filter_and_fetch_size(cql, test_keyspace, use_index, driver_bug_1):
    with new_test_table(cql, test_keyspace, 'pk int primary key, x int, y int') as table:
        if use_index:
            cql.execute(f'CREATE INDEX ON {table}(x)')
        stmt = cql.prepare(f'INSERT INTO {table} (pk, x, y) VALUES (?, ?, ?)')
        cql.execute(stmt, [0, 1, 0])
        cql.execute(stmt, [1, 1, 1])
        cql.execute(stmt, [2, 1, 0])
        cql.execute(stmt, [3, 1, 1])
        cql.execute(stmt, [4, 1, 0])
        cql.execute(stmt, [5, 1, 1])
        cql.execute(stmt, [6, 1, 0])
        cql.execute(stmt, [7, 1, 1])
        # Fetch a filtered page with fetch_size=3 and expect to see 3 rows
        # in the post-filtering results.
        s = cql.prepare(f'SELECT pk FROM {table} WHERE x = 1 AND y = 0 ALLOW FILTERING')
        s.fetch_size = 3
        results = cql.execute(s)
        assert len(results.current_rows) == 3

# Test filtering on a regular column, which is not selected, when only a
# static column is selected. The result should have one row for each
# clustering row that matches the filter, not one row for each partition,
# also when the matches are in several partitions with different static
# values and paging splits the results.
# The similar case where a secondary index is used instead of filtering is
# tested in test_static.py::test_static_column_via_regular_index_multiple_partitions
def test_filter_regular_column_select_only_static(cql, test_keyspace):
    schema = 'p int, c int, s int static, v int, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        insert = cql.prepare(f'INSERT INTO {table} (p, c, s, v) VALUES (?, ?, ?, ?)')
        cql.execute(insert, [1, 1, 11, 1])
        cql.execute(insert, [1, 2, 11, 2])
        cql.execute(insert, [2, 3, 22, 3])
        cql.execute(insert, [2, 4, 22, 4])
        for fetch_size in [None, 1, 2]:
            stmt = SimpleStatement(f'SELECT s FROM {table} WHERE v >= 2 ALLOW FILTERING', fetch_size=fetch_size)
            assert sorted(cql.execute(stmt)) == [(11,), (22,), (22,)]
            stmt = SimpleStatement(f'SELECT s FROM {table} WHERE v > 2 ALLOW FILTERING', fetch_size=fetch_size)
            assert sorted(cql.execute(stmt)) == [(22,), (22,)]
            stmt = SimpleStatement(f'SELECT s FROM {table} WHERE v >= 2 AND v <= 3 ALLOW FILTERING', fetch_size=fetch_size)
            assert sorted(cql.execute(stmt)) == [(11,), (22,)]

# token() function should either take all partition key components or none of them,
# if the key(s) are specified, they should be listed in the partition key order
# Reproduces #13468
def test_filter_token(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, x int, PRIMARY KEY (pk, ck)') as table:
        # In this case token() expects just a single argument, if we pass
        # it the same one twice we get different errors in ScyllaDB and
        # Cassandra - in ScyllaDB we get "Invalid number of arguments",
        # Cassandra complains that the two arguments are the same - "The
        # token() function contains duplicate partition key components":
        with pytest.raises(InvalidRequest, match='Invalid number of arguments|duplicate partition key'):
            cql.execute(f'SELECT pk FROM {table} WHERE token(pk, pk) = 0')
    with new_test_table(cql, test_keyspace, 'pk1 int, pk2 int, x int, PRIMARY KEY ((pk1, pk2))') as table:
        # In the following cases, token() expects two arguments but they
        # should be the two partition key columns, not something else like
        # fewer columns or the same column twice. ScyllaDB and Cassandra
        # print different error messages: ScyllaDB complains about the
        # specific problem in the request (e.g., duplicate pk1) while
        # Cassandra makes a general complaint about the that token() "must
        # be applied to all partition key components or none of them".
        with pytest.raises(InvalidRequest, match='duplicate partition key|must be applied to all partition key components or none of them'):
            cql.execute(f'SELECT pk1 FROM {table} WHERE token(pk1, pk1) = 0')
        with pytest.raises(InvalidRequest, match='Invalid number of arguments|must be applied to all partition key components or none of them'):
            cql.execute(f'SELECT pk1 FROM {table} WHERE token(x) = 0')
        with pytest.raises(InvalidRequest, match='Invalid number of arguments|must be applied to all partition key components or none of them'):
            cql.execute(f'SELECT pk1 FROM {table} WHERE token(pk1) = 0')
        with pytest.raises(InvalidRequest, match='partition key order'):
            cql.execute(f'SELECT pk1 FROM {table} WHERE token(pk2, pk1) = 0')


# In a query with a token restriction the name assigned to the bind marker should be "partition key token".
# Java driver relies on this assumption, having a different name there breaks it.
# Reproduces #13769
def test_token_bind_marker_name(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p1 int, p2 int, c1 int, c2 int, r int, PRIMARY KEY ((p1, p2), c1, c2)') as table:
        stmt = cql.prepare(f'SELECT * FROM {table} WHERE token(p1, p2) = ?')
        assert stmt.column_metadata[0].name == 'partition key token'


# In the following test we check what error message appears if multi-column relation contains null values.    
# Reproduces #13217
def test_multi_column_relation_tuples_null_check(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b, c, d)') as table:
         with pytest.raises(InvalidRequest, match='Invalid null value in condition for column'): 
                cql.execute(f'SELECT * FROM {table} WHERE a = 0 AND (b, c, d) IN ((null, 2, 3), (2, 1, 4))')
         with pytest.raises(InvalidRequest, match='Invalid null value in condition for column'): 
                cql.execute(f'SELECT * FROM {table} WHERE a = 0 AND (b, c, d) IN ((1, 2, 3), (2, 1, null))')      

# This test shows how Scylla handles invalid number of items in one of the tuples of multi-column relation.
# Reproduces #13217
# Reproduces #13241
def test_multi_column_relation_wrong_number_of_items_in_tuple(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b, c, d)') as table:
        with pytest.raises(InvalidRequest, match='Expected 3 elements in value tuple, but got 2'):
            cql.execute(f'SELECT * FROM {table} WHERE a = 0 AND (b, c, d) IN ((1, 2), (2, 1, 4))')
        with pytest.raises(InvalidRequest, match='Expected 3 elements in value tuple, but got 5'):
            cql.execute(f'SELECT * FROM {table} WHERE a = 0 AND (b, c, d) IN ((1, 2, 3, 4, 5), (2, 1, 4))')

# Test for a bug found while developing the fix for #10357. The bug was that if a regular column was selected
# but not filtered, it could cause filtering of the static row to fail.
def test_selecting_a_regular_column_does_not_poison_filtering_of_a_static_row(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, s int static, primary key (a, b)') as table:
        cql.execute(f"INSERT INTO {table} (a, s) VALUES (1, 2)")
        res = cql.execute(f"SELECT a, b, c, s FROM {table} WHERE s = 2 ALLOW FILTERING")
        assert list(res) == [(1, None, None, 2)]

# A tuple range on descending clustering columns keeps its CQL meaning when
# the query also needs filtering.
# Reproduces #31631
def test_multi_column_slice_with_filtering_on_desc_clustering(cql, test_keyspace):
    schema = 'p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)'
    with new_test_table(cql, test_keyspace, schema,
            'WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)') as table:
        for i in range(1, 6):
            cql.execute(f"INSERT INTO {table} (p, c1, c2, v) VALUES (1, {i}, {i}, {i % 2})")
        assert list(cql.execute(f"SELECT c1, c2 FROM {table} WHERE p = 1 AND (c1, c2) < (4, 4) AND v = 1 ALLOW FILTERING")) == [(3, 3), (1, 1)]
        assert list(cql.execute(f"SELECT c1, c2 FROM {table} WHERE p = 1 AND (c1, c2) > (2, 2) AND v = 1 ALLOW FILTERING")) == [(5, 5), (3, 3)]
        assert list(cql.execute(f"SELECT c1, c2 FROM {table} WHERE p = 1 AND (c1) < (4) AND v = 1 ALLOW FILTERING")) == [(3, 3), (1, 1)]

# The descending column of a mixed-order clustering key is only a tie-break
# in a tuple comparison.
# Reproduces #31631
def test_multi_column_slice_with_filtering_on_mixed_clustering_order(cql, test_keyspace):
    schema = 'p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)'
    with new_test_table(cql, test_keyspace, schema,
            'WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)') as table:
        for c1 in range(1, 3):
            for c2 in range(1, 4):
                cql.execute(f"INSERT INTO {table} (p, c1, c2, v) VALUES (1, {c1}, {c2}, 1)")
        assert list(cql.execute(f"SELECT c1, c2 FROM {table} WHERE p = 1 AND (c1, c2) < (2, 2) AND v = 1 ALLOW FILTERING")) == [(1, 3), (1, 2), (1, 1), (2, 1)]

# A tuple range on descending clustering columns keeps its CQL meaning when an
# index, rather than a clustering range, picks the rows to filter.
# Reproduces #31631
def test_multi_column_slice_with_filtering_on_indexed_column(cql, test_keyspace):
    schema = 'p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)'
    with new_test_table(cql, test_keyspace, schema,
            'WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)') as table:
        cql.execute(f"CREATE INDEX ON {table}(v)")
        for i in range(1, 6):
            cql.execute(f"INSERT INTO {table} (p, c1, c2, v) VALUES (1, {i}, {i}, {i % 2})")
        assert set(cql.execute(f"SELECT c1, c2 FROM {table} WHERE (c1, c2) < (4, 4) AND v = 1 ALLOW FILTERING")) == {(3, 3), (1, 1)}
        assert set(cql.execute(f"SELECT c1, c2 FROM {table} WHERE (c1, c2) > (2, 2) AND v = 1 ALLOW FILTERING")) == {(5, 5), (3, 3)}

# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_filter_by_counter.
# Filtering on counter columns: =, >, IN and >= on one counter combined with
# a restriction on another, the ALLOW FILTERING error, a counter column added
# by ALTER TABLE (null in the old rows) and a deleted counter cell.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_filter_counter_in_alter_delete(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int, ck int, c1 counter, c2 counter, PRIMARY KEY (pk, ck)") as table:
        for i in range(5):
            cql.execute(f"UPDATE {table} SET c1 = c1+{i} WHERE pk=0 and ck={i}")
        for i in range(5):
            cql.execute(f"UPDATE {table} SET c1 = c1+{i}, c2 = c2+{i} WHERE pk=1 and ck={i}")
        assert list(cql.execute(f"SELECT count(*) FROM {table}")) == [(10,)]
        assert list(cql.execute(f"select * from {table} where c1 = 2 ALLOW FILTERING")) == [(1, 2, 2, 2), (0, 2, 2, None)]
        assert list(cql.execute(f"select * from {table} where c1 > 2 ALLOW FILTERING")) == [(1, 3, 3, 3), (1, 4, 4, 4), (0, 3, 3, None), (0, 4, 4, None)]
        assert list(cql.execute(f"select * from {table} where c1 > 3 and c2 = 4 ALLOW FILTERING")) == [(1, 4, 4, 4)]
        assert list(cql.execute(f"select * from {table} where c1 in (0, 2) and c2 >= 2 ALLOW FILTERING")) == [(1, 2, 2, 2)]
        assert list(cql.execute(f"select * from {table} where pk = 1 and ck = 4 and c1 < 3 and c2 = 0 ALLOW FILTERING")) == []
        with pytest.raises(InvalidRequest, match="Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. "
                           "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"):
            cql.execute(f"select * from {table} where pk = 1 and ck = 4 and c1 > 3 and c2 = 0")
        cql.execute(f"ALTER TABLE {table} ADD c3 counter")
        assert list(cql.execute(f"select * from {table} where pk=1 and ck=1")) == [(1, 1, 1, 1, None)]
        # c3 is null in all rows, which no filter on its value matches
        assert list(cql.execute(f"select count(*) from {table} where c3 = 0 ALLOW FILTERING")) == [(0,)]
        cql.execute(f"UPDATE {table} SET c3 = c3-1 WHERE pk=0 and ck=1")
        assert list(cql.execute(f"select * from {table} where c3 = -1 ALLOW FILTERING")) == [(0, 1, 1, None, -1)]
        cql.execute(f"DELETE c1 FROM {table} WHERE pk = 1 and ck = 1")
        assert list(cql.execute(f"select * from {table} where c1 = 1 ALLOW FILTERING")) == [(0, 1, 1, None, -1)]
        assert list(cql.execute(f"select * from {table} where c1 = 0 ALLOW FILTERING")) == [(1, 0, 0, 0, None), (0, 0, 0, None, None)]

# Ported from scylla-dtest cql_additional_tests.py::TestsMultiColumnRestrictionSimple::test_filter_by_one_non_indexed_columns.
# ALLOW FILTERING with an equality on one regular column, for several column
# types (int, varchar, timestamp, boolean, ascii, uuid, blob), selecting the
# timestamp and uuid columns cast as text; some reads at QUORUM.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_filter_regular_column_types(cql, test_keyspace):
    # eight_columns_table() and its data are defined below, with the other
    # TestsMultiColumnRestrictionSimple ports.
    with eight_columns_table(cql, test_keyspace) as table:
        insert_eight_columns_data(cql, table)
        expected = MULTI_EXPECTED_DATA
        def check(where, rows, cl=None):
            query = f"select {MULTI_SELECT_COLUMNS} from {table} {where}"
            stmt = SimpleStatement(query, consistency_level=cl) if cl else query
            assert sorted(tuple(r) for r in cql.execute(stmt)) == sorted(rows)
        check("where clmn_int = 0 ALLOW FILTERING", expected[:2])
        check("where clmn_text = 'text3' ALLOW FILTERING", [expected[2]])
        check("where clmn_timestamp = 398793781719 ALLOW FILTERING", expected[2:4])
        check("where clmn_bool = True ALLOW FILTERING", [expected[0], expected[2]], ConsistencyLevel.QUORUM)
        check("where clmn_ascii = 'abcdefj' ALLOW FILTERING", [expected[1]], ConsistencyLevel.QUORUM)
        check("where clmn_uuid = de5cba0d-41a2-4f39-8834-35130d8b5d86 ALLOW FILTERING", [expected[0], expected[2]], ConsistencyLevel.QUORUM)
        check(" where clmn_blob = textAsBlob('{}') ALLOW FILTERING".format("b" * 10), expected[1:3], ConsistencyLevel.QUORUM)

# LIKE on a partition key still needs ALLOW FILTERING, and the pattern on the
# right-hand side of LIKE must be a string: a function returning another type
# (uuid, date, time) is rejected.
# Currently, Cassandra only allows LIKE on a SASI index, not when filtering
# (CASSANDRA-17198), see test_filter_like_on_desc_column.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_filter_like_invalid(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, "pk text, ck text, v text, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES ('teststring0', '0teststring', 'test0string')")
        with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
            cql.execute(f"SELECT * FROM {table} WHERE pk LIKE 'teststring%'")
        for func in ['uuid()', 'currentDate()', 'currentTime()']:
            with pytest.raises(InvalidRequest, match='cannot assign result of function'):
                cql.execute(f"SELECT * FROM {table} WHERE ck LIKE {func} ALLOW FILTERING")

# Ported from scylla-dtest like_operator_test.py::TestLikeOperatorForBaseTable::test_filtering_with_long_values.
# On the dtest's text table, LIKE filtering on the partition key returns
# all of its rows; then the rows get long (64 KB) values, and LIKE filtering
# on them, read with paging, matches the pattern against the whole value,
# both at its end and at its beginning.
# Currently, Cassandra only allows LIKE on a SASI index, not when filtering
# (CASSANDRA-17198), see test_filter_like_on_desc_column.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_filter_like_long_values(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, "pk text, ck text, test text, PRIMARY KEY (pk, ck)") as table:
        all_data = [(f"teststring{i}", f"{i}teststring", f"test{i}string") for i in range(5)]
        for data in all_data:
            cql.execute(f"INSERT INTO {table} (pk, ck, test) VALUES ('{data[0]}', '{data[1]}', '{data[2]}')")
        assert sorted(cql.execute(f"SELECT * FROM {table} WHERE pk LIKE 't%ts%g%' ALLOW FILTERING")) == sorted(all_data)
        for i in range(5):
            value = "a" * 65520 + "findme" if i % 2 == 0 else "findme" + "a" * 65520
            cql.execute(f"INSERT INTO {table} (pk, ck, test) VALUES ('teststring{i}', '{i}teststring', '{value}')")
        for pattern, expected in [("%me", ["teststring0", "teststring2", "teststring4"]), ("_indme%", ["teststring1", "teststring3"])]:
            actual = [row.pk for row in cql.execute(SimpleStatement(f"SELECT pk FROM {table} WHERE test LIKE '{pattern}' ALLOW FILTERING"))]
            for pk in expected:
                assert pk in actual


# Tests ported from scylla-dtest's cql_additional_tests.py. Like the dtests,
# they run in a keyspace with vnodes, and keep the dtests' table names.

# The TestsMultiColumnRestrictionSimple dtests: a table with columns of
# eight types, filtered by several of them at once, with the partition and
# clustering key, a global or a local index, or a materialized view.
MULTI_INSERT_COLUMNS = "key,clmn_int,clmn_text,clmn_timestamp,clmn_bool,clmn_ascii,clmn_uuid,clmn_blob"
MULTI_SELECT_COLUMNS = MULTI_INSERT_COLUMNS.replace("clmn_timestamp", "cast(clmn_timestamp as text)").replace("clmn_uuid", "cast(clmn_uuid as text)")
MULTI_TEST_DATA = [
    [0, 0, "text1", 12345674987, True, "045asciitext", "de5cba0d-41a2-4f39-8834-35130d8b5d86", "c" * 10],
    [1, 0, "text2", 63873478378, False, "abcdefj", "fa80080c-a4c5-46d6-afe4-5e184fec35ae", "b" * 10],
    [2, 2, "text3", 398793781719, True, "354dsfsd", "de5cba0d-41a2-4f39-8834-35130d8b5d86", "b" * 10],
    [3, 3, "text4", 398793781719, False, "897dfjka9", "fa80080c-a4c5-46d6-afe4-5e184fec35ae", "a" * 10],
    [4, 4],
]
MULTI_EXPECTED_DATA = [
    (0, 0, "text1", "1970-05-23T21:21:14.987Z", True, "045asciitext", "de5cba0d-41a2-4f39-8834-35130d8b5d86", b"c" * 10),
    (1, 0, "text2", "1972-01-10T06:37:58.378Z", False, "abcdefj", "fa80080c-a4c5-46d6-afe4-5e184fec35ae", b"b" * 10),
    (2, 2, "text3", "1982-08-21T16:03:01.719Z", True, "354dsfsd", "de5cba0d-41a2-4f39-8834-35130d8b5d86", b"b" * 10),
    (3, 3, "text4", "1982-08-21T16:03:01.719Z", False, "897dfjka9", "fa80080c-a4c5-46d6-afe4-5e184fec35ae", b"a" * 10),
    (4, 4, None, None, None, None, None, None),
]

@contextmanager
def eight_columns_table(cql, keyspace, add_ck=False):
    ck = ", clmn_int" if add_ck else ""
    schema = f"key int, clmn_int int, clmn_text varchar, clmn_timestamp timestamp, clmn_bool boolean, clmn_ascii ascii, clmn_uuid uuid, clmn_blob blob, PRIMARY KEY(key{ck})"
    table = f"{keyspace}.cf"
    cql.execute(f"CREATE COLUMNFAMILY {table} ({schema})")
    try:
        yield table
    finally:
        cql.execute(f"DROP TABLE {table}")

def insert_eight_columns_data(cql, table, insert_data=MULTI_TEST_DATA):
    for data in insert_data:
        if len(data) == 2:
            cql.execute(f"INSERT INTO {table}(key,clmn_int) VALUES({data[0]},{data[1]})")
        else:
            cql.execute(f"INSERT INTO {table}({MULTI_INSERT_COLUMNS}) VALUES({data[0]},{data[1]},'{data[2]}','{data[3]}',{data[4]},'{data[5]}',{data[6]},textAsBlob('{data[7]}'))")

def multi_select(cql, source, where):
    return sorted(cql.execute(f"select {MULTI_SELECT_COLUMNS} from {source} where {where} ALLOW FILTERING"), key=lambda r: r[0])

@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filter_by_three_non_indexed_columns(cql, test_keyspace_vnodes):
    with eight_columns_table(cql, test_keyspace_vnodes) as table:
        insert_eight_columns_data(cql, table)
        assert multi_select(cql, table, "clmn_int = 2 and clmn_uuid = de5cba0d-41a2-4f39-8834-35130d8b5d86 and clmn_timestamp = 398793781719") == [MULTI_EXPECTED_DATA[2]]
        assert multi_select(cql, table, f"clmn_ascii = '897dfjka9' and clmn_text = 'text4' and clmn_blob = textAsBlob('{'a' * 10}')") == [MULTI_EXPECTED_DATA[3]]

@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filter_by_pk_ck_and_non_indexed_columns(cql, test_keyspace_vnodes):
    with eight_columns_table(cql, test_keyspace_vnodes, add_ck=True) as table:
        insert_eight_columns_data(cql, table)
        for where in ["key = 0 and clmn_timestamp = 12345674987",
                      "key = 0 and clmn_int = 0 and clmn_timestamp = 12345674987",
                      "key = 0 and clmn_timestamp = 12345674987 and clmn_bool = True",
                      "key = 0 and clmn_int = 0 and clmn_timestamp = 12345674987 and clmn_uuid=de5cba0d-41a2-4f39-8834-35130d8b5d86"]:
            assert multi_select(cql, table, where) == [MULTI_EXPECTED_DATA[0]]

# The same four queries with a global (global_idx) or a local index on
# clmn_text, created before the data is written.
def do_test_filter_by_pk_ck_index_and_non_indexed_columns(cql, keyspace, create_index):
    with eight_columns_table(cql, keyspace, add_ck=True) as table:
        cql.execute(create_index.format(table=table))
        insert_eight_columns_data(cql, table)
        for where in ["key = 1 and clmn_text = 'text2' and clmn_timestamp = 63873478378",
                      "key = 1 and clmn_int = 0 and clmn_text = 'text2' and clmn_timestamp = 63873478378",
                      "key = 1 and clmn_text = 'text2' and clmn_uuid = fa80080c-a4c5-46d6-afe4-5e184fec35ae and clmn_timestamp = 63873478378",
                      "key = 1 and clmn_int = 0 and clmn_text = 'text2' and clmn_bool = False and clmn_timestamp = 63873478378"]:
            assert multi_select(cql, table, where) == [MULTI_EXPECTED_DATA[1]]

@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filter_by_pk_ck_globalSI_and_non_indexed_columns(cql, test_keyspace_vnodes):
    do_test_filter_by_pk_ck_index_and_non_indexed_columns(cql, test_keyspace_vnodes, "CREATE INDEX global_idx ON {table} (clmn_text)")

@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filter_by_pk_ck_localSI_and_non_indexed_columns(cql, test_keyspace_vnodes, scylla_only):
    do_test_filter_by_pk_ck_index_and_non_indexed_columns(cql, test_keyspace_vnodes, "CREATE INDEX global_idx ON {table} ((key), clmn_text)")

@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filter_by_two_non_indexed_columns_with_operator(cql, test_keyspace_vnodes):
    with eight_columns_table(cql, test_keyspace_vnodes) as table:
        insert_eight_columns_data(cql, table)
        assert multi_select(cql, table, "clmn_int < 2 and clmn_uuid <= fa80080c-a4c5-46d6-afe4-5e184fec35ae") == MULTI_EXPECTED_DATA[:2]

def wait_for_view_built(cql, view, timeout=60):
    ks, name = view.split('.')
    query = SimpleStatement(f"SELECT status FROM system_distributed.view_build_status WHERE keyspace_name='{ks}' AND view_name='{name}'",
                            consistency_level=ConsistencyLevel.ONE)
    start = time.time()
    while [r.status for r in cql.execute(query)] != ["SUCCESS"]:
        assert time.time() < start + timeout, f"view {view} not built"
        time.sleep(0.1)

# Filtering that matches nothing, on a base table with a materialized view
# and on the view.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filter_with_empty_result_on_table_and_view(cql, test_keyspace_vnodes, scylla_only):
    with eight_columns_table(cql, test_keyspace_vnodes, add_ck=True) as table:
        view = f"{test_keyspace_vnodes}.cf_mv"
        cql.execute(f"CREATE MATERIALIZED VIEW {view} as SELECT * FROM {table} WHERE key IS NOT NULL AND clmn_int IS NOT NULL and clmn_text IS NOT NULL PRIMARY KEY (key, clmn_int, clmn_text)")
        try:
            wait_for_view_built(cql, view)
            insert_eight_columns_data(cql, table, MULTI_TEST_DATA[:4])
            assert multi_select(cql, table, "key > 1 and clmn_int < 5 and clmn_text <= 'text2' and clmn_timestamp < 63873478378") == []
            assert multi_select(cql, table, "key > 1 and clmn_int < 5 and clmn_uuid = fa80080c-a4c5-46d6-afe4-5e184fec35ae and clmn_blob = textAsBlob('bbbbbbbbbb')") == []
            assert multi_select(cql, view, "key > 1 and clmn_int < 5 and clmn_text <= 'text2' and clmn_timestamp < 63873478378") == []
        finally:
            cql.execute(f"DROP MATERIALIZED VIEW {view}")

# The TestsMultiColumnRestrictionCollection dtests: filtering with CONTAINS,
# CONTAINS KEY and equality on non-frozen and frozen lists, sets and maps.
COLLECTIONS_TEST_DATA = [
    [0, "[0, 1, 2]", "[textAsBlob('t1'), textAsBlob('t2')]", "{de5cba0d-41a2-4f39-8834-35130d8b5d86, fa80080c-a4c5-46d6-afe4-5e184fec35ae}",
     "{'t3', 't4', 't5'}", "{'a': True, 'b': True, 'c': False}", "{'a': f34f6a76-b383-11e9-a2a3-2a2ae2dbcce4, 'c': f34f6cec-b383-11e9-a2a3-2a2ae2dbcce4}",
     "[5, 6]", "['f1', 'f2']", "{7, 9}", "{'f3', 'f4', 'f5'}", "{'fa': 'b', 'fc': 'd'}", "{'fa': 1, 'fb': 2, 'fc':3}"],
    [1, "[3, 4, 5]", "[textAsBlob('t3'), textAsBlob('t4')]", "{8e4fe826-b383-11e9-a2a3-2a2ae2dbcce4, 8e4fea9c-b383-11e9-a2a3-2a2ae2dbcce4}",
     "{'t5', 't6', 't7'}", "{'a1': False, 'c1': False}",
     "{'a1': 26a8e352-b384-11e9-a2a3-2a2ae2dbcce4, 'b1': 26a8e5be-b384-11e9-a2a3-2a2ae2dbcce4, 'c1': 26a8e712-b384-11e9-a2a3-2a2ae2dbcce4}",
     "[7, 8]", "['f3', 'f4']", "{9, 10}", "{'f6', 'f7', 'f8'}", "{'f1': 'c', 'f2': 'e'}", "{'f1': 1, 'f2': 2, 'f3': 3}"],
]

@contextmanager
def all_collections_table(cql, keyspace):
    schema = ("id int PRIMARY KEY, list_int list<int>, list_blob list<blob>, set_uuid set<uuid>, set_text set<text>, map_bool map<text, boolean>, "
              "map_uuid map<text, uuid>, f_list_int frozen<list<int>>, f_list_text frozen<list<text>>, f_set_int frozen<set<int>>, "
              "f_set_text frozen<set<text>>,f_map_text frozen<map<text, text>>, f_map_int frozen<map<text, int>>")
    columns = "id, list_int, list_blob, set_uuid, set_text, map_bool, map_uuid, f_list_int, f_list_text, f_set_int, f_set_text, f_map_text, f_map_int"
    with new_named_table(cql, keyspace, "cf", schema) as table:
        for data in COLLECTIONS_TEST_DATA:
            cql.execute(f"INSERT INTO {table}({columns}) VALUES({','.join(str(d) for d in data)})")
        yield table

def select_ids(cql, table, where):
    return sorted(r.id for r in cql.execute(f"select id from {table} where {where} ALLOW FILTERING"))

@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filter_by_one_non_indexed_collection_column(cql, test_keyspace_vnodes):
    with all_collections_table(cql, test_keyspace_vnodes) as table:
        for where, expected in [("list_int CONTAINS 4", [1]),
                                ("list_blob CONTAINS textAsBlob('t1')", [0]),
                                ("set_uuid CONTAINS 8e4fe826-b383-11e9-a2a3-2a2ae2dbcce4", [1]),
                                ("set_text CONTAINS 't5'", [0, 1]),
                                ("map_bool CONTAINS False and map_bool CONTAINS KEY 'a1'", [1]),
                                ("map_uuid CONTAINS f34f6a76-b383-11e9-a2a3-2a2ae2dbcce4 ", [0]),
                                ("map_uuid CONTAINS KEY 'a1'", [1]),
                                ("f_list_int CONTAINS 8", [1]),
                                ("f_list_text CONTAINS 'f4'", [1]),
                                ("f_set_int = {9, 7} ", [0]),
                                ("f_set_int CONTAINS 9 and f_set_int CONTAINS 10 ", [1]),
                                ("f_set_text CONTAINS 'f6'", [1]),
                                ("f_map_text = {'f2': 'e', 'f1': 'c'}", [1]),
                                ("f_map_text CONTAINS 'c'", [1]),
                                ("f_map_text CONTAINS KEY 'f2'", [1]),
                                ("f_map_int CONTAINS 2", [0, 1])]:
            assert select_ids(cql, table, where) == expected, where

@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filter_by_pk_and_two_non_indexed_collection_column(cql, test_keyspace_vnodes):
    with all_collections_table(cql, test_keyspace_vnodes) as table:
        assert select_ids(cql, table, "id = 0 and map_uuid CONTAINS f34f6a76-b383-11e9-a2a3-2a2ae2dbcce4 and f_set_int CONTAINS 9") == [0]
        assert select_ids(cql, table, "id = 1 and set_uuid CONTAINS 8e4fe826-b383-11e9-a2a3-2a2ae2dbcce4 and f_map_int = {'f1': 1, 'f2': 2, 'f3': 3}") == [1]

@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_filter_collections_with_empty_result(cql, test_keyspace_vnodes):
    with all_collections_table(cql, test_keyspace_vnodes) as table:
        assert select_ids(cql, table, "id = 0 and map_uuid CONTAINS f54f6a76-b383-11e9-a2a3-2a2ae2dbcce4 and f_set_int CONTAINS 9") == []
