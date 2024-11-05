# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the SELECT requests with various filtering expressions.
# We have a separate test file test_allow_filtering.py, for tests that
# focus on whether the string "ALLOW FILTERING" is needed or not needed
# for a query. In the tests in this file we focus more on the correctness
# of various filtering expressions - regardless of whether ALLOW FILTERING
# is or isn't necessary.

import pytest
import re
from util import new_test_table, new_type, user_type
from cassandra.protocol import InvalidRequest
from cassandra.query import UNSET_VALUE

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

# Although the "!=" operator exists in the parser and might be allowed in
# other places (e.g., LWT), it is *NOT* supported in WHERE clauses - not
# for filtering, and also not in relations that don't need filtering
# (on partition keys or tokens). It is not supported in either Cassandra or
# Scylla, and there are no plans to add this support, so for now the test
# verifies that at least we get the expected error.
def test_operator_ne_not_supported(cql, table1):
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE b != 0 ALLOW FILTERING')
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE a != 0')
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE token(a) != 0')

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

#This test shows how Scylla handles invalid number of items in one of the tuples of multi-column relation.
#If the number of items is lesser than required, Scylla throws "Expected {} elements in value tuple, but got {}"
#But if the number of items is greater than required, Scylla throws different error: 
#"Invalid list literal for in((b,c,d)): value (1, 2, 3, 4) is not of type frozen<tuple<int, int, int>>"
# Reproduces #13217
def test_multi_column_relation_wrong_number_of_items_in_tuple(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'a int, b int, c int, d int, primary key (a, b, c, d)') as table:
        with pytest.raises(InvalidRequest, match='elements in value tuple, but got'):
            cql.execute(f'SELECT * FROM {table} WHERE a = 0 AND (b, c, d) IN ((1, 2), (2, 1, 4))')
        with pytest.raises(InvalidRequest, match='Invalid list literal for in'):
            cql.execute(f'SELECT * FROM {table} WHERE a = 0 AND (b, c, d) IN ((1, 2, 3, 4, 5), (2, 1, 4))')
