# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the SELECT requests with various restriction (WHERE) expressions.
# We have a separate test file test_filtering.py, for tests that focus on
# post-read filtering, so the tests in this file focus on restrictions that
# determine what to read. As in the future more sophisticated query planning
# techniques may mix up pre-read and post-read filtering, perhaps in the future
# we should just merge these two test files.

import pytest
from util import new_test_table, unique_key_int
from cassandra.protocol import InvalidRequest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a, b)") as table:
        yield table

@pytest.fixture(scope="module")
def table3(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, c int, d int, PRIMARY KEY (a, b, c, d)") as table:
        yield table

@pytest.fixture(scope="module")
def table4(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, c int, PRIMARY KEY (a, b, c)") as table:
        yield table

# Cassandra does not allow a WHERE clause restricting the same column with
# an equality more than once. It complains that that column "cannot be
# restricted by more than one relation if it includes an Equal".
# Scylla *does* allow this, but we want to check in this test that it works
# correctly - "WHERE a = 1 AND a = 2" should return nothing, and "WHERE
# a = 1 AND a = 1" should return the same as just "WHERE a = 1".
# Because these expressions are not allowed in Cassandra, this is a
# scylla_only test.
def test_multiple_eq_restrictions_on_pk(cql, table1, scylla_only):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (a, b) VALUES ({p}, 3)') 
    p2 = unique_key_int()
    assert [] == list(cql.execute(f'SELECT * FROM {table1} WHERE a = {p} AND a = {p2}'))
    assert [] == list(cql.execute(f'SELECT * FROM {table1} WHERE a = {p2} AND a = {p}'))
    assert [(p, 3)] == list(cql.execute(f'SELECT * FROM {table1} WHERE a = {p} AND a = {p}'))

# Cassandra also doesn't allow restricting the same clustering-key column
# more than once, producing all sorts of different error messages depending
# if the operator is an equality or comparison and which order (see examples
# in issue #12472). Scylla allows all these combination, and this test
# verifies that they are implemented correctly:
def test_multiple_restrictions_on_ck(cql, table2, scylla_only):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table2} (a, b) VALUES ({p}, 3)')
    cql.execute(f'INSERT INTO {table2} (a, b) VALUES ({p}, 5)')
    cql.execute(f'INSERT INTO {table2} (a, b) VALUES ({p}, 0)')
    # b = ? and b = ?
    assert [] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b = 3 AND b = 4'))
    assert [] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b = 4 AND b = 3'))
    assert [(p, 3)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b = 3 AND b = 3'))
    # b = ? and b > ?
    assert [] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b = 3 AND b > 4'))
    assert [] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b > 4 AND b = 3'))
    assert [(p, 3)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b = 3 AND b > 2'))
    assert [(p, 3)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b > 2 AND b = 3'))
    # b = ? and b < ?
    assert [(p, 3)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b = 3 AND b < 4'))
    assert [(p, 3)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b < 4 AND b = 3'))
    assert [] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b = 3 AND b < 2'))
    assert [] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b < 2 AND b = 3'))
    # b > ? and b > ?
    assert [(p, 3), (p, 5)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b > 2 AND b > 1'))
    assert [(p, 5)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b > 4 AND b > 1'))
    assert [(p, 5)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b > 1 AND b > 4'))
    assert [(p, 5)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b > 3 AND b > 4'))
    # b < ? and b < ?
    assert [(p, 0), (p, 3)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b < 4 AND b < 5'))
    assert [(p, 0)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b < 4 AND b < 1'))
    assert [(p, 0)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b < 1 AND b < 4'))
    assert [(p, 0)] == list(cql.execute(f'SELECT * FROM {table2} WHERE a = {p} AND b < 1 AND b < 2'))

# In the above test we noted that Cassandra doesn't allow restricting the
# same clustering-key column more than once in the same direction, such as
# b < 1 AND b < 2, but Scylla *does* allow it. But it turns out that for
# multi-column restrictions we have different validation code, and Scylla
# forbids  (b) < (1) AND (b,c) < (2,2) in exactly the same way that is
# forbidden in Cassandra.
# This should be reconsidered (this is what issue #18690 asks) - we should
# consider allowing this query in Scylla, but until we do, this test checks
# the existing situation that it refused on both Scylla and Cassandra:
def test_multiple_multi_column_restrictions_on_ck(cql, table4):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table4} (a, b, c) VALUES ({p}, 0, 1)')
    cql.execute(f'INSERT INTO {table4} (a, b, c) VALUES ({p}, 0, 2)')
    cql.execute(f'INSERT INTO {table4} (a, b, c) VALUES ({p}, 0, 3)')
    cql.execute(f'INSERT INTO {table4} (a, b, c) VALUES ({p}, 1, 1)')
    cql.execute(f'INSERT INTO {table4} (a, b, c) VALUES ({p}, 1, 2)')
    cql.execute(f'INSERT INTO {table4} (a, b, c) VALUES ({p}, 1, 3)')
    assert [(0,1),(0,2),(0,3)] == list(cql.execute(f'SELECT b,c FROM {table4} WHERE a = {p} AND (b) < (1)'))
    assert [(0,1),(0,2),(0,3),(1,1)] == list(cql.execute(f'SELECT b,c FROM {table4} WHERE a = {p} AND (b,c) < (1,2)'))
    # Ideally the following query should return the intersection of the
    # previous two results, but it's currently not supported by Scylla
    # (#18690) or by Cassandra.
    with pytest.raises(InvalidRequest, match='More than one restriction was found for the end bound on b'):
        assert [(0,1),(0,2),(0,3)] == list(cql.execute(f'SELECT b,c FROM {table4} WHERE a = {p} AND (b) < (1) AND (b,c) < (1,2)'))

# Reproducer for issue #18688 where a specially-crafted multi-column
# restriction with two start restrictions could hang Scylla.
def test_intersection_hang(cql, table4):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table4} (a, b, c) VALUES ({p}, 0, 0)')
    cql.execute(f'INSERT INTO {table4} (a, b, c) VALUES ({p}, 0, 1)')
    cql.execute(f'INSERT INTO {table4} (a, b, c) VALUES ({p}, 0, 2)')
    cql.execute(f'INSERT INTO {table4} (a, b, c) VALUES ({p}, 1, 1)')
    # The restriction below should either be rejected with InvalidRequest (as
    # it is in Cassandra, see discussion in #18690) or return the correct
    # results. Of course it mustn't hang as it did in #18688.
    try:
        assert [(0,1),(0,2)] == list(cql.execute(f'SELECT b,c FROM {table4} WHERE a = {p} AND (b) < (1) AND (b) >= (0) AND (b,c) > (0,0)'))
    except InvalidRequest as err:
        assert 'More than one restriction was found for the start bound on b' in str(err)

# Try a multi-column restriction on several clustering columns, with the wrong
# number of values on the right-hand-side of the restriction. Scylla should
# cleanly report the error - and not silently ignore it or even crash as in
# issue #13241.
@pytest.mark.skip("Crashes due to issue #13241")
def test_multi_column_restriction_in(cql, table3):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table3} (a, b, c, d) VALUES ({p}, 1, 2, 3)')
    # The tuple (1,2,3) is the same length as (b,c,d) so the query works:
    assert [(p,1,2,3)] == list(cql.execute(f'SELECT * FROM {table3} WHERE a={p} AND (b,c,d) IN ((1,2,3))'))
    # IN can also list several tuples of the same length:
    assert [(p,1,2,3)] == list(cql.execute(f'SELECT * FROM {table3} WHERE a={p} AND (b,c,d) IN ((4,5,6), (1,2,3))'))
    # The tuple (1,2,3,4) is too long, so fails - with different error-
    # messages in Scylla and Cassandra:
    with pytest.raises(InvalidRequest):
        cql.execute(f'SELECT * FROM {table3} WHERE a={p} AND (b,c,d) IN ((1, 2, 3, 4))')
    # The tuple (1,2) is too short, so should also fail. Reproduces #13241:
    with pytest.raises(InvalidRequest):
        cql.execute(f'SELECT * FROM {table3} WHERE a={p} AND (b,c,d) IN ((1, 2))')

def test_multi_column_restriction_eq(cql, table3):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table3} (a, b, c, d) VALUES ({p}, 1, 2, 3)')
    # The tuple (1,2,3) is the same length as (b,c,d) so the query works:
    assert [(p,1,2,3)] == list(cql.execute(f'SELECT * FROM {table3} WHERE a={p} AND (b,c,d) = (1,2,3)'))
    # The tuple (1,2,3,4) is too long, so fails - with different error-
    # messages in Scylla and Cassandra:
    with pytest.raises(InvalidRequest):
        cql.execute(f'SELECT * FROM {table3} WHERE a={p} AND (b,c,d) = (1, 2, 3, 4)')
    # The tuple (1,2) is too short, so should also fail.
    with pytest.raises(InvalidRequest):
        cql.execute(f'SELECT * FROM {table3} WHERE a={p} AND (b,c,d) = (1, 2)')
