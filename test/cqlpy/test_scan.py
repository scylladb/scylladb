# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for scanning SELECT requests (which read many rows and/or many
# partitions).
# We have a separate test file test_filtering.py for scans which also involve
# filtering, and test_allow_filtering.py for checking when "ALLOW FILTERING"
# is needed in scan. test_secondary_index.py also contains tests for scanning
# using a secondary index.
#############################################################################

import pytest
from util import new_test_table, new_type, user_type
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement

# Test that in a table with multiple clustering-key columns, we can have
# multi-column restrictions on involving various legal combinations of
# clustering key columns. multi-column restrictions are expressions involving
# tuples of columns - such as (c2, c3) = (2,3) or (c2, c3) < (2,3).
# This test focuses on cases which do not need ALLOW FILTERING. The next
# test will focus on those that do.
# Reproduces issue #64 and #4244
@pytest.mark.xfail(reason="issues #64 and #4244")
def test_multi_column_restrictions_ck(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c1 int, c2 int, c3 int, PRIMARY KEY (p, c1, c2, c3)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c1, c2, c3) VALUES (1, ?, ?, ?)")
        for i in range(3):
            for j in range(3):
                for k in range(3):
                    cql.execute(stmt, [i, j, k])

        # Restrictions with equality on a full prefix of clustering keys do
        # not require ALLOW FILTERING. This prefix may be composed of
        # single-column restrictions or multi-column restrictions:
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND c1=1")) == [(1,0,0), (1,0,1), (1,0,2), (1,1,0), (1,1,1), (1,1,2), (1,2,0), (1,2,1), (1,2,2)]
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND c1=1 AND c2=2")) == [(1,2,0), (1,2,1), (1,2,2)]
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND (c1, c2) = (1, 2)")) == [(1,2,0), (1,2,1), (1,2,2)]
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND c1=1 AND c2=2 and c3=1")) == [(1,2,1)]
        # Reproduces #4244:
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND (c1, c2) = (1, 2) and c3=1")) == [(1,2,1)]
        # Reproduces #64:
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND c1=1 AND (c2, c3) = (2, 1)")) == [(1,2,1)]
        # Multi-column restrictions are only allowed on adjacent clustering
        # columns - it cannot involved non-adjacent clustering columns, or
        # partition columns.
        with pytest.raises(InvalidRequest, match='ulti-column'):
            assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND c2=2 AND (c1, c3) = (1, 1)")) == [(1,2,1)]
        with pytest.raises(InvalidRequest, match='ulti-column'):
            assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE (p, c1) = (1,1) AND c2 = 2 AND c3 = 1")) == [(1,2,1)]

        # Restrictions with inequality on a full prefix of clustering keys
        # ending in the inequality also do not require ALLOW FILTERING.
        # The inequality may be composed of single-column restrictions or
        # multi-column restrictions:
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND c1<1")) == [(0,0,0), (0,0,1), (0,0,2), (0,1,0), (0,1,1), (0,1,2), (0,2,0), (0,2,1), (0,2,2)]
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND c1=1 AND c2<1")) == [(1,0,0), (1,0,1), (1,0,2)]
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND (c1,c2) < (0,2)")) == [(0,0,0), (0,0,1), (0,0,2), (0,1,0), (0,1,1), (0,1,2)]
        # Reproduces #64:
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE p=1 AND c1=1 AND (c2,c3) < (1,2)")) == [(1,0,0), (1,0,1), (1,0,2), (1,1,0), (1,1,1)]

# Test that if we have multi-column restrictions on the clustering key
# and additional filtering on regular columns, both restrictions are obeyed.
# Reproduces #6200.
def test_multi_column_restrictions_and_filtering(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c1 int, c2 int, r int, PRIMARY KEY (p, c1, c2)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c1, c2, r) VALUES (1, ?, ?, ?)")
        for i in range(2):
            for j in range(2):
                cql.execute(stmt, [i, j, j])
        assert list(cql.execute(f"SELECT c1,c2,r FROM {table} WHERE p=1 AND (c1, c2) = (0,1)")) == [(0,1,1)]
        # Since in that result r=1, adding "AND r=1" should return the same
        # result, and adding "AND r=0" should return nothing.
        assert list(cql.execute(f"SELECT c1,c2,r FROM {table} WHERE p=1 AND (c1, c2) = (0,1) AND r=1 ALLOW FILTERING")) == [(0,1,1)]
        # Reproduces #6200:
        assert list(cql.execute(f"SELECT c1,c2,r FROM {table} WHERE p=1 AND (c1, c2) = (0,1) AND r=0 ALLOW FILTERING")) == []

# Test that if we have a range multi-column restrictions on the clustering key
# and additional filtering on regular columns, both restrictions are obeyed.
# Similar to test_multi_column_restrictions_and_filtering, but uses a range
# restriction on the clustering key columns.
# Reproduces #12014, the code is taken from a reproducer provided by a user.
def test_multi_column_range_restrictions_and_filtering(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int, ts timestamp, id int, processed boolean, PRIMARY KEY (pk, ts, id)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ts, id, processed) VALUES (0, currentTimestamp(), 0, true)")
        cql.execute(f"INSERT INTO {table} (pk, ts, id, processed) VALUES (0, currentTimestamp(), 1, true)")
        cql.execute(f"INSERT INTO {table} (pk, ts, id, processed) VALUES (0, currentTimestamp(), 2, false)")
        cql.execute(f"INSERT INTO {table} (pk, ts, id, processed) VALUES (0, currentTimestamp(), 3, false)")
        # This select doesn't use multi-column restrictions, the result shouldn't change when it does.
        rows1 = list(cql.execute(f"SELECT id, processed FROM {table} WHERE pk = 0 AND ts >= 0 AND processed = false ALLOW FILTERING"))
        assert rows1 == [(2, False), (3, False)]
        # Reproduces #12014
        rows2 = list(cql.execute(f"SELECT id, processed FROM {table} WHERE pk = 0 AND (ts, id) >= (0, 0) AND processed = false ALLOW FILTERING"))
        assert rows1 == rows2

# Like the previous test, just in cases that require ALLOW FILTERING.
# We add another clustering key column to ensure that filtering *in*
# a long partition is really needed - not just filtering on the partitions
# (these are two different code paths).
@pytest.mark.xfail(reason="issue #64")
def test_multi_column_restrictions_ck_filtering(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c0 int, c1 int, c2 int, c3 int, PRIMARY KEY (p, c0, c1, c2, c3)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c0, c1, c2, c3) VALUES (1, 1, ?, ?, ?)")
        for i in range(3):
            for j in range(3):
                for k in range(3):
                    cql.execute(stmt, [i, j, k])

        # Check various equality conditions with single and multi-column
        # restrictions. The conditions do not restrict p or c0, so they
        # require ALLOW FILTERING and use Scylla's filtering code path.
        # All of the tests below reproduce #64 because the restriction
        # "skipped" c0.
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE c1=1 ALLOW FILTERING")) == [(1,0,0), (1,0,1), (1,0,2), (1,1,0), (1,1,1), (1,1,2), (1,2,0), (1,2,1), (1,2,2)]
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE c2=1 ALLOW FILTERING")) == [(0,1,0), (0,1,1), (0,1,2), (1,1,0), (1,1,1), (1,1,2), (2,1,0), (2,1,1), (2,1,2)]
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE (c1,c2) = (1,2) ALLOW FILTERING")) == [(1,2,0), (1,2,1), (1,2,2)]
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE (c2,c3) = (1,2) ALLOW FILTERING")) == [(0,1,2), (1,1,2), (2,1,2)]
        with pytest.raises(InvalidRequest, match='ulti-column'):
            assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE (c1,c3) = (1,2) ALLOW FILTERING")) == [(1,0,2), (1,1,2), (1,2,2)]
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE (c1,c2) = (1,2) AND c3=1 ALLOW FILTERING")) == [(1,2,1)]
        assert list(cql.execute(f"SELECT c1,c2,c3 FROM {table} WHERE c1 = 1 AND (c2,c3) = (2,1) ALLOW FILTERING")) == [(1,2,1)]

# Test that it is allowed for the same column to participate in both a muli-
# column restriction and a single-column restriction - for example
# (c1,c2) > (0,1) AND c1<10
# Reproduces #4244. Contrasting with the other reproducers for #4244 above,
# in this test the single-column restriction is on the same column as the
# multi-column restriction, not a different column.
@pytest.mark.xfail(reason="issue #4244")
def test_multi_column_and_single_column_restriction_same_ck(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c1 int, c2 int, PRIMARY KEY (p, c1, c2)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c1, c2) VALUES (1, ?, ?)")
        for i in range(3):
            for j in range(4):
                cql.execute(stmt, [i, j])
        assert list(cql.execute(f"SELECT c1,c2 FROM {table} WHERE p=1 AND (c1,c2) > (1,2)")) == [(1,3), (2,0), (2,1), (2,2), (2,3)]
        # Reproduces #4244:
        assert list(cql.execute(f"SELECT c1,c2 FROM {table} WHERE p=1 AND (c1,c2) > (1,2) AND c1 < 2")) == [(1,3)]
        # Cassandra does not support the following request, saying that
        # "Column "c2" cannot be restricted by two inequalities not starting
        # with the same column". I think this is a Cassandra bug - such a
        # query could have been supported with ALLOW FILTERING, but for
        # now, let's just not test this case.
        #assert list(cql.execute(f"SELECT c1,c2 FROM {table} WHERE p=1 AND (c1,c2) > (1,2) AND c2 < 2 ALLOW FILTERING")) == [(2,0), (2,1)]

# Test that a token restriction can be combined with a non-token restriction,
# on the same column and on a different column..
# Reproduces issue #4244 (note that this is a different aspect of #4244 than
# the multi-column restriction problems reproduced by other tests above).
@pytest.mark.xfail(reason="issue #4244")
def test_restriction_token_and_nontoken(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c) VALUES (?, ?)")
        for i in range(10):
            for j in range(2):
                cql.execute(stmt, [i, j])
        # We don't know which tokens the partitions keys 0..9 got, so we need
        # to retrieve them first:
        p_tokens = [(x[0], x[1]) for x in cql.execute(f"SELECT p,token(p) FROM {table} WHERE c = 1 ALLOW FILTERING")]
        # Check just token(p) < sometoken:
        somep, sometoken = p_tokens[5]   # the fifth element
        result = list(cql.execute(f"SELECT p,c FROM {table} WHERE token(p) < {sometoken}"))
        expected = [(x[0], y) for x in p_tokens if x[1] < sometoken for y in range(2)]
        assert result == expected
        # Now check combination of restriction on token(p) and on c:
        result = list(cql.execute(f"SELECT p,c FROM {table} WHERE token(p) < {sometoken} AND c=1 ALLOW FILTERING"))
        expected = [(x[0], 1) for x in p_tokens if x[1] < sometoken]
        assert result == expected
        # Now check combination of restriction on token(p) and on p itself
        # This reproduces issue #4244.
        result = list(cql.execute(f"SELECT p,c FROM {table} WHERE token(p) <= {sometoken} AND p = {somep}"))
        assert result == [(somep,0), (somep,1)]

# Until #4244 is fixed Scylla should forbid combining restrictions on both
# token and partition key columns. Correctness of some functions depends
# on the assumption that when a token restriction is present there
# no restrictions on partition key columns. One such function is is_satisfied_by,
# which needs to be modified when #4244 gets fixed
def test_restriction_token_and_nontoken_forbidden(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY") as table:
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE token(p) = 0 AND p = 0")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE p = 0 AND token(p) = 0")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE token(p) < 0 AND p = 1")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE token(p) > 0 AND token(p) < 10 AND p = 1")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE token(p) > 0 AND token(p) < 0 AND p = 1")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE token(p) = 0 AND p < 0")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE token(p) = 0 AND p <= 0")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE token(p) = 0 AND p > 0")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE token(p) = 0 AND p >= 0")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE token(p) = 0 AND p != 0")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT p FROM {table} WHERE token(p) = 0 AND p IN (0, 1, 2)")

# Regression test for #9482
def test_scan_ending_with_static_row(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int, ck int, s int STATIC, v int, PRIMARY KEY (pk, ck)") as table:
        stmt = cql.prepare(f"UPDATE {table} SET s = ? WHERE pk = ?")
        for pk in range(100):
            cql.execute(stmt, (0, pk))

        statement = SimpleStatement(f"SELECT * FROM {table}", fetch_size=10)
        # This will trigger an error in either processing or building the query
        # results. The success criteria for this test is the query finishing
        # without errors.
        res = list(cql.execute(statement))
