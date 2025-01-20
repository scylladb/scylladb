# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the UNSET_VALUE value introduced in CQL version 4. Unset values
# can be bound to variables, which cause certain CQL assignments to be skipped,
# and may have other effects on other requests - and cause errors in places
# where it's not allowed.
#############################################################################

import pytest
from util import new_test_table, unique_key_int
from cassandra.query import UNSET_VALUE
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import InvalidRequest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, a int, b int, c int, li list<int>") as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, PRIMARY KEY (p, c)") as table:
        yield table

@pytest.fixture(scope="module")
def table3(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, r int, PRIMARY KEY (p, c)") as table:
        yield table

# A basic test that in a prepared statement with three assignments, one
# bound by an UNSET_VALUE is simply not done, but the other ones are.
# Try all 2^3 combinations of a 3 column updates with each one set to either
# a real value or an UNSET_VALUE.
def test_update_unset_value_basic(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f'UPDATE {table1} SET a=?, b=?, c=? WHERE p={p}')
    a = 1
    b = 2
    c = 3
    cql.execute(stmt, [a, b, c])
    assert [(a, b, c)] == list(cql.execute(f'SELECT a,b,c FROM {table1} WHERE p = {p}'))
    i = 4
    for unset_a in [False, True]:
        for unset_b in [False, True]:
            for unset_c in [False, True]:
                if unset_a:
                    newa = UNSET_VALUE
                else:
                    newa = i
                    a = i
                    i += 1
                if unset_b:
                    newb = UNSET_VALUE
                else:
                    newb = i
                    b = i
                    i += 1
                if unset_c:
                    newc = UNSET_VALUE
                else:
                    newc = i
                    c = i
                    i += 1
                cql.execute(stmt, [newa, newb, newc])
                assert [(a, b, c)] == list(cql.execute(f'SELECT a,b,c FROM {table1} WHERE p = {p}'))

# The expression "SET a=?" is skipped if the bound value is UNSET_VALUE.
# But what if it is part of a more complex expression like "SET a=(int)?+1"
# (arithmetic expression on the bind variable)? Does the SET also get
# skipped? Cassandra, and Scylla, decided that the answer will be no:
# We refuse to evaluate expressions involving an UNSET_VALUE, and in
# such case the whole write request will fail instead of parts of it being
# skipped. See discussion in pull request #12517.

@pytest.mark.xfail(reason="issue #2693 - Scylla doesn't yet support arithmetic expressions")
def test_update_unset_value_expr_arithmetic(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f'UPDATE {table1} SET a=(int)?+1 WHERE p={p}')
    cql.execute(stmt, [7])
    assert [(8,)] == list(cql.execute(f'SELECT a FROM {table1} WHERE p = {p}'))
    with pytest.raises(InvalidRequest):
        cql.execute(stmt, [UNSET_VALUE])

# Despite the decision that expressions will not allow UNSET_VALUE, Cassandra
# decided that (quoting its NEWS.txt) "an unset bind counter operation does
# not change the counter value.".  So "c = c + ?" for a counter, when given
# an UNSET_VALUE, will causes the write to be skipped, without error.
# The rationale is that "c = c + ?" is not an expression - it doesn't actually
# calculate c + ?, but rather it is a primitive increment operation, and
# passing ?=UNSET_VALUE should be able to skip this primitive operation.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def test_unset_counter_increment(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table:
        p = unique_key_int()
        stmt = cql.prepare(f'UPDATE {table} SET c=c+? WHERE p={p}')
        cql.execute(stmt, [3])
        assert [(3,)] == list(cql.execute(f'SELECT c FROM {table} WHERE p = {p}'))
        cql.execute(stmt, [UNSET_VALUE])
        assert [(3,)] == list(cql.execute(f'SELECT c FROM {table} WHERE p = {p}'))

# Like the counter increment, a list append operation (li=li+?) is a primitive
# operation and not expression, so we believe UNSET_VALUE should be able
# to skip it, and Scylla indeed does as this test shows. Cassandra fails
# this test - it produces an internal error on a bad cast, and we consider
# this a Cassandra bug and hence the cassandra_bug tag.
def test_unset_list_append(cql, table1, cassandra_bug):
    p = unique_key_int()
    stmt = cql.prepare(f'UPDATE {table1} SET li=li+? WHERE p={p}')
    cql.execute(stmt, [[7]])
    assert [([7],)] == list(cql.execute(f'SELECT li FROM {table1} WHERE p = {p}'))
    cql.execute(stmt, [UNSET_VALUE])
    assert [([7],)] == list(cql.execute(f'SELECT li FROM {table1} WHERE p = {p}'))

# According to Cassandra's NEWS.txt, "an unset bind ttl is treated as
# 'unlimited'". It shouldn't skip the write.
# Note that the NEWS.txt is not accurate: An unset ttl isn't really treated
# as unlimited, but rather as the default ttl set on the table. The default
# ttl is usually unlimited, but not always. We test that case in
# test_ttl.py::test_default_ttl_unset()
def test_unset_ttl(cql, table1):
    p = unique_key_int()
    # First write using a normal TTL:
    stmt = cql.prepare(f'UPDATE {table1} USING TTL ? SET a=? WHERE p={p}')
    cql.execute(stmt, [20000, 3])
    res = list(cql.execute(f'SELECT a, ttl(a) FROM {table1} WHERE p = {p}'))
    assert res[0].a == 3
    assert res[0].ttl_a > 10000
    # Check that an UNSET_VALUE ttl didn't skip the write but reset the TTL
    # to unlimited (None)
    cql.execute(stmt, [UNSET_VALUE, 4])
    assert [(4, None)] == list(cql.execute(f'SELECT a, ttl(a) FROM {table1} WHERE p = {p}'))

# According to Cassadra's NEWS.txt, "an unset bind timestamp is treated
# as 'now'". It shouldn't skip the write.
def test_unset_timestamp(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f'UPDATE {table1} USING TIMESTAMP ? SET a=? WHERE p={p}')
    cql.execute(stmt, [UNSET_VALUE, 3])
    assert [(3,)] == list(cql.execute(f'SELECT a FROM {table1} WHERE p = {p}'))

# According to Cassandra's NEWS.txt, "In a QUERY request an unset limit
# is treated as 'unlimited'.". It mustn't cause the query to fail (let alone
# be skipped somehow).
def test_unset_limit(cql, table2):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table2} (p, c) VALUES ({p}, 1)')
    cql.execute(f'INSERT INTO {table2} (p, c) VALUES ({p}, 2)')
    cql.execute(f'INSERT INTO {table2} (p, c) VALUES ({p}, 3)')
    cql.execute(f'INSERT INTO {table2} (p, c) VALUES ({p}, 4)')
    stmt = cql.prepare(f'SELECT c FROM {table2} WHERE p={p} limit ?')
    assert [(1,),(2,)] == list(cql.execute(stmt, [2]))
    assert [(1,),(2,),(3,),(4,)] == list(cql.execute(stmt, [UNSET_VALUE]))

# According Cassandra's NEWS.txt, "Unset WHERE clauses with unset
# partition column, clustering column or index column are not allowed.".
# For partition column, the Python driver itself complains that it's
# bound to UNSET_VALUE because it can't decide which node to send the
# request. So let's test the behavior for a clustering key column.
# Reproduces #10358
def test_unset_where_clustering(cql, table2):
    p = unique_key_int()
    stmt = cql.prepare(f'SELECT * FROM {table2} WHERE p = {p} and c = ?')
    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [UNSET_VALUE])

# ... but the NEWS.txt doesn't say what happens in other WHERE restrictions
# that involve a non-key column. In practice, those should be an error as
# well, and do cause an error in Cassandra.
# Reproduces #10358
def test_unset_where_regular(cql, table1):
    p = unique_key_int()
    # We need to add some data for the filtering to find, otherwise the
    # expression never gets evaluated and the UNSET_VALUE error is never
    # detected. Cassandra does detect this error even if there is no data,
    # but we don't care about reproducing that specific error case.
    cql.execute(f'INSERT INTO {table1} (p, a) VALUES ({p}, 1)')
    stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p = {p} and a = ? ALLOW FILTERING')
    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [UNSET_VALUE])

# TODO: check that (according to NEWS.txt documentation): "Unset tuple field,
# UDT field and map key are not allowed.".

# Although UNSET_VALUE is designed to skip part of a SET, it is not designed
# to skip an entire write which uses a an UNSET_VALUE in its WHERE clause -
# this should be treated as an error, not a silent skip.
#
# As in test_unset_where_clustering() above (the SELECT version of this test)
# we need to check the UNSET_VALUE on the clustering key because if we try
# an UNSET_VALUE on the partition key, the Python driver will refuse to
# send the request (it uses the partition key to decide which node to send
# the request).
def test_unset_insert_where(cql, table2):
    p = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table2} (p, c) VALUES ({p}, ?)')
    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [UNSET_VALUE])

# Similar to test_unset_insert_where() above, just use an LWT write ("IF
# NOT EXISTS"). Test that using an UNSET_VALUE in an LWT condition causes
# a clear error, not silent skip and not a crash as in issue #13001.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def test_unset_insert_where_lwt(cql, test_keyspace):
  # FIXME: new_test_table is used here due to https://github.com/scylladb/scylladb/issues/18066
  # When fixed, this test can go back to using the `table2` fixture.
  with new_test_table(cql, test_keyspace, "p int, c int, PRIMARY KEY (p, c)") as table2:
    p = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table2} (p, c) VALUES ({p}, ?) IF NOT EXISTS')
    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [UNSET_VALUE])

# Like test_unset_insert_where, but using UPDATE
# Python driver doesn't allow sending an UNSET_VALUE for the partition key,
# so only the clustering key is tested.
def test_unset_update_where(cql, table3):
    stmt = cql.prepare(f"UPDATE {table3} SET r = 42 WHERE p = 0 AND c = ?")

    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [UNSET_VALUE])

# Like test_unset_insert_where_lwt, but using UPDATE
# Python driver doesn't allow sending an UNSET_VALUE for the partition key,
# so only the clustering key is tested.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def test_unset_update_where_lwt(cql, test_keyspace):
  # FIXME: new_test_table is used here due to https://github.com/scylladb/scylladb/issues/18066
  # When fixed, this test can go back to using the `table3` fixture.
  with new_test_table(cql, test_keyspace, "p int, c int, r int, PRIMARY KEY (p, c)") as table3:
    stmt = cql.prepare(f"UPDATE {table3} SET r = 42 WHERE p = 0 AND c = ? IF r = ?")

    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [UNSET_VALUE, 2])

    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [1, UNSET_VALUE])
