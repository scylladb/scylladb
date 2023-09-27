# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for preparing various kinds of statements. When a client asks to prepare
# a statement, Scylla has to process it and return the correct prepared statement
# metadata. The metadata contains information about the keyspace, table and bind variables.
# Let's ensure that this information is correct.
# Here's the description of prepared metadata in CQL protocol spec:
# https://github.com/apache/cassandra/blob/1959502d8b16212479eecb076c89945c3f0f180c/doc/native_protocol_v4.spec#L675

import pytest
from util import new_test_table, unique_key_int
from cassandra.protocol import InvalidRequest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, PRIMARY KEY (p, c)") as table:
        yield table


@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 int, p2 int, p3 int, p4 int, c1 int, c2 int, r1 int, r2 int, PRIMARY KEY ((p1, p2, p3, p4), c1, c2)") as table:
        yield table

# The following tests test the generation of "pk indexes"
# "pk indexes" tell the driver which bind variable values it should use to calculate the partition token, so that it can send queries to the correct shard.
# https://github.com/apache/cassandra/blob/1959502d8b16212479eecb076c89945c3f0f180c/doc/native_protocol_v4.spec#L699-L707


# Test generating pk indexes for a single column partition key.
def test_single_pk_indexes(cql, table1):
    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = ?")
    assert prepared.routing_key_indexes == [0]

    prepared = cql.prepare(f"SELECT p, c FROM {table1} WHERE c = ? AND p = ?")
    assert prepared.routing_key_indexes == [1]

# Test that pk indexes aren't generated when the partition key column isn't restricted using a bind variable.
# In this situation the driver won't be able to calculate the token, so pk indexes should be empty (None).
def test_single_pk_no_indexes(cql, table1):
    prepared = cql.prepare(f"SELECT p, c FROM {table1}")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(f"SELECT p, c FROM {table1} WHERE c = ? ALLOW FILTERING")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = 0 AND c = ?")
    assert prepared.routing_key_indexes is None

# Test generating pk indexes for a composite partition key.
def test_composite_pk_indexes(cql, table2):
    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = ? AND p2 = ? AND p3 = ? AND p4 = ? AND c1 = ? AND c2 = ?")
    assert prepared.routing_key_indexes == [0, 1, 2, 3]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p4 = ? AND p3 = ? AND p2 = ? AND p1 = ? AND c1 = ? AND c2 = ?")
    assert prepared.routing_key_indexes == [3, 2, 1, 0]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE r1 = ? AND c2 = ? AND p3 = ? AND p1 = ? AND r2 = ? AND p4 = ? AND c1 = ? AND p2 = ? ALLOW FILTERING")
    assert prepared.routing_key_indexes == [3, 7, 2, 5]

# Test that pk indexes aren't generated when not all partition key columns are restricted using bind variables.
# In this situation the driver won't be able to calculate the token, so pk indexes should be empty (None).
def test_composite_pk_no_indexes(cql, table2):
    prepared = cql.prepare(
        f"SELECT * FROM {table2}")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = ? ALLOW FILTERING")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = ? AND p2 = ? AND p4 = ? ALLOW FILTERING")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = ? AND p2 = 0 AND p3 = ? AND p4 = ?")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = ? AND p2 = 0 AND p3 = ? AND p4 = ? AND c1 = ? AND c2 = ?")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = 0 AND p2 = 1 AND p3 = 2 AND p4 = 3 AND c1 = 5 AND c2 = 5")
    assert prepared.routing_key_indexes is None

# Test generating pk indexes for a single column partition key using named bind variables.
def test_single_pk_indexes_named_variables(cql, table1):
    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = :a")
    assert prepared.routing_key_indexes == [0]

    prepared = cql.prepare(f"SELECT p, c FROM {table1} WHERE c = :a AND p = :b")
    assert prepared.routing_key_indexes == [1]

# Test generating pk indexes for a composite partition key using named bind variables.
def test_composite_pk_indexes_named_variables(cql, table2):
    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :b AND p3 = :c AND p4 = :d AND c1 = :e AND c2 = :f")
    assert prepared.routing_key_indexes == [0, 1, 2, 3]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = :f AND p2 = :e AND p3 = :d AND p4 = :c AND c1 = :b AND c2 = :a")
    assert prepared.routing_key_indexes == [0, 1, 2, 3]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE c1 = :a AND c2 = :b AND p1 = :f AND p2 = :e AND p3 = :d AND p4 = :c")
    assert prepared.routing_key_indexes == [2, 3, 4, 5]

# Test generating pk indexes with named bind variables where the same variable is used multiple times.
# The test is scylla_only because Scylla treats :x as a single bind variable, but Cassandra thinks
# that there are two bind variables, both of them named :x.
def test_single_pk_indexes_duplicate_named_variables(cql, table1, scylla_only):
    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = :x")
    assert prepared.routing_key_indexes == [0]

    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = :x AND c = :x")
    assert prepared.routing_key_indexes == [0]

    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE c = :x AND p = :x")
    assert prepared.routing_key_indexes == [0]

# Test generating pk indexes with named bind variables where the same variable is used multiple times.
# The test is scylla_only because Scylla treats :x as a single bind variable, but Cassandra thinks
# that there are multiple bind variables, all of them named :x.
def test_composite_pk_indexes_duplicate_named_variables(cql, table2, scylla_only):
    prepared = cql.prepare(f"SELECT * FROM {table2} WHERE p1 = :x AND p2 = :x AND p3 = :x AND p4 = :x")
    assert prepared.routing_key_indexes == [0, 0, 0, 0]

    prepared = cql.prepare(f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :a AND p3 = :b AND p4 = :b")
    assert prepared.routing_key_indexes == [0, 0, 1, 1]

    prepared = cql.prepare(f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :b AND p3 = :a AND p4 = :b")
    assert prepared.routing_key_indexes == [0, 1, 0, 1]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE c1 = :a AND c2 = :b AND p1 = :a AND p2 = :b AND p3 = :a AND p4 = :b")
    assert prepared.routing_key_indexes == [0, 1, 0, 1]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :b AND p3 = :a AND p4 = :b AND c1 = :a AND c2 = :b ")
    assert prepared.routing_key_indexes == [0, 1, 0, 1]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = :x AND p2 = :x AND p3 = :z AND p4 = :y AND c1 = :y AND c2 = :z ")
    assert prepared.routing_key_indexes == [0, 0, 1, 2]

# Test what happens when using a bind marker with the same name (e.g., ":x")
# twice in a query. Above we tested which "routing_key_indexes" is returned
# in a PREPARE request, but here we test the functionality of the duplicate
# marker when actually executing the query.
# Note: The CQL protocol allows using bind markers in both QUERY (unprepared
# statement) and EXECUTE (prepared statement) cases, but it appears that the
# Python driver only supports bind markers with prepared statements (and
# for unprepared statements uses a different mechanisms with Python "%s" and
# "%(name)s"), so this test is only for the prepared-statement case.
# Reproduces issue #15559.
@pytest.mark.xfail(reason="Different behavior in Scylla and Cassandra - see issue #15559")
def test_duplicate_named_bind_marker_prepared(cql, table1):
    x = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p,c) VALUES ({x},{x})')
    cql.execute(f'INSERT INTO {table1} (p,c) VALUES ({x},{x+1})')
    # Sanity check: query without bind markers, with unnamed bind markers,
    # and with two different bind-marker names. All should work.
    assert [(x,x)] == list(cql.execute(f'SELECT * FROM {table1} WHERE p={x} AND c={x}'))
    stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p=? AND c=?')
    assert [(x,x)] == list(cql.execute(stmt, (x,x)))
    stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p=:x1 AND c=:x2')
    assert [(x,x)] == list(cql.execute(stmt, {'x1': x, 'x2': x}))
    assert [(x,x)] == list(cql.execute(stmt, (x,x)))
    # Now for the real test: Use the same bind-marker name twice.
    stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p=:x AND c=:x')
    # If EXECUTE is passed a bound value with a name "x", both bind markers
    # named "x" are assigned, and the query works:
    assert [(x,x)] == list(cql.execute(stmt, {'x': x}))
    # In Cassandra, if EXECUTE is passed unnamed bound values, they go to the
    # bind markers no matter what their name is - using ":x" twice in the
    # query has the same effect as using "?" twice - you still need to pass
    # two bound values.
    # The following currently fails on Scylla - the driver complains that
    # "Too many arguments provided to bind() (got 2, expected 1)", because
    # Scylla told it there is just one bind variable in the query.
    assert [(x,x)] == list(cql.execute(stmt, (x,x)))
    # Passing unnamed values, one can pass two different values even when the
    # query's bind markers have the same name :x. Exactly like two different
    # "?" bind markers can also be bound to different values:
    assert [(x,x+1)] == list(cql.execute(stmt, (x,x+1)))
