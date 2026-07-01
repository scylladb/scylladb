# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Various tests for Light-Weight Transactions (LWT) support in Scylla.
# Note that we have many more LWT tests in the cql-repl framework:
# ../cql/lwt*_test.cql, ../cql/cassandra_cql_test.cql.
#############################################################################

import re
import time

import pytest
import requests
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.protocol import InvalidRequest, SyntaxException

from .util import new_test_table, unique_key_int, new_user, new_session
from .test_service_levels import new_service_level

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    schema='p int, c int, r int, s int static, PRIMARY KEY(p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        yield table

# An LWT UPDATE whose condition uses non-static columns begins by reading
# the clustering row which must be specified by the WHERE. If there is a
# static column in the partition, it is read as well. The value of the all
# these columns - regular and static - is then passed to the condition.
# As discovered in issue #10081, if the row determined by WHERE does NOT
# exist, Scylla still needs to read the static column, but forgets to do so.
# this test reproduces this issue.
def test_lwt_missing_row_with_static(cql, table1):
    p = unique_key_int()
    # Insert into partition p just the static column - and no clustering rows.
    cql.execute(f'INSERT INTO {table1}(p, s) values ({p}, 1)')
    # Now, do an update with WHERE p={p} AND c=1. This clustering row does
    # *not* exist, so we expect to see r=null - and s=1 from before.
    r = list(cql.execute(f'UPDATE {table1} SET s=2,r=1 WHERE p={p} AND c=1 IF s=1 and r=null'))
    assert len(r) == 1
    assert r[0].applied == True
    # At this point we should have one row, for c=1 
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == [(p, 1, 2, 1)]

# The fact that to reproduce #10081 above we needed the condition (IF) to
# mention a non-static column as well, suggests that Scylla has a different code
# path for the case that the condition has *only* static columns. In fact,
# in that case, the WHERE doesn't even need to specify the clustering key -
# the partition key should be enough. The following test confirms that this
# is indeed the case.
def test_lwt_static_condition(cql, table1):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1}(p, s) values ({p}, 1)')
    # When the condition only mentions static (partition-wide) columns,
    # it is allowed not to specify the clustering key in the WHERE:
    r = list(cql.execute(f'UPDATE {table1} SET s=2 WHERE p={p} IF s=1'))
    assert len(r) == 1
    assert r[0].applied == True
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == [(p, None, 2, None)]
    # When the condition also mentions a non-static column, WHERE must point
    # to a clustering column, i.e., mention the clustering key. If the
    # clustering key is missing, we get an InvalidRequest error, where the
    # message is slightly different between Scylla and Cassandra ("Missing
    # mandatory PRIMARY KEY part c" and "Some clustering keys are missing: c",
    # respectively.
    with pytest.raises(InvalidRequest, match=re.compile('missing', re.IGNORECASE)):
        cql.execute(f'UPDATE {table1} SET s=2 WHERE p={p} IF r=1')

# Generate an LWT update where there is no value for the partition key,
# as the WHERE restricts it using `p = {p} AND p = {p+1}`.
# Such queries are rejected.
def test_lwt_empty_partition_range(cql, table1):
    with pytest.raises(InvalidRequest):
        cql.execute(f"UPDATE {table1} SET r = 9000 WHERE p = 1 AND p = 1000 AND c = 2 IF r = 3")

# Generate an LWT update where there is no value for the clustering key,
# as the WHERE restricts it using `c = 2 AND c = 3`.
# Such queries are rejected.
def test_lwt_empty_clustering_range(cql, table1):
    with pytest.raises(InvalidRequest):
        cql.execute(f"UPDATE {table1} SET r = 9000 WHERE p = 1  AND c = 2 AND c = 2000 IF r = 3")

# In an LWT batch, if one of the condition fails the entire batch is not
# applied. All conditions in a batch use the same values before the batch,
# so if a batch has both a IF EXISTS and IF NOT EXISTS on the same row, they
# can't possibly both be true, so this batch is guaranteed to fail
# regardless of the data. Cassandra detects this specific conflict, and
# prints an error instead of silently failing the batch, but in ScyllaDB
# we considered this check to be inconsistent and unhelpful, and
# decided not to implement it, and this case is treated as a normal batch
# failure (not all conditions are true), not an error. See discussion in #13011.
# The test is marked scylla_only because it will fail on Cassandra which will
# report an error, not a batch failure.
def test_lwt_with_batch_conflict_1(cql, table1, scylla_only):
    p = unique_key_int()
    rs = cql.execute(f'BEGIN BATCH DELETE FROM {table1} WHERE p={p} AND c=1 IF EXISTS; INSERT INTO {table1}(p,c,r) VALUES ({p},1,2) IF NOT EXISTS; APPLY BATCH;')
    # Cassandra fails with: "Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row"
    for r in rs:
        assert r.applied == False

# However, Cassandra does not detect every case of a conflict between
# different conditions in a batch. For example, trying both "IF r=1"
# and "IF r=2" returns a not-applied - not an error message.
def test_lwt_with_batch_conflict_2(cql, table1):
    p = unique_key_int()
    rs = list(cql.execute(f'BEGIN BATCH UPDATE {table1} SET r=10 WHERE p={p} AND c=1 IF r=1; UPDATE {table1} SET r=20 WHERE p={p} AND c=1 IF r=2;  APPLY BATCH;'))
    # Note that as a documented difference between Scylla and Cassandra,
    # Cassandra returns just one applied=False in the result r, while
    # Scylla returns a separate row for each of the two conditions.
    for r in rs:
        assert r.applied == False

# Moreover, there are cases where Cassandra prevents mixing
# IF EXISTS with different conditions such as IF r=1, despite
# the fact that the conditions are not contradictory.
def test_lwt_with_batch_conflict_3(cql, table1, scylla_only):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1}(p,c,r) VALUES ({p},1,1)')
    rs = cql.execute(f'BEGIN BATCH UPDATE {table1} SET r=10 WHERE p={p} AND c=1 IF r=1; UPDATE {table1} SET r=20 WHERE p={p} AND c=1 IF EXISTS;  APPLY BATCH;')
    # Cassandra fails with: "Cannot mix IF conditions and IF EXISTS for the same row"
    for r in rs:
        assert r.applied == True

# During a static column update, the clustering key is not required in the WHERE clause.
# A batch composed of a query that updates a static column and a query that inserts
# a new row is allowed and will be applied successfully.
def test_lwt_with_batch_conflict_4(cql, table1):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1}(p,c,r,s) VALUES ({p},1,1,1)')
    rs = cql.execute(f'BEGIN BATCH UPDATE {table1} SET s=NULL WHERE p={p} IF EXISTS; INSERT INTO {table1}(p,c,r) VALUES ({p},2,2) IF NOT EXISTS; APPLY BATCH;')
    for r in rs:
        assert r.applied == True

# However, even for static columns, mixing IF EXISTS and other conditions
# is disallowed in Cassandra.
def test_lwt_with_batch_conflict_5(cql, table1, scylla_only):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1}(p,c,r,s) VALUES ({p},1,1,1)')
    rs = cql.execute(f'BEGIN BATCH UPDATE {table1} SET s=NULL WHERE p={p} IF EXISTS; UPDATE {table1} SET s=NULL WHERE p={p} IF s = 1; APPLY BATCH;')
    # Cassandra fails with: "Cannot mix IF conditions and IF NOT EXISTS for the same row"
    # The fact that "IF NOT EXISTS" is used in the error message is a bit misleading,
    # but that's what Cassandra returns
    for r in rs:
        assert r.applied == True


# Test NOT IN condition in LWT IF clause
#
# Cassandra rejects this with "line 1:84 no viable alternative at input 'NOT' (...AND c=1 IF r [NOT]...)"",
# indicating its grammar only supports this for WHERE, not IF.
def test_lwt_not_in(cql, table1, cassandra_bug):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1}(p, c, r) values ({p}, 1, 1)')
    rs = list(cql.execute(f'UPDATE {table1} SET r=2 WHERE p={p} AND c=1 IF r NOT IN (1, 2)'))
    for r in rs:
        assert r.applied == False
    # Check that we look at the entire list, not just the first element
    rs = list(cql.execute(f'UPDATE {table1} SET r=2 WHERE p={p} AND c=1 IF r NOT IN (2, 1)'))
    for r in rs:
        assert r.applied == False
    rs = list(cql.execute(f'UPDATE {table1} SET r=2 WHERE p={p} AND c=1 IF r NOT IN (7, 8)'))
    for r in rs:
        assert r.applied == True
    # LWT IF conditions don't treat NULL as a special value
    rs = list(cql.execute(f'UPDATE {table1} SET r=NULL WHERE p={p} AND c=1 IF r NOT IN (NULL, 7, 8)'))
    for r in rs:
        assert r.applied == True
    # Similar, but now show that NULL input fails the condition
    rs = list(cql.execute(f'UPDATE {table1} SET r=NULL WHERE p={p} AND c=1 IF r NOT IN (NULL, 7, 8)'))
    for r in rs:
        assert r.applied == False

# Currently in Cassandra and in Scylla (see discussion in #24229), the full
# LWT "IF" syntax is allowed on UPDATE and DELETE, but only the IF NOT EXISTS
# condition is allowed for INSERT. This test confirms that INSERT with
# IF NOT EXISTS works correctly, in other words, it:
# 1. Returns a response with a boolean "applied" attribute.
# 2. Doesn't do anything (and returns applied=False) when the row already
#    exists.
def test_lwt_insert_if_not_exists(cql, table1):
    p = unique_key_int()
    # The row doesn't yet exist, IF NOT EXISTS will insert it.
    rs = list(cql.execute(f'INSERT INTO {table1}(p, c, r) values ({p}, 1, 1) IF NOT EXISTS'))
    assert len(rs) == 1
    assert rs[0].applied
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == [(p, 1, None, 1)]
    # Try again to insert the same row with IF NOT EXISTS, it won't do it,
    # and the row won't change:
    rs = list(cql.execute(f'INSERT INTO {table1}(p, c, r) values ({p}, 1, 2) IF NOT EXISTS'))
    assert len(rs) == 1
    assert not rs[0].applied
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == [(p, 1, None, 1)]

# Similarly to the above test, INSERT JSON should also accept IF NOT EXISTS
# and work as expected with it. Reproduces issue #8682.
@pytest.mark.xfail(reason="issue #8682")
def test_lwt_insert_json_if_not_exists(cql, table1):
    p = unique_key_int()
    # The row doesn't yet exist, IF NOT EXISTS will insert it.
    rs = list(cql.execute("""INSERT INTO %s JSON '{"p": %s, "c": 1, "r": 1}' IF NOT EXISTS""" % (table1, p)))
    # The following assert failed in #8682 (INSERT didn't return a response
    # row).
    assert len(rs) == 1
    assert rs[0].applied
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == [(p, 1, None, 1)]
    # Try again to insert the same row with IF NOT EXISTS, it won't do it,
    # and the row won't change:
    rs = list(cql.execute("""INSERT INTO %s JSON '{"p": %s, "c": 1, "r": 2}' IF NOT EXISTS""" % (table1, p)))
    assert len(rs) == 1
    assert not rs[0].applied
    # The following assert failed in #8682 (the INSERT was done despite the
    # row existing).
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == [(p, 1, None, 1)]

# Test that the counter syntax SET i = i + 1 is not allowed on non-counter
# columns. This prepares us to test the same thing for LWT updates, in the
# next test.
def test_counter_syntax_non_counter(cql, table1):
    p = unique_key_int()
    # Without an LWT condition, arithmetic on non-counter columns is rejected.
    with pytest.raises(InvalidRequest):
        cql.execute(f'UPDATE {table1} SET r = r + 1 WHERE p={p} AND c=1')
    with pytest.raises(InvalidRequest):
        cql.execute(f'UPDATE {table1} SET r = r - 1 WHERE p={p} AND c=1')

# Test that arithmetic SET without an IF clause is rejected at prepare time,
# not silently cached and only rejected at execution (in the previous test,
# test_counter_syntax_non_counter, we tested execution).
def test_counter_syntax_non_counter_prepare(cql, table1):
    # PREPARE without IF clause must fail immediately, not succeed and then
    # fail later at EXECUTE time.
    with pytest.raises(InvalidRequest):
        cql.prepare(f'UPDATE {table1} SET r = r + 1 WHERE p = ? AND c = ?')
    with pytest.raises(InvalidRequest):
        cql.prepare(f'UPDATE {table1} SET r = r - 1 WHERE p = ? AND c = ?')

# Test that the counter syntax SET r = r + 1 IS allowed in an LWT update
# on non-counter integer columns (issue #10568). This is a Scylla extension
# (Cassandra rejects it). The underlying CAS mechanism reads the old value
# and writes the incremented result atomically.
def test_lwt_counter_syntax(cql, table1, scylla_only):
    p = unique_key_int()
    # Insert a row with r explicitly set to 0. Arithmetic on a null column
    # is an error, so the column must have a value before using arithmetic SET.
    cql.execute(f'INSERT INTO {table1} (p, c, r) VALUES ({p}, 1, 0)')
    # Increment r from 0 to 1:
    rs = list(cql.execute(f'UPDATE {table1} SET r = r + 1 WHERE p={p} AND c=1 IF EXISTS'))
    assert len(rs) == 1 and rs[0].applied
    assert list(cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1')) == [(1,)]
    # Increment again by 3. r is now 1, so it will increment to 4:
    rs = list(cql.execute(f'UPDATE {table1} SET r = r + 3 WHERE p={p} AND c=1 IF EXISTS'))
    assert len(rs) == 1 and rs[0].applied
    assert list(cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1')) == [(4,)]
    # Subtraction also works, decrement r by 2 so it will go from 4 to 2. This
    # time we'll use a condition on r itself, the condition is on r before the
    # update.
    rs = list(cql.execute(f'UPDATE {table1} SET r = r - 2 WHERE p={p} AND c=1 IF r = 4'))
    assert len(rs) == 1 and rs[0].applied
    assert list(cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1')) == [(2,)]
    # Try a more sophisticated condition on the arithmetic operation:
    # Decrement N from r, but only if r>=N. Try it for one N where it
    # fails (3) and one where it succeeds (1).
    rs = list(cql.execute(f'UPDATE {table1} SET r = r - 3 WHERE p={p} AND c=1 IF r >= 3'))
    assert len(rs) == 1 and not rs[0].applied
    assert list(cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1')) == [(2,)]
    rs = list(cql.execute(f'UPDATE {table1} SET r = r - 1 WHERE p={p} AND c=1 IF r >= 1'))
    assert len(rs) == 1 and rs[0].applied
    assert list(cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1')) == [(1,)]

# Arithmetic on a null just results in a null, so "r = r + 1" just does nothing
# if r was never initialized - it is NOT caught as an error. This is how
# expressions work in SQL, but can be considered a footgun; In contrast,
# DynamoDB does throw an error when an expression uses an uninitialized
# attribute.
def test_lwt_counter_syntax_null_column(cql, table1, scylla_only):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p, c) VALUES ({p}, 1) IF NOT EXISTS')
    # At this point, the row (p, 1) exists but has r is null.
    rs = list(cql.execute(f'UPDATE {table1} SET r = r + 1 WHERE p={p} AND c=1 IF EXISTS'))
    # The condition IF EXISTS was true (the row exists), so the LWT was applied.
    assert len(rs) == 1 and rs[0].applied
    # But the column r was not written: r + 1 where r is null results in null,
    # so r should still be unset.
    assert cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1').one().r is None

    # Verify the same for subtraction:
    rs = list(cql.execute(f'UPDATE {table1} SET r = r - 1 WHERE p={p} AND c=1 IF EXISTS'))
    assert len(rs) == 1 and rs[0].applied
    assert cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1').one().r is None

    # We can achieve the same thing with a condition on r (r != null)
    # instead of on the row (IF EXISTS). But the difference in this case is
    # that a condition on r allows the user to catch an uninitialized r, by
    # noticing that the LWT condition failed.
    rs = list(cql.execute(f'UPDATE {table1} SET r = r + 1 WHERE p={p} AND c=1 IF r != null'))
    assert len(rs) == 1 and not rs[0].applied
    assert cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1').one().r is None
    # After initializing r, the condition passes and the increment takes effect.
    cql.execute(f'UPDATE {table1} SET r = 0 WHERE p={p} AND c=1')
    rs = list(cql.execute(f'UPDATE {table1} SET r = r + 1 WHERE p={p} AND c=1 IF r != null'))
    assert len(rs) == 1 and rs[0].applied
    assert cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1').one().r == 1

# Test that the LWT counter syntax is allowed for all numeric types, not just
# integers. This is a Scylla extension (issue #10568).
def test_lwt_counter_syntax_numeric_types(cql, test_keyspace, scylla_only):
    # All CQL numeric types that should support arithmetic SET in LWT updates.
    numeric_types = ['tinyint', 'smallint', 'int', 'bigint', 'varint', 'float', 'double', 'decimal']
    # Build a table with one column per numeric type, all named col_<type>.
    col_defs = ', '.join(f'col_{t} {t}' for t in numeric_types)
    schema = f'p int PRIMARY KEY, {col_defs}'
    with new_test_table(cql, test_keyspace, schema) as table:
        p = unique_key_int()
        # Initialize all columns to 0; arithmetic requires a non-null value.
        col_names = ', '.join(f'col_{t}' for t in numeric_types)
        zero_vals = ', '.join(['0'] * len(numeric_types))
        cql.execute(f'INSERT INTO {table} (p, {col_names}) VALUES ({p}, {zero_vals})')
        for t in numeric_types:
            col = f'col_{t}'
            # Increment from 0 to 1 using IF EXISTS.
            rs = list(cql.execute(f'UPDATE {table} SET {col} = {col} + 1 WHERE p = {p} IF EXISTS'))
            assert len(rs) == 1 and rs[0].applied, f'increment from 0 failed for type {t}'
            row = cql.execute(f'SELECT {col} FROM {table} WHERE p = {p}').one()
            assert getattr(row, col) == 1, f'expected 1 after increment for type {t}, got {getattr(row, col)}'
            # Increment again from 1 to 2.
            rs = list(cql.execute(f'UPDATE {table} SET {col} = {col} + 1 WHERE p = {p} IF EXISTS'))
            assert len(rs) == 1 and rs[0].applied, f'second increment failed for type {t}'
            row = cql.execute(f'SELECT {col} FROM {table} WHERE p = {p}').one()
            assert getattr(row, col) == 2, f'expected 2 after second increment for type {t}, got {getattr(row, col)}'
            # Subtract 1 from 2, leaving 1.
            rs = list(cql.execute(f'UPDATE {table} SET {col} = {col} - 1 WHERE p = {p} IF EXISTS'))
            assert len(rs) == 1 and rs[0].applied, f'subtraction failed for type {t}'
            row = cql.execute(f'SELECT {col} FROM {table} WHERE p = {p}').one()
            assert getattr(row, col) == 1, f'expected 1 after subtraction for type {t}, got {getattr(row, col)}'

# Currently, the syntax "SET r = p + 1" (different column on LHS and RHS) is
# NOT allowed - the CQL grammar only allows "X = X +/- value", so mismatching
# columns is a syntax error, regardless of whether the statement has an IF
# clause. We may decide to allow this syntax in the future, in which case
# this test should be changed - but for now we don't support it.
def test_lwt_counter_syntax_mismatched_column(cql, table1):
    p = unique_key_int()
    # The grammar rejects r = p + 1 (p != r) as a SyntaxException.
    with pytest.raises(SyntaxException, match='Only expressions of the form X = X'):
        cql.execute(f'UPDATE {table1} SET r = p + 1 WHERE p={p} AND c=1 IF EXISTS')
    with pytest.raises(SyntaxException, match='Only expressions of the form X = X'):
        cql.execute(f'UPDATE {table1} SET r = p - 1 WHERE p={p} AND c=1 IF EXISTS')
    # Also rejected without an IF clause:
    with pytest.raises(SyntaxException, match='Only expressions of the form X = X'):
        cql.execute(f'UPDATE {table1} SET r = p + 1 WHERE p={p} AND c=1')

# We checked the LWT counter syntax SET r = r + 1 on regular columns, let's
# check that it's *not* allowed for key columns: p and c are numeric but still
# not allowed because they cannot be set by an UPDATE.
def test_lwt_counter_forbidden_key_columns(cql, table1):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match='PRIMARY KEY'):
        cql.execute(f'UPDATE {table1} SET p = p + 1 WHERE p={p} AND c=1 IF EXISTS')
    with pytest.raises(InvalidRequest, match='PRIMARY KEY'):
        cql.execute(f'UPDATE {table1} SET c = c + 1 WHERE p={p} AND c=1 IF EXISTS')

# Test that the LWT counter syntax works in prepared statements, including
# the operand coming from a bind variable.
def test_lwt_counter_syntax_prepared(cql, table1, scylla_only):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p, c, r) VALUES ({p}, 1, 10)')
    # Prepare a statement with a bind variable for the increment delta.
    inc_stmt = cql.prepare(f'UPDATE {table1} SET r = r + ? WHERE p = ? AND c = ? IF EXISTS')
    dec_stmt = cql.prepare(f'UPDATE {table1} SET r = r - ? WHERE p = ? AND c = ? IF EXISTS')
    # Increment r by 5: 10 -> 15.
    rs = list(cql.execute(inc_stmt, [5, p, 1]))
    assert len(rs) == 1 and rs[0].applied
    assert cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1').one().r == 15
    # Decrement r by 3: 15 -> 12.
    rs = list(cql.execute(dec_stmt, [3, p, 1]))
    assert len(rs) == 1 and rs[0].applied
    assert cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1').one().r == 12
    # Execute the same prepared statement again with a different delta: 12 -> 17.
    rs = list(cql.execute(inc_stmt, [5, p, 1]))
    assert len(rs) == 1 and rs[0].applied
    assert cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1').one().r == 17
    # A failed condition leaves r unchanged.
    rs = list(cql.execute(dec_stmt, [100, p, 999]))  # c=999 does not exist
    assert len(rs) == 1 and not rs[0].applied
    assert cql.execute(f'SELECT r FROM {table1} WHERE p={p} AND c=1').one().r == 17

# Test that the LWT counter syntax (add and subtract) catches overflows and
# underflows for fixed-width integer types (tinyint, smallint, int, bigint)
# and doesn't allow them to wrap around.
def test_lwt_counter_syntax_overflow(cql, test_keyspace, scylla_only):
    # (type name, bits) for each fixed-width signed integer type.
    integer_types = [
        ('tinyint',  8),
        ('smallint', 16),
        ('int',      32),
        ('bigint',   64),
    ]
    col_defs = ', '.join(f'col_{t} {t}' for t, _ in integer_types)
    schema = f'p int PRIMARY KEY, {col_defs}'
    with new_test_table(cql, test_keyspace, schema) as table:
        p = unique_key_int()
        for t, bits in integer_types:
            col = f'col_{t}'
            max_val = 2**(bits-1) - 1
            min_val = -(2**(bits-1))
            # Incrementing past the maximum must be rejected, not wrap around.
            cql.execute(f'UPDATE {table} SET {col} = {max_val} WHERE p = {p}')
            with pytest.raises(InvalidRequest, match='overflow'):
                cql.execute(f'UPDATE {table} SET {col} = {col} + 1 WHERE p = {p} IF {col} = {max_val}')
            # Likewise, decrementing past the minimum must be rejected.
            cql.execute(f'UPDATE {table} SET {col} = {min_val} WHERE p = {p}')
            with pytest.raises(InvalidRequest, match='overflow'):
                cql.execute(f'UPDATE {table} SET {col} = {col} - 1 WHERE p = {p} IF {col} = {min_val}')
            # The subtraction -1 - MININT should *not* overflow, it has a valid
            # result MAXINT. This forced us to implement a separate SUB
            # operation, not just ADD and NEG (unary minus), because NEG on
            # MININT overflows.
            cql.execute(f'UPDATE {table} SET {col} = -1 WHERE p = {p}')
            stmt = cql.prepare(f'UPDATE {table} SET {col} = {col} - ? WHERE p = ? IF {col} = -1')
            cql.execute(stmt, [min_val, p])
            assert cql.execute(f'SELECT {col} FROM {table} WHERE p = {p}').one()[0] == max_val

# Test that adding a float literal (3.5) to an int column is rejected at
# because 3.5 is not a valid integer value.
def test_lwt_counter_syntax_float_on_integer(cql, table1, scylla_only):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p, c, r) VALUES ({p}, 1, 0)')
    with pytest.raises(InvalidRequest, match='of type int'):
        cql.execute(f'UPDATE {table1} SET r = r + 3.5 WHERE p={p} AND c=1 IF EXISTS')
    with pytest.raises(InvalidRequest, match='of type int'):
        cql.execute(f'UPDATE {table1} SET r = r - 3.5 WHERE p={p} AND c=1 IF EXISTS')
    # Verify the type check is enforced at prepare time if 3.5 is a constant,
    # or at execute time if 3.5 is a bind variable:
    with pytest.raises(InvalidRequest, match='of type int'):
        cql.prepare(f'UPDATE {table1} SET r = r + 3.5 WHERE p = ? AND c = ? IF EXISTS')
    with pytest.raises(InvalidRequest, match='of type int'):
        cql.prepare(f'UPDATE {table1} SET r = r - 3.5 WHERE p = ? AND c = ? IF EXISTS')
    inc_stmt = cql.prepare(f'UPDATE {table1} SET r = r + ? WHERE p = ? AND c = ? IF EXISTS')
    dec_stmt = cql.prepare(f'UPDATE {table1} SET r = r - ? WHERE p = ? AND c = ? IF EXISTS')
    # When 3.5 is a bind variable, the Python driver catches the type mismatch
    # itself before sending the request to Scylla, raising a TypeError.
    # We don't intend the Python driver, but this check verifies that the
    # server correctly told the driver which type it expects for the bind
    # variable.
    with pytest.raises(TypeError):
        cql.execute(inc_stmt, [3.5, p, 1])
    with pytest.raises(TypeError):
        cql.execute(dec_stmt, [3.5, p, 1])

# Test that trying to add "decimal" values with wildly different scales is
# rejected with an error, not allowed to proceed with ridiculous amount of
# CPU and memory usage. Reproduces SCYLLADB-1576.
# This test needs to be skipped while SCYLLADB-1576 is not fixed, otherwise
# it will cause the test suite to hang or crash.
@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-1576",
    reason="Hangs or OOMs instead of rejecting",
)
def test_lwt_counter_syntax_decimal_magnitude_difference(cql, test_keyspace, scylla_only):
    # 1e100000000 is stored compactly as (unscaled=1, scale=-100000000), but
    # adding 1 to it forces alignment of decimal points, potentially allocating
    # 100 million digits and running out of memory.
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, d decimal') as table:
        p = unique_key_int()
        cql.execute(f"INSERT INTO {table} (p, d) VALUES ({p}, 1e100000000)")
        with pytest.raises(InvalidRequest):
            cql.execute(f"UPDATE {table} SET d = d + 1 WHERE p = {p} IF EXISTS")


# Helpers for verifying LWT read accounting via Prometheus metrics.
# Each service level has its own reader_concurrency_semaphore exposed as
# scylla_database_total_reads{class="sl:<name>"}.
_METRIC_RE = re.compile(r'^scylla_database_total_reads\{([^}]*)\}\s+([0-9.eE+-]+)$')
_LABEL_RE = re.compile(r'(\w+)="([^"]*)"')

def _total_reads_by_class(host, sl_classes):
    resp = requests.get(f"http://{host}:9180/metrics", timeout=30)
    resp.raise_for_status()
    totals = {c: 0.0 for c in sl_classes}
    for line in resp.text.splitlines():
        m = _METRIC_RE.match(line)
        if not m:
            continue
        cls = dict(_LABEL_RE.findall(m.group(1))).get("class")
        if cls in totals:
            totals[cls] += float(m.group(2))
    return totals


# This test verifies that LWT (Paxos) internal reads are admitted under
# the caller's service level, not always under sl:default.  It disproves
# a theory in SCYLLADB-2379 that service levels might not be propagated
# to LWT reads.
def test_lwt_reads_use_attached_service_level(scylla_only, cql, host, test_keyspace_vnodes):
    with new_user(cql, with_superuser_privileges=True) as user, \
         new_service_level(cql, shares=200, role=user) as sl, \
         new_test_table(cql, test_keyspace_vnodes,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        sl_class = f"sl:{sl}"
        insert = f"INSERT INTO {table} (p, c, v) VALUES (?, 0, 0) IF NOT EXISTS"
        N = 1000

        # Phase 1: Wait for the service level to propagate. A session's
        # service level is resolved at login from an asynchronously-populated
        # cache, so open a new session on each attempt until one lands on the
        # SL's semaphore.
        deadline = time.time() + 30
        while True:
            with new_session(cql, user) as session:
                stmt = session.prepare(insert)
                before = _total_reads_by_class(host, [sl_class])
                execute_concurrent_with_args(session, stmt, [(0,)])
                after = _total_reads_by_class(host, [sl_class])
                if after[sl_class] - before[sl_class] > 0:
                    break
            assert time.time() < deadline, f"service level {sl_class} never took effect"

        # Phase 2: Run the actual measurement. All N LWTs should account
        # their reads to the SL's semaphore; none should leak to sl:default.
        with new_session(cql, user) as session:
            stmt = session.prepare(insert)
            params = [(p,) for p in range(1, N + 1)]
            classes = [sl_class, "sl:default"]
            before = _total_reads_by_class(host, classes)
            execute_concurrent_with_args(session, stmt, params)
            after = _total_reads_by_class(host, classes)
            sl_delta = after[sl_class] - before[sl_class]
            default_delta = after["sl:default"] - before["sl:default"]

        assert sl_delta >= N, (
            f"expected >= {N} reads on {sl_class}, got {sl_delta}")
        assert default_delta == 0, (
            f"LWT reads leaked to sl:default: {default_delta} reads "
            f"accounted to sl:default vs {sl_delta} to {sl_class}")
