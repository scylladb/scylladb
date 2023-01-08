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

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
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
