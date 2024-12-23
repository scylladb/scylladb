# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for clustering key ordering, namely the WITH CLUSTERING ORDER BY
# setting in the table schema, and ORDER BY in select.
#
# We have many other tests for this feature - in C++ tests, in translated
# unit tests from Cassandra (cassandra_tests), and its interaction with
# other features (filtering, secondary indexes, etc.) in other test files.

import pytest

from util import new_test_table, unique_key_int

@pytest.fixture(scope="module")
def table_int_desc(cql, test_keyspace):
    schema="k INT, c INT, PRIMARY KEY (k, c)"
    order="WITH CLUSTERING ORDER BY (c DESC)"
    with new_test_table(cql, test_keyspace, schema, order) as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    schema="p int, c1 int, c2 int, PRIMARY KEY (p, c1, c2)"
    order="WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)"
    with new_test_table(cql, test_keyspace, schema, order) as table:
        yield table

# Verify that if a table is created with descending order for its
# clustering key, the default ordering of SELECT is changed to descending
# order. This was contrary to our documentation which used to suggest
# that SELECT always defaults to ascending order.
def test_select_default_order(cql, table_int_desc):
    k = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table_int_desc} (k, c) VALUES ({k}, ?)')
    numbers = range(5)
    for i in numbers:
        cql.execute(stmt, [i])
    # In a table created with descending sort order, the default select
    # order is descending:
    rows = [(i,) for i in numbers]
    reverse_rows = [(i,) for i in reversed(numbers)]
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k}'))
    # Confirm that when specifying the order explicitly, both work:
    assert rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c ASC'))
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c DESC'))
    # Repeat the same three assertions as above, adding a "limit" of N=3:
    N=3
    rows = rows[0:N]
    reverse_rows = reverse_rows[0:N]
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} LIMIT {N}'))
    assert rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c ASC LIMIT {N}'))
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c DESC LIMIT {N}'))

# Reproduce issue #65: Test SELECT with a compound clustering key c1,c2
# and a multi-column relation (c1, c2) >= (1,1) when one of the clustering
# column has reversed order. We need to uphold the correct inequality (not
# reversed), just return results in a the reversed order.
def test_multi_column_relation_desc(cql, table2):
    k = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table2} (p, c1, c2) VALUES (?, ?, ?)')
    cql.execute(stmt, [0, 1, 0])
    cql.execute(stmt, [0, 1, 1])
    cql.execute(stmt, [0, 1, 2])
    assert [(1, 2), (1, 1)] == list(cql.execute(f'SELECT c1,c2 FROM {table2} WHERE p = 0 AND (c1, c2) >= (1, 1)'))
