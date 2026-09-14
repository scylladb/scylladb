# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# A collection of tests for grammar that should be rejected, but wasn't at
# some point in the past.

import pytest
from .util import new_test_table
from cassandra.protocol import SyntaxException


# table1 is just there so we can execute bad queries against it.
@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        yield table

# LIMIT should be followed by an expression (#14705)
def test_limit_empty(cql, table1):
    with pytest.raises(SyntaxException):
        cql.execute(f'SELECT * FROM {table1} LIMIT')
    with pytest.raises(SyntaxException):
        cql.execute(f'SELECT * FROM {table1} PER PARTITION LIMIT')

# INSERT JSON should require a JSON value
# Reproduces https://github.com/scylladb/scylladb/issues/14709
def test_json_empty(cql, table1):
    with pytest.raises(SyntaxException):
        cql.execute(f'INSERT INTO {table1} JSON')

# A column update is "X = X + <value>" or "X = <value> + X"; the column being
# assigned has to be one of the two operands.  Both operands can be a bare
# column name now that terms and selectors are one grammar, so the grammar
# checks which of them names the column.
def test_column_update_names_neither_operand(cql, table1):
    with pytest.raises(SyntaxException, match='Only expressions of the form X = <value>'):
        cql.execute(f'UPDATE {table1} SET v = v + c WHERE p=1 AND c=1')
    with pytest.raises(SyntaxException, match='Only expressions of the form X = X'):
        cql.execute(f'UPDATE {table1} SET v = c + 1 WHERE p=1 AND c=1')
