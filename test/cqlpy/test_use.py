# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the "USE" statement, which modifies the default keyspace used
# by subsequent statements.
#
# Note that because the "USE" statement modifies the state of the current
# connection, and there is no way to undo its effects (there is no "UNUSE"
# or way to do an empty "USE"), the following tests should all use a new_cql
# wrapper over the cql fixture, instead of the cql fixture directly. This
# wrapper creates a new connection, with its own default USE.
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest
from util import unique_name, new_cql

# Check that CREATE TABLE and DROP TABLE work without an explicit keyspace
# name if a default keyspace name is specified with "USE".
def test_create_table_use_keyspace(cql, test_keyspace):
    with new_cql(cql) as ncql:
        ncql.execute(f'USE {test_keyspace}')
        table = unique_name()
        ncql.execute(f'CREATE TABLE {table} (k int PRIMARY KEY)')
        ncql.execute(f'DROP TABLE {table}')

# Check that without a USE, one cannot CREATE TABLE if the keyspace is not
# explicitly specified.
def test_create_table_no_keyspace(cql, test_keyspace):
    with new_cql(cql) as ncql:
        with pytest.raises(InvalidRequest, match='No keyspace'):
            ncql.execute(f'CREATE TABLE {unique_name()} (k int PRIMARY KEY)')
