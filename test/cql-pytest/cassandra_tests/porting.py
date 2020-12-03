# Copyright 2020 ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

# This file contains various utility functions which are useful for porting
# Cassandra's tests into Python. Most of them are intended to resemble those
# in Cassandra's test/unit/org/apache/cassandra/cql3/CQLTester.java which is
# used by many of those tests.

import pytest
from util import unique_name
from contextlib import contextmanager

# A utility function for creating a new temporary table with a given schema.
# Because Scylla becomes slower when a huge number of uniquely-named tables
# are created and deleted (see https://github.com/scylladb/scylla/issues/7620)
# we keep here a list of previously used but now deleted table names, and
# reuse one of these names when possible.
# This function can be used in a "with", as:
#   with create_table(cql, test_keyspace, '...') as table:
previously_used_table_names = []
@contextmanager
def create_table(cql, keyspace, schema):
    global previously_used_table_names
    if not previously_used_table_names:
        previously_used_table_names.append(unique_name())
    table_name = previously_used_table_names.pop()
    table = keyspace + "." + table_name
    cql.execute("CREATE TABLE " + table + " " + schema)
    yield table
    cql.execute("DROP TABLE " + table)
    previously_used_table_names.append(table_name)

def execute(cql, table, cmd, *args):
    if args:
        prepared = cql.prepare(cmd % table)
        return cql.execute(prepared, args)
    else:
        return cql.execute(cmd % table)

def assert_invalid_throw(cql, table, type, cmd, *args):
    with pytest.raises(type):
        execute(cql, table, cmd, *args)

def assert_invalid(cql, table, cmd, *args):
    assert_invalid_throw(cql, table, Exception, cmd, *args)

# Cassandra's CQLTester.java has a much more elaborate implementation
# of these functions, which carefully prints out the differences when
# the assertion fails. We can also do this in the future, but pytest's
# assert rewriting is already pretty good that makes such eleborate
# comparison code less critical.
def assert_row_count(result, expected):
    assert len(list(result)) == expected

def assert_empty(result):
    assert len(list(result)) == 0

def assert_rows(result, *expected):
    allresults = list(result)
    assert len(allresults) == len(expected)
    for r,e in zip(allresults, expected):
        assert list(r) == e
