# Copyright 2020-present ScyllaDB
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
##################################################################

# Various utility functions which are useful for multiple tests.
# Note that fixtures aren't here - they are in conftest.py.

import string
import random
import time
from contextlib import contextmanager

def random_string(length=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(length))

# A function for picking a unique name for test keyspace or table.
# This name doesn't need to be quoted in CQL - it only contains
# lowercase letters, numbers, and underscores, and starts with a letter.
unique_name_prefix = 'cql_test_'
def unique_name():
    current_ms = int(round(time.time() * 1000))
    # If unique_name() is called twice in the same millisecond...
    if unique_name.last_ms >= current_ms:
        current_ms = unique_name.last_ms + 1
    unique_name.last_ms = current_ms
    return unique_name_prefix + str(current_ms)
unique_name.last_ms = 0

# A utility function for creating a new temporary keyspace with given options.
# It can be used in a "with", as:
#   with new_test_keyspace(cql, '...') as keyspace:
# This is not a fixture - see those in conftest.py.
@contextmanager
def new_test_keyspace(cql, opts):
    keyspace = unique_name()
    cql.execute("CREATE KEYSPACE " + keyspace + " " + opts)
    try:
        yield keyspace
    finally:
        cql.execute("DROP KEYSPACE " + keyspace)

# A utility function for creating a new temporary table with a given schema.
# It can be used in a "with", as:
#   with new_test_table(cql, keyspace, '...') as table:
# This is not a fixture - see those in conftest.py.
@contextmanager
def new_test_table(cql, keyspace, schema, extra=""):
    table = keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table + "(" + schema + ")" + extra)
    try:
        yield table
    finally:
        cql.execute("DROP TABLE " + table)

def project(column_name_string, rows):
    """Returns a list of column values from each of the rows."""
    return [getattr(r, column_name_string) for r in rows]
