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
#############################################################################

# This file contains various utility functions which are useful for porting
# Cassandra's tests into Python. Most of them are intended to resemble those
# in Cassandra's test/unit/org/apache/cassandra/cql3/CQLTester.java which is
# used by many of those tests.

import pytest
import re
import collections
import struct
import time
from util import unique_name
from contextlib import contextmanager
from cassandra.protocol import SyntaxException, InvalidRequest
from cassandra.util import SortedSet, OrderedMapSerializedKey

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
    try:
        yield table
    finally:
        cql.execute("DROP TABLE " + table)
        previously_used_table_names.append(table_name)

@contextmanager
def create_type(cql, keyspace, cmd):
    type_name = keyspace + "." + unique_name()
    cql.execute("CREATE TYPE " + type_name + " " + cmd)
    try:
        yield type_name
    finally:
        cql.execute("DROP TYPE " + type_name)

@contextmanager
def create_function(cql, keyspace, arg):
    function_name = keyspace + "." + unique_name()
    cql.execute("CREATE FUNCTION " + function_name + " " + arg)
    try:
        yield function_name
    finally:
        cql.execute("DROP FUNCTION " + function_name)

# utility function to substitute table name in command.
# For easy conversion of Java code, support %s as used in Java. Also support
# it *multiple* times (always interpolating the same table name). In Java,
# a %1$s is needed for this.
def subs_table(cmd, table):
    return cmd.replace('%s', table)

def execute(cql, table, cmd, *args):
    if args:
        prepared = cql.prepare(subs_table(cmd, table))
        return cql.execute(prepared, args)
    else:
        return cql.execute(subs_table(cmd, table))

def execute_with_paging(cql, table, cmd, page_size, *args):
    prepared = cql.prepare(subs_table(cmd, table))
    prepared.fetch_size = page_size
    return cql.execute(prepared, args)

def execute_without_paging(cql, table, cmd, *args):
    prepared = cql.prepare(subs_table(cmd, table))
    prepared.fetch_size = 0
    return cql.execute(prepared, args)

def assert_invalid_throw(cql, table, type, cmd, *args):
    with pytest.raises(type):
        execute(cql, table, cmd, *args)

def assert_invalid(cql, table, cmd, *args):
    # In the original Java code, assert_invalid() accepts any exception
    # (it's like passing "Exception" below for the exception type).
    # However, this makes it a very fragile test - any silly typo will
    # cause a SyntaxException and assert_invalid() will seem to succeed
    # without testing anything! It seems most of the tests do fine with
    # InvalidRequest tested here. If in the future it turns out that this
    #  doesn't work, maybe we should check for any exception *except*
    # SyntaxException.
    assert_invalid_throw(cql, table, InvalidRequest, cmd, *args)

def assert_invalid_message(cql, table, message, cmd, *args):
    with pytest.raises(InvalidRequest, match=re.escape(message)):
        execute(cql, table, cmd, *args)

def assert_invalid_throw_message(cql, table, message, typ, cmd, *args):
    with pytest.raises(typ, match=re.escape(message)):
        execute(cql, table, cmd, *args)

# Cassandra's CQLTester.java has a much more elaborate implementation
# of these functions, which carefully prints out the differences when
# the assertion fails. We can also do this in the future, but pytest's
# assert rewriting is already pretty good that makes such eleborate
# comparison code less critical.
def assert_row_count(result, expected):
    assert len(list(result)) == expected

def assert_empty(result):
    assert len(list(result)) == 0

# Result objects contain some strange types specific to the CQL driver, which
# normally compare well against normal Python types, but in some cases of nested
# types they do not, and require some cleanup:
def result_cleanup(item):
    if isinstance(item, OrderedMapSerializedKey):
        # Because of issue #7856, we can't use item.items() if the test
        # used prepared statement with wrongly ordered nested item, so
        # we can use _items instead.
        return { freeze(item[0]): item[1] for item in item._items }
    if isinstance(item, SortedSet):
        return { freeze(x) for x in item }
    return item

def assert_rows(result, *expected):
    allresults = list(result)
    assert len(allresults) == len(expected)
    for r,e in zip(allresults, expected):
        r = [result_cleanup(col) for col in r]
        assert r == e

# To compare two lists of items (each is a dict) without regard for order,
# The following function, multiset() converts the list into a multiset
# (set with duplicates) where order doesn't matter, so the multisets can
# be compared.
def freeze(item):
    if isinstance(item, dict):
        return frozenset((freeze(key), freeze(value)) for key, value in item.items())
    if isinstance(item, OrderedMapSerializedKey):
        # Because of issue #7856, we can't use item.items() if the test
        # used prepared statement with wrongly ordered nested item, so
        # we can use _items instead.
        return frozenset((freeze(item[0]), freeze(item[1])) for item in item._items)
    elif hasattr(item, '_asdict'):
        # Probably a namedtuple, e.g., a Row. It is not hashable so we need
        # to convert it to something else. For simplicity, let's drop the
        # names and convert it to a regular tuple. Note that _asdict()
        # returns an *ordered* dict, so the result is a tuple, not frozenset.
        return tuple(freeze(value) for key, value in item._asdict().items())
    elif isinstance(item, list):
        return tuple(freeze(value) for value in item)
    elif isinstance(item, SortedSet):
        return frozenset(freeze(val) for val in item)
    elif isinstance(item, set):
        return frozenset(val for val in item)
    return item
def multiset(items):
    return collections.Counter([freeze(item) for item in items])

def assert_rows_ignoring_order(result, *expected):
    allresults = list(result)
    assert len(allresults) == len(expected)
    assert multiset(allresults) == multiset(expected)

# Note that the Python driver has a function _clean_column_name() which
# "cleans" column names in result sets by dropping any non-alphanumeric
# character at the beginning or end of the name and replacing such characters
# in the middle by a "_".  So, for example the column name "m['1']" is
# converted to "m__1".
def assert_column_names(result, *expected):
    assert result.one()._fields == expected

# FIXME: implement flush() - differently for Cassandra (need to run nodetool)
# and Scylla (use REST API). For now, flush() does nothing... The tests will
# work and test CQL, but not test for flush-specific bugs which apparently
# some specific tests were written to check (e.g., testCollectionFlush).
# I'm worried, though, that some tests (e.g., in collections_test.py)
# needelessly stuck flush() in the middle of tests, and implementing flush()
# will make them slower with no real benefit.
def flush(cql, table):
    # FIXME! Currently this doesn't flush at all!
    print("NOTE: flush() stubbed, and doesn't really flush")

# Looping "for _ in before_and_after_flush(cql, table)" runs code twice -
# once immediately, then a flush, and then a second time.
def before_and_after_flush(cql, table):
    yield None
    flush(cql, table)
    yield None

# Utility function for truncating a number to single-precision floating point,
# which is what we can expect when reading a "float" column from Scylla.
# unfortunately, I couldn't find a function from the Cassandra driver or
# Python to do this more easily (maybe I'm missing something?)
def to_float(num):
    return struct.unpack('f', struct.pack('f', num))[0]

# Index creation is asynchronous, this method searches in the system table
# IndexInfo for the specified index and returns true if it finds it, which
# indicates the index was built on the pre-existing data.
# If we haven't found it after 30 seconds we give-up.
def wait_for_index(cql, table, index):
    keyspace = table.split('.')[0]
    start_time = time.time()
    while time.time() < start_time + 30:
        # Implementation 1: a full partition scan. This is the code which
        # Cassandra's unit test had originally.
        #for row in cql.execute(f"SELECT index_name FROM system.\"IndexInfo\" WHERE table_name = '{keyspace}'"):
        #    if row.index_name == index:
        #        return True
        # Implementation 2 (the most efficient): Directly read the specific
        # clustering key we want. Needs clustering key slices to work
        # correctly in the virtual reader. (issue #8600)
        if list(cql.execute(f"SELECT index_name FROM system.\"IndexInfo\" WHERE table_name = '{keyspace}' and index_name = '{index}'")):
            return True
        time.sleep(0.1)
    return False
