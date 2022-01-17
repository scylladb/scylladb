# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
##################################################################

# Various utility functions which are useful for multiple tests.
# Note that fixtures aren't here - they are in conftest.py.

import string
import random
import time
from contextlib import contextmanager

def random_string(length=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(length))

def random_bytes(length=10):
    return bytearray(random.getrandbits(8) for _ in range(length))

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


# A utility function for creating a new temporary user-defined function.
@contextmanager
def new_function(cql, keyspace, body, name=None):
    fun = name if name else unique_name()
    cql.execute(f"CREATE FUNCTION {keyspace}.{fun} {body}")
    try:
        yield fun
    finally:
        cql.execute(f"DROP FUNCTION {keyspace}.{fun}")

# A utility function for creating a new temporary user-defined aggregate.
@contextmanager
def new_aggregate(cql, keyspace, body):
    aggr = unique_name()
    cql.execute(f"CREATE AGGREGATE {keyspace}.{aggr} {body}")
    try:
        yield aggr
    finally:
        cql.execute(f"DROP AGGREGATE {keyspace}.{aggr}")

# A utility function for creating a new temporary materialized view in
# an existing table.
@contextmanager
def new_materialized_view(cql, table, select, pk, where):
    keyspace = table.split('.')[0]
    mv = keyspace + "." + unique_name()
    cql.execute(f"CREATE MATERIALIZED VIEW {mv} AS SELECT {select} FROM {table} WHERE {where} PRIMARY KEY ({pk})")
    try:
        yield mv
    finally:
        cql.execute(f"DROP MATERIALIZED VIEW {mv}")

def project(column_name_string, rows):
    """Returns a list of column values from each of the rows."""
    return [getattr(r, column_name_string) for r in rows]
