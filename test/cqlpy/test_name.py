# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for limits on the *names* of various objects such as keyspaces,
# tables, indexes, and columns.
#############################################################################

import pytest
import re
from contextlib import contextmanager
from cassandra.protocol import InvalidRequest
from .util import unique_name

# passes_or_raises() is similar to pytest.raises(), except that while raises()
# expects a certain exception must happen, the new passes_or_raises()
# expects the code to either pass (not raise), or if it throws, it must
# throw the specific specified exception.
# This function is useful for tests that want to verify that if some feature
# is still not supported, it must throw some expected graceful error, but
# if one day it *will* be supported, the test should continue to pass.
@contextmanager
def passes_or_raises(expected_exception, match=None):
    # Sadly __tracebackhide__=True only drops some of the unhelpful backtrace
    # lines. See https://github.com/pytest-dev/pytest/issues/2057
    __tracebackhide__ = True
    try:
        yield
        # The user's "with" code is running during the yield. If it didn't
        # throw we return from the function - passes_or_raises() succeeded
        # in the "passes" case.
        return
    except expected_exception as err:
        if match == None or re.search(match, str(err)):
            # The passes_or_raises() succeeded in the "raises" case
            return
        pytest.fail(f"exception message '{err}' did not match '{match}'")
    except Exception as err:
        pytest.fail(f"Got unexpected exception type {type(err).__name__} instead of {expected_exception.__name__}: {err}")

# This context manager is similar to new_test_table() - creating a table and
# keeping it alive while the context manager is in scope - but allows the
# caller to pick the name of the table. This is useful to attempt to create
# a table whose name has different lengths or characters.
# Note that if used in a shared keyspace, it is recommended to base the
# given table name on the output of unique_name(), to avoid name clashes.
# See the padded_name() function below as an example.
@contextmanager
def new_named_table(cql, keyspace, table, schema, extra=""):
    tbl  = keyspace+'.'+table
    cql.execute(f'CREATE TABLE {tbl} ({schema}) {extra}')
    try:
        yield tbl
    finally:
        cql.execute(f'DROP TABLE {tbl}')

# padded_name() creates a unique name of given length by taking the
# output of unique_name() and padding it with extra 'x' characters:
def padded_name(length):
    u = unique_name()
    assert length >= len(u)
    return u + 'x'*(length-len(u))

# Cassandra's documentation states that "Both keyspace and table name ... are
# limited in size to 48 characters". This was actually only true in Cassandra
# 3, and by Cassandra 4 and 5 this limitation was dropped (see discussion
# in CASSANDRA-20425). So let's split the test for this into two: the first
# test verifies that a 48-character name is allowed, and passes on all versions
# of Scylla and Cassandra:
def test_table_name_length_48(cql, test_keyspace):
    schema = 'p int, c int, PRIMARY KEY (p, c)'
    with new_named_table(cql, test_keyspace, padded_name(48), schema) as table:
        pass

# In Scylla we limit the table name to 222 characters. This is because when the existing
# code creates a directory to store the new table, this directory's name is
# created by sstables::init_table_storage(), which takes the table's full name,
# a dash (-), and a 32-byte UUID string. Because most Linux filesystems limit filename
# components to 255 bytes, this means that any table name longer than 222 bytes will attempt
# to mkdir() a directory name longer than the allowed 255 bytes, and fail.
# We also reserve 4 bytes for the "_cdc" suffix that is added to the CDC table name.
# Having table names longer than 48 characters is compatible with the current implementation
# of Cassandra 4 and 5 as well.
def test_table_name_length_218(cql, test_keyspace):
    schema = 'p int, c int, PRIMARY KEY (p, c)'
    with new_named_table(cql, test_keyspace, padded_name(218), schema) as table:
        pass

# On the other hand it should fail with the table name exceeding the limit even by 1.
# This will still pass on Cassandra 4 and 5 as they do not need to reserve
# the 4 bytes for the "_cdc" suffix and the limit is 222 bytes there.
def test_table_name_length_219(cql, test_keyspace):
    schema = 'p int, c int, PRIMARY KEY (p, c)'
    n = padded_name(219)
    with passes_or_raises(InvalidRequest, match=n):
        with new_named_table(cql, test_keyspace, n, schema) as table:
            pass

# If we try an even longer table name length, e.g., 500 characters, we run
# into the problem that an attempt to create a file or directory name based
# on the table name will fail. Even if we lift the 48-character limitation
# introduced in Cassandra 3, creating a 500-character name should either
# succeed, or fail gracefully. We mark this test cassandra_bug because
# Cassandra 5 hangs on this test (CASSANDRA-20425 and CASSANDRA-20389).
def test_table_name_length_500(cql, test_keyspace, cassandra_bug):
    schema = 'p int, c int, PRIMARY KEY (p, c)'
    n = padded_name(500)
    with passes_or_raises(InvalidRequest, match=n):
        with new_named_table(cql, test_keyspace, n, schema) as table:
            pass

# TODO: add tests for the allowed characters in a table name
# TODO: add tests like we have above for table names also for keyspace
# names, index names, function names, column names, etc.
