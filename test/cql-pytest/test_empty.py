# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for empty values (especially, but not just, empty strings)
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest
from util import unique_name, unique_key_string, new_test_table


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p text, c text, v text, primary key (p, c))")
    yield table
    cql.execute("DROP TABLE " + table)

# In test_insert_null_key in test_null.py we verified that a null value is not
# allowed as a key column - neither as a partition key nor clustering key.
# An *empty string*, in contrast, is NOT a null. So ideally should have been
# allowed as a key. However, for undocumented reasons (having to do with how
# partition keys are serialized in sstables), an empty string is NOT allowed
# as a partition key. It is allowed as a clustering key, though. In the
# following test we confirm those things.
# See issue #9352.
def test_insert_empty_string_key(cql, table1):
    s = unique_key_string()
    # An empty-string clustering *is* allowed:
    cql.execute(f"INSERT INTO {table1} (p,c,v) VALUES ('{s}', '', 'cat')")
    assert list(cql.execute(f"SELECT v FROM {table1} WHERE p='{s}' AND c=''")) == [('cat',)]
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        cql.execute(f"INSERT INTO {table1} (p,c,v) VALUES ('', '{s}', 'dog')")

# test_update_empty_string_key() is the same as test_insert_empty_string_key()
# just uses an UPDATE instead of INSERT. It turns out that exactly the cases
# which are allowed by INSERT are also allowed by UPDATE.
def test_update_empty_string_key(cql, table1):
    s = unique_key_string()
    # An empty-string clustering *is* allowed:
    cql.execute(f"UPDATE {table1} SET v = 'cat' WHERE p='{s}' AND c=''")
    assert list(cql.execute(f"SELECT v FROM {table1} WHERE p='{s}' AND c=''")) == [('cat',)]
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        cql.execute(f"UPDATE {table1} SET v = 'dog' WHERE p='' AND c='{s}'")

# ... and same for DELETE
def test_delete_empty_string_key(cql, table1):
    s = unique_key_string()
    # An empty-string clustering *is* allowed:
    cql.execute(f"DELETE FROM {table1} WHERE p='{s}' AND c=''")
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        cql.execute(f"DELETE FROM {table1} WHERE p='' AND c='{s}'")

# Another test like test_insert_empty_string_key() just using an INSERT JSON
# instead of a regular INSERT. Because INSERT JSON takes a different code path
# from regular INSERT, we need the emptiness test in yet another place.
# Reproduces issue #9853 (the empty-string partition key was allowed, and
# actually inserted into the table.)
def test_insert_json_empty_string_key(cql, table1):
    s = unique_key_string()
    # An empty-string clustering *is* allowed:
    cql.execute("""INSERT INTO %s JSON '{"p": "%s", "c": "", "v": "cat"}'""" % (table1, s))
    assert list(cql.execute(f"SELECT v FROM {table1} WHERE p='{s}' AND c=''")) == [('cat',)]
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        cql.execute("""INSERT INTO %s JSON '{"p": "", "c": "%s", "v": "cat"}'""" % (table1, s))

# Although an empty string is not allowed as a partition key (as tested
# above by test_empty_string_key()), it turns out that in a *compound*
# partition key (with multiple partition-key columns), any or all of them
# may be empty strings! This inconsistency is known in Cassandra, but
# deemed unworthy to fix - see:
#    https://issues.apache.org/jira/browse/CASSANDRA-11487
def test_empty_string_key2(cql, test_keyspace):
    schema = 'p1 text, p2 text, c text, v text, primary key ((p1, p2), c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (p1,p2,c,v) VALUES ('', '', '', 'cat')")
        cql.execute(f"INSERT INTO {table} (p1,p2,c,v) VALUES ('x', 'y', 'z', 'dog')")
        assert list(cql.execute(f"SELECT v FROM {table} WHERE p1='' AND p2='' AND c=''")) == [('cat',)]

# For historical reasons, CQL allows any type to be empty, not just strings.
# An "empty" int value is a value with size 0 - and is distinct from a null
# int (size -1) or UNSET_VALUE (size -2) or a normal int value (size 4).
# This is not an important behavior to preserve in modern CQL, but we should
# probably be aware if we ever break it, so it's good to have a regression
# test for it.
def test_empty_int(cql, test_keyspace):
    schema = 'p text, v int, primary key (p)'
    with new_test_table(cql, test_keyspace, schema) as table:
        # blobAsInt(0x) is the way to generate an empty int in CQL:
        cql.execute(f"INSERT INTO {table} (p,v) VALUES ('hi', blobAsInt(0x))")
        # When the Python driver returns an empty int, it returns it just like
        # a null int - None. Note that some other drivers may have problems
        # with an empty integer being returned - e.g., see
        # https://github.com/scylladb/scylla-rust-driver/issues/278
        assert list(cql.execute(f"SELECT v FROM {table} WHERE p='hi'")) == [(None,)]
