# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for empty values (especially, but not just, empty strings)
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest
from util import unique_name, unique_key_string, unique_key_int, new_test_table

import nodetool

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

# Test an empty-string clustering key again, but this time doing a flush
# bypassing the cache - checking that empty-string clustering keys can
# be correctly written to sstables and read back. See issue #12561.
# This test is scylla_only, because it uses BYPASS CACHE.
def test_insert_empty_string_key_with_flush(cql, table1, scylla_only):
    s = unique_key_string()
    cql.execute(f"INSERT INTO {table1} (p,c,v) VALUES ('{s}', '', 'cat')")
    nodetool.flush(cql, table1)
    assert list(cql.execute(f"SELECT v FROM {table1} WHERE p='{s}' AND c='' BYPASS CACHE")) == [('cat',)]
    assert list(cql.execute(f"SELECT v FROM {table1} WHERE p='{s}' BYPASS CACHE")) == [('cat',)]

# In contrast with normal tables where an empty clustering key is allowed,
# in a WITH COMPACT STORAGE table, an empty clustering key is not allowed.
# As usual, an empty partition key is not allowed either.
@pytest.mark.xfail(reason="issue #12749, misleading error message")
def test_insert_empty_string_key_compact(cql, test_keyspace):
    schema = 'p text, c text, v text, primary key (p, c)'
    with new_test_table(cql, test_keyspace, schema, 'WITH COMPACT STORAGE') as table:
        s = unique_key_string()
        with pytest.raises(InvalidRequest, match='empty'):
            cql.execute(f"INSERT INTO {table} (p,c,v) VALUES ('{s}', '', 'cat')")
        with pytest.raises(InvalidRequest, match='Key may not be empty'):
            cql.execute(f"INSERT INTO {table} (p,c,v) VALUES ('', '{s}', 'dog')")

# However, in a COMPACT STORAGE table with a *compound* clustering key (more
# than one clustering key column), setting one of them to empty *is* allowed.
@pytest.mark.xfail(reason="issue #12749")
def test_insert_empty_string_compound_clustering_key_compact(cql, test_keyspace):
    schema = 'p text, c1 text, c2 text, v text, primary key (p, c1, c2)'
    with new_test_table(cql, test_keyspace, schema, 'WITH COMPACT STORAGE') as table:
        s = unique_key_string()
        # Setting just one of the clustering key components, or both, to
        # an empty string, is allowed. This is in contrast with a single-
        # component clustering key which we saw above is cannot be set empty.
        cql.execute(f"INSERT INTO {table} (p,c1,c2,v) VALUES ('{s}', '', 'dog', 'cat')")
        cql.execute(f"INSERT INTO {table} (p,c1,c2,v) VALUES ('{s}', 'hello', '', 'mouse')")
        cql.execute(f"INSERT INTO {table} (p,c1,c2,v) VALUES ('{s}', '', '', 'horse')")
        assert list(cql.execute(f"SELECT v FROM {table} WHERE p='{s}'")) == [('horse',),('cat',),('mouse',)]

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

# Above in test_empty_int() we noted a bizarre and archaic (not used by
# modern CQL users) feature where any type can be set to "empty".
# But the way to write this bizarre empty values in CQL was via an ugly
# blobAsInt(0x). It mustn't be something as benign as trying to use an
# empty string to set an integer. The following tests verify that trying
# to use an empty string to set other types results in an error. Each type
# will be a separate test, using one common test table. Each test will be
# repeated with regular INSERT and with INSERT JSON.
# The bugs uncovered by these tests were reported in issue #10625 and #7944.

# Create a test table with an integer partition key, and many regular
# columns of all scalar types. The column of type int is called "vint".
@pytest.fixture(scope="module")
def table_all_scalar(cql, test_keyspace):
    types = ['ascii', 'bigint', 'blob', 'boolean', 'date', 'decimal', 'double', 'duration', 'float', 'inet', 'int', 'smallint', 'text', 'time', 'timestamp', 'timeuuid', 'tinyint', 'uuid', 'varchar', 'varint']
    vars = ', '.join(['v'+x+' '+x for x in types])
    with new_test_table(cql, test_keyspace, f'p int primary key, {vars}') as table:
        yield table

# The following types can be assigned any string, and in particular it
# is perfectly fine to assign to them an empty string.
@pytest.mark.parametrize("t", ["ascii", "text", "varchar"])
def test_empty_string_for_string_types(cql, table_all_scalar, t):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table_all_scalar} (p,v{t}) VALUES ({p}, '')")
    assert list(cql.execute(f"SELECT v{t} FROM {table_all_scalar} WHERE p={p}")) == [('',)]

@pytest.mark.parametrize("t", ["ascii", "text", "varchar"])
def test_empty_string_for_string_types_json(cql, table_all_scalar, t):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table_all_scalar} JSON '{{\"p\": {p}, \"v{t}\": \"\"}}'")
    assert list(cql.execute(f"SELECT v{t} FROM {table_all_scalar} WHERE p={p}")) == [('',)]

# "fussy" string types are types that in CQL can be assigned a string
# constant, but the string needs to follow a particular format and in
# particular the empty string should not be allowed.
# Reproduces #10625.
@pytest.mark.xfail(reason="issue #10625")
@pytest.mark.parametrize("t", ["date", "time"])
def test_empty_string_for_fussy_string_types(cql, table_all_scalar, t):
    p = unique_key_int()
    with pytest.raises(InvalidRequest):
        cql.execute(f"INSERT INTO {table_all_scalar} (p,v{t}) VALUES ({p}, '')")

@pytest.mark.xfail(reason="issue #10625")
@pytest.mark.parametrize("t", ["date", "time"])
def test_empty_string_for_fussy_string_types_json(cql, table_all_scalar, t):
    p = unique_key_int()
    with pytest.raises(InvalidRequest):
        cql.execute(f"INSERT INTO {table_all_scalar} JSON '{{\"p\": {p}, \"v{t}\": \"\"}}'")

# Same as test_empty_string_for_fussy_string_types, but for some reason
# Cassandra allows the empty string for these two types, even though an
# empty string doesn't make sense as an IP address or a timestamp.
# We consider this a Cassandra bug, hence the tag "cassandra_bug" below.
# Reproduces #10625.
@pytest.mark.xfail(reason="issue #10625")
@pytest.mark.parametrize("t", ["inet", "timestamp"])
def test_empty_string_for_fussy_string_types2(cql, table_all_scalar, t, cassandra_bug):
    p = unique_key_int()
    with pytest.raises(InvalidRequest):
        cql.execute(f"INSERT INTO {table_all_scalar} (p,v{t}) VALUES ({p}, '')")

@pytest.mark.xfail(reason="issue #10625")
@pytest.mark.parametrize("t", ["inet", "timestamp"])
def test_empty_string_for_fussy_string_types2_json(cql, table_all_scalar, t, cassandra_bug):
    p = unique_key_int()
    with pytest.raises(InvalidRequest):
        cql.execute(f"INSERT INTO {table_all_scalar} JSON '{{\"p\": {p}, \"v{t}\": \"\"}}'")

# All other types cannot be assigned a string constant at all (the error
# is not specific to the empty string)
@pytest.mark.parametrize("t", ["bigint", "blob", "boolean", "decimal", "double", "duration", "float", "int", "smallint", "timeuuid", "tinyint", "uuid", "varint"])
def test_empty_string_for_other_types(cql, table_all_scalar, t):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match='Invalid STRING constant'):
        cql.execute(f"INSERT INTO {table_all_scalar} (p,v{t}) VALUES ({p}, '')")

# Although INSERT JSON can convert strings to numbers, it shouldn't allow
# an *empty* string.
# Reproduces #7944 and #10625.
# See also test_json.py::test_fromjson_{varint,int}_empty_string*
# which reproduces the same bug but just for two specific types.
@pytest.mark.xfail(reason="issue #7944")
@pytest.mark.parametrize("t", ["bigint", "blob", "boolean", "decimal", "double", "duration", "float", "int", "smallint", "timeuuid", "tinyint", "uuid", "varint"])
def test_empty_string_for_other_types_json(cql, table_all_scalar, t, cassandra_bug):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match='Error decoding JSON'):
        cql.execute(f"INSERT INTO {table_all_scalar} JSON '{{\"p\": {p}, \"v{t}\": \"\"}}'")
        assert list(cql.execute(f"SELECT v{t} FROM {table_all_scalar} WHERE p={p}")) == [('',)]

# Some of the tests that failed above allowed inserting a empty value
# in an "unofficial" way by assigning an empty string to it. This is not
# a big problem for regular columns, but for key columns it is bad -
# namely, because the partition key cannot be empty.
# See #10625, #7944
@pytest.mark.xfail(reason="issue #7944, Scylla returns internal server error")
def test_empty_string_for_nonstring_partition_key1(cql, test_keyspace):
    schema = 'p int primary key, v int'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Cassandra returns a "Key may not be empty" but even better would
        # be to report earlier that an empty string cannot be converted to
        # a number.
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {table} JSON '{{\"p\": \"\", \"v\": 3}}'")

def test_empty_string_for_nonstring_partition_key2(cql, test_keyspace):
    schema = 'p inet primary key, v int'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Cassandra returns a "Key may not be empty" but even better would
        # be to report earlier that an empty string cannot be converted to
        # a number.
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {table} (p,v) VALUES ('', 3)")
