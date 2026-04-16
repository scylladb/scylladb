# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for SELECT of a specific key in a collection column
#############################################################################

import time
import pytest
from cassandra.protocol import InvalidRequest
from .util import unique_name, unique_key_int, new_test_table, new_type, new_function


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, m map<int, int>)")
    yield table
    cql.execute("DROP TABLE " + table)

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, s set<int>)")
    yield table
    cql.execute("DROP TABLE " + table)

def test_basic_int_key_selection(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1}(p,m) VALUES ({p}, " + "{1:10,2:20})")
    assert list(cql.execute(f"SELECT m[1] FROM {table1} WHERE p={p}")) == [(10,)]
    assert list(cql.execute(f"SELECT m[2] FROM {table1} WHERE p={p}")) == [(20,)]
    assert list(cql.execute(f"SELECT m[3] FROM {table1} WHERE p={p}")) == [(None,)]

def test_basic_string_key_selection(cql, test_keyspace):
    schema = 'p int PRIMARY KEY, m map<text, int>'
    with new_test_table(cql, test_keyspace, schema) as table:
        p = unique_key_int()
        cql.execute(f"INSERT INTO {table}(p,m) VALUES ({p}, " + "{'aa':10,'ab':20})")
        assert list(cql.execute(f"SELECT m['aa'] FROM {table} WHERE p={p}")) == [(10,)]
        assert list(cql.execute(f"SELECT m['ab'] FROM {table} WHERE p={p}")) == [(20,)]
        assert list(cql.execute(f"SELECT m['ac'] FROM {table} WHERE p={p}")) == [(None,)]

def test_subscript_type_mismatch(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1}(p,m) VALUES ({p}, " + "{1:10,2:20})")
    with pytest.raises(InvalidRequest):
        cql.execute(f"SELECT m['x'] FROM {table1} WHERE p={p}")

def test_subscript_with_alias(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1}(p,m) VALUES ({p}, " + "{1:10,2:20})")
    assert [(r.m1, r.m2) for r in cql.execute(f"SELECT m[1] as m1, m[2] as m2 FROM {table1} WHERE p={p}")] == [(10, 20)]

def test_frozen_map_subscript(cql, test_keyspace):
    schema = 'p int PRIMARY KEY, m frozen<map<int, int>>'
    with new_test_table(cql, test_keyspace, schema) as table:
        p = unique_key_int()
        cql.execute(f"INSERT INTO {table}(p,m) VALUES ({p}, " + "{1:10,2:20})")
        assert list(cql.execute(f"SELECT m[1] FROM {table} WHERE p={p}")) == [(10,)]
        assert list(cql.execute(f"SELECT m[2] FROM {table} WHERE p={p}")) == [(20,)]
        assert list(cql.execute(f"SELECT m[3] FROM {table} WHERE p={p}")) == [(None,)]

def test_nested_key_selection(cql, test_keyspace):
    schema = 'p int PRIMARY KEY, m map<text, frozen<map<text, int>>>'
    with new_test_table(cql, test_keyspace, schema) as table:
        p = unique_key_int()
        cql.execute(f"INSERT INTO {table}(p, m) VALUES ({p}, " + "{'1': {'a': 10, 'b': 11}, '2': {'a': 12}})")
        assert list(cql.execute(f"SELECT m['1']['a'] FROM {table} WHERE p={p}")) == [(10,)]
        assert list(cql.execute(f"SELECT m['1']['b'] FROM {table} WHERE p={p}")) == [(11,)]
        assert list(cql.execute(f"SELECT m['2']['a'] FROM {table} WHERE p={p}")) == [(12,)]
        assert list(cql.execute(f"SELECT m['2']['b'] FROM {table} WHERE p={p}")) == [(None,)]

def test_prepare_key(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p,m) VALUES ({p}, " + "{1:10,2:20})")

    lookup1 = cql.prepare(f"SELECT m[?] FROM {table1} WHERE p = ?")
    assert list(cql.execute(lookup1, [1, p])) == [(10,)]
    assert list(cql.execute(lookup1, [2, p])) == [(20,)]
    assert list(cql.execute(lookup1, [3, p])) == [(None,)]

    lookup2 = cql.prepare(f"SELECT m[:x1], m[:x2] FROM {table1} WHERE p = :key")
    assert list(cql.execute(lookup2, {'x1':2, 'x2':1, 'key':p})) == [(20,10)]

def test_null_map(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1}(p) VALUES ({p})")
    assert list(cql.execute(f"SELECT m[1] FROM {table1} WHERE p={p}")) == [(None,)]

# scylla only because scylla returns null while cassandra returns error
def test_null_subscript(scylla_only, cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1}(p,m) VALUES ({p}, " + "{1:10,2:20})")
    assert list(cql.execute(f"SELECT m[null] FROM {table1} WHERE p={p}")) == [(None,)]

def test_subscript_and_field(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int)') as typ:
        schema = f"p int PRIMARY KEY, m map<int, frozen<{typ}>>"
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            cql.execute(f"INSERT INTO {table}(p,m) VALUES ({p}, " + "{1:{a:10}})")
            assert list(cql.execute(f"SELECT m[1].a FROM {table} WHERE p={p}")) == [(10,)]

def test_field_and_subscript(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a frozen<map<int,int>>)') as typ:
        schema = f"p int PRIMARY KEY, t {typ}"
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            cql.execute(f"INSERT INTO {table}(p,t) VALUES ({p}, " + "{a:{1:10}})")
            assert list(cql.execute(f"SELECT t.a[1] FROM {table} WHERE p={p}")) == [(10,)]

def test_field_and_subscript_and_field(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(b int)') as typ1, \
         new_type(cql, test_keyspace, f"(a frozen<map<int,{typ1}>>)") as typ2:
            schema = f"p int PRIMARY KEY, t {typ2}"
            with new_test_table(cql, test_keyspace, schema) as table:
                p = unique_key_int()
                cql.execute(f"INSERT INTO {table}(p,t) VALUES ({p}, " + "{a:{1:{b:10}}})")
                assert list(cql.execute(f"SELECT t.a[1].b FROM {table} WHERE p={p}")) == [(10,)]

def test_other_types_cannot_be_subscripted(cql, table1):
    with pytest.raises(InvalidRequest, match='not a'):
        cql.execute(f"SELECT p[2] FROM {table1}")
    with pytest.raises(InvalidRequest, match='not a'):
        cql.execute(f"SELECT token(p)[2] FROM {table1}")

def test_udf_subscript(scylla_only, cql, test_keyspace, table1):
    fn = "(k int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return k+1'"
    with new_function(cql, test_keyspace, fn, 'add_one'):
        p = unique_key_int()
        cql.execute(f"INSERT INTO {table1}(p,m) VALUES ({p}, " + "{1:10,2:20})")
        assert list(cql.execute(f"SELECT m[add_one(1)] FROM {table1} WHERE p={p}")) == [(20,)]

# cassandra doesn't support subscript on a list
def test_list_subscript(scylla_only, cql, test_keyspace):
    schema = 'p int PRIMARY KEY, l list<int>'
    with new_test_table(cql, test_keyspace, schema) as table:
        p = unique_key_int()
        cql.execute(f"INSERT INTO {table}(p,l) VALUES ({p}, " + "[10,20])")
        assert list(cql.execute(f"SELECT l[0] FROM {table} WHERE p={p}")) == [(10,)]
        assert list(cql.execute(f"SELECT l[1] FROM {table} WHERE p={p}")) == [(20,)]
        assert list(cql.execute(f"SELECT l[2] FROM {table} WHERE p={p}")) == [(None,)]
        assert list(cql.execute(f"SELECT l[10] FROM {table} WHERE p={p}")) == [(None,)]

def test_set_subscript(cql, table2):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table2}(p,s) VALUES ({p}, " + "{10,20})")
    assert list(cql.execute(f"SELECT s[0] FROM {table2} WHERE p={p}")) == [(None,)]
    assert list(cql.execute(f"SELECT s[10] FROM {table2} WHERE p={p}")) == [(10,)]
    assert list(cql.execute(f"SELECT s[11] FROM {table2} WHERE p={p}")) == [(None,)]
    assert list(cql.execute(f"SELECT s[20] FROM {table2} WHERE p={p}")) == [(20,)]

# scylla only because cassandra doesn't support lua language
@pytest.mark.xfail(reason="#22075")
def test_subscript_function_arg(scylla_only, cql, test_keyspace, table1):
    fn = "(k int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return k+1'"
    with new_function(cql, test_keyspace, fn, 'add_one'):
        p = unique_key_int()
        cql.execute(f"INSERT INTO {table1}(p,m) VALUES ({p}, " + "{1:10,2:20})")
        assert list(cql.execute(f"SELECT add_one(m[1]) FROM {table1} WHERE p={p}")) == [(11,)]

# Test selecting the WRITETIME() of a specific key in a map column.
# Cassandra added support for this in Cassandra 5.0 (CASSANDRA-8877).
# Reproduces issue #15427.
def test_writetime_map_element(cql, table1):
    p = unique_key_int()
    # Generate two reasonable but different timestamps
    timestamp = int(time.time() * 1000000)
    timestamp1 = timestamp - 1234
    timestamp2 = timestamp - 1217 # newer than timestamp1
    # Insert two elements with INSERT and check their timestamps
    cql.execute(f'INSERT INTO {table1}(p,m) VALUES ({p}, {{1:10,2:20}}) USING TIMESTAMP {timestamp1}')
    assert list(cql.execute(f"SELECT WRITETIME(m[1]) FROM {table1} WHERE p={p}")) == [(timestamp1,)]
    assert list(cql.execute(f"SELECT WRITETIME(m[2]) FROM {table1} WHERE p={p}")) == [(timestamp1,)]
    # Replace one element and add another with UPDATE, using a newer timestamp
    # and check the WRITETIME.
    cql.execute(f'UPDATE {table1} USING TIMESTAMP {timestamp2} SET m = m + {{2:30}}  WHERE p={p}')
    cql.execute(f'UPDATE {table1} USING TIMESTAMP {timestamp2} SET m = m + {{3:30}} WHERE p={p}')
    assert list(cql.execute(f"SELECT WRITETIME(m[1]) FROM {table1} WHERE p={p}")) == [(timestamp1,)]
    assert list(cql.execute(f"SELECT WRITETIME(m[2]) FROM {table1} WHERE p={p}")) == [(timestamp2,)]
    assert list(cql.execute(f"SELECT WRITETIME(m[3]) FROM {table1} WHERE p={p}")) == [(timestamp2,)]

# Additional tests for WRITETIME() of a specific key in a map, with missing
# items, missing maps, and missing keys, to check that it returns null in
# those cases instead of erroring.
# Reproduces issue #15427.
def test_writetime_map_element_nulls(cql, table1):
    # Missing item (row doesn't exist): WRITETIME(m[key]) returns no row.
    p_missing = unique_key_int()
    assert list(cql.execute(f"SELECT WRITETIME(m[1]) FROM {table1} WHERE p={p_missing}")) == []
    # Existing row but map column is null (never written): returns null
    # timestamp (looks like "None" in Python).
    p_null_map = unique_key_int()
    cql.execute(f"INSERT INTO {table1}(p) VALUES ({p_null_map})")
    assert list(cql.execute(f"SELECT WRITETIME(m[1]) FROM {table1} WHERE p={p_null_map}")) == [(None,)]
    # Existing row with a map, but the requested key is absent: returns null
    # timestamp.
    p = unique_key_int()
    timestamp = int(time.time() * 1000000) - 1234
    cql.execute(f'INSERT INTO {table1}(p,m) VALUES ({p}, {{1:10}}) USING TIMESTAMP {timestamp}')
    assert list(cql.execute(f"SELECT WRITETIME(m[1]) FROM {table1} WHERE p={p}")) == [(timestamp,)]
    assert list(cql.execute(f"SELECT WRITETIME(m[99]) FROM {table1} WHERE p={p}")) == [(None,)]

# Also, null as a subscript is allowed and returns null timestamp, instead of
# erroring. This test is marked cassandra_bug because Cassandra returns an
# ugly "NoHostAvailable ... java.lang.NullPointerException" error in this case
# (see CASSANDRA-21248).
def test_writetime_map_element_null_subscript(cql, table1, cassandra_bug):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1}(p,m) VALUES ({p}, {{1:10}})')
    assert list(cql.execute(f"SELECT WRITETIME(m[null]) FROM {table1} WHERE p={p}")) == [(None,)]

# Test selecting the TTL() of a specific key in a map column.
# Cassandra added support for this in Cassandra 5.0 (CASSANDRA-8877).
# Reproduces issue #15427.
def test_ttl_map_element(cql, table1):
    p = unique_key_int()
    ttl1 = 3600  # 1 hour
    ttl2 = 7200  # 2 hours, different from ttl1
    # Insert two elements with INSERT and check their TTLs
    cql.execute(f'INSERT INTO {table1}(p,m) VALUES ({p}, {{1:10,2:20}}) USING TTL {ttl1}')
    # Because of the time taken to execute the statements and reading the
    # TTL() reads the remaining TTL, we can't check for an exact TTL value,
    # but we can check that it's less than or equal to the TTL we set and
    # greater than TTL minus some small amount of time.
    [(t,)] = cql.execute(f"SELECT TTL(m[1]) FROM {table1} WHERE p={p}")
    assert t is not None and t <= ttl1 and t > ttl1 - 60
    [(t,)] = cql.execute(f"SELECT TTL(m[2]) FROM {table1} WHERE p={p}")
    assert t is not None and t <= ttl1 and t > ttl1 - 60
    # Replace one element and add another with UPDATE, using a different TTL
    # and check the TTLs.
    cql.execute(f'UPDATE {table1} USING TTL {ttl2} SET m = m + {{2:30}} WHERE p={p}')
    cql.execute(f'UPDATE {table1} USING TTL {ttl2} SET m = m + {{3:30}} WHERE p={p}')
    [(t,)] = cql.execute(f"SELECT TTL(m[1]) FROM {table1} WHERE p={p}")
    assert t is not None and t <= ttl1 and t > ttl1 - 60
    [(t,)] = cql.execute(f"SELECT TTL(m[2]) FROM {table1} WHERE p={p}")
    assert t is not None and t <= ttl2 and t > ttl2 - 60
    [(t,)] = cql.execute(f"SELECT TTL(m[3]) FROM {table1} WHERE p={p}")
    assert t is not None and t <= ttl2 and t > ttl2 - 60

# Additional tests for TTL() of a specific key in a map. Just like in the
# similar test for WRITETIME() above, also for TTL we can have missing
# items, missing maps, and missing keys. For for TTL() there is also the
# case where the TTL itself is missing for some cells but not others.
def test_ttl_map_element_nulls(cql, table1):
    # Missing item (row doesn't exist): TTL(m[key]) returns no row.
    p_missing = unique_key_int()
    assert list(cql.execute(f"SELECT TTL(m[1]) FROM {table1} WHERE p={p_missing}")) == []
    # Existing row but map column is null (never written): returns null
    # TTL (looks like "None" in Python).
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1}(p) VALUES ({p})")
    assert list(cql.execute(f"SELECT TTL(m[1]) FROM {table1} WHERE p={p}")) == [(None,)]
    # Existing row with a map, but the requested key is absent: returns null
    # TTL.
    p = unique_key_int()
    ttl1 = 1000
    cql.execute(f'INSERT INTO {table1}(p,m) VALUES ({p}, {{1:10}}) USING TTL {ttl1}')
    assert list(cql.execute(f"SELECT TTL(m[2]) FROM {table1} WHERE p={p}")) == [(None,)]
    # Row with some cells having TTL, others not.
    cql.execute(f'UPDATE {table1} SET m = m + {{2:20}} WHERE p={p}')
    [(t,)] = cql.execute(f'SELECT TTL(m[1]) FROM {table1} WHERE p={p}')
    assert t is not None and t <= ttl1 and t > ttl1 - 60
    [(t,)] = cql.execute(f'SELECT TTL(m[2]) FROM {table1} WHERE p={p}')
    assert t is None

# Also, null as a subscript is allowed and returns null ttl, instead of
# erroring. This test is marked cassandra_bug because Cassandra returns an
# ugly "NoHostAvailable ... java.lang.NullPointerException" error in this case
# (see CASSANDRA-21248).
def test_ttl_map_element_null_subscript(cql, table1, cassandra_bug):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1}(p,m) VALUES ({p}, {{1:10}})')
    assert list(cql.execute(f"SELECT TTL(m[null]) FROM {table1} WHERE p={p}")) == [(None,)]

# Test that WRITETIME() and TTL() cannot be used on an entire unfrozen
# collection column. Because an unfrozen collection is stored as multiple
# cells that may each have different timestamps and TTLs, there is no single
# WRITETIME or TTL to return for the whole collection.
# It appears that Cassandra has a bug here: It allows selecting WRITETIME()
# of an entire unfrozen map, but returns an array of timestamps, where you
# can't even tell which timestamp belongs to which element. So we'll mark
# this test cassandra_bug - see CASSANDRA-21240.
def test_writetime_ttl_whole_collection_forbidden(cql, table1, cassandra_bug):
    p = unique_key_int()
    timestamp = int(time.time() * 1000000) - 1234 # a reasonable timestamp
    cql.execute(f'INSERT INTO {table1}(p,m) VALUES ({p}, {{1:10,2:20}}) USING TIMESTAMP {timestamp}')
    with pytest.raises(InvalidRequest):
        cql.execute(f"SELECT WRITETIME(m) FROM {table1} WHERE p={p}")
    with pytest.raises(InvalidRequest):
        cql.execute(f"SELECT TTL(m) FROM {table1} WHERE p={p}")

# Test selecting the WRITETIME() of a specific element in a set column.
# Like non-frozen maps, non-frozen sets store each element as a separate cell
# with its own write timestamp, so WRITETIME(s[key]) should return the
# timestamp of the last write to that element.
# The syntax writetime(s[key]) derives from the syntax s[key] for checking
# the presence of a key in a set - tested in test_set_subscript() above.
def test_writetime_set_element(cql, table2):
    p = unique_key_int()
    timestamp = int(time.time() * 1000000)
    timestamp1 = timestamp - 1234
    timestamp2 = timestamp - 1217  # newer than timestamp1
    # Insert two elements with INSERT and check their timestamps.
    cql.execute(f'INSERT INTO {table2}(p,s) VALUES ({p}, {{10,20}}) USING TIMESTAMP {timestamp1}')
    assert list(cql.execute(f"SELECT WRITETIME(s[10]) FROM {table2} WHERE p={p}")) == [(timestamp1,)]
    assert list(cql.execute(f"SELECT WRITETIME(s[20]) FROM {table2} WHERE p={p}")) == [(timestamp1,)]
    # An element not present in the set returns null.
    assert list(cql.execute(f"SELECT WRITETIME(s[99]) FROM {table2} WHERE p={p}")) == [(None,)]
    # Add a new element with UPDATE using a newer timestamp and check that
    # the existing elements keep their old timestamp while the new one has
    # the newer timestamp.
    cql.execute(f'UPDATE {table2} USING TIMESTAMP {timestamp2} SET s = s + {{30}} WHERE p={p}')
    assert list(cql.execute(f"SELECT WRITETIME(s[10]) FROM {table2} WHERE p={p}")) == [(timestamp1,)]
    assert list(cql.execute(f"SELECT WRITETIME(s[20]) FROM {table2} WHERE p={p}")) == [(timestamp1,)]
    assert list(cql.execute(f"SELECT WRITETIME(s[30]) FROM {table2} WHERE p={p}")) == [(timestamp2,)]

# Test that WRITETIME(col[key]) fails with a clear error when col is not a
# map/set.
def test_writetime_subscript_on_wrong_type(cql, test_keyspace):
    schema = 'p int PRIMARY KEY, v int'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Subscript on a non-map/set column should be rejected.
        # Cassandra says "Invalid element selection: v is of type int is not a
        # collection". Scylla says "Column v is not a map/set/list, cannot be
        # subscripted"
        with pytest.raises(InvalidRequest, match=' v '):
            cql.execute(f"SELECT WRITETIME(v[3]) FROM {table}")

# Test that TTL(col[key]) fails with a clear error when col is not a
# map/set.
def test_ttl_subscript_on_wrong_type(cql, test_keyspace):
    schema = 'p int PRIMARY KEY, v int'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Subscript on a non-map/set column should be rejected.
        # Cassandra says "Invalid element selection: v is of type int is not a
        # collection". Scylla says "Column v is not a map/set/list, cannot be
        # subscripted"
        with pytest.raises(InvalidRequest, match=' v '):
            cql.execute(f"SELECT TTL(v[3]) FROM {table}")

# Test that WRITETIME(col[key]) and TTL(col[key]) are not allowed for list
# elements. In theory, this could be allowed - lists really do have individual
# timestamps and ttls for each elements, just like maps and sets. But Cassandra
# doesn't allow it, so we don't allow it either. Note that list[index] is an
# inefficient way to access a list element, so it shouldn't be encouraged.
def test_writetime_ttl_list_element_forbidden(cql, test_keyspace):
    schema = 'p int PRIMARY KEY, l list<int>'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Scylla an Cassandra have different error messages: Cassandra says
        # "Element selection is only allowed on sets and maps, but l is a list"
        # Scylla says "WRITETIME on a subscript is only valid for non-frozen
        # map or set columns"
        with pytest.raises(InvalidRequest, match='Element|subscript'):
            cql.execute(f"SELECT WRITETIME(l[0]) FROM {table}")
        with pytest.raises(InvalidRequest, match='Element|subscript'):
            cql.execute(f"SELECT TTL(l[0]) FROM {table}")

# In a *frozen* map, the entire map is stored as a single atomic cell and has
# a single timestamp and TTL, so WRITETIME(col[key]) and TTL(col[key]) are
# not useful. Moreover we noted in test_writetime_field_on_frozen_udt,
# that Cassandra does *not* allow them for fields frozen tuples, so it
# is arguably wrong to allow them for fields of frozen UDTs - and also in
# members of frozen maps.
# Nevertheless, Cassandra does allow WRITETIME() and TTL() on individual
# members of frozen maps, and returns the timestamp and TTL of the whole map.
# Because of this difference from Cassandra, we mark this test as xfail.
# In the future we can consider if Cassandra's support for member timestamps
# in frozen maps is a mistake, and replace the xfail by cassandra_bug.
@pytest.mark.xfail(reason="Cassandra allows WRITETIME on members of frozen maps, but Scylla does not")
def test_writetime_frozen_map_elements(cql, test_keyspace):
    schema = f'p int PRIMARY KEY, x frozen<map<int, int>>'
    with new_test_table(cql, test_keyspace, schema) as table:
        p = unique_key_int()
        timestamp = int(time.time() * 1000000) - 1234
        cql.execute(f'INSERT INTO {table}(p, x) VALUES ({p}, {{1: 10}}) USING TIMESTAMP {timestamp}')
        # Scylla's error message: "WRITETIME on a subscript is only valid for
        # non-frozen map or set columns". Cassandra allows this.
        assert list(cql.execute(f'SELECT WRITETIME(x[1]) FROM {table} WHERE p={p}')) == [(timestamp,)]

# This is a variant of the previous test (test_writetime_frozen_map_elements)
# with the frozen map also being a primary key column (which is allowed for
# a *frozen* map). Primary key columns don't have a timestamp or TLL at
# all, so this case should be rejected even if we generally allow WRITETIME
# or TTL on frozen map members.
def test_writetime_frozen_map_elements_pk(cql, test_keyspace):
    schema = 'pk frozen<map<int, int>> PRIMARY KEY'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Scylla's error message is "WRITETIME is not legal on primary key
        # component pk". Cassandra's is "Cannot use selection function
        # writetime on PRIMARY KEY part pk".
        with pytest.raises(InvalidRequest, match='primary key|PRIMARY KEY'):
            cql.execute(f"SELECT WRITETIME(pk[1]) FROM {table}")
        with pytest.raises(InvalidRequest, match='primary key|PRIMARY KEY'):
            cql.execute(f"SELECT TTL(pk[1]) FROM {table}")

# Test that we can select m, m[key] and writetime(m[key]) in the same query.
# This requires the implementation to be able to handle a mix of an original
# copy of m and also the writetime information.
def test_writetime_mixed_map_and_map_element(cql, table1):
    p = unique_key_int()
    timestamp = int(time.time() * 1000000)
    timestamp1 = timestamp - 1234
    timestamp2 = timestamp - 1217
    cql.execute(f'UPDATE {table1} USING TIMESTAMP {timestamp1} SET m = m + {{1:10}}  WHERE p={p}')
    cql.execute(f'UPDATE {table1} USING TIMESTAMP {timestamp2} SET m = m + {{2:20}} WHERE p={p}')
    assert list(cql.execute(f"SELECT m, m[1], m[2], WRITETIME(m[2]), WRITETIME(m[1]) FROM {table1} WHERE p={p}")) == [({1: 10, 2: 20}, 10, 20, timestamp2, timestamp1)]
