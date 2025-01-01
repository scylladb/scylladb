# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for SELECT of a specific key in a collection column
#############################################################################

import pytest
import re
import time
from cassandra.protocol import InvalidRequest
from .util import unique_name, unique_key_int, unique_key_string, new_test_table, new_type, new_function


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, m map<int, int>)")
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

def test_set_subscript(cql, test_keyspace):
    schema = 'p int PRIMARY KEY, s set<int>'
    with new_test_table(cql, test_keyspace, schema) as table:
        p = unique_key_int()
        cql.execute(f"INSERT INTO {table}(p,s) VALUES ({p}, " + "{10,20})")
        assert list(cql.execute(f"SELECT s[0] FROM {table} WHERE p={p}")) == [(None,)]
        assert list(cql.execute(f"SELECT s[10] FROM {table} WHERE p={p}")) == [(10,)]
        assert list(cql.execute(f"SELECT s[11] FROM {table} WHERE p={p}")) == [(None,)]
        assert list(cql.execute(f"SELECT s[20] FROM {table} WHERE p={p}")) == [(20,)]

# scylla only because cassandra doesn't support lua language
@pytest.mark.xfail(reason="#22075")
def test_subscript_function_arg(scylla_only, cql, test_keyspace, table1):
    fn = "(k int) CALLED ON NULL INPUT RETURNS int LANGUAGE Lua AS 'return k+1'"
    with new_function(cql, test_keyspace, fn, 'add_one'):
        p = unique_key_int()
        cql.execute(f"INSERT INTO {table1}(p,m) VALUES ({p}, " + "{1:10,2:20})")
        assert list(cql.execute(f"SELECT add_one(m[1]) FROM {table1} WHERE p={p}")) == [(11,)]
