# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Various tests for User-Defined Type (UDT) support in Scylla.
#
# There are additional tests involving UDTs in the context of other features
# (describe, aggregates, filtering, json, etc.) in other files. We also have
# many tests for UDTs ported from Cassandra in cassandra_tests/. Here we only
# have a few tests that didn't fit elsewhere.
#############################################################################

import time
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import pytest
from cassandra.protocol import InvalidRequest
from .util import new_test_keyspace, new_test_table, unique_key_int, unique_name, new_type

# In test_select_collection_element.py we test that we can select
# WRITETIME(col[key]) and TTL(col[key]) for an element in a map and a set,
# reproducing issue #15427.
# Here we check that we can also select WRITETIME(a.field) for a field of
# an UDT:
def test_writetime_udt_field(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f"p int PRIMARY KEY, x {typ}"
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            timestamp = int(time.time() * 1000000)
            timestamp1 = timestamp - 1234
            timestamp2 = timestamp - 1217
            cql.execute(f"UPDATE {table} USING TIMESTAMP {timestamp1} SET x.a = 42 WHERE p={p}")
            cql.execute(f"UPDATE {table} USING TIMESTAMP {timestamp2} SET x.b = 17 WHERE p={p}")
            assert list(cql.execute(f"SELECT WRITETIME(x.a) FROM {table} WHERE p={p}")) == [(timestamp1,)]
            assert list(cql.execute(f"SELECT WRITETIME(x.b) FROM {table} WHERE p={p}")) == [(timestamp2,)]

# And TTL(a.field) for a field of an UDT:
def test_ttl_udt_field(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f"p int PRIMARY KEY, x {typ}"
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            ttl1 = 3600
            ttl2 = 7200
            cql.execute(f"UPDATE {table} USING TTL {ttl1} SET x.a = 42 WHERE p={p}")
            cql.execute(f"UPDATE {table} USING TTL {ttl2} SET x.b = 17 WHERE p={p}")
            # TTL() on a field returns the remaining TTL for that field, which may be slightly less than the original TTL by the time we read it, so we check that it's between ttl and ttl-100.
            [(t,)] = cql.execute(f"SELECT TTL(x.a) FROM {table} WHERE p={p}")
            assert t is not None and t <= ttl1 and t > ttl1 - 100
            [(t,)] = cql.execute(f"SELECT TTL(x.b) FROM {table} WHERE p={p}")
            assert t is not None and t <= ttl2 and t > ttl2 - 100
            # A field written without a TTL returns null TTL.
            cql.execute(f"UPDATE {table} SET x.a = 99 WHERE p={p}")
            [(t,)] = cql.execute(f"SELECT TTL(x.a) FROM {table} WHERE p={p}")
            assert t is None

# Test that WRITETIME(col.field) and TTL(col.field) fail with a clear error
# when col is not a UDT, e.g., an int.
def test_writetime_field_on_wrong_type(cql, test_keyspace):
    schema = f'p int PRIMARY KEY, v int'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Subscript on a non-map/set column should be rejected.
        # Cassandra says "Invalid element selection: v is of type int is not a
        # collection". Scylla says "Column v is not a map/set/list, cannot be
        # subscripted"
        with pytest.raises(InvalidRequest, match=' v '):
            cql.execute(f"SELECT WRITETIME(v[3]) FROM {table}")
        with pytest.raises(InvalidRequest, match=' v '):
            cql.execute(f"SELECT TTL(v[3]) FROM {table}")
        # Field selection on a non-UDT column should be rejected.
        # Cassandra and Scylla both say "Invalid field selection:
        # v of type int is not a user type".
        with pytest.raises(InvalidRequest, match='user type'):
            cql.execute(f"SELECT WRITETIME(v.a) FROM {table}")
        with pytest.raises(InvalidRequest, match='user type'):
            cql.execute(f"SELECT TTL(v.a) FROM {table}")

# If a WRITETIME() of individual fields of a tuple is not supported (we have
# test for this - test_type_tuple.py::test_writetime_on_tuple), then it's
# reasonable that it is also not supported on a *frozen* UDT, since just like
# in a tuple, a frozen UDT does not have timestamps for individual fields.
# But, surprisingly, Cassandra does allow WRITETIME() on individual fields of
# a frozen UDT, and returns the timestamp of the whole UDT. Because of this
# difference from Cassandra, we mark this test as xfail. In the future we
# can consider if Cassandra's support for field timestamps in frozen UDTs
# but not tuples is a mistake, and replace the xfail by cassandra_bug.
# Reproduces SCYLLADB-5167 (WRITETIME and TTL of a field of a frozen UDT).
@pytest.mark.xfail(reason="SCYLLADB-5167")
def test_writetime_field_on_frozen_udt(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f'p int PRIMARY KEY, x frozen<{typ}>'
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            timestamp = int(time.time() * 1000000) - 1234
            cql.execute(f'INSERT INTO {table}(p, x) VALUES ({p}, {{a: 1, b: 2}}) USING TIMESTAMP {timestamp}')
            # Scylla's error message: "WRITETIME on a field selection is
            # only valid for non-frozen UDT columns". Cassandra allows this.
            assert list(cql.execute(f'SELECT WRITETIME(x.a) FROM {table} WHERE p={p}')) == [(timestamp,)]

# Like test_writetime_field_on_frozen_udt, but for TTL.
# Reproduces SCYLLADB-5167 (WRITETIME and TTL of a field of a frozen UDT).
@pytest.mark.xfail(reason="SCYLLADB-5167")
def test_ttl_field_on_frozen_udt(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f'p int PRIMARY KEY, x frozen<{typ}>'
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            ttl = 3600
            cql.execute(f'INSERT INTO {table}(p, x) VALUES ({p}, {{a: 1, b: 2}}) USING TTL {ttl}')
            # Scylla's error message: "TTL on a field selection is
            # only valid for non-frozen UDT columns". Cassandra allows this.
            [(t,)] = cql.execute(f'SELECT TTL(x.a) FROM {table} WHERE p={p}')
            assert t is not None and t <= ttl and t > ttl - 100

# This is a variant of the previous test (test_writetime_field_on_frozen_udt)
# with the frozen udt also being a primary key column (which is allowed for
# a *frozen* udt). Primary key columns don't have a timestamp or TTL at
# all, so this case should be rejected even if we generally allow WRITETIME
# or TTL on frozen udt members.
def test_writetime_ttl_frozen_udt_elements_pk(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f'pk frozen<{typ}> PRIMARY KEY'
        with new_test_table(cql, test_keyspace, schema) as table:
            # Scylla's error message is "WRITETIME is not legal on primary key
            # component pk". Cassandra's is "Cannot use selection function
            # writetime on PRIMARY KEY part pk".
            with pytest.raises(InvalidRequest, match='primary key|PRIMARY KEY'):
                cql.execute(f"SELECT WRITETIME(pk.a) FROM {table}")
            with pytest.raises(InvalidRequest, match='primary key|PRIMARY KEY'):
                cql.execute(f"SELECT TTL(pk.a) FROM {table}")

# CREATE TABLE refuses a non-frozen column of a UDT which has a non-frozen
# collection field. But if such a field is added to the UDT with ALTER TYPE
# ADD while a table already has a non-frozen column of this UDT, the ALTER
# is allowed - in Cassandra as well - and the new field works like a frozen
# collection: it can be set and read as a whole (CQL has no syntax for
# modifying an element of a collection inside a UDT field anyway). We decided
# to keep allowing this, so as not to break existing applications nor
# diverge from Cassandra, and this test checks that it keeps working.
def test_alter_type_add_collection_to_type_of_nonfrozen_column(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int)') as typ:
        with new_test_table(cql, test_keyspace, f'p int PRIMARY KEY, v {typ}') as table:
            p = unique_key_int()
            cql.execute(f'ALTER TYPE {typ} ADD b list<int>')
            cql.execute(f'UPDATE {table} SET v.b = [1, 2] WHERE p = {p}')
            assert list(cql.execute(f'SELECT v.b FROM {table} WHERE p = {p}')) == [([1, 2],)]

# Test WRITETIME() on an entire frozen UDT column. A frozen UDT is stored as
# a single atomic cell, so it has a single timestamp like any atomic column.
def test_writetime_frozen_udt(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f"p int PRIMARY KEY, x frozen<{typ}>"
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            timestamp = int(time.time() * 1000000) - 1234
            cql.execute(f"INSERT INTO {table}(p, x) VALUES ({p}, {{a: 1, b: 2}}) USING TIMESTAMP {timestamp}")
            assert list(cql.execute(f"SELECT WRITETIME(x) FROM {table} WHERE p={p}")) == [(timestamp,)]

# Test TTL() on an entire frozen UDT column. A frozen UDT is stored as
# a single atomic cell, so it has a single TTL like any atomic column.
def test_ttl_frozen_udt(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f"p int PRIMARY KEY, x frozen<{typ}>"
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            ttl = 3600
            cql.execute(f"INSERT INTO {table}(p, x) VALUES ({p}, {{a: 1, b: 2}}) USING TTL {ttl}")
            [(t,)] = cql.execute(f"SELECT TTL(x) FROM {table} WHERE p={p}")
            assert t is not None and t <= ttl and t > ttl - 100

# Both Scylla and Cassandra refuse to ALTER TYPE ... ADD a field to a UDT
# used in a partition key (CASSANDRA-15933, tested in cassandra_tests/'s
# AlterTest.testAlterTypeUsedInPartitionKey). Scylla also refuses to rename
# a field of such a UDT, while Cassandra allows it - renaming a field
# doesn't change the serialized partition key. So this test is Scylla-only.
# It replaces dtest's udtencoding_test.py::test_udt_change_in_partition_key
# (see SCYLLADB-1929).
def test_alter_type_rename_used_in_partition_key(cql, test_keyspace, scylla_only):
    with new_type(cql, test_keyspace, '(v1 int)') as typ, new_type(cql, test_keyspace, '(city text)') as other:
        with new_test_table(cql, test_keyspace, f'pk frozen<{typ}>, ck int, val {other}, PRIMARY KEY(pk, ck)') as table:
            with pytest.raises(InvalidRequest, match=table.split('.')[1]):
                cql.execute(f'ALTER TYPE {typ} RENAME v1 TO v1_renamed')
            # Only the partition key's UDT is protected, not other UDTs of the table.
            cql.execute(f'ALTER TYPE {other} ADD zip int')

# A UDT literal nested in a map, with an unknown (or misspelled) field or a field
# of the wrong type, is rejected. Replaces dtest's udtencoding_test.py::test_udt (see SCYLLADB-1929).
def test_udt_literal_invalid_field_in_map(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(city text, zip int)') as typ:
        with new_test_table(cql, test_keyspace, f'p int PRIMARY KEY, m map<text, frozen<{typ}>>') as table:
            cql.execute(f"INSERT INTO {table} (p, m) VALUES (1, {{'home': {{city: 'SF', zip: 94110}}}})")
            with pytest.raises(InvalidRequest, match="Unknown field 'street' in value of user defined type"):
                cql.execute(f"INSERT INTO {table} (p, m) VALUES (2, {{'home': {{street: 'x', city: 'SF', zip: 94110}}}})")
            with pytest.raises(InvalidRequest, match="Unknown field 'cityname' in value of user defined type"):
                cql.execute(f"INSERT INTO {table} (p, m) VALUES (2, {{'home': {{cityname: 'SF', zip: 94110}}}})")
            with pytest.raises(InvalidRequest, match='Invalid map literal for m: value'):
                cql.execute(f"INSERT INTO {table} (p, m) VALUES (2, {{'home': {{city: 'SF', zip: '94110'}}}})")

#############################################################################
# The following tests were translated from scylla-dtest's user_types_test.py.
#############################################################################

# Test that a table can't use a user-defined type from another keyspace
# (CASSANDRA-9409).
# Translated from user_types_test.py::TestUserTypes::test_user_type_isolation.
def test_udt_from_another_keyspace(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(first text, second int, third int)') as typ:
        with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
            with pytest.raises(InvalidRequest, match=f'Statement on keyspace {ks} cannot refer to a user type in keyspace {test_keyspace}'):
                cql.execute(f'CREATE TABLE {ks}.{unique_name()} (id int PRIMARY KEY, v frozen<{typ}>)')

# Test that a keyspace containing a table with a user-defined type column
# can be dropped. Reproduces #3068.
# Translated from user_types_test.py::TestUserTypes::test_keyspace_drop_with_table_containing_udt.
def test_drop_keyspace_with_udt_table(cql):
    ks = unique_name()
    cql.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    cql.execute(f'CREATE TYPE {ks}.udt (first text, second int)')
    cql.execute(f'CREATE TABLE {ks}.table1 (id uuid PRIMARY KEY, x frozen<udt>)')
    cql.execute(f'DROP KEYSPACE {ks}')

# Test writing and reading a row with collections, a tuple and user-defined
# types of all kinds, nested in each other: a UDT with fields of many
# types, a list of UDTs containing a list of UDTs, and a list and a map of
# UDTs. All the values are written as literals, and then each column is
# read back. Finally, a list is replaced.
# Translated from user_types_test.py::TestUserTypes::test_complex_data_types.
def test_udt_complex_data_types(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(label text, details text)') as todo_item, \
         new_type(cql, test_keyspace, f'(name text, todo_list list<frozen<{todo_item}>>)') as todo_list, \
         new_type(cql, test_keyspace, '(item1 ascii, item2 blob, item3 inet, item4 text, item5 timestamp, item6 timeuuid, '
                                      'item7 uuid, item8 varchar, item9 bigint, item10 decimal, item11 double, item12 float, '
                                      'item13 int, item14 varint, item15 boolean, item16 list<int>)') as sink, \
         new_test_table(cql, test_keyspace, f'key1 text PRIMARY KEY, mylist list<text>, myset set<uuid>, mymap map<text, int>, '
                                            f'mytuple frozen<tuple<text, int, uuid, boolean>>, myudt frozen<{sink}>, '
                                            f'mytodolists list<frozen<{todo_list}>>, many_sinks list<frozen<{sink}>>, '
                                            f'named_sinks map<text, frozen<{sink}>>') as table:
        sink1 = ("{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', "
                 "item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, "
                 "item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, "
                 "item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]}")
        sink2 = ("{item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', "
                 "item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, "
                 "item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, "
                 "item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]}")
        sink3 = ("{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', "
                 "item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, "
                 "item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, "
                 "item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}")
        cql.execute(f"""
            INSERT INTO {table} (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
            VALUES (
            'row1', ['five', 'six', 'seven', 'eight'],
            {{4b66458a-2a19-41d3-af25-6faef4dea9fe, 080fdd90-ae74-41d6-9883-635625d3b069, 6cd7fab5-eacc-45c3-8414-6ad0177651d6}},
            {{'one': 1, 'two': 2, 'three': 3, 'four': 4}},
            ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
            {{item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000',
              item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a,
              item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222,
              item12: 98712312.5252, item13: -2147483648, item14: 2147483647, item15: false, item16: [1,3,5,7,11,13]}},
            [{{name: 'stuff to do!', todo_list: [
                {{label: 'buy groceries', details: 'bread and milk'}},
                {{label: 'pick up car from shop', details: '$325 due'}},
                {{label: 'call dave', details: 'for some reason'}}]}},
             {{name: 'more stuff to do!', todo_list: [
                {{label: 'buy new car', details: 'the old one is getting expensive'}},
                {{label: 'price insurance', details: 'current cost is $95/mo'}}]}}],
            [{sink1}, {sink2}, {sink3}],
            {{'namedsink1': {sink1}, 'namedsink2': {sink2}, 'namedsink3': {sink3}}})
        """)
        # The float field item12 has only 24 bits of precision, so it
        # loses the fraction (and more) of the values written above.
        expected_sink1 = ('asdf', b'\x00\x12', '127.0.0.2', 'whatev1', datetime(2012, 2, 3, 4, 5),
                          UUID('d05a10c8-7c12-11e4-949d-b4b6763e9d6f'), UUID('f90b04b1-f9ad-4ffa-b869-a7d894ce6003'),
                          'tyru', -9223372036854771111, Decimal('4321.45678'), 10012312.1222, 40012312.0,
                          -1147483648, 2047483648, True, [1, 1, 2, 3, 5, 8])
        expected_sink2 = ('fdsa', b'\x00\x13', '127.0.0.3', 'whatev2', datetime(2013, 2, 3, 4, 5),
                          UUID('d8ac38c8-7c12-11e4-8955-b4b6763e9d6f'), UUID('e3e84f21-f28c-4e0f-80e0-068a640ae53a'),
                          'uytr', -3333372036854775808, Decimal('1234.12321'), 20012312.1222, 50012312.0,
                          -1547483648, 1947483648, False, [3, 6, 9, 12, 15])
        expected_sink3 = ('zxcv', b'\x00\x14', '127.0.0.4', 'whatev3', datetime(2014, 2, 3, 4, 5),
                          UUID('de30838a-7c12-11e4-a907-b4b6763e9d6f'), UUID('f9381f0e-9467-4d4c-9315-eb9f0232487b'),
                          'fghj', -2239372036854775808, Decimal('5555.55555'), 30012312.1222, 60012312.0,
                          2147483647, 1347483648, True, [0, 1, 0, 1, 2, 0])
        def select(column):
            return cql.execute(f"SELECT {column} FROM {table} WHERE key1 = 'row1'").one()[0]
        assert select('mylist') == ['five', 'six', 'seven', 'eight']
        assert select('myset') == {UUID('080fdd90-ae74-41d6-9883-635625d3b069'), UUID('4b66458a-2a19-41d3-af25-6faef4dea9fe'),
                                   UUID('6cd7fab5-eacc-45c3-8414-6ad0177651d6')}
        assert select('mymap') == {'four': 4, 'one': 1, 'three': 3, 'two': 2}
        assert select('mytuple') == ('hey', 10, UUID('16e69fba-a656-4932-8a01-6782a34505d9'), True)
        assert select('myudt') == ('heyimascii', b'\x00\x11', '127.0.0.1', 'whatev', datetime(2011, 2, 3, 4, 5),
                                   UUID('0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f'), UUID('bdf5e8ac-a75e-4321-9ac8-938fc9576c4a'),
                                   'bleh', -9223372036854775808, Decimal('1234.45678'), 98712312.1222, 98712312.0,
                                   -2147483648, 2147483647, False, [1, 3, 5, 7, 11, 13])
        assert select('mytodolists') == [
            ('stuff to do!', [('buy groceries', 'bread and milk'), ('pick up car from shop', '$325 due'),
                              ('call dave', 'for some reason')]),
            ('more stuff to do!', [('buy new car', 'the old one is getting expensive'),
                                   ('price insurance', 'current cost is $95/mo')])]
        assert select('many_sinks') == [expected_sink1, expected_sink2, expected_sink3]
        assert select('named_sinks') == {'namedsink1': expected_sink1, 'namedsink2': expected_sink2, 'namedsink3': expected_sink3}
        cql.execute(f"UPDATE {table} SET mylist = ['nine', 'ten', 'eleven'] WHERE key1 = 'row1'")
        assert select('mylist') == ['nine', 'ten', 'eleven']
