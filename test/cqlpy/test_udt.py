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

from test import DEBUG_MODES
import time
from textwrap import dedent
import pytest
from cassandra import ConsistencyLevel
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement
from .util import new_test_table, unique_key_int, new_type, unique_name, new_test_keyspace, new_cql

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


# A UDT can't have a counter field (CASSANDRA-7672).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_udt_cannot_contain_counter(cql, test_keyspace):
    # As in the dtest, an unqualified type name after USE.
    with new_cql(cql) as session:
        session.execute(f"USE {test_keyspace}")
        with pytest.raises(InvalidRequest, match='A user type cannot contain counters'):
            session.execute(f'CREATE TYPE {unique_name()} (sub_one COUNTER)')

# The statements and results of the dtest test_complex_data_types below.
COMPLEX_DATA_TYPES_STATEMENTS = [
    "CREATE TYPE t_todo_item (label text, details text)",
    "CREATE TYPE t_todo_list (name text, todo_list list<frozen<t_todo_item>>)",
    dedent(
        """
    CREATE TYPE t_kitchen_sink
    (
    item1 ascii,
    item2 blob,
    item3 inet,
    item4 text,
    item5 timestamp,
    item6 timeuuid,
    item7 uuid,
    item8 varchar,
    item9 bigint,
    item10 decimal,
    item11 double,
    item12 float,
    item13 int,
    item14 varint,
    item15 boolean,
    item16 list<int>
    )
    """
    ),
    dedent(
        """
    CREATE TABLE complex_types
    (
    key1 text PRIMARY KEY,
    mylist list<text>,
    myset set<uuid>,
    mymap map<text, int>,
    mytuple frozen<tuple<text, int, uuid, boolean>>,
    myudt frozen<t_kitchen_sink>,
    mytodolists list<frozen<t_todo_list>>,
    many_sinks list<frozen<t_kitchen_sink>>,
    named_sinks map<text, frozen<t_kitchen_sink>>
    )
    """
    ),
    dedent(
        """
    INSERT INTO complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
    VALUES (
    'row1', ['five', 'six', 'seven', 'eight'],
    {4b66458a-2a19-41d3-af25-6faef4dea9fe,
    080fdd90-ae74-41d6-9883-635625d3b069,
    6cd7fab5-eacc-45c3-8414-6ad0177651d6
    },
    {'one': 1, 'two': 2, 'three': 3, 'four': 4},
    ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
    {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000',
    item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a,
    item8: 'bleh',  item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222,
    item12: 98712312.5252, item13: -2147483648,
    item14: 2147483647,
    item15: false,
    item16: [1,3,5,7,11,13]
    },
    [{name: 'stuff to do!', todo_list:
    [
    {label: 'buy groceries', details: 'bread and milk'},
    {label: 'pick up car from shop', details: '$325 due'},
    {label: 'call dave', details: 'for some reason'}
    ]},
    {name: 'more stuff to do!', todo_list:
    [
    {label: 'buy new car', details: 'the old one is getting expensive'},
    {label: 'price insurance', details: 'current cost is $95/mo'}
    ]}],
    [
    {
    item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000',
    item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003,
    item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222,
    item12: 40012312.5252, item13: -1147483648, item14: 2047483648,item15: true, item16: [1,1,2,3,5,8]
    },
    {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000',
    item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a,
    item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222,
    item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]
    },
    {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000',
    item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b,
    item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222,
    item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]
    }],
    {
    'namedsink1':
    {item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1',item5: '2012-02-03 04:05+0000',
    item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003,
    item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222,
    item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]},
    'namedsink2':
    {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000',
    item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a,
    item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222,
    item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},
    'namedsink3':
    {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000',
    item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b,
    item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222,
    item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}
    })
    """
    ),
]

COMPLEX_DATA_TYPES_SELECTS = [
    "SELECT mylist from complex_types where key1 = 'row1'",
    "SELECT myset from complex_types where key1 = 'row1'",
    "SELECT mymap from complex_types where key1 = 'row1'",
    "SELECT mytuple from complex_types where key1 = 'row1'",
    "SELECT myudt from complex_types where key1 = 'row1'",
    "SELECT mytodolists from complex_types where key1 = 'row1'",
    "SELECT many_sinks from complex_types where key1 = 'row1'",
    "SELECT named_sinks from complex_types where key1 = 'row1'",
]

COMPLEX_DATA_TYPES_EXPECTED = [
    "[[['five', 'six', 'seven', 'eight']]]",
    "[[SortedSet([UUID('080fdd90-ae74-41d6-9883-635625d3b069'), UUID('4b66458a-2a19-41d3-af25-6faef4dea9fe'), UUID('6cd7fab5-eacc-45c3-8414-6ad0177651d6')])]]",
    "[[OrderedMapSerializedKey([('four', 4), ('one', 1), ('three', 3), ('two', 2)])]]",
    "[[('hey', 10, UUID('16e69fba-a656-4932-8a01-6782a34505d9'), True)]]",
    "[[t_kitchen_sink(item1='heyimascii', item2=b'\\x00\\x11', item3='127.0.0.1', item4='whatev', "
    "item5=datetime.datetime(2011, 2, 3, 4, 5), item6=UUID('0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f'), "
    "item7=UUID('bdf5e8ac-a75e-4321-9ac8-938fc9576c4a'), item8='bleh', item9=-9223372036854775808, "
    "item10=Decimal('1234.45678'), item11=98712312.1222, item12=98712312.0, item13=-2147483648, "
    "item14=2147483647, item15=False, item16=[1, 3, 5, 7, 11, 13])]]",
    "[[[t_todo_list(name='stuff to do!', todo_list=[t_todo_item(label='buy groceries', "
    "details='bread and milk'), t_todo_item(label='pick up car from shop', details='$325 due'), "
    "t_todo_item(label='call dave', details='for some reason')]), t_todo_list(name='more stuff to do!', "
    "todo_list=[t_todo_item(label='buy new car', details='the old one is getting expensive'), "
    "t_todo_item(label='price insurance', details='current cost is $95/mo')])]]]",
    "[[[t_kitchen_sink(item1='asdf', item2=b'\\x00\\x12', item3='127.0.0.2', item4='whatev1', "
    "item5=datetime.datetime(2012, 2, 3, 4, 5), item6=UUID('d05a10c8-7c12-11e4-949d-b4b6763e9d6f'), "
    "item7=UUID('f90b04b1-f9ad-4ffa-b869-a7d894ce6003'), item8='tyru', item9=-9223372036854771111, "
    "item10=Decimal('4321.45678'), item11=10012312.1222, item12=40012312.0, item13=-1147483648, "
    "item14=2047483648, item15=True, item16=[1, 1, 2, 3, 5, 8]), t_kitchen_sink(item1='fdsa', "
    "item2=b'\\x00\\x13', item3='127.0.0.3', item4='whatev2', item5=datetime.datetime(2013, 2, 3, 4, 5), "
    "item6=UUID('d8ac38c8-7c12-11e4-8955-b4b6763e9d6f'), item7=UUID('e3e84f21-f28c-4e0f-80e0-068a640ae53a'), "
    "item8='uytr', item9=-3333372036854775808, item10=Decimal('1234.12321'), item11=20012312.1222, "
    "item12=50012312.0, item13=-1547483648, item14=1947483648, item15=False, item16=[3, 6, 9, 12, 15]), "
    "t_kitchen_sink(item1='zxcv', item2=b'\\x00\\x14', item3='127.0.0.4', item4='whatev3', "
    "item5=datetime.datetime(2014, 2, 3, 4, 5), item6=UUID('de30838a-7c12-11e4-a907-b4b6763e9d6f'), "
    "item7=UUID('f9381f0e-9467-4d4c-9315-eb9f0232487b'), item8='fghj', item9=-2239372036854775808, "
    "item10=Decimal('5555.55555'), item11=30012312.1222, item12=60012312.0, item13=2147483647, "
    "item14=1347483648, item15=True, item16=[0, 1, 0, 1, 2, 0])]]]",
    "[[OrderedMapSerializedKey([('namedsink1', t_kitchen_sink(item1='asdf', item2=b'\\x00\\x12', "
    "item3='127.0.0.2', item4='whatev1', item5=datetime.datetime(2012, 2, 3, 4, 5), "
    "item6=UUID('d05a10c8-7c12-11e4-949d-b4b6763e9d6f'), item7=UUID('f90b04b1-f9ad-4ffa-b869-a7d894ce6003'), "
    "item8='tyru', item9=-9223372036854771111, item10=Decimal('4321.45678'), item11=10012312.1222, "
    "item12=40012312.0, item13=-1147483648, item14=2047483648, item15=True, item16=[1, 1, 2, 3, 5, 8])), "
    "('namedsink2', t_kitchen_sink(item1='fdsa', item2=b'\\x00\\x13', item3='127.0.0.3', item4='whatev2', "
    "item5=datetime.datetime(2013, 2, 3, 4, 5), item6=UUID('d8ac38c8-7c12-11e4-8955-b4b6763e9d6f'), "
    "item7=UUID('e3e84f21-f28c-4e0f-80e0-068a640ae53a'), item8='uytr', item9=-3333372036854775808, "
    "item10=Decimal('1234.12321'), item11=20012312.1222, item12=50012312.0, item13=-1547483648, "
    "item14=1947483648, item15=False, item16=[3, 6, 9, 12, 15])), "
    "('namedsink3', t_kitchen_sink(item1='zxcv', item2=b'\\x00\\x14', item3='127.0.0.4', item4='whatev3', "
    "item5=datetime.datetime(2014, 2, 3, 4, 5), item6=UUID('de30838a-7c12-11e4-a907-b4b6763e9d6f'), "
    "item7=UUID('f9381f0e-9467-4d4c-9315-eb9f0232487b'), item8='fghj', item9=-2239372036854775808, "
    "item10=Decimal('5555.55555'), item11=30012312.1222, item12=60012312.0, item13=2147483647, "
    "item14=1347483648, item15=True, item16=[0, 1, 0, 1, 2, 0]))])]]",
]


# Ported from scylla-dtest user_types_test.py::TestUserTypes::test_complex_data_types.
# A user type with a field of every scalar type, and lists, sets, maps,
# tuples and lists and maps of user types holding lists of user types, all
# written in one INSERT at LOCAL_QUORUM, read back column by column, then a
# list column replaced. As in the dtest (a single node), the keyspace has
# replication_factor 1 and vnodes, the session USEs it, and the expected
# results are the driver's rows as a string.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_complex_data_types(cql):
    with new_test_keyspace(cql, "WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1} AND TABLETS = {'enabled': false}") as ks:
        with new_cql(cql) as session:
            session.execute(f"USE {ks}")
            for stmt in COMPLEX_DATA_TYPES_STATEMENTS:
                session.execute(SimpleStatement(stmt, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
            # The dtests' assert_all(result_as_string=True): rows at CL=ONE, as lists, as a string.
            def assert_all(query, expected):
                rows = session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ONE))
                assert str([list(row) for row in rows]) == expected
            for query, expected in zip(COMPLEX_DATA_TYPES_SELECTS, COMPLEX_DATA_TYPES_EXPECTED):
                assert_all(query, expected)
            session.execute(SimpleStatement("UPDATE complex_types SET mylist = ['nine', 'ten', 'eleven'] WHERE key1 = 'row1'",
                                            consistency_level=ConsistencyLevel.LOCAL_QUORUM))
            assert_all("SELECT mylist from complex_types", "[[['nine', 'ten', 'eleven']]]")


# A table cannot use a user type of another keyspace (CASSANDRA-9409).
# Ported from dtest user_types_test.py, with vnodes keyspaces like the dtest.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_user_type_isolation(cql, this_dc):
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': false}") as ks1:
        # As in the dtest, unqualified names after USE (create_ks's USE).
        with new_cql(cql) as session:
            session.execute(f"USE {ks1}")
            session.execute("CREATE TYPE udt (first text, second int, third int)")
            with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': false}") as ks2:
                session.execute(f"USE {ks2}")
                with pytest.raises(InvalidRequest, match=f"Statement on keyspace {ks2} cannot refer to a user type in keyspace {ks1}"):
                    session.execute(f"CREATE TABLE t (id int PRIMARY KEY, v frozen<{ks1}.udt>)")
