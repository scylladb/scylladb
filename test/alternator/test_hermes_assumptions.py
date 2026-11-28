# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests of the Alternator behaviour that Hermes assumes. Hermes, ScyllaDB's
# tool for migration from DynamoDB, copies items into Alternator tables
# through CQL. Its contract with Alternator,
# https://github.com/scylladb/scylla-hermes/blob/main/docs/alternator-encoding-contract.md,
# says: "It writes the items straight into the CQL table that Alternator
# keeps underneath each Alternator table, in the same bytes `PutItem` would
# have stored." The contract numbers what Hermes assumes as A1 to A21 and
# keeps these numbers stable. The tests below quote the assumptions that they
# pin, word for word, with their numbers.
#
# These tests do not check DynamoDB compatibility, and they do not define
# what Alternator must do. They pin only what Hermes, our own tool, expects
# of Alternator today, so that a change of this behaviour is not silent.
# A change of what PutItem writes can break Hermes, even when a newer version
# still reads the old data. For example, if PutItem wrote a number hash key
# in other bytes, a copied item and a later PutItem of the same item would go
# to two partitions. Such a change is allowed. But if a test in this file, or
# a test in the list below, fails, tell the maintainers of Hermes before you
# change the test, so that Hermes follows the change before it ships.
# SCYLLADB-1575 (number normalization) is a known planned change of this
# type.
#
# Tests in other files pin Alternator behaviour that Hermes uses:
#  - A1, test_encoding.py, test_table_naming: an Alternator table "T" is the
#    CQL table "T" in the keyspace "alternator_T".
#  - A2, test_encoding.py, test_key_column_types: the CQL types of the key
#    columns.
#  - Section 2.2, test_encoding.py, test_attrs_encoding: in ":attrs", the
#    type byte and the value of each type.
#  - A14, test/cluster/test_alternator_hermes_assumptions.py,
#    test_alternator_upgrade_index_key_column: an older table keeps the own
#    column of an index key, and PutItem continues to write it. This test
#    needs an upgrade from 2025.1, so it cannot run in this suite.
#
# Open gap: test.py cannot run a version older than 2025.1. So no test has a
# table in which a GSI key attribute that is not also an LSI key has its own
# column: only such older versions created this table.
# test_alternator_upgrade_index_key_column creates the nearest table on
# 2025.1, with a GSI and an LSI on the same attribute. There the GSI reads its
# key from the own column too. PutItem handles all these tables with the same
# code, which does not check the type of the index.
#
# Open gap: A18 "Plain CQL writes are safe beside the API's, unless every
# write is LWT." This is about concurrent writes, and this test suite cannot
# test the isolation between concurrent writes (see the header of
# test_write_isolation.py). The tests pin only where the write isolation of a
# table is stored, and which isolations use LWT for a write that needs no
# read.

import json
import random
import time
from decimal import Decimal

import pytest
from cassandra.query import BatchStatement, BatchType

from test.cqlpy.util import new_session
from test.cqlpy.test_permissions import eventually_authorized, eventually_unauthorized
from .util import new_test_table, new_role, random_string, client_no_transform, full_query, full_scan, multiset, get_table_arn, metrics, check_increases_metric_exact
from .test_cql_rbac import temporary_grant
from .test_encoding import _serialize_number, table_with_indexes
from .test_ttl import waits_for_expiration
from .test_vector import table_vs, need_vector_search_in_botocore

# All tests in this file are scylla-only (they access CQL internals)
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

# The underlying CQL table of an Alternator table.
def cql_table(table):
    return f'"alternator_{table.name}"."{table.name}"'

# A write time in microseconds, one minute in the past. Hermes gives such a
# time to an item that it copies from a scan of a DynamoDB table.
def past_timestamp():
    return int(time.time() * 1_000_000) - 60_000_000

# A random key for an item of the table. All its key attributes are strings.
def new_key(table):
    return {k['AttributeName']: random_string() for k in table.key_schema}

# Write an item as Hermes does: set the key columns and the whole ":attrs"
# map, with the given write time.
def cql_insert(cql, table, key):
    columns = [*key, ':attrs']
    names = ', '.join(f'"{c}"' for c in columns)
    marks = ', '.join('?' for _ in columns)
    return cql.prepare(f'INSERT INTO {cql_table(table)} ({names}) VALUES ({marks}) USING TIMESTAMP ?')

def cql_put(cql, table, key, attrs, ts):
    cql.execute(cql_insert(cql, table, key), [*key.values(), attrs, ts])

# Write several items to a single partition, as Hermes's load does: one
# UNLOGGED batch of the same INSERT statements.
def cql_put_batch(cql, table, items, ts):
    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
    for key, attrs in items:
        batch.add(cql_insert(cql, table, key), [*key.values(), attrs, ts])
    cql.execute(batch)

# Delete an item as Hermes does: delete its row, with the given write time.
def cql_delete(cql, table, key, ts):
    where = ' AND '.join(f'"{c}" = ?' for c in key)
    stmt = cql.prepare(f'DELETE FROM {cql_table(table)} USING TIMESTAMP ? WHERE {where}')
    cql.execute(stmt, [ts, *key.values()])

# Test that PutItem stores a number in ":attrs" in the bytes that its text
# gives, without normalization. test_encoding.py::test_attrs_encoding checks
# the encoding of each type. This test adds the forms of a number. Hermes
# assumes:
#  - A7 "The scale is taken from the text (`1.50` scale 2, `1E+3` scale −3),
#    leading zeros and a `+` are accepted, `-0` is zero with no sign, and the
#    varint is minimal."
#  - A10, for a number in a set or a list: "numbers inside are strings as
#    given".
# client_no_transform() makes boto3 send each text as it is.
def test_attrs_number_encoding(cql, test_table_ss):
    # DynamoDB's canonical form: Hermes gets numbers in this form from
    # DynamoDB, and writes their bytes.
    canonical = [
        '1.5', '1000', '-12.34', '1' + '0' * 125, '0.' + '0' * 129 + '1',
        '12345678901234567890123456789012345678',
        '-12345678901234567890123456789012345678',
        '-0.12345678901234567890123456789012345678',
    ]
    # Other forms, which DynamoDB does not give. Hermes's start-up check
    # writes them.
    other = ['1.50', '1E+3', '1e-130', '-0', '0.000', '007.700', '+42', '9.9E+125']
    p = random_string()
    c = random_string()
    # Each number is also the name of its attribute.
    item = {n: {'N': n} for n in canonical + other}
    item.update({'p': {'S': p}, 'c': {'S': c},
                 'ns': {'NS': ['1.50', '2']},
                 'l': {'L': [{'S': 'x'}, {'N': '1.50'}]}})
    with client_no_transform(test_table_ss.meta.client) as client:
        client.put_item(TableName=test_table_ss.name, Item=item)
    rows = list(cql.execute(
        f'SELECT ":attrs" FROM {cql_table(test_table_ss)} WHERE p = %s AND c = %s',
        [p, c]
    ))
    assert len(rows) == 1
    attrs = rows[0][0]
    # PutItem stored these bytes for each number in this form so far, and
    # Hermes writes them for each number that it copies. If these bytes
    # change, SCYLLADB-1575 included, a later PutItem of a number key in this
    # form goes to another partition than the existing item. So such a change
    # needs a plan for the existing keys, and Hermes must follow it.
    for n in canonical:
        assert attrs[n] == b'\x03' + _serialize_number(n)
    # SCYLLADB-1575 can change the bytes of these forms.
    for n in other:
        assert attrs[n] == b'\x03' + _serialize_number(n)
    assert attrs['ns'][0:1] == b'\x04'
    assert sorted(json.loads(attrs['ns'][1:])['NS']) == ['1.50', '2']
    assert attrs['l'][0:1] == b'\x04'
    assert json.loads(attrs['l'][1:]) == {'L': [{'S': 'x'}, {'N': '1.50'}]}

# Test that PutItem stores a number key in the bytes that its text gives, as
# test_attrs_number_encoding checks for a number in ":attrs". PutItem
# serializes a key with other code than ":attrs". Hermes writes these bytes
# for a number key. If PutItem wrote other bytes, then as A8 says, "Hermes's
# number keys would land in partitions Alternator does not look in". Each
# number starts with a random integer, so that no other test writes a key
# with the same bytes. Each number has its own hash key in test_table_sn,
# because a range key compares by value: 5.5 and 5.50 would be the same row.
def test_number_key_encoding(cql, test_table_n, test_table_sn):
    r = str(random.randrange(10**8, 10**9))
    # DynamoDB's canonical form, and other forms.
    canonical = [r, r + '000', r + '.5', '-' + r + '.25']
    other = [r + '.50', r + 'E+3']
    # The driver sends Decimal(n) with the scale of the text.
    hash_select = cql.prepare(f'SELECT decimalAsBlob(p) FROM {cql_table(test_table_n)} WHERE p = ?')
    range_select = cql.prepare(f'SELECT decimalAsBlob(c) FROM {cql_table(test_table_sn)} WHERE p = ? AND c = ?')
    # The bytes of each number as a hash key and as a range key.
    stored = {}
    with client_no_transform(test_table_n.meta.client) as client:
        for n in canonical + other:
            p = random_string()
            client.put_item(TableName=test_table_n.name, Item={'p': {'N': n}})
            client.put_item(TableName=test_table_sn.name, Item={'p': {'S': p}, 'c': {'N': n}})
            rows = [cql.execute(hash_select, [Decimal(n)]).one(), cql.execute(range_select, [p, Decimal(n)]).one()]
            stored[n] = [row and row[0] for row in rows]
    # As in test_attrs_number_encoding: a change of these bytes needs a plan
    # for the existing keys, and Hermes must follow it.
    for n in canonical:
        assert stored[n] == [_serialize_number(n)] * 2, n
    # SCYLLADB-1575 can change the bytes of these forms.
    for n in other:
        assert stored[n] == [_serialize_number(n)] * 2, n

# Test the columns of a base table that has a GSI and an LSI. The table has
# only its key columns and ":attrs", so neither "x" (GSI key) nor "y" (LSI
# key) is a column. test_encoding.py::test_index_key_not_schema_column checks
# that these attributes are in ":attrs". This test checks the full list of
# columns, because Hermes refuses a table of another shape. It assumes, in
# A2: "At most one `partition_key` column (the hash key) and one `clustering`
# column (the range key)". In A3: "One `regular` column named `:attrs`, type
# exactly `map<text, blob>`, non-frozen." In A4: "No `static` columns." It
# takes any other regular column as "an index key attribute's own column", so
# a new table must have none.
def test_base_table_columns(cql, table_with_indexes):
    table = table_with_indexes
    rows = cql.execute(
        "SELECT column_name, kind, type FROM system_schema.columns "
        "WHERE keyspace_name = %s AND table_name = %s",
        ['alternator_' + table.name, table.name])
    assert {(r.column_name, r.kind, r.type) for r in rows} == {
        ('p', 'partition_key', 'text'),
        ('c', 'clustering', 'text'),
        (':attrs', 'regular', 'map<text, blob>'),
    }

# Test how Hermes finds the GSI and the LSI of a table, and their keys.
# test_encoding.py::test_index_naming checks the names of their views. Hermes
# assumes:
#  - A1 "Its GSIs and LSIs are materialized views in that keyspace whose
#    `base_table_name` is `T`; Hermes finds them by `base_table_name`, not by
#    view name."
#  - A5 "A view's key columns that are not base keys are GSI/LSI key
#    attributes, with the type their index declares."
#  - A4 "A table with an entry in `system_schema.indexes` (a vector index, a
#    search schema) is refused". So a GSI and an LSI must have no such entry.
#    test_vector_index_in_schema_indexes checks that a vector index has one.
def test_index_views(cql, table_with_indexes):
    table_name = table_with_indexes.name
    ks = 'alternator_' + table_name
    gsi_view = table_name + ':gsi1'
    lsi_view = table_name + '!:lsi1'
    for view in (gsi_view, lsi_view):
        rows = list(cql.execute(
            "SELECT view_name, base_table_name FROM system_schema.views "
            f"WHERE keyspace_name = '{ks}' AND view_name = '{view}'"
        ))
        assert len(rows) == 1
        assert rows[0].base_table_name == table_name
    def index_keys(view):
        rows = cql.execute("SELECT column_name, kind, type FROM system_schema.columns "
                           "WHERE keyspace_name = %s AND table_name = %s", [ks, view])
        return {(r.column_name, r.type) for r in rows
                if r.kind in ('partition_key', 'clustering') and r.column_name not in ('p', 'c')}
    assert index_keys(gsi_view) == {('x', 'text')}
    assert index_keys(lsi_view) == {('y', 'text')}
    assert list(cql.execute("SELECT index_name FROM system_schema.indexes "
                            "WHERE keyspace_name = %s AND table_name = %s",
                            [ks, table_name])) == []

# Test that a vector index has an entry in system_schema.indexes. Hermes
# refuses a table with such an entry. It assumes, in A4: "A table with an
# entry in `system_schema.indexes` (a vector index, a search schema) is
# refused, since Hermes cannot know what the CQL write would miss there."
# test_index_views checks that a GSI and an LSI have no such entry.
def test_vector_index_in_schema_indexes(table_vs, cql):
    rows = list(cql.execute("SELECT index_name FROM system_schema.indexes "
                            "WHERE keyspace_name = %s AND table_name = %s",
                            ['alternator_' + table_vs.name, table_vs.name]))
    assert len(rows) == 1

# The tests below write rows through CQL, with the statements that Hermes
# uses, and check that Alternator reads them as items.

# Test that a row which Hermes writes through CQL is the item that it wrote.
# The test writes a row, then writes it again one microsecond later without
# one attribute, and writes a second row with an empty ":attrs". It uses a
# table with only a hash key and a table with a hash key and a range key.
# Hermes assumes:
#  - A12 "Alternator reads that as exactly the item written: an attribute the
#    item no longer has is gone, as after `PutItem`".
#  - A13 "An item with only its key is a row with its marker and an empty
#    `:attrs`; `GetItem` returns the key attributes alone."
#  - A17 "an item stamped in the past is a live item".
#  - A10, for the JSON of type 4: "Alternator reads it by parsing, so member
#    order and spacing do not matter; numbers inside are strings as given;
#    binary inside is base64".
@pytest.mark.parametrize('table_name', ['test_table_s', 'test_table_ss'])
def test_cql_write_is_an_item(request, cql, table_name):
    table = request.getfixturevalue(table_name)
    key = new_key(table)
    ts = past_timestamp()
    attrs = {
        's': b'\x00' + 'héllo ☃'.encode(),
        'b': b'\x01\x00\x01\xff',
        'bool': b'\x02\x01',
        'n': b'\x03' + _serialize_number('1.5'),
        # Spaces, and members out of order: Alternator parses the JSON.
        'l': b'\x04' + b'{"L": [ {"S": "x"}, {"N": "1.50"} ]}',
        'm': b'\x04' + b'{"M":{"b":{"N":"2"},"a":{"S":"y"}}}',
        'null': b'\x04' + b'{"NULL":true}',
        'ss': b'\x04' + b'{"SS":["b","a"]}',
        'ns': b'\x04' + b'{"NS":["2","1.50"]}',
        'bs': b'\x04' + b'{"BS":["/w==","AA=="]}',
    }
    expected = {**key, 's': 'héllo ☃', 'b': b'\x00\x01\xff', 'bool': True,
                'n': Decimal('1.5'), 'l': ['x', Decimal('1.5')],
                'm': {'a': 'y', 'b': Decimal('2')}, 'null': None, 'ss': {'a', 'b'},
                'ns': {Decimal('1.5'), Decimal('2')}, 'bs': {b'\x00', b'\xff'}}
    cql_put(cql, table, key, attrs, ts)
    assert table.get_item(Key=key, ConsistentRead=True)['Item'] == expected
    del attrs['s']
    del expected['s']
    cql_put(cql, table, key, attrs, ts + 1)
    assert table.get_item(Key=key, ConsistentRead=True)['Item'] == expected
    key = new_key(table)
    cql_put(cql, table, key, {}, ts)
    assert table.get_item(Key=key, ConsistentRead=True)['Item'] == key

# Test that a CQL delete of the row of an item is a DeleteItem: after it,
# GetItem, Query and Scan do not find the item. On a table with only a hash
# key, the delete is a partition tombstone. Then a PutItem of the same key,
# with the server's write time, makes a visible item. A later CQL write or
# delete with an older write time does not change this item, and after a
# DeleteItem, it does not bring the item back. This lets Hermes copy a table
# while the application writes to it. Hermes assumes:
#  - A16 "`DELETE ... USING TIMESTAMP` (a row tombstone, or a partition
#    tombstone for a table with no range key) removes the item for `GetItem`,
#    `Query`, `Scan` and the indexes." test_cql_write_updates_indexes checks
#    the indexes.
#  - A17 "a later write through the API, stamped by the server, wins over
#    Hermes's older stamp".
@pytest.mark.parametrize('table_name', ['test_table_s', 'test_table_ss'])
def test_cql_delete_is_deleteitem(request, cql, table_name):
    table = request.getfixturevalue(table_name)
    key = new_key(table)
    ts = past_timestamp()
    cql_put(cql, table, key, {'a': b'\x00x'}, ts)
    assert table.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'a': 'x'}
    cql_delete(cql, table, key, ts + 1)
    assert 'Item' not in table.get_item(Key=key, ConsistentRead=True)
    assert full_query(table, KeyConditionExpression='p = :p',
                      ExpressionAttributeValues={':p': key['p']}) == []
    assert full_scan(table, FilterExpression='p = :p',
                     ExpressionAttributeValues={':p': key['p']}) == []
    table.put_item(Item={**key, 'a': 'y'})
    assert table.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'a': 'y'}
    cql_put(cql, table, key, {'b': b'\x00z'}, ts + 2)
    cql_delete(cql, table, key, ts + 3)
    assert table.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'a': 'y'}
    table.delete_item(Key=key)
    cql_put(cql, table, key, {'a': b'\x00x'}, ts + 4)
    assert 'Item' not in table.get_item(Key=key, ConsistentRead=True)

# Wait until a query of an index finds exactly the expected items. Unlike
# assert_index_query() in test_gsi.py, this also waits for an item to leave
# the index.
def assert_index_query_eventually(table, index_name, expected, **kwargs):
    for _ in range(100):
        got = full_query(table, IndexName=index_name, ConsistentRead=False, **kwargs)
        if multiset(got) == multiset(expected):
            return
        time.sleep(0.1)
    got = full_query(table, IndexName=index_name, ConsistentRead=False, **kwargs)
    assert multiset(got) == multiset(expected)

# Test that the GSI and the LSI of a table follow the rows that Hermes writes
# through CQL. The first write is an UNLOGGED batch of two items of one
# partition, as in a load of Hermes. A rewrite of the row without the GSI key
# removes the item from the GSI, and a CQL delete of the row removes the item
# from both indexes. Hermes assumes:
#  - A15 "On newer tables the GSI and LSI keys are computed from `:attrs`
#    (`extract_from_attrs_column_computation`), so a CQL write of `:attrs`
#    updates the indexes as `PutItem` would".
#  - A16 "`DELETE ... USING TIMESTAMP` (a row tombstone, or a partition
#    tombstone for a table with no range key) removes the item for `GetItem`,
#    `Query`, `Scan` and the indexes."
def test_cql_write_updates_indexes(cql, table_with_indexes):
    table = table_with_indexes
    key = {'p': random_string(), 'c': random_string()}
    x = random_string()
    y = random_string()
    ts = past_timestamp()
    full = {'x': b'\x00' + x.encode(), 'y': b'\x00' + y.encode()}
    item = {**key, 'x': x, 'y': y}
    def check(gsi_items, lsi_items, x=x, y=y):
        assert_index_query_eventually(table, 'gsi1', gsi_items,
            KeyConditionExpression='x = :x', ExpressionAttributeValues={':x': x})
        assert_index_query_eventually(table, 'lsi1', lsi_items,
            KeyConditionExpression='p = :p AND y = :y',
            ExpressionAttributeValues={':p': key['p'], ':y': y})
    key2 = {'p': key['p'], 'c': random_string()}
    x2 = random_string()
    y2 = random_string()
    cql_put_batch(cql, table, [(key, full), (key2, {'x': b'\x00' + x2.encode(), 'y': b'\x00' + y2.encode()})], ts)
    check([item], [item])
    check([{**key2, 'x': x2, 'y': y2}], [{**key2, 'x': x2, 'y': y2}], x2, y2)
    cql_put(cql, table, key, {'y': full['y']}, ts + 1)
    check([], [{**key, 'y': y}])
    cql_put(cql, table, key, full, ts + 2)
    check([item], [item])
    cql_delete(cql, table, key, ts + 3)
    check([], [])

# Test that the expiration scanner deletes an expired item which was written
# directly through CQL, not through PutItem. Hermes assumes, in A20:
# "Alternator's expiration reads the TTL attribute (an N in `:attrs`, seconds
# since the epoch) of an item Hermes wrote and deletes it when due." The
# expiration time of the item is one minute in the past. The test enables
# TTL only after it reads the item, so that it sees the item before the
# expiration scanner deletes it. As in test_ttl.py, the test runs with vnodes
# and with tablets. Like test_ttl.py::test_ttl_expiration_with_rangekey, the
# test ends as soon as the item expires.
@pytest.mark.parametrize('tags', [
    [{'Key': 'system:initial_tablets', 'Value': 'none'}],
    [{'Key': 'system:initial_tablets', 'Value': '0'}],
], ids=['using vnodes', 'using tablets'])
def test_ttl_expiration_cql_write(dynamodb, cql, waits_for_expiration, tags):
    max_duration = 240
    sleep = 0.1
    with new_test_table(dynamodb,
        Tags=tags,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' },
                               { 'AttributeName': 'c', 'AttributeType': 'S' }]) as table:
        # This item never expires: it has no "expiration" attribute.
        key1 = {'p': random_string(), 'c': random_string()}
        cql_put(cql, table, key1, {'a': b'\x00x'}, past_timestamp())
        # This item expired one minute ago.
        key2 = {'p': random_string(), 'c': random_string()}
        expiration = int(time.time()) - 60
        cql_put(cql, table, key2, {'expiration': b'\x03' + _serialize_number(str(expiration))}, past_timestamp())
        assert table.get_item(Key=key2, ConsistentRead=True)['Item'] == {**key2, 'expiration': expiration}
        table.meta.client.update_time_to_live(TableName=table.name,
            TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})
        start_time = time.time()
        while time.time() < start_time + max_duration:
            if ('Item' in table.get_item(Key=key1)) and not ('Item' in table.get_item(Key=key2)):
                break
            time.sleep(sleep)
        assert 'Item' in table.get_item(Key=key1)
        assert not 'Item' in table.get_item(Key=key2)

# Decode the "scylla_tags" schema extension of a table: a map of strings,
# serialized with 32-bit little-endian counts and lengths.
def decode_tags(b):
    strings = []
    pos = 4
    while pos < len(b):
        n = int.from_bytes(b[pos:pos + 4], 'little')
        strings.append(b[pos + 4:pos + 4 + n].decode())
        pos += 4 + n
    assert int.from_bytes(b[:4], 'little') * 2 == len(strings)
    return dict(zip(strings[::2], strings[1::2]))

# Test where Hermes finds the write isolation of a table. Hermes assumes:
#  - A18 "At start Hermes reads `system:write_isolation` from the table's tags
#    and, without one, `alternator_write_isolation` from `system.config`."
#  - A19 "The table's tags are the `scylla_tags` entry of
#    `system_schema.tables.extensions`, a map with 32-bit little-endian
#    lengths, with keys such as `system:write_isolation`."
def test_write_isolation_location(dynamodb, cql):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        table.meta.client.tag_resource(ResourceArn=get_table_arn(table),
            Tags=[{'Key': 'system:write_isolation', 'Value': 'forbid_rmw'}])
        row = cql.execute(
            "SELECT extensions FROM system_schema.tables "
            "WHERE keyspace_name = %s AND table_name = %s",
            ['alternator_' + table.name, table.name]).one()
        assert decode_tags(row.extensions['scylla_tags'])['system:write_isolation'] == 'forbid_rmw'
    value = cql.execute(
        "SELECT value FROM system.config WHERE name = 'alternator_write_isolation'").one().value
    # system.config gives a string value in double quotes.
    assert value.strip('"') in ['a', 'always', 'always_use_lwt',
        'f', 'forbid', 'forbid_rmw', 'o', 'only_rmw_uses_lwt',
        'u', 'unsafe', 'unsafe_rmw']

# Test which values of the write isolation make an unconditional PutItem and
# DeleteItem use LWT. Hermes writes items with plain CQL writes. It assumes,
# in A18: "On `always_use_lwt` (`a`, `always`) it refuses unless the operator
# confirms the application is not writing" and "With the other isolations
# Hermes assumes a plain write is no different from what Alternator itself
# does for a write that needs no read".
# test_metrics.py::test_reads_before_write_and_write_using_lwt_metrics checks
# these metrics for conditional writes.
def test_write_isolation_lwt(dynamodb, test_table_s, metrics):
    table_rbw = 'scylla_alternator_table_reads_before_write'
    table_lwt = 'scylla_alternator_table_write_using_lwt'
    p = random_string()
    cf = {'cf': test_table_s.name}
    # test_table_s has no tag, so it has the default write isolation of the
    # server, 'only_rmw_uses_lwt' in this test suite. With it, an
    # unconditional PutItem and DeleteItem neither read before the write nor
    # use LWT.
    with check_increases_metric_exact(metrics, table_rbw, [[0, cf]]):
        with check_increases_metric_exact(metrics, table_lwt, [[0, cf]]):
            test_table_s.put_item(Item={'p': p})
            test_table_s.delete_item(Key={'p': p})
    # Of all the values of the tag, only the three forms of 'always_use_lwt'
    # make an unconditional PutItem and DeleteItem use LWT.
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            Tags=[{'Key': 'system:write_isolation', 'Value': 'always_use_lwt'}]) as lwt_table:
        lwt_cf = {'cf': lwt_table.name}
        arn = get_table_arn(lwt_table)
        for value, uses_lwt in [('a', 1), ('always', 1), ('always_use_lwt', 1),
                                ('o', 0), ('only_rmw_uses_lwt', 0),
                                ('f', 0), ('forbid', 0), ('forbid_rmw', 0),
                                ('u', 0), ('unsafe', 0), ('unsafe_rmw', 0)]:
            lwt_table.meta.client.tag_resource(ResourceArn=arn,
                Tags=[{'Key': 'system:write_isolation', 'Value': value}])
            with check_increases_metric_exact(metrics, table_lwt, [[2 * uses_lwt, lwt_cf]]):
                lwt_table.put_item(Item={'p': p})
                lwt_table.delete_item(Key={'p': p})

# Test the permissions that a CQL write needs, when Hermes writes and deletes
# items through CQL. Such a write needs the permission that PutItem and
# DeleteItem need, MODIFY on the base table. It needs no permission on the
# GSI, the LSI or the CDC log of the table, although the write changes them
# too. The test also checks the reads that Hermes makes: the schema and the
# topology need no grant, and system.config and system.tablets need SELECT.
# Hermes assumes, in section 1 of its contract: "The role it needs: `MODIFY`
# and `SELECT` on `alternator_<table>.<table>`, and `SELECT` on
# `system_schema` and `system.config`; nothing more is recommended."
def test_rbac_cql_write(dynamodb, cql):
    schema = {
        'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                      {'AttributeName': 'c', 'KeyType': 'RANGE'}],
        'AttributeDefinitions': [{'AttributeName': a, 'AttributeType': 'S'} for a in 'pcxy'],
        'GlobalSecondaryIndexes': [
            {'IndexName': 'gsi',
             'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
             'Projection': {'ProjectionType': 'ALL'}}],
        'LocalSecondaryIndexes': [
            {'IndexName': 'lsi',
             'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                           {'AttributeName': 'y', 'KeyType': 'RANGE'}],
             'Projection': {'ProjectionType': 'ALL'}}],
        'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
    }
    with new_test_table(dynamodb, **schema) as table:
        p, c, x, y = (random_string() for _ in range(4))
        name = cql_table(table)
        insert = f'INSERT INTO {name} (p, c, ":attrs") VALUES (%s, %s, %s)'
        delete = f'DELETE FROM {name} WHERE p = %s AND c = %s'
        select = f'SELECT ":attrs" FROM {name} WHERE p = %s AND c = %s'
        config = ("SELECT value FROM system.config "
                  "WHERE name IN ('alternator_write_isolation', 'batch_size_warn_threshold_in_kb')")
        tablets = "SELECT * FROM system.tablets LIMIT 1"
        values = [p, c, {'x': b'\x00' + x.encode(), 'y': b'\x00' + y.encode()}]
        with new_role(cql) as (role, _):
            # new_role() sets the password of the role to its name.
            with new_session(cql, role) as session:
                eventually_unauthorized(lambda: session.execute(insert, values))
                with temporary_grant(cql, 'MODIFY', name, role):
                    eventually_authorized(lambda: session.execute(insert, values))
                    assert table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c, 'x': x, 'y': y}
                    # Hermes reads its own test row back through CQL.
                    eventually_unauthorized(lambda: session.execute(select, [p, c]))
                    with temporary_grant(cql, 'SELECT', name, role):
                        assert eventually_authorized(lambda: session.execute(select, [p, c]).one()) is not None
                    eventually_authorized(lambda: session.execute(delete, [p, c]))
                    assert not 'Item' in table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)
                # At start, Hermes reads the schema and the topology, which
                # need no grant. It also reads two values of system.config, and
                # the tablets, to route a write to the replicas of its tablet.
                session.execute("SELECT column_name FROM system_schema.columns WHERE keyspace_name = %s",
                                ['alternator_' + table.name])
                session.execute("SELECT * FROM system.local")
                session.execute("SELECT * FROM system.peers")
                for read, resource in ((config, 'system.config'), (tablets, 'system.tablets')):
                    eventually_unauthorized(lambda: session.execute(read))
                    with temporary_grant(cql, 'SELECT', resource, role):
                        eventually_authorized(lambda: session.execute(read))
