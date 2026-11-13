# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the on-disk encoding of Alternator data. Specifically, these
# tests verify that the internal format used to store DynamoDB attribute
# values in the underlying Scylla table hasn't accidentally changed. If
# Alternator's encoding were to change, sstables written by an older version
# would become unreadable by a newer version - an unacceptable compatibility
# breakage. So if any of these tests fail, the reason should be carefully
# analyzed, and the test should only be updated if the encoding change was
# intentional and backward compatibility was handled.
#
# Background on the encoding (see also issue #19770):
# Alternator stores each DynamoDB table in keyspace "alternator_{table_name}",
# table "{table_name}". The key attributes (hash key and optional range key)
# are stored as regular CQL columns with their native CQL types (text for S,
# blob for B, decimal for N). All other (non-key) attributes are stored
# together in a single CQL column named ":attrs" of type map<text, blob>.
# The map key is the attribute name; the map value encodes the type and value
# of the attribute:
# - "Optimized" types S, B, BOOL, N are encoded as one type byte followed by
#   Scylla's native serialization of the value. The type bytes are defined by
#   enum class alternator_type in alternator/serialization.hh:
#     S    = 0
#     B    = 1
#     BOOL = 2
#     N    = 3
# - All other DynamoDB types (NULL, L, M, SS, NS, BS) are stored as type byte
#   4 (NOT_SUPPORTED_YET) followed by the JSON encoding of the full typed
#   DynamoDB value (e.g., {"NULL":true} or {"L":[...]}).
# - There is also the byte 5 (FLOAT32VECTOR). This encoding is not checked
#   in this file, it is checked in test_vector.py::test_float32vector_encoding.
#
# The order of entries in the alternator_type enum is critical: the numeric
# value of each type is written to disk, so it must not change.
#
# This file is related to issue #19770.
#
# This encoding is also an interface to an external tool. Hermes, ScyllaDB's
# tool for migration from DynamoDB, copies items into Alternator tables
# through CQL. Its contract with Alternator,
# https://github.com/scylladb/scylla-hermes/blob/main/docs/alternator-encoding-contract.md,
# says: "It writes the items straight into the CQL table that Alternator
# keeps underneath each Alternator table, in the same bytes `PutItem` would
# have stored." The contract numbers what Hermes assumes as A1 to A21 and
# keeps these numbers stable. The tests below quote the assumptions that they
# pin, word for word, with their numbers.
#
# So a change of what PutItem writes can break Hermes, even when a newer
# version still reads the old data. For example, if PutItem wrote a number
# hash key in other bytes, a copied item and a later PutItem of the same item
# would go to two partitions. Such a change is allowed. But if a test in this
# file, or a test in the list below, fails, tell the maintainers of Hermes
# before you change the test, so that Hermes follows the change before it
# ships. SCYLLADB-1575 (number normalization) is a known planned change of
# this type.
#
# Tests in other files pin Alternator behaviour that Hermes uses:
#  - A4, test_vector.py, test_vector_index_in_schema_indexes: a vector index
#    has an entry in system_schema.indexes, so Hermes refuses its table.
#  - A14, test/cluster/test_alternator.py,
#    test_alternator_upgrade_index_key_column: an older table keeps the own
#    column of an index key, and PutItem continues to write it.
#  - A18, test_metrics.py,
#    test_reads_before_write_and_write_using_lwt_metrics: which values of the
#    write isolation make a plain write use LWT.
#  - A20, test_ttl.py, test_ttl_expiration_cql_write: TTL expires an item
#    that CQL wrote.
#  - Section 1, test_cql_rbac.py, test_rbac_cql_write: the permissions that
#    Hermes needs.
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

from .util import new_test_table, random_string, client_no_transform, full_query, full_scan, multiset, get_table_arn

# All tests in this file are scylla-only (they access CQL internals)
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

# A module-scoped table with both a GSI and an LSI, where "x" is the
# GSI hash key and "y" is the LSI range key - both are non-base-table-key
# attributes. Used by test_index_naming and test_index_key_not_schema_column.
@pytest.fixture(scope='module')
def table_with_indexes(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[
            {'AttributeName': 'p', 'KeyType': 'HASH'},
            {'AttributeName': 'c', 'KeyType': 'RANGE'},
        ],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
            {'AttributeName': 'c', 'AttributeType': 'S'},
            {'AttributeName': 'x', 'AttributeType': 'S'},
            {'AttributeName': 'y', 'AttributeType': 'S'},
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'lsi1',
                'KeySchema': [
                    {'AttributeName': 'p', 'KeyType': 'HASH'},
                    {'AttributeName': 'y', 'KeyType': 'RANGE'},
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'gsi1',
                'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'}
            }
        ],
    ) as table:
        yield table

# Serialize a DynamoDB number string as Scylla's decimal_type binary format:
# 4-byte big-endian signed scale followed by a big-endian two's-complement
# varint (minimum bytes) for the unscaled value.
# This matches how big_decimal::big_decimal(string_view) in Scylla parses
# the number string and how decimal_type serializes it.
def _serialize_number(s):
    d = Decimal(s)
    sign, digits, exp = d.as_tuple()
    # Scylla's big_decimal sets scale = -exp and unscaled = int(digits)
    cql_scale = -exp
    unscaled = int(''.join(str(x) for x in digits)) if digits else 0
    if sign:
        unscaled = -unscaled
    scale_bytes = cql_scale.to_bytes(4, 'big', signed=True)
    # Encode unscaled as a Cassandra varint (big-endian two's complement, minimum bytes)
    if unscaled == 0:
        varint_bytes = b'\x00'
    elif unscaled > 0:
        # Need enough bytes so the most-significant bit is 0 (positive sign)
        num_bytes = (unscaled.bit_length() + 8) // 8
        varint_bytes = unscaled.to_bytes(num_bytes, 'big')
    else:
        num_bytes = ((-unscaled).bit_length() + 7) // 8
        try:
            varint_bytes = unscaled.to_bytes(num_bytes, 'big', signed=True)
        except OverflowError:
            varint_bytes = unscaled.to_bytes(num_bytes + 1, 'big', signed=True)
    return scale_bytes + varint_bytes

# Test that the encoding of all DynamoDB attribute types in the ":attrs"
# map column of the underlying CQL table is as documented in the header
# comment of this file, and has not accidentally changed. Specifically:
# - For S, B, BOOL, N (optimized types): check the exact binary encoding.
# - For NULL, L, M, SS, NS, BS: check the type byte (0x04) and verify the
#   remaining bytes decode to the expected JSON structure.
# Multiple N values are stored to exercise _serialize_number() with diverse
# inputs: positive scale, zero, negative unscaled, negative scale, and a
# large number whose unscaled value requires multiple bytes.
# Hermes assumes, in section 2.2: "`:attrs` is `map<text, blob>`: the
# attribute name, then one type byte and the value."
def test_attrs_encoding(dynamodb, cql, test_table_ss):
    p = random_string()
    c = random_string()
    test_table_ss.put_item(Item={
        'p': p,
        'c': c,
        'a_s': 'hello',                          # S
        'a_n': Decimal('3.14'),                  # N: positive scale=2, positive unscaled=314
        'a_n_zero': Decimal('0'),                # N: zero
        'a_n_neg': Decimal('-5'),                # N: negative unscaled, zero scale
        'a_n_negscale': Decimal('1e10'),         # N: negative scale (stored as 1E+10)
        'a_n_large': Decimal('12345678901234567890'),  # N: large multi-byte unscaled
        'a_b': b'\x01\x02\x03',                 # B
        'a_bool_t': True,                        # BOOL true
        'a_bool_f': False,                       # BOOL false
        'a_null': None,                          # NULL
        'a_l': ['x'],                            # L (list with one string)
        'a_m': {'k': 'v'},                       # M (map with one string value)
        'a_ss': {'hello'},                       # SS (single-element string set)
        'a_ns': {Decimal('1')},                  # NS (single-element number set)
        'a_bs': {b'\x01'},                       # BS (single-element binary set)
    })

    ks = 'alternator_' + test_table_ss.name
    rows = list(cql.execute(
        f'SELECT ":attrs" FROM "{ks}"."{test_table_ss.name}" WHERE p = %s AND c = %s',
        [p, c]
    ))
    assert len(rows) == 1
    attrs = rows[0][0]

    # S (alternator_type::S = 0): type byte 0x00 followed by raw UTF-8 bytes
    assert attrs['a_s'] == b'\x00' + b'hello'

    # N (alternator_type::N = 3): type byte 0x03 followed by decimal_type serialization
    # (4-byte big-endian scale + varint unscaled value)
    assert attrs['a_n'] == b'\x03' + _serialize_number('3.14')
    assert attrs['a_n_zero'] == b'\x03' + _serialize_number('0')
    assert attrs['a_n_neg'] == b'\x03' + _serialize_number('-5')
    assert attrs['a_n_negscale'] == b'\x03' + _serialize_number('1e10')
    assert attrs['a_n_large'] == b'\x03' + _serialize_number('12345678901234567890')

    # B (alternator_type::B = 1): type byte 0x01 followed by raw bytes
    assert attrs['a_b'] == b'\x01' + b'\x01\x02\x03'

    # BOOL true (alternator_type::BOOL = 2): type byte 0x02 followed by 0x01
    assert attrs['a_bool_t'] == b'\x02\x01'

    # BOOL false: type byte 0x02 followed by 0x00
    assert attrs['a_bool_f'] == b'\x02\x00'

    # For the following types (NOT_SUPPORTED_YET = 4), the encoding is:
    # type byte 0x04 followed by the compact JSON of the full typed value.
    # We check the type byte and that the JSON decodes to the expected structure.

    # NULL
    assert attrs['a_null'][0:1] == b'\x04'
    assert json.loads(attrs['a_null'][1:]) == {'NULL': True}

    # L (list)
    assert attrs['a_l'][0:1] == b'\x04'
    assert json.loads(attrs['a_l'][1:]) == {'L': [{'S': 'x'}]}

    # M (map)
    assert attrs['a_m'][0:1] == b'\x04'
    assert json.loads(attrs['a_m'][1:]) == {'M': {'k': {'S': 'v'}}}

    # SS (string set, single element so no ordering ambiguity)
    assert attrs['a_ss'][0:1] == b'\x04'
    assert json.loads(attrs['a_ss'][1:]) == {'SS': ['hello']}

    # NS (number set, single element so no ordering ambiguity)
    assert attrs['a_ns'][0:1] == b'\x04'
    assert json.loads(attrs['a_ns'][1:]) == {'NS': ['1']}

    # BS (binary set, binary values are base64-encoded in the JSON)
    assert attrs['a_bs'][0:1] == b'\x04'
    assert json.loads(attrs['a_bs'][1:]) == {'BS': ['AQ==']}

    # The bytes of a number come from its text, without normalization. Hermes
    # assumes, in A7: "The scale is taken from the text (`1.50` scale 2,
    # `1E+3` scale −3), leading zeros and a `+` are accepted, `-0` is zero
    # with no sign, and the varint is minimal." For a number in a set or a
    # list, it assumes in A10: "numbers inside are strings as given".
    # client_no_transform() makes boto3 send each text as it is.
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
    numbers = canonical + other
    p = random_string()
    c = random_string()
    # Each number is also the name of its attribute.
    item = {n: {'N': n} for n in numbers}
    item.update({'p': {'S': p}, 'c': {'S': c},
                 'ns': {'NS': ['1.50', '2']},
                 'l': {'L': [{'S': 'x'}, {'N': '1.50'}]}})
    with client_no_transform(test_table_ss.meta.client) as client:
        client.put_item(TableName=test_table_ss.name, Item=item)
    rows = list(cql.execute(
        f'SELECT ":attrs" FROM "{ks}"."{test_table_ss.name}" WHERE p = %s AND c = %s',
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

# Test that both hash keys and range keys of all three DynamoDB key types
# (S, B, N) are stored as the correct native CQL types. Specifically:
#   S (string) -> CQL text
#   B (binary) -> CQL blob
#   N (number) -> CQL decimal
# These type mappings are part of the on-disk format and must not change.
# Hermes assumes, in A2, key columns "typed `text`, `blob` or `decimal` for
# S, B, N, holding the plain CQL value with no type byte."
def test_key_column_types(dynamodb, cql, test_table_sn, test_table_b, test_table_ss, test_table_sb):
    def get_col_type(table, col_name):
        ks = 'alternator_' + table.name
        rows = list(cql.execute(
            "SELECT type FROM system_schema.columns "
            f"WHERE keyspace_name = '{ks}' AND table_name = '{table.name}' "
            f"AND column_name = '{col_name}'"
        ))
        return rows[0].type if rows else None

    # Hash key type S -> CQL text
    assert get_col_type(test_table_sn, 'p') == 'text'

    # Hash key type B -> CQL blob
    assert get_col_type(test_table_b, 'p') == 'blob'

    # Hash key type N -> CQL decimal (no shared fixture for N-hash tables)
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'N'}]) as table:
        assert get_col_type(table, 'p') == 'decimal'

    # Range key type S -> CQL text
    assert get_col_type(test_table_ss, 'c') == 'text'

    # Range key type B -> CQL blob
    assert get_col_type(test_table_sb, 'c') == 'blob'

    # Range key type N -> CQL decimal
    assert get_col_type(test_table_sn, 'c') == 'decimal'

# Test that Alternator tables use the expected keyspace and table names in
# the underlying CQL schema. This naming convention must remain stable, as
# changing it would break access to existing data. Hermes assumes, in A1:
# "An Alternator table `T` is CQL keyspace `alternator_T`, table `T`."
# See also test_cql_schema.py::test_cql_keyspace_and_table which tests this
# in a different way.
def test_table_naming(cql, test_table_s):
    table_name = test_table_s.name
    ks = 'alternator_' + table_name
    # Verify we can query the underlying CQL table using the expected keyspace
    # and table name. The query will succeed only if naming is as expected.
    cql.execute(f'SELECT p FROM "{ks}"."{table_name}"')

# Test that GSI and LSI view names in the underlying CQL schema follow the
# expected naming convention:
#   GSI named "gsi1" -> CQL view "{table_name}:gsi1"
#   LSI named "lsi1" -> CQL view "{table_name}!:lsi1"
# These naming conventions must remain stable, as changing them would break
# access to existing data. See also test_cql_schema.py::test_alternator_aux_tables
# which tests related properties in a different way.
# The test also checks base_table_name, because Hermes assumes, in A1: "Its
# GSIs and LSIs are materialized views in that keyspace whose
# `base_table_name` is `T`; Hermes finds them by `base_table_name`, not by
# view name." It also checks the key columns of the views, and that the GSI
# and the LSI have no entry in system_schema.indexes. Hermes assumes, in A5:
# "A view's key columns that are not base keys are GSI/LSI key attributes,
# with the type their index declares." In A4, it refuses a table with an
# entry in system_schema.indexes (see
# test_vector.py::test_vector_index_in_schema_indexes).
def test_index_naming(cql, table_with_indexes):
    table_name = table_with_indexes.name
    ks = 'alternator_' + table_name
    gsi_view = table_name + ':gsi1'
    lsi_view = table_name + '!:lsi1'
    # Check the view names in system_schema.views (the view names contain
    # special characters such as '!' and ':', so we look them up in the
    # system table rather than attempting to query the views directly).
    rows = list(cql.execute(
        "SELECT view_name, base_table_name FROM system_schema.views "
        f"WHERE keyspace_name = '{ks}' AND view_name = '{gsi_view}'"
    ))
    assert len(rows) == 1, f"Expected GSI view '{gsi_view}' not found in system_schema.views"
    assert rows[0].base_table_name == table_name
    rows = list(cql.execute(
        "SELECT view_name, base_table_name FROM system_schema.views "
        f"WHERE keyspace_name = '{ks}' AND view_name = '{lsi_view}'"
    ))
    assert len(rows) == 1, f"Expected LSI view '{lsi_view}' not found in system_schema.views"
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

# Test that GSI/LSI key attributes that are not base-table key attributes
# are NOT stored as separate CQL schema columns in the base table - they are
# stored in ":attrs" instead. In the table_with_indexes fixture, "x" is the
# GSI hash key and "y" is the LSI range key, but neither is a base-table key.
# Note: this behavior changed recently. Before https://github.com/scylladb/scylladb/pull/24991,
# LSI key columns (and before an even earlier change, also GSI key columns) were
# added as real schema columns. Now all non-base-table-key attributes, including
# GSI and LSI key attributes, are stored in ":attrs".
# The test checks the full list of columns of the base table, because Hermes
# refuses a table of another shape. It assumes, in A2: "At most one
# `partition_key` column (the hash key) and one `clustering` column (the
# range key)". In A3: "One `regular` column named `:attrs`, type exactly
# `map<text, blob>`, non-frozen." In A4: "No `static` columns." It takes any
# other regular column as "an index key attribute's own column", so a new
# table must have none.
def test_index_key_not_schema_column(dynamodb, cql, table_with_indexes):
    table = table_with_indexes
    ks = 'alternator_' + table.name
    # The base table has only its key columns and ":attrs". Thus neither "x"
    # (GSI key) nor "y" (LSI key) is a column.
    rows = cql.execute(
        "SELECT column_name, kind, type FROM system_schema.columns "
        f"WHERE keyspace_name = '{ks}' AND table_name = '{table.name}'"
    )
    assert {(r.column_name, r.kind, r.type) for r in rows} == {
        ('p', 'partition_key', 'text'),
        ('c', 'clustering', 'text'),
        (':attrs', 'regular', 'map<text, blob>'),
    }
    # Write an item with both "x" and "y" set, then confirm they are stored in ":attrs".
    p = random_string()
    c = random_string()
    table.put_item(Item={'p': p, 'c': c, 'x': 'hello', 'y': 'world'})
    rows = list(cql.execute(
        f'SELECT ":attrs" FROM "{ks}"."{table.name}" WHERE p = %s AND c = %s',
        [p, c]
    ))
    assert len(rows) == 1
    attrs = rows[0][0]
    # Both "x" and "y" should be stored in ":attrs" with S-type encoding
    # (type byte 0x00 followed by raw UTF-8 bytes).
    assert attrs['x'] == b'\x00' + b'hello'
    assert attrs['y'] == b'\x00' + b'world'

# The tests below write rows through CQL, with the statements that Hermes
# uses, and check that Alternator reads them as items.

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

# Write several items of one partition as a load of Hermes does: one
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

# Test that PutItem stores a number key in the bytes that its text gives, as
# test_attrs_encoding checks for a number in ":attrs". PutItem serializes a
# key with other code than ":attrs". Hermes writes these bytes for a number
# key. If PutItem wrote other bytes, then as A8 says, "Hermes's number keys
# would land in partitions Alternator does not look in". Each number starts
# with a random integer, so that no other test writes a key with the same
# bytes. Each number has its own hash key in test_table_sn, because a range
# key compares by value: 5.5 and 5.50 would be the same row.
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
    # As in test_attrs_encoding: a change of these bytes needs a plan for the
    # existing keys, and Hermes must follow it.
    for n in canonical:
        assert stored[n] == [_serialize_number(n)] * 2, n
    # SCYLLADB-1575 can change the bytes of these forms.
    for n in other:
        assert stored[n] == [_serialize_number(n)] * 2, n

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
