# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

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
#
# The order of entries in the alternator_type enum is critical: the numeric
# value of each type is written to disk, so it must not change.
#
# This file is related to issue #19770.

import json
from decimal import Decimal

import pytest

from .util import new_test_table, random_string

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

# Test that both hash keys and range keys of all three DynamoDB key types
# (S, B, N) are stored as the correct native CQL types. Specifically:
#   S (string) -> CQL text
#   B (binary) -> CQL blob
#   N (number) -> CQL decimal
# These type mappings are part of the on-disk format and must not change.
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
# changing it would break access to existing data.
# See also test_cql_schema.py::test_cql_keyspace_and_table which tests this
# in a different way.
def test_table_naming(cql, test_table_s):
    table_name = test_table_s.name
    ks = 'alternator_' + table_name
    # Verify we can query the underlying CQL table using the expected keyspace
    # and table name. The query will succeed only if naming is as expected.
    rows = list(cql.execute(f'SELECT p FROM "{ks}"."{table_name}"'))
    assert rows == []

# Test that GSI and LSI view names in the underlying CQL schema follow the
# expected naming convention:
#   GSI named "gsi1" -> CQL view "{table_name}:gsi1"
#   LSI named "lsi1" -> CQL view "{table_name}!:lsi1"
# These naming conventions must remain stable, as changing them would break
# access to existing data. See also test_cql_schema.py::test_alternator_aux_tables
# which tests related properties in a different way.
def test_index_naming(cql, table_with_indexes):
    table_name = table_with_indexes.name
    ks = 'alternator_' + table_name
    gsi_view = table_name + ':gsi1'
    lsi_view = table_name + '!:lsi1'
    # Check the view names in system_schema.views (the view names contain
    # special characters such as '!' and ':', so we look them up in the
    # system table rather than attempting to query the views directly).
    rows = list(cql.execute(
        "SELECT view_name FROM system_schema.views "
        f"WHERE keyspace_name = '{ks}' AND view_name = '{gsi_view}'"
    ))
    assert len(rows) == 1, f"Expected GSI view '{gsi_view}' not found in system_schema.views"
    rows = list(cql.execute(
        "SELECT view_name FROM system_schema.views "
        f"WHERE keyspace_name = '{ks}' AND view_name = '{lsi_view}'"
    ))
    assert len(rows) == 1, f"Expected LSI view '{lsi_view}' not found in system_schema.views"

# Test that GSI/LSI key attributes that are not base-table key attributes
# are NOT stored as separate CQL schema columns in the base table - they are
# stored in ":attrs" instead. In the table_with_indexes fixture, "x" is the
# GSI hash key and "y" is the LSI range key, but neither is a base-table key.
# Note: this behavior changed recently. Before https://github.com/scylladb/scylladb/pull/24991,
# LSI key columns (and before an even earlier change, also GSI key columns) were
# added as real schema columns. Now all non-base-table-key attributes, including
# GSI and LSI key attributes, are stored in ":attrs".
def test_index_key_not_schema_column(dynamodb, cql, table_with_indexes):
    table = table_with_indexes
    ks = 'alternator_' + table.name
    # Neither "x" (GSI key) nor "y" (LSI key) must appear as columns in the
    # base table's CQL schema.
    for attr in ('x', 'y'):
        rows = list(cql.execute(
            "SELECT column_name FROM system_schema.columns "
            f"WHERE keyspace_name = '{ks}' AND table_name = '{table.name}' "
            f"AND column_name = '{attr}'"
        ))
        assert rows == [], f"Attribute '{attr}' (GSI/LSI key) should not be a schema column, but found: {rows}"
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
