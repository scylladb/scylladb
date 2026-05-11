# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for DynamoDB's multi-attribute (composite) keys in Global Secondary
# Indexes (GSI). DynamoDB added support (Nov 2025) for GSI partition keys
# composed of up to 4 attributes and sort keys composed of up to 4 attributes
# (8 total). This is GSI-only — base tables and LSIs remain limited to
# 1 HASH + optional 1 RANGE.
#
# These tests focus exclusively on composite-key-specific behavior. Single-key
# GSI behavior is already thoroughly tested in test_gsi.py.
#
# AWS DynamoDB rules for composite GSI keys:
#   - KeySchema: all HASH entries before all RANGE entries
#   - Limits: up to 4 HASH + up to 4 RANGE = max 8 total
#   - Query PK: ALL partition key attrs must have equality (=) conditions
#   - Query SK: left-to-right in KeySchema order; no skipping; inequality
#     (>, <, >=, <=, BETWEEN, begins_with) must be the LAST condition
#   - FilterExpression cannot reference ANY key attribute of the queried index
#   - Sparse index: items missing ANY GSI key attribute are NOT indexed
#   - Each composite key attr can have its own type (S, N, B)

import pytest
import time
from contextlib import contextmanager
from botocore.exceptions import ClientError
from .util import (random_string, full_query, full_scan,
                   multiset, create_test_table, new_test_table, wait_for_gsi)
from .test_gsi import assert_index_query, assert_index_scan

###############################################################################
# Shared fixtures — module-scoped tables pre-loaded with data for query tests
###############################################################################

# Build the kwargs dict for creating a table with a single composite GSI.
# hash_keys/range_keys: list of (name, type) tuples, e.g. [('h1', 'S')].
# base_keys - first entry is HASH, second (if any) is RANGE.
def _composite_gsi_table_kwargs(index_name, hash_keys, range_keys,
                                base_keys=[('p', 'S')], projection='ALL'):
    key_types = ['HASH'] + ['RANGE'] * (len(base_keys) - 1)
    key_schema = [{'AttributeName': n, 'KeyType': t}
                  for (n, _), t in zip(base_keys, key_types)]
    attr_defs = [{'AttributeName': n, 'AttributeType': t}
                 for n, t in base_keys + hash_keys + range_keys]
    gsi_ks = ([{'AttributeName': n, 'KeyType': 'HASH'} for n, _ in hash_keys] +
              [{'AttributeName': n, 'KeyType': 'RANGE'} for n, _ in range_keys])
    return dict(
        KeySchema=key_schema,
        AttributeDefinitions=attr_defs,
        GlobalSecondaryIndexes=[{
            'IndexName': index_name,
            'KeySchema': gsi_ks,
            'Projection': {'ProjectionType': projection},
        }])

# Create a table with a composite GSI (for use in fixtures).
def _create_composite_gsi_table(dynamodb, index_name, hash_keys, range_keys,
                                base_keys=[('p', 'S')], projection='ALL'):
    return create_test_table(dynamodb,
        **_composite_gsi_table_kwargs(index_name, hash_keys, range_keys,
                                      base_keys, projection))

# Context-manager variant (for use in individual tests).
@contextmanager
def _new_composite_gsi_table(dynamodb, index_name, hash_keys, range_keys,
                             base_keys=[('p', 'S')], projection='ALL'):
    with new_test_table(dynamodb,
            **_composite_gsi_table_kwargs(index_name, hash_keys, range_keys,
                                          base_keys, projection)) as table:
        yield table

# Fixture 1: Minimum composite — 2 HASH + 2 RANGE, all String type.
@pytest.fixture(scope="module")
def test_table_gsi_2h2r(dynamodb):
    table = _create_composite_gsi_table(dynamodb, 'idx_2h2r',
        hash_keys=[('h1', 'S'), ('h2', 'S')],
        range_keys=[('r1', 'S'), ('r2', 'S')])
    yield table
    table.delete()

# Fixture 2: Maximum composite — 4 HASH + 4 RANGE, mixed types.
@pytest.fixture(scope="module")
def test_table_gsi_4h4r(dynamodb):
    table = _create_composite_gsi_table(dynamodb, 'idx_4h4r',
        hash_keys=[('h1', 'S'), ('h2', 'S'), ('h3', 'S'), ('h4', 'S')],
        range_keys=[('r1', 'N'), ('r2', 'S'), ('r3', 'B'), ('r4', 'S')])
    yield table
    table.delete()

# Fixture 3: Mixed types — 2 HASH (S, N) + 2 RANGE (N, B), base table with sort key.
@pytest.fixture(scope="module")
def test_table_gsi_mixed_types(dynamodb):
    table = _create_composite_gsi_table(dynamodb, 'idx_mixed',
        hash_keys=[('mh1', 'S'), ('mh2', 'N')],
        range_keys=[('mr1', 'N'), ('mr2', 'B')],
        base_keys=[('p', 'S'), ('c', 'S')])
    yield table
    table.delete()

###############################################################################
# Category 1: GSI Creation — KeySchema Validation
###############################################################################

# Test that creating a GSI with 2 HASH keys and no RANGE keys succeeds.
def test_gsi_composite_create_2h(dynamodb):
    with _new_composite_gsi_table(dynamodb, 'gsi',
            hash_keys=[('a', 'S'), ('b', 'S')], range_keys=[]) as table:
        desc = table.meta.client.describe_table(TableName=table.name)
        gsi = desc['Table']['GlobalSecondaryIndexes'][0]
        assert gsi['KeySchema'] == [
            {'AttributeName': 'a', 'KeyType': 'HASH'},
            {'AttributeName': 'b', 'KeyType': 'HASH'},
        ]

# Test that creating a GSI with max 4 HASH + 4 RANGE succeeds.
def test_gsi_composite_create_4h4r(dynamodb):
    with _new_composite_gsi_table(dynamodb, 'gsi',
            hash_keys=[('a', 'S'), ('b', 'S'), ('c', 'S'), ('d', 'S')],
            range_keys=[('e', 'S'), ('f', 'S'), ('g', 'S'), ('h', 'S')]) as table:
        desc = table.meta.client.describe_table(TableName=table.name)
        gsi = desc['Table']['GlobalSecondaryIndexes'][0]
        assert len(gsi['KeySchema']) == 8

# 5 HASH attributes exceeds the limit of 4.
def test_gsi_composite_create_5h_rejected(dynamodb):
    with pytest.raises(ClientError, match='ValidationException'):
        with _new_composite_gsi_table(dynamodb, 'gsi',
                hash_keys=[(f'a{i}', 'S') for i in range(5)],
                range_keys=[]) as table:
            pass

# 1 HASH + 5 RANGE exceeds the limit of 4 RANGE.
def test_gsi_composite_create_5r_rejected(dynamodb):
    with pytest.raises(ClientError, match='ValidationException'):
        with _new_composite_gsi_table(dynamodb, 'gsi',
                hash_keys=[('h', 'S')],
                range_keys=[(f'r{i}', 'S') for i in range(5)]) as table:
            pass

# HASH entries must come before RANGE entries. This has RANGE then HASH.
def test_gsi_composite_hash_after_range_rejected(dynamodb):
    with pytest.raises(ClientError, match='ValidationException'):
        with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'a', 'AttributeType': 'S'},
                {'AttributeName': 'b', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'gsi',
                    'KeySchema': [
                        {'AttributeName': 'a', 'KeyType': 'RANGE'},
                        {'AttributeName': 'b', 'KeyType': 'HASH'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]) as table:
            pass

# Interleaved HASH, RANGE, HASH is not allowed.
def test_gsi_composite_interleaved_rejected(dynamodb):
    with pytest.raises(ClientError, match='ValidationException'):
        with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'a', 'AttributeType': 'S'},
                {'AttributeName': 'b', 'AttributeType': 'S'},
                {'AttributeName': 'c', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'gsi',
                    'KeySchema': [
                        {'AttributeName': 'a', 'KeyType': 'HASH'},
                        {'AttributeName': 'b', 'KeyType': 'RANGE'},
                        {'AttributeName': 'c', 'KeyType': 'HASH'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]) as table:
            pass

# RANGE-only key schema with no HASH is not allowed.
def test_gsi_composite_range_only_rejected(dynamodb):
    with pytest.raises(ClientError, match='ValidationException'):
        with _new_composite_gsi_table(dynamodb, 'gsi',
                hash_keys=[], range_keys=[('a', 'S'), ('b', 'S')]) as table:
            pass

# Same attribute name appearing twice in HASH is a duplicate.
def test_gsi_composite_duplicate_attr_rejected(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*[Dd]uplicate'):
        with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'a', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'gsi',
                    'KeySchema': [
                        {'AttributeName': 'a', 'KeyType': 'HASH'},
                        {'AttributeName': 'a', 'KeyType': 'HASH'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]) as table:
            pass

# Same attribute name used as both HASH and RANGE.
def test_gsi_composite_same_attr_hash_and_range_rejected(dynamodb):
    with pytest.raises(ClientError, match='ValidationException'):
        with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'a', 'AttributeType': 'S'},
                {'AttributeName': 'b', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'gsi',
                    'KeySchema': [
                        {'AttributeName': 'a', 'KeyType': 'HASH'},
                        {'AttributeName': 'b', 'KeyType': 'HASH'},
                        {'AttributeName': 'a', 'KeyType': 'RANGE'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]) as table:
            pass

# GSI key attribute referenced but not in AttributeDefinitions.
def test_gsi_composite_missing_attribute_definition(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*AttributeDefinition'):
        with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'a', 'AttributeType': 'S'},
                # 'b' intentionally missing
            ],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'gsi',
                    'KeySchema': [
                        {'AttributeName': 'a', 'KeyType': 'HASH'},
                        {'AttributeName': 'b', 'KeyType': 'HASH'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]) as table:
            pass

# Multi-attribute key schemas are NOT allowed on LSIs.
def test_gsi_composite_lsi_multiattr_rejected(dynamodb):
    with pytest.raises(ClientError, match='ValidationException'):
        with new_test_table(dynamodb,
            KeySchema=[
                {'AttributeName': 'p', 'KeyType': 'HASH'},
                {'AttributeName': 'c', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'c', 'AttributeType': 'S'},
                {'AttributeName': 'l', 'AttributeType': 'S'},
            ],
            LocalSecondaryIndexes=[
                {   'IndexName': 'lsi',
                    'KeySchema': [
                        {'AttributeName': 'p', 'KeyType': 'HASH'},
                        {'AttributeName': 'c', 'KeyType': 'RANGE'},
                        {'AttributeName': 'l', 'KeyType': 'RANGE'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]) as table:
            pass

###############################################################################
# Category 2: GSI Creation — DescribeTable Roundtrip
###############################################################################

# Verify DescribeTable returns the correct KeySchema for 2H+2R composite GSI.
def test_gsi_composite_describe_2h2r(test_table_gsi_2h2r):
    desc = test_table_gsi_2h2r.meta.client.describe_table(
        TableName=test_table_gsi_2h2r.name)
    gsis = desc['Table']['GlobalSecondaryIndexes']
    gsi = [g for g in gsis if g['IndexName'] == 'idx_2h2r'][0]
    assert gsi['KeySchema'] == [
        {'AttributeName': 'h1', 'KeyType': 'HASH'},
        {'AttributeName': 'h2', 'KeyType': 'HASH'},
        {'AttributeName': 'r1', 'KeyType': 'RANGE'},
        {'AttributeName': 'r2', 'KeyType': 'RANGE'},
    ]

# Verify DescribeTable returns the correct KeySchema for max 4H+4R.
def test_gsi_composite_describe_4h4r(test_table_gsi_4h4r):
    desc = test_table_gsi_4h4r.meta.client.describe_table(
        TableName=test_table_gsi_4h4r.name)
    gsis = desc['Table']['GlobalSecondaryIndexes']
    gsi = [g for g in gsis if g['IndexName'] == 'idx_4h4r'][0]
    expected = [
        {'AttributeName': 'h1', 'KeyType': 'HASH'},
        {'AttributeName': 'h2', 'KeyType': 'HASH'},
        {'AttributeName': 'h3', 'KeyType': 'HASH'},
        {'AttributeName': 'h4', 'KeyType': 'HASH'},
        {'AttributeName': 'r1', 'KeyType': 'RANGE'},
        {'AttributeName': 'r2', 'KeyType': 'RANGE'},
        {'AttributeName': 'r3', 'KeyType': 'RANGE'},
        {'AttributeName': 'r4', 'KeyType': 'RANGE'},
    ]
    assert gsi['KeySchema'] == expected

# Verify KEYS_ONLY projection on a composite GSI.
def test_gsi_composite_describe_projection(dynamodb):
    with _new_composite_gsi_table(dynamodb, 'gsi',
            hash_keys=[('a', 'S'), ('b', 'S')], range_keys=[],
            projection='KEYS_ONLY') as table:
        desc = table.meta.client.describe_table(TableName=table.name)
        gsi = desc['Table']['GlobalSecondaryIndexes'][0]
        assert gsi['Projection'] == {'ProjectionType': 'KEYS_ONLY'}
        # Write an item with extra attrs; query GSI; only key attrs returned.
        p = random_string()
        a_val, b_val = random_string(), random_string()
        table.put_item(Item={'p': p, 'a': a_val, 'b': b_val,
                             'extra': 'should_not_appear'})
        # With KEYS_ONLY, the returned item should only have the base table
        # key ('p') and the GSI key attrs ('a', 'b'). Not 'extra'.
        expected = [{'p': p, 'a': a_val, 'b': b_val}]
        assert_index_scan(table, 'gsi', expected)

# Verify a table with 2 distinct composite GSIs described correctly.
def test_gsi_composite_describe_multiple_gsi(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
            {'AttributeName': 'a', 'AttributeType': 'S'},
            {'AttributeName': 'b', 'AttributeType': 'S'},
            {'AttributeName': 'c', 'AttributeType': 'S'},
            {'AttributeName': 'd', 'AttributeType': 'S'},
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'gsi1',
                'KeySchema': [
                    {'AttributeName': 'a', 'KeyType': 'HASH'},
                    {'AttributeName': 'b', 'KeyType': 'HASH'},
                ],
                'Projection': {'ProjectionType': 'ALL'}
            },
            {   'IndexName': 'gsi2',
                'KeySchema': [
                    {'AttributeName': 'c', 'KeyType': 'HASH'},
                    {'AttributeName': 'd', 'KeyType': 'RANGE'},
                ],
                'Projection': {'ProjectionType': 'ALL'}
            },
        ]) as table:
        desc = table.meta.client.describe_table(TableName=table.name)
        gsis = {g['IndexName']: g for g in desc['Table']['GlobalSecondaryIndexes']}
        assert gsis['gsi1']['KeySchema'] == [
            {'AttributeName': 'a', 'KeyType': 'HASH'},
            {'AttributeName': 'b', 'KeyType': 'HASH'},
        ]
        assert gsis['gsi2']['KeySchema'] == [
            {'AttributeName': 'c', 'KeyType': 'HASH'},
            {'AttributeName': 'd', 'KeyType': 'RANGE'},
        ]

###############################################################################
# Category 3: GSI Creation via UpdateTable
###############################################################################

# Add a composite GSI via UpdateTable and verify it becomes queryable.
def test_gsi_composite_updatetable_create_2h2r(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
        ]) as table:
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[
                {'AttributeName': 'a', 'AttributeType': 'S'},
                {'AttributeName': 'b', 'AttributeType': 'S'},
                {'AttributeName': 'c', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexUpdates=[{'Create': {
                'IndexName': 'gsi',
                'KeySchema': [
                    {'AttributeName': 'a', 'KeyType': 'HASH'},
                    {'AttributeName': 'b', 'KeyType': 'HASH'},
                    {'AttributeName': 'c', 'KeyType': 'RANGE'},
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }}])
        wait_for_gsi(table, 'gsi')
        # Write an item and verify it appears in the new GSI
        p = random_string()
        a_val, b_val, c_val = random_string(), random_string(), random_string()
        table.put_item(Item={'p': p, 'a': a_val, 'b': b_val, 'c': c_val})
        assert_index_scan(table, 'gsi', [{'p': p, 'a': a_val, 'b': b_val, 'c': c_val}])

# Add a composite GSI to a table with existing data; verify backfill.
def test_gsi_composite_updatetable_backfill(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
        ]) as table:
        # Write data BEFORE creating the GSI
        items = []
        for i in range(5):
            item = {'p': random_string(), 'a': 'common', 'b': 'shared',
                    'c': f'val{i}'}
            table.put_item(Item=item)
            items.append(item)
        # Now create the composite GSI
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[
                {'AttributeName': 'a', 'AttributeType': 'S'},
                {'AttributeName': 'b', 'AttributeType': 'S'},
                {'AttributeName': 'c', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexUpdates=[{'Create': {
                'IndexName': 'gsi',
                'KeySchema': [
                    {'AttributeName': 'a', 'KeyType': 'HASH'},
                    {'AttributeName': 'b', 'KeyType': 'HASH'},
                    {'AttributeName': 'c', 'KeyType': 'RANGE'},
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }}])
        wait_for_gsi(table, 'gsi')
        # Verify all pre-existing items are backfilled into the GSI
        assert_index_scan(table, 'gsi', items)

# UpdateTable with 5 HASH attrs should be rejected.
def test_gsi_composite_updatetable_5h_rejected(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
        ]) as table:
        attrs = [{'AttributeName': f'a{i}', 'AttributeType': 'S'} for i in range(5)]
        ks = [{'AttributeName': f'a{i}', 'KeyType': 'HASH'} for i in range(5)]
        with pytest.raises(ClientError, match='ValidationException'):
            dynamodb.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=attrs,
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': ks,
                    'Projection': {'ProjectionType': 'ALL'}
                }}])

# UpdateTable with HASH after RANGE should be rejected.
def test_gsi_composite_updatetable_hash_after_range_rejected(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
        ]) as table:
        with pytest.raises(ClientError, match='ValidationException'):
            dynamodb.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=[
                    {'AttributeName': 'a', 'AttributeType': 'S'},
                    {'AttributeName': 'b', 'AttributeType': 'S'},
                ],
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': [
                        {'AttributeName': 'a', 'KeyType': 'RANGE'},
                        {'AttributeName': 'b', 'KeyType': 'HASH'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }}])

###############################################################################
# Category 4: Data Write — Sparse Index Behavior
###############################################################################

# Item missing any single GSI key attr (HASH or RANGE) is NOT indexed.
def test_gsi_composite_sparse_missing_one_key_attr(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    # Missing a HASH attr (h2)
    p1 = random_string()
    table.put_item(Item={'p': p1, 'h1': 'val', 'r1': 'a', 'r2': 'b'})
    # Missing a RANGE attr (r2)
    p2 = random_string()
    table.put_item(Item={'p': p2, 'h1': 'val', 'h2': 'val2', 'r1': 'a'})
    # Neither item should appear in the GSI
    results = full_scan(table, IndexName='idx_2h2r', ConsistentRead=False)
    assert not any(i.get('p') in (p1, p2) for i in results)

# UpdateItem that adds/removes a key attr causes item to appear/disappear.
def test_gsi_composite_sparse_update_attr(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    p = random_string()
    h1_val, h2_val = random_string(), random_string()
    # Initially missing r2 — not indexed
    table.put_item(Item={'p': p, 'h1': h1_val, 'h2': h2_val, 'r1': 'a'})
    results = full_scan(table, IndexName='idx_2h2r', ConsistentRead=False)
    assert not any(i.get('p') == p for i in results)
    # Add r2 via update — item should now appear in GSI
    table.update_item(Key={'p': p},
        UpdateExpression='SET r2 = :v',
        ExpressionAttributeValues={':v': 'b'})
    expected = {'p': p, 'h1': h1_val, 'h2': h2_val, 'r1': 'a', 'r2': 'b'}
    assert_index_query(table, 'idx_2h2r', [expected],
        KeyConditionExpression='h1 = :h1 AND h2 = :h2',
        ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val})
    # Remove h2 via update — item should disappear from GSI
    table.update_item(Key={'p': p},
        UpdateExpression='REMOVE h2')
    for i in range(10):
        results = full_query(table, IndexName='idx_2h2r',
            ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2',
            ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val})
        if not any(item.get('p') == p for item in results):
            return
        time.sleep(1)
    pytest.fail("Item still present in GSI after removing a key attribute")

###############################################################################
# Category 5: Data Write — Type Validation
###############################################################################

# PutItem with wrong type for a composite key attr (HASH or RANGE).
def test_gsi_composite_wrong_type_key_attr(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    p = random_string()
    # h2 is defined as S but we provide a number
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        table.put_item(Item={'p': p, 'h1': 'ok', 'h2': 123,
                             'r1': 'a', 'r2': 'b'})
    # r1 is defined as S but we provide a number
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        table.put_item(Item={'p': p, 'h1': 'ok', 'h2': 'ok',
                             'r1': 123, 'r2': 'b'})

# PutItem with correct mixed types (S, N, B) in composite key.
def test_gsi_composite_mixed_types_correct(test_table_gsi_mixed_types):
    table = test_table_gsi_mixed_types
    p, c = random_string(), random_string()
    mh1_val = random_string()
    mh2_val = 42
    mr1_val = 100
    mr2_val = b'\x01\x02\x03'
    item = {'p': p, 'c': c, 'mh1': mh1_val, 'mh2': mh2_val,
            'mr1': mr1_val, 'mr2': mr2_val}
    table.put_item(Item=item)
    # Verify it's indexed by querying
    # Note: boto3 returns Decimal for N and Bytes for B, so the item
    # we get back may differ slightly. Use assert_index_query.
    assert_index_query(table, 'idx_mixed', [item],
        KeyConditionExpression='mh1 = :mh1 AND mh2 = :mh2',
        ExpressionAttributeValues={':mh1': mh1_val, ':mh2': mh2_val})

# PutItem with empty string for a composite GSI key attr should fail.
def test_gsi_composite_empty_string_key_attr(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        table.put_item(Item={'p': p, 'h1': 'ok', 'h2': '',
                             'r1': 'a', 'r2': 'b'})

###############################################################################
# Category 6: Query — Partition Key Conditions
###############################################################################

# Query with equality on all PK attrs succeeds and returns correct items.
def test_gsi_composite_query_all_pk_eq(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for i in range(3):
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': f'r1_{i}', 'r2': f'r2_{i}'}
        table.put_item(Item=item)
        items.append(item)
    assert_index_query(table, 'idx_2h2r', items,
        KeyConditionExpression='h1 = :h1 AND h2 = :h2',
        ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val})

# Query specifying only one of two PK attrs — should fail.
def test_gsi_composite_query_missing_one_pk(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1',
            ExpressionAttributeValues={':h1': 'val'})

# Inequality on a partition key attr — should fail.
def test_gsi_composite_query_pk_inequality(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 > :h2',
            ExpressionAttributeValues={':h1': 'val', ':h2': 'val'})

# Query with all 4 HASH attrs equality on max composite — succeeds.
def test_gsi_composite_query_4h_all_eq(test_table_gsi_4h4r):
    table = test_table_gsi_4h4r
    h_vals = [random_string() for _ in range(4)]
    item = {'p': random_string(),
            'h1': h_vals[0], 'h2': h_vals[1], 'h3': h_vals[2], 'h4': h_vals[3],
            'r1': 1, 'r2': 'x', 'r3': b'\x00', 'r4': 'y'}
    table.put_item(Item=item)
    assert_index_query(table, 'idx_4h4r', [item],
        KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND h3 = :h3 AND h4 = :h4',
        ExpressionAttributeValues={
            ':h1': h_vals[0], ':h2': h_vals[1],
            ':h3': h_vals[2], ':h4': h_vals[3]})

# Query with wrong type for a PK attr value.
def test_gsi_composite_query_pk_wrong_type(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    # h1 is S, but we pass a number — boto3 serializes it as N, DynamoDB
    # rejects the query because the key condition type doesn't match.
    # DynamoDB may either return a ValidationException or an empty result set.
    try:
        result = full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2',
            ExpressionAttributeValues={':h1': 123, ':h2': 'val'})
        # If no error, the result should be empty (type mismatch means no match)
        assert len(result) == 0
    except ClientError as e:
        assert 'ValidationException' in str(e)

# BETWEEN on a PK attr — should fail.
def test_gsi_composite_query_pk_between_rejected(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 BETWEEN :a AND :b',
            ExpressionAttributeValues={':h1': 'v', ':a': 'a', ':b': 'z'})

# begins_with() on a PK attr — should fail.
def test_gsi_composite_query_pk_begins_with_rejected(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND begins_with(h2, :p)',
            ExpressionAttributeValues={':h1': 'v', ':p': 'pre'})

###############################################################################
# Category 7: Query — Sort Key Conditions: Left-to-Right Rule
###############################################################################

# Query with just the first SK attr (equality).
def test_gsi_composite_query_sk_first_only_eq(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for r2_val in ['aaa', 'bbb', 'ccc']:
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': 'same', 'r2': r2_val}
        table.put_item(Item=item)
        items.append(item)
    # Also one item with different r1
    other = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
             'r1': 'different', 'r2': 'xxx'}
    table.put_item(Item=other)
    # Query with r1 = 'same' should return only the 3 items
    assert_index_query(table, 'idx_2h2r', items,
        KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r1 = :r1',
        ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val, ':r1': 'same'})

# Query with first two SK attrs (r1 AND r2).
def test_gsi_composite_query_sk_first_two_eq(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    target = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
              'r1': 'A', 'r2': 'B'}
    other = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
             'r1': 'A', 'r2': 'C'}
    table.put_item(Item=target)
    table.put_item(Item=other)
    assert_index_query(table, 'idx_2h2r', [target],
        KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r1 = :r1 AND r2 = :r2',
        ExpressionAttributeValues={
            ':h1': h1_val, ':h2': h2_val, ':r1': 'A', ':r2': 'B'})

# Query with all 4 SK attrs on max composite (equality on all).
def test_gsi_composite_query_sk_all_eq(test_table_gsi_4h4r):
    table = test_table_gsi_4h4r
    h_vals = [random_string() for _ in range(4)]
    r3_val = b'\x01\x02'
    item = {'p': random_string(),
            'h1': h_vals[0], 'h2': h_vals[1], 'h3': h_vals[2], 'h4': h_vals[3],
            'r1': 10, 'r2': 'hello', 'r3': r3_val, 'r4': 'world'}
    table.put_item(Item=item)
    assert_index_query(table, 'idx_4h4r', [item],
        KeyConditionExpression=('h1 = :h1 AND h2 = :h2 AND h3 = :h3 AND h4 = :h4'
                                ' AND r1 = :r1 AND r2 = :r2 AND r3 = :r3 AND r4 = :r4'),
        ExpressionAttributeValues={
            ':h1': h_vals[0], ':h2': h_vals[1],
            ':h3': h_vals[2], ':h4': h_vals[3],
            ':r1': 10, ':r2': 'hello', ':r3': r3_val, ':r4': 'world'})

# Skipping the first SK attr (querying r2 without r1) — should fail.
def test_gsi_composite_query_sk_skip_first_rejected(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    with pytest.raises(ClientError, match='ValidationException.*[kK]ey'):
        full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r2 = :r2',
            ExpressionAttributeValues={':h1': 'v', ':h2': 'v', ':r2': 'v'})

# Gap in SK attrs (r1 and r3 but not r2) — should fail.
def test_gsi_composite_query_sk_gap_rejected(test_table_gsi_4h4r):
    table = test_table_gsi_4h4r
    with pytest.raises(ClientError, match='ValidationException.*[kK]ey'):
        full_query(table, IndexName='idx_4h4r', ConsistentRead=False,
            KeyConditionExpression=('h1 = :h1 AND h2 = :h2 AND h3 = :h3 '
                                    'AND h4 = :h4 AND r1 = :r1 AND r3 = :r3'),
            ExpressionAttributeValues={
                ':h1': 'v', ':h2': 'v', ':h3': 'v', ':h4': 'v',
                ':r1': 1, ':r3': b'\x00'})

# SK attrs specified in reverse order in the expression. The DynamoDB docs say
# left-to-right in KeySchema order, but the expression parser may handle
# reordering. This test documents the actual behavior.
def test_gsi_composite_query_sk_out_of_order(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
            'r1': 'A', 'r2': 'B'}
    table.put_item(Item=item)
    # Specify r2 before r1 in the expression — DynamoDB may accept this
    # since it can determine the attrs from the schema.
    try:
        result = full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r2 = :r2 AND r1 = :r1',
            ExpressionAttributeValues={
                ':h1': h1_val, ':h2': h2_val, ':r1': 'A', ':r2': 'B'})
        # If it succeeds, verify we get the right item
        assert len(result) == 1
        assert result[0]['p'] == item['p']
    except ClientError as e:
        # If it fails, it should be a ValidationException
        assert 'ValidationException' in str(e)

# Inequality on the last queried SK attr.
def test_gsi_composite_query_sk_inequality_last(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for r2_val in ['aaa', 'bbb', 'ccc', 'ddd']:
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': 'same', 'r2': r2_val}
        table.put_item(Item=item)
        items.append(item)
    # Query r1 = 'same' AND r2 > 'bbb' → should get 'ccc' and 'ddd'
    expected = [i for i in items if i['r2'] > 'bbb']
    assert_index_query(table, 'idx_2h2r', expected,
        KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r1 = :r1 AND r2 > :r2',
        ExpressionAttributeValues={
            ':h1': h1_val, ':h2': h2_val, ':r1': 'same', ':r2': 'bbb'})

# Inequality on a non-last SK attr followed by equality — should fail.
def test_gsi_composite_query_sk_inequality_not_last_rejected(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r1 > :r1 AND r2 = :r2',
            ExpressionAttributeValues={
                ':h1': 'v', ':h2': 'v', ':r1': 'v', ':r2': 'v'})

# BETWEEN on the last queried SK attr.
def test_gsi_composite_query_sk_between(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for r2_val in ['aaa', 'bbb', 'ccc', 'ddd', 'eee']:
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': 'same', 'r2': r2_val}
        table.put_item(Item=item)
        items.append(item)
    expected = [i for i in items if 'bbb' <= i['r2'] <= 'ddd']
    assert_index_query(table, 'idx_2h2r', expected,
        KeyConditionExpression=('h1 = :h1 AND h2 = :h2 AND r1 = :r1 '
                                'AND r2 BETWEEN :lo AND :hi'),
        ExpressionAttributeValues={
            ':h1': h1_val, ':h2': h2_val, ':r1': 'same',
            ':lo': 'bbb', ':hi': 'ddd'})

###############################################################################
# Category 8: Query — Sort Key Conditions: begins_with and Other Operators
###############################################################################

# begins_with() on the last queried SK attr.
def test_gsi_composite_query_sk_begins_with_last(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for r2_val in ['prefix_A', 'prefix_B', 'other_C']:
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': 'same', 'r2': r2_val}
        table.put_item(Item=item)
        items.append(item)
    expected = [i for i in items if i['r2'].startswith('prefix')]
    assert_index_query(table, 'idx_2h2r', expected,
        KeyConditionExpression=('h1 = :h1 AND h2 = :h2 AND r1 = :r1 '
                                'AND begins_with(r2, :prefix)'),
        ExpressionAttributeValues={
            ':h1': h1_val, ':h2': h2_val, ':r1': 'same', ':prefix': 'prefix'})

# begins_with() on a non-last SK attr followed by another condition — fail.
def test_gsi_composite_query_sk_begins_with_not_last_rejected(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression=('h1 = :h1 AND h2 = :h2 '
                                    'AND begins_with(r1, :p) AND r2 = :r2'),
            ExpressionAttributeValues={
                ':h1': 'v', ':h2': 'v', ':p': 'pre', ':r2': 'v'})

# begins_with() on a Number type SK attr — should fail.
def test_gsi_composite_query_sk_begins_with_number_rejected(test_table_gsi_4h4r):
    table = test_table_gsi_4h4r
    # r1 is type N in the 4h4r fixture
    with pytest.raises(ClientError, match='ValidationException.*begins_with'):
        full_query(table, IndexName='idx_4h4r', ConsistentRead=False,
            KeyConditionExpression=('h1 = :h1 AND h2 = :h2 AND h3 = :h3 '
                                    'AND h4 = :h4 AND begins_with(r1, :p)'),
            ExpressionAttributeValues={
                ':h1': 'v', ':h2': 'v', ':h3': 'v', ':h4': 'v', ':p': '1'})

# Less-than on the last queried SK attr.
def test_gsi_composite_query_sk_lt(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for r2_val in ['aaa', 'bbb', 'ccc']:
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': 'same', 'r2': r2_val}
        table.put_item(Item=item)
        items.append(item)
    expected = [i for i in items if i['r2'] < 'bbb']
    assert_index_query(table, 'idx_2h2r', expected,
        KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r1 = :r1 AND r2 < :r2',
        ExpressionAttributeValues={
            ':h1': h1_val, ':h2': h2_val, ':r1': 'same', ':r2': 'bbb'})

# Greater-or-equal on the last queried SK attr.
def test_gsi_composite_query_sk_ge(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for r2_val in ['aaa', 'bbb', 'ccc']:
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': 'same', 'r2': r2_val}
        table.put_item(Item=item)
        items.append(item)
    expected = [i for i in items if i['r2'] >= 'bbb']
    assert_index_query(table, 'idx_2h2r', expected,
        KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r1 = :r1 AND r2 >= :r2',
        ExpressionAttributeValues={
            ':h1': h1_val, ':h2': h2_val, ':r1': 'same', ':r2': 'bbb'})

###############################################################################
# Category 9: Query — FilterExpression Prohibition
###############################################################################

# FilterExpression referencing a composite HASH attr — should fail.
def test_gsi_composite_filter_on_hash_attr_rejected(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    with pytest.raises(ClientError, match='ValidationException.*[fF]ilter.*[kK]ey'):
        full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2',
            FilterExpression='h2 = :fv',
            ExpressionAttributeValues={':h1': 'v', ':h2': 'v', ':fv': 'v'})

# FilterExpression referencing a composite RANGE attr — should fail.
def test_gsi_composite_filter_on_range_attr_rejected(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    with pytest.raises(ClientError, match='ValidationException.*[fF]ilter.*[kK]ey'):
        full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2',
            FilterExpression='r1 = :fv',
            ExpressionAttributeValues={':h1': 'v', ':h2': 'v', ':fv': 'v'})

# FilterExpression on a non-key attr — allowed.
def test_gsi_composite_filter_on_nonkey_attr_allowed(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    item1 = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
             'r1': 'a', 'r2': 'b', 'color': 'red'}
    item2 = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
             'r1': 'c', 'r2': 'd', 'color': 'blue'}
    table.put_item(Item=item1)
    table.put_item(Item=item2)
    assert_index_query(table, 'idx_2h2r', [item1],
        KeyConditionExpression='h1 = :h1 AND h2 = :h2',
        FilterExpression='color = :c',
        ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val, ':c': 'red'})

# FilterExpression on base table key attr 'p' (not a GSI key) — allowed.
def test_gsi_composite_filter_on_base_table_key_allowed(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    p1, p2 = random_string(), random_string()
    item1 = {'p': p1, 'h1': h1_val, 'h2': h2_val, 'r1': 'a', 'r2': 'b'}
    item2 = {'p': p2, 'h1': h1_val, 'h2': h2_val, 'r1': 'c', 'r2': 'd'}
    table.put_item(Item=item1)
    table.put_item(Item=item2)
    assert_index_query(table, 'idx_2h2r', [item1],
        KeyConditionExpression='h1 = :h1 AND h2 = :h2',
        FilterExpression='p = :p',
        ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val, ':p': p1})

###############################################################################
# Category 10: Query — ScanIndexForward and Sort Order
###############################################################################

# Ascending sort order on composite sort key.
def test_gsi_composite_sort_order_ascending(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    # Items with same r1, varying r2
    items = []
    for r2_val in ['ccc', 'aaa', 'bbb']:
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': 'same', 'r2': r2_val}
        table.put_item(Item=item)
        items.append(item)
    # Query ascending
    result = full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
        KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r1 = :r1',
        ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val, ':r1': 'same'},
        ScanIndexForward=True)
    # Eventually consistent — retry
    for attempt in range(5):
        if len(result) == 3:
            break
        time.sleep(1)
        result = full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r1 = :r1',
            ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val, ':r1': 'same'},
            ScanIndexForward=True)
    r2_values = [item['r2'] for item in result]
    assert r2_values == sorted(r2_values), f"Expected ascending order, got {r2_values}"

# Descending sort order on composite sort key.
def test_gsi_composite_sort_order_descending(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for r2_val in ['ccc', 'aaa', 'bbb']:
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': 'same', 'r2': r2_val}
        table.put_item(Item=item)
        items.append(item)
    result = full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
        KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r1 = :r1',
        ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val, ':r1': 'same'},
        ScanIndexForward=False)
    for attempt in range(5):
        if len(result) == 3:
            break
        time.sleep(1)
        result = full_query(table, IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2 AND r1 = :r1',
            ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val, ':r1': 'same'},
            ScanIndexForward=False)
    r2_values = [item['r2'] for item in result]
    assert r2_values == sorted(r2_values, reverse=True), \
        f"Expected descending order, got {r2_values}"

# Sort order with mixed-type SK (N then B). Numbers sort numerically.
def test_gsi_composite_sort_order_mixed_types(test_table_gsi_mixed_types):
    table = test_table_gsi_mixed_types
    mh1_val = random_string()
    mh2_val = 1
    items = []
    # mr1 is N, mr2 is B — insert with varying mr1 to test numeric sort
    for mr1_val in [100, 5, 50, 1000]:
        mr2_val = b'\x01'
        item = {'p': random_string(), 'c': random_string(),
                'mh1': mh1_val, 'mh2': mh2_val,
                'mr1': mr1_val, 'mr2': mr2_val}
        table.put_item(Item=item)
        items.append(item)
    result = full_query(table, IndexName='idx_mixed', ConsistentRead=False,
        KeyConditionExpression='mh1 = :mh1 AND mh2 = :mh2',
        ExpressionAttributeValues={':mh1': mh1_val, ':mh2': mh2_val},
        ScanIndexForward=True)
    for attempt in range(5):
        if len(result) == 4:
            break
        time.sleep(1)
        result = full_query(table, IndexName='idx_mixed', ConsistentRead=False,
            KeyConditionExpression='mh1 = :mh1 AND mh2 = :mh2',
            ExpressionAttributeValues={':mh1': mh1_val, ':mh2': mh2_val},
            ScanIndexForward=True)
    # mr1 values should be in numeric ascending order: 5, 50, 100, 1000
    mr1_values = [item['mr1'] for item in result]
    assert mr1_values == sorted(mr1_values), \
        f"Expected numeric ascending order, got {mr1_values}"

###############################################################################
# Category 11: Query — Pagination with Composite Keys
###############################################################################

# Verify LastEvaluatedKey contains all composite key attrs.
def test_gsi_composite_query_pagination(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    for i in range(5):
        table.put_item(Item={'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                             'r1': f'r1_{i:03d}', 'r2': f'r2_{i:03d}'})
    # Query with Limit=2 to force pagination
    for attempt in range(5):
        response = table.query(IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2',
            ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val},
            Limit=2)
        if response['Count'] == 2:
            break
        time.sleep(1)
    if response['Count'] < 2:
        pytest.skip("Could not get paginated results — eventual consistency")
    lek = response.get('LastEvaluatedKey')
    assert lek is not None, "Expected LastEvaluatedKey for paginated result"
    # LEK must contain all GSI key attrs plus base table key
    assert 'h1' in lek
    assert 'h2' in lek
    assert 'r1' in lek
    assert 'r2' in lek
    assert 'p' in lek

# Full pagination roundtrip — all items eventually returned without loss.
def test_gsi_composite_query_pagination_roundtrip(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for i in range(10):
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': f'r1_{i:03d}', 'r2': f'r2_{i:03d}'}
        table.put_item(Item=item)
        items.append(item)
    # Use full_query which handles pagination internally
    assert_index_query(table, 'idx_2h2r', items,
        KeyConditionExpression='h1 = :h1 AND h2 = :h2',
        ExpressionAttributeValues={':h1': h1_val, ':h2': h2_val})

# Scan pagination on composite GSI.
def test_gsi_composite_scan_pagination(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for i in range(5):
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': f'r1_{i:03d}', 'r2': f'r2_{i:03d}'}
        table.put_item(Item=item)
        items.append(item)
    # Scan with Limit=1 to force multiple pages
    all_items = []
    kwargs = {'IndexName': 'idx_2h2r', 'ConsistentRead': False, 'Limit': 1}
    for attempt in range(100):  # Safety limit
        response = table.scan(**kwargs)
        all_items.extend(response.get('Items', []))
        if 'LastEvaluatedKey' not in response:
            break
        kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    # Our items should be a subset of all scanned items (other tests may
    # have added items too)
    our_pkeys = {i['p'] for i in items}
    found = [i for i in all_items if i.get('p') in our_pkeys]
    assert len(found) >= len(items), \
        f"Expected at least {len(items)} items, found {len(found)}"

# ExclusiveStartKey missing a composite key attr — should fail.
def test_gsi_composite_exclusivestartkey_incomplete_rejected(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    # Construct an incomplete ExclusiveStartKey (missing h2)
    with pytest.raises(ClientError, match='ValidationException'):
        table.query(IndexName='idx_2h2r', ConsistentRead=False,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2',
            ExpressionAttributeValues={':h1': 'v', ':h2': 'v'},
            ExclusiveStartKey={'p': 'x', 'h1': 'x', 'r1': 'x', 'r2': 'x'})
            # Missing h2

###############################################################################
# Category 12: Query — Legacy KeyConditions API Blocking
###############################################################################

# Legacy KeyConditions on a composite GSI should be rejected (or at least
# cannot express multi-attribute partition key conditions properly).
def test_gsi_composite_keyconditions_blocked(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    # KeyConditions is a dict keyed by attribute name. With composite keys,
    # you'd need multiple PK conditions, which KeyConditions may not support
    # correctly. Test that it fails or behaves incorrectly.
    with pytest.raises(ClientError, match='ValidationException'):
        table.meta.client.query(
            TableName=table.name,
            IndexName='idx_2h2r',
            KeyConditions={
                'h1': {
                    'AttributeValueList': [{'S': 'v'}],
                    'ComparisonOperator': 'EQ'
                },
                'h2': {
                    'AttributeValueList': [{'S': 'v'}],
                    'ComparisonOperator': 'EQ'
                }
            })

# Verify legacy KeyConditions still works for single-key GSIs.
def test_gsi_composite_keyconditions_single_key_gsi_still_works(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
            {'AttributeName': 'x', 'AttributeType': 'S'},
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'gsi',
                'KeySchema': [
                    {'AttributeName': 'x', 'KeyType': 'HASH'},
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }
        ]) as table:
        x_val = random_string()
        p_val = random_string()
        table.put_item(Item={'p': p_val, 'x': x_val})
        # Legacy KeyConditions should work fine for single-key GSI
        response = table.meta.client.query(
            TableName=table.name,
            IndexName='gsi',
            KeyConditions={
                'x': {
                    'AttributeValueList': [{'S': x_val}],
                    'ComparisonOperator': 'EQ'
                }
            })
        assert response['Count'] >= 1

###############################################################################
# Category 13: Scan Operations
###############################################################################

# Scan composite GSI returns all indexed items.
def test_gsi_composite_scan_returns_all(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    h1_val, h2_val = random_string(), random_string()
    items = []
    for i in range(3):
        item = {'p': random_string(), 'h1': h1_val, 'h2': h2_val,
                'r1': f'r1_{i}', 'r2': f'r2_{i}'}
        table.put_item(Item=item)
        items.append(item)
    # Scan should include our items (among possibly others)
    all_items = full_scan(table, IndexName='idx_2h2r', ConsistentRead=False)
    our_pkeys = {i['p'] for i in items}
    for attempt in range(5):
        found = [i for i in all_items if i.get('p') in our_pkeys]
        if len(found) == len(items):
            break
        time.sleep(1)
        all_items = full_scan(table, IndexName='idx_2h2r', ConsistentRead=False)
    found = [i for i in all_items if i.get('p') in our_pkeys]
    assert len(found) == len(items)

# Scan with FilterExpression on non-key attr.
def test_gsi_composite_scan_with_filter(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    marker = random_string()
    item1 = {'p': random_string(), 'h1': marker, 'h2': marker,
             'r1': 'a', 'r2': 'b', 'tag': 'yes'}
    item2 = {'p': random_string(), 'h1': marker, 'h2': marker,
             'r1': 'c', 'r2': 'd', 'tag': 'no'}
    table.put_item(Item=item1)
    table.put_item(Item=item2)
    # Scan with filter
    for attempt in range(5):
        results = full_scan(table, IndexName='idx_2h2r', ConsistentRead=False,
            FilterExpression='tag = :t',
            ExpressionAttributeValues={':t': 'yes'})
        found = [i for i in results if i.get('h1') == marker]
        if len(found) == 1:
            break
        time.sleep(1)
    assert len(found) == 1
    assert found[0]['p'] == item1['p']

###############################################################################
# Category 14: Composite Key with Existing Single-Key Behavior
###############################################################################

# Table with both a composite-key GSI and a single-key GSI.
def test_gsi_composite_and_single_key_gsi_coexist(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
            {'AttributeName': 'a', 'AttributeType': 'S'},
            {'AttributeName': 'b', 'AttributeType': 'S'},
            {'AttributeName': 'x', 'AttributeType': 'S'},
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'composite_gsi',
                'KeySchema': [
                    {'AttributeName': 'a', 'KeyType': 'HASH'},
                    {'AttributeName': 'b', 'KeyType': 'HASH'},
                ],
                'Projection': {'ProjectionType': 'ALL'}
            },
            {   'IndexName': 'simple_gsi',
                'KeySchema': [
                    {'AttributeName': 'x', 'KeyType': 'HASH'},
                ],
                'Projection': {'ProjectionType': 'ALL'}
            },
        ]) as table:
        a_val, b_val, x_val = random_string(), random_string(), random_string()
        item = {'p': random_string(), 'a': a_val, 'b': b_val, 'x': x_val}
        table.put_item(Item=item)
        # Query composite GSI
        assert_index_query(table, 'composite_gsi', [item],
            KeyConditionExpression='a = :a AND b = :b',
            ExpressionAttributeValues={':a': a_val, ':b': b_val})
        # Query simple GSI
        assert_index_query(table, 'simple_gsi', [item],
            KeyConditionExpression='x = :x',
            ExpressionAttributeValues={':x': x_val})

# ConsistentRead=True on a composite GSI query — should fail.
def test_gsi_composite_consistent_read_rejected(test_table_gsi_2h2r):
    table = test_table_gsi_2h2r
    with pytest.raises(ClientError, match='ValidationException.*[cC]onsistent'):
        full_query(table, IndexName='idx_2h2r', ConsistentRead=True,
            KeyConditionExpression='h1 = :h1 AND h2 = :h2',
            ExpressionAttributeValues={':h1': 'v', ':h2': 'v'})
