# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests of LSI (Local Secondary Indexes)
#
# Note that many of these tests are slower than usual, because many of them
# need to create new tables and/or new LSIs of different types, operations
# which are extremely slow in DynamoDB, often taking minutes (!).

import time

import pytest
import requests
from botocore.exceptions import ClientError

from test.alternator.util import create_test_table, new_test_table, random_string, full_scan, full_query, multiset, unique_table_name
from test.alternator.test_metrics import metrics, get_metric


# LSIs support strongly-consistent reads, so the following functions do not
# need to retry like we did in test_gsi.py for GSIs:
def assert_index_query(table, index_name, expected_items, **kwargs):
    assert multiset(expected_items) == multiset(full_query(table, IndexName=index_name, **kwargs))
def assert_index_scan(table, index_name, expected_items, **kwargs):
    assert multiset(expected_items) == multiset(full_scan(table, IndexName=index_name, **kwargs))

# A version doing retries instead of ConsistentRead, to be used just for the
# one test below which has both GSI and LSI:
def retrying_assert_index_query(table, index_name, expected_items, **kwargs):
    for i in range(3):
        if multiset(expected_items) == multiset(full_query(table, IndexName=index_name, ConsistentRead=False, **kwargs)):
            return
        print('retrying_assert_index_query retrying')
        time.sleep(1)
    assert multiset(expected_items) == multiset(full_query(table, IndexName=index_name, ConsistentRead=False, **kwargs))

# Although quite silly, it is actually allowed to create an index which is
# identical to the base table.
def test_lsi_identical(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }, { 'AttributeName': 'c', 'AttributeType': 'S' }],
        LocalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ])
    items = [{'p': random_string(), 'c': random_string()} for i in range(10)]
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Scanning the entire table directly or via the index yields the same
    # results (in different order).
    assert multiset(items) == multiset(full_scan(table))
    assert_index_scan(table, 'hello', items)
    # We can't scan a non-existent index
    with pytest.raises(ClientError, match='ValidationException'):
        full_scan(table, IndexName='wrong')
    table.delete()

# Check that providing a hash key different than the base table is not
# allowed:
def test_lsi_wrong_different_hash(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*hash key'):
        table = create_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
            AttributeDefinitions=[
                        { 'AttributeName': 'p', 'AttributeType': 'S' },
                        { 'AttributeName': 'c', 'AttributeType': 'S' },
                        { 'AttributeName': 'b', 'AttributeType': 'S' }
            ],
            LocalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [
                        { 'AttributeName': 'b', 'KeyType': 'HASH' },
                        { 'AttributeName': 'p', 'KeyType': 'RANGE' }
                    ],
                    'Projection': { 'ProjectionType': 'ALL' }
                }
            ])
        table.delete()

# Check that it's not allowed to create an LSI without specifying a range
# key cannot be missing, or (obviously) making it the same as the hash key:
def test_lsi_wrong_bad_range(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*same'):
        table = create_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[
                        { 'AttributeName': 'p', 'AttributeType': 'S' },
                        { 'AttributeName': 'c', 'AttributeType': 'S' }
            ],
            LocalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [
                        { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'p', 'KeyType': 'RANGE' }
                    ],
                    'Projection': { 'ProjectionType': 'ALL' }
                }
            ])
        table.delete()
    with pytest.raises(ClientError, match='ValidationException.*'):
        table = create_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[
                        { 'AttributeName': 'p', 'AttributeType': 'S' },
                        { 'AttributeName': 'c', 'AttributeType': 'S' },
            ],
            LocalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [
                        { 'AttributeName': 'p', 'KeyType': 'HASH' }
                    ],
                    'Projection': { 'ProjectionType': 'ALL' }
                }
            ])
        table.delete()

# The purpose of an LSI is to allow an alternative sort key for the
# existing partitions - the partitions do not change. So it doesn't make
# sense to create an LSI on a table that did not originally have a sort key
# (so has only single-item partitions) - and this case is not allowed.
def test_lsi_wrong_no_sort_key(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*'):
        table = create_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
            AttributeDefinitions=[
                        { 'AttributeName': 'p', 'AttributeType': 'S' },
                        { 'AttributeName': 'c', 'AttributeType': 'S' },
            ],
            LocalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [
                        { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'c', 'KeyType': 'RANGE' }
                    ],
                    'Projection': { 'ProjectionType': 'ALL' }
                }
            ])
        table.delete()

# A simple scenario for LSI. Base table has a partition key and a sort key,
# index has the same partition key key but a different sort key - one of
# the non-key attributes from the base table.
@pytest.fixture(scope="module")
def test_table_lsi_1(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
                    { 'AttributeName': 'b', 'AttributeType': 'S' },
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'b', 'KeyType': 'RANGE' }
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ])
    yield table
    table.delete()

def test_lsi_1(test_table_lsi_1):
    items1 = [{'p': random_string(), 'c': random_string(), 'b': random_string()} for i in range(10)]
    p1, b1 = items1[0]['p'], items1[0]['b']
    p2, b2 = random_string(), random_string()
    items2 = [{'p': p2, 'c': p2, 'b': b2}]
    items = items1 + items2
    with test_table_lsi_1.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    expected_items = [i for i in items if i['p'] == p1 and i['b'] == b1]
    assert_index_query(test_table_lsi_1, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p1], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b1], 'ComparisonOperator': 'EQ'}})
    expected_items = [i for i in items if i['p'] == p2 and i['b'] == b2]
    assert_index_query(test_table_lsi_1, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p2], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b2], 'ComparisonOperator': 'EQ'}})

# The same as test_table_lsi_1, but with a clustering key of type bytes
@pytest.fixture(scope="module")
def test_table_lsi_2(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
                    { 'AttributeName': 'b', 'AttributeType': 'B' },
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'b', 'KeyType': 'RANGE' }
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ])
    yield table
    table.delete()

# A second scenario of LSI. Base table has both hash and sort keys,
# a local index is created on each non-key parameter
@pytest.fixture(scope="module")
def test_table_lsi_4(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
                    { 'AttributeName': 'x1', 'AttributeType': 'S' },
                    { 'AttributeName': 'x2', 'AttributeType': 'S' },
                    { 'AttributeName': 'x3', 'AttributeType': 'S' },
                    { 'AttributeName': 'x4', 'AttributeType': 'S' },
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'hello_' + column,
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': column, 'KeyType': 'RANGE' }
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            } for column in ['x1','x2','x3','x4']
        ])
    yield table
    table.delete()

def test_lsi_4(test_table_lsi_4):
    items1 = [{'p': random_string(), 'c': random_string(),
               'x1': random_string(), 'x2': random_string(), 'x3': random_string(), 'x4': random_string()} for i in range(10)]
    i_values = items1[0]
    i5 = random_string()
    items2 = [{'p': i5, 'c': i5, 'x1': i5, 'x2': i5, 'x3': i5, 'x4': i5}]
    items = items1 + items2
    with test_table_lsi_4.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    for column in ['x1', 'x2', 'x3', 'x4']:
        expected_items = [i for i in items if (i['p'], i[column]) == (i_values['p'], i_values[column])]
        assert_index_query(test_table_lsi_4, 'hello_' + column, expected_items,
            KeyConditions={'p': {'AttributeValueList': [i_values['p']], 'ComparisonOperator': 'EQ'},
                           column: {'AttributeValueList': [i_values[column]], 'ComparisonOperator': 'EQ'}})
        expected_items = [i for i in items if (i['p'], i[column]) == (i5, i5)]
        assert_index_query(test_table_lsi_4, 'hello_' + column, expected_items,
            KeyConditions={'p': {'AttributeValueList': [i5], 'ComparisonOperator': 'EQ'},
                           column: {'AttributeValueList': [i5], 'ComparisonOperator': 'EQ'}})

# Test that setting an indexed string column to an empty string is illegal,
# since keys cannot contain empty strings
def test_lsi_empty_value(test_table_lsi_1):
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_lsi_1.put_item(Item={'p': random_string(), 'c': random_string(), 'b': ''})

# Setting a binary key to an empty value is also illegal.
def test_lsi_empty_value_binary(test_table_lsi_2):
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_lsi_2.put_item(Item={'p':  random_string(), 'c': random_string(), 'b': b''})

# Test that if an item in a batch has an empty indexed column and fails the
# verification, none of the other writes in the batch get done either.
def test_lsi_empty_value_in_bigger_batch_write(test_table_lsi_1):
    items = [
        {'p': random_string(), 'c': random_string(), 'b': random_string()},
        {'p': random_string(), 'c': random_string(), 'b': random_string()},
        {'p': random_string(), 'c': random_string(), 'b': ''}
    ]
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        with test_table_lsi_1.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
    for item in items:
        assert not 'Item' in test_table_lsi_1.get_item(Key={'p': item['p'], 'c': item['c']}, ConsistentRead=True)

def test_lsi_null_index(test_table_lsi_1):
    # Dynamodb supports special way of setting NULL value. It's different than
    # non existing value.
    p = random_string()
    c = random_string()
    with pytest.raises(ClientError, match='ValidationException.*NULL'):
        test_table_lsi_1.put_item(Item={'p': p, 'c': c, 'b': None})
    with pytest.raises(ClientError, match='ValidationException.*NULL'):
        test_table_lsi_1.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'b': {'Value': None, 'Action': 'PUT'}})
    with pytest.raises(ClientError, match='ValidationException.*NULL'):
        with test_table_lsi_1.batch_writer() as batch:
            batch.put_item({'p': p, 'c': c, 'b': None})

def test_lsi_describe(test_table_lsi_4):
    desc = test_table_lsi_4.meta.client.describe_table(TableName=test_table_lsi_4.name)
    assert 'Table' in desc
    assert 'LocalSecondaryIndexes' in desc['Table']
    lsis = desc['Table']['LocalSecondaryIndexes']
    assert(sorted([lsi['IndexName'] for lsi in lsis]) == ['hello_x1', 'hello_x2', 'hello_x3', 'hello_x4'])
    for lsi in lsis:
        assert lsi['IndexArn'] == desc['Table']['TableArn'] + '/index/' + lsi['IndexName']
        assert lsi['Projection'] == {'ProjectionType': 'ALL'}

# Whereas GSIs have an IndexStatus when described by DescribeTable,
# LSIs do not. IndexStatus is not needed because LSIs cannot be added
# after the base table is created.
def test_lsi_describe_indexstatus(test_table_lsi_1):
    desc = test_table_lsi_1.meta.client.describe_table(TableName=test_table_lsi_1.name)
    assert 'Table' in desc
    assert 'LocalSecondaryIndexes' in desc['Table']
    lsis = desc['Table']['LocalSecondaryIndexes']
    assert len(lsis) == 1
    lsi = lsis[0]
    assert not 'IndexStatus' in lsi

# In addition to the basic listing of an LSI in DescribeTable tested above,
# in this test we check additional fields that should appear in each LSI's
# description.
@pytest.mark.xfail(reason="issues #7550, #11466")
def test_lsi_describe_fields(test_table_lsi_1):
    desc = test_table_lsi_1.meta.client.describe_table(TableName=test_table_lsi_1.name)
    assert 'Table' in desc
    assert 'LocalSecondaryIndexes' in desc['Table']
    lsis = desc['Table']['LocalSecondaryIndexes']
    assert len(lsis) == 1
    lsi = lsis[0]
    assert lsi['IndexName'] == 'hello'
    assert 'IndexSizeBytes' in lsi     # actual size depends on content
    assert 'ItemCount' in lsi
    # Whereas GSIs has ProvisionedThroughput, LSIs do not. An LSI shares
    # its provisioning with the base table.
    assert not 'ProvisionedThroughput' in lsi
    assert lsi['KeySchema'] == [{'KeyType': 'HASH', 'AttributeName': 'p'},
                                {'KeyType': 'RANGE', 'AttributeName': 'b'}]
    # The index's ARN should look like the table's ARN followed by /index/<indexname>.
    assert lsi['IndexArn'] == desc['Table']['TableArn'] + '/index/hello'

# A table with selective projection - only keys are projected into the index
@pytest.fixture(scope="module")
def test_table_lsi_keys_only(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
                    { 'AttributeName': 'b', 'AttributeType': 'S' }
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'b', 'KeyType': 'RANGE' }
                ],
                'Projection': { 'ProjectionType': 'KEYS_ONLY' }
            }
        ])
    yield table
    table.delete()

# Check that it's possible to extract a non-projected attribute from the index,
# as the documentation promises
def test_lsi_get_not_projected_attribute(test_table_lsi_keys_only):
    items1 = [{'p': random_string(), 'c': random_string(), 'b': random_string(), 'd': random_string()} for i in range(10)]
    p1, b1, d1 = items1[0]['p'], items1[0]['b'], items1[0]['d']
    p2, b2, d2 = random_string(), random_string(), random_string()
    items2 = [{'p': p2, 'c': p2, 'b': b2, 'd': d2}]
    items = items1 + items2
    with test_table_lsi_keys_only.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    expected_items = [i for i in items if i['p'] == p1 and i['b'] == b1 and i['d'] == d1]
    assert_index_query(test_table_lsi_keys_only, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p1], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b1], 'ComparisonOperator': 'EQ'}},
        Select='ALL_ATTRIBUTES')
    expected_items = [i for i in items if i['p'] == p2 and i['b'] == b2 and i['d'] == d2]
    assert_index_query(test_table_lsi_keys_only, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p2], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b2], 'ComparisonOperator': 'EQ'}},
        Select='ALL_ATTRIBUTES')
    expected_items = [{'d': i['d']} for i in items if i['p'] == p2 and i['b'] == b2 and i['d'] == d2]
    assert_index_query(test_table_lsi_keys_only, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p2], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b2], 'ComparisonOperator': 'EQ'}},
        Select='SPECIFIC_ATTRIBUTES', AttributesToGet=['d'])

# Check that querying an entire LSI partition (all items with the same hash
# key) works correctly: ALL_PROJECTED_ATTRIBUTES returns only the projected
# attributes (in this case, key columns p, c, b), while ALL_ATTRIBUTES returns
# the full items (including the non-projected attribute d). The items are
# returned sorted by the LSI sort key 'b', which is a different order than the
# base table sort key 'c'.
def test_lsi_get_whole_partition(test_table_lsi_keys_only):
    p = random_string()
    # Write several items into the same partition 'p', with distinct 'b' and
    # 'c' values so the LSI order (by b) differs from the base order (by c).
    items = [{'p': p, 'c': str(i), 'b': str(9 - i), 'd': random_string()} for i in range(5)]
    with test_table_lsi_keys_only.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Query the whole partition via the LSI.
    # ALL_PROJECTED_ATTRIBUTES: expect only projected columns p, c, b - no d,
    # returned in LSI sort order (by 'b').
    expected_projected_ordered = sorted(
        [{'p': i['p'], 'c': i['c'], 'b': i['b']} for i in items],
        key=lambda x: x['b'])
    result = full_query(test_table_lsi_keys_only, IndexName='hello',
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        Select='ALL_PROJECTED_ATTRIBUTES')
    assert result == expected_projected_ordered
    # ALL_ATTRIBUTES: expect full items including non-projected attribute d,
    # also in LSI sort order (by 'b').
    expected_ordered = sorted(items, key=lambda x: x['b'])
    result = full_query(test_table_lsi_keys_only, IndexName='hello',
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        Select='ALL_ATTRIBUTES')
    assert result == expected_ordered

# Check that Scan on a KEYS_ONLY LSI with Select=ALL_ATTRIBUTES or
# Select=SPECIFIC_ATTRIBUTES (for a non-projected attribute) correctly
# fetches full items from the base table rather than returning only
# the projected key columns. This is the Scan version of the other tests
# we have for Query.
def test_lsi_keys_only_scan_all_attributes(dynamodb):
    # Scan does not support restricting to a specific partition so
    # unfortunately we can't re-use a shared table.
    with new_test_table(dynamodb,
        KeySchema=[
            { 'AttributeName': 'p', 'KeyType': 'HASH' },
            { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'c', 'AttributeType': 'S' },
            { 'AttributeName': 'b', 'AttributeType': 'S' }
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'b', 'KeyType': 'RANGE' }
                ],
                'Projection': { 'ProjectionType': 'KEYS_ONLY' }
            }
        ]) as table:
        items = [{'p': random_string(), 'c': random_string(), 'b': random_string(), 'd': random_string()} for i in range(5)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        # Scan with Select=ALL_PROJECTED_ATTRIBUTES: only projected key columns
        # p, c, b are returned (not the non-projected attribute d).
        results = full_scan(table, IndexName='hello', Select='ALL_PROJECTED_ATTRIBUTES')
        expected_projected = [{'p': i['p'], 'c': i['c'], 'b': i['b']} for i in items]
        assert multiset(results) == multiset(expected_projected)
        # Scan with Select=ALL_ATTRIBUTES: full items including the non-
        # projected attribute d must be fetched from the base table.
        results = full_scan(table, IndexName='hello', Select='ALL_ATTRIBUTES')
        assert multiset(results) == multiset(items)
        # Scan with Select=SPECIFIC_ATTRIBUTES requesting only the non-
        # projected attribute d: also requires a base-table fetch per item.
        results = full_scan(table, IndexName='hello', Select='SPECIFIC_ATTRIBUTES', AttributesToGet=['d'])
        assert multiset(results) == multiset([{'d': i['d']} for i in items])

# Check that filtering (FilterExpression) on non-projected attributes is not
# allowed on a KEYS_ONLY LSI. DynamoDB rejects this with a ValidationException
# saying the index does not project the filter attribute. DynamoDB forbids
# filtering on non-projected attributes even when the read is with
# Select=ALL_ATTRIBUTES (which LSI supports). Filtering on non-projected
# attributes could have been allowed in this case - because the full items
# are read from the base table so filtering could access them. But since
# DynamoDB forbids it, Alternator will forbid it too.
# Forbidding filtering on non-projected attributes has a performance advantage
# because it allows DynamoDB to apply the filter before fetching unneeded
# rows from the base table.
def test_lsi_keys_only_filter_on_non_projected_attribute(test_table_lsi_keys_only):
    p = random_string()
    items = [{'p': p, 'c': str(i), 'b': str(i), 'd': str(i)} for i in range(5)]
    with test_table_lsi_keys_only.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Filtering on 'd' (a non-projected attribute) is not allowed.
    with pytest.raises(ClientError, match='ValidationException.*hello.*does not project.*filter.*d'):
        full_query(test_table_lsi_keys_only, IndexName='hello',
            KeyConditionExpression='p = :p',
            FilterExpression='d = :d',
            ExpressionAttributeValues={':p': p, ':d': '2'})
    # With Select=ALL_ATTRIBUTES, DynamoDB reads the entire items and
    # could in theory filter on 'd', but still forbids it.
    with pytest.raises(ClientError, match='ValidationException.*hello.*does not project.*filter.*d'):
        full_query(test_table_lsi_keys_only, IndexName='hello',
            KeyConditionExpression='p = :p',
            FilterExpression='d = :d',
            ExpressionAttributeValues={':p': p, ':d': '2'},
            Select='ALL_ATTRIBUTES')
    # Similarly, Select=SPECIFIC_ATTRIBUTES (and ProjectionExpression)
    # requesting 'd' also makes 'd' available and could in theory filter on
    # 'd', but is still forbidden.
    with pytest.raises(ClientError, match='ValidationException.*hello.*does not project.*filter.*d'):
        full_query(test_table_lsi_keys_only, IndexName='hello',
            KeyConditionExpression='p = :p',
            FilterExpression='d = :d',
            ExpressionAttributeValues={':p': p, ':d': '2'},
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression='d')
    # Filtering on the LSI's key columns (p, b) is also not allowed, but for a
    # different reason not related to projection: Key attributes must be used
    # in KeyConditions/KeyConditionExpression, not in FilterExpression.
    # The error message is different.
    for key_attr in ['p', 'b']:
        with pytest.raises(ClientError, match='ValidationException.*primary key'):
            full_query(test_table_lsi_keys_only, IndexName='hello',
                KeyConditionExpression='p = :p',
                FilterExpression=f'{key_attr} = :val',
                ExpressionAttributeValues={':p': p, ':val': '2'})
    # Curiously, there remains one single attribute we may filter on - c.
    # This is because c is not officially part of the LSI key schema, but
    # is still projected because it is one of the base table's key attributes
    # so DynamoDB does allow filtering on it.
    results = full_query(test_table_lsi_keys_only, IndexName='hello',
        KeyConditionExpression='p = :p',
        FilterExpression='c = :val',
        ExpressionAttributeValues={':p': p, ':val': '2'})
    assert len(results) == 1 and results[0]['c'] == '2'
    # Make sure filtering on 'c' still works even if explicitly asks for
    # with SPECIFIC_ATTRIBUTES just for 'd'.
    results = full_query(test_table_lsi_keys_only, IndexName='hello',
        KeyConditionExpression='p = :p',
        FilterExpression='c = :val',
        ExpressionAttributeValues={':p': p, ':val': '2'},
        Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='d')
    assert len(results) == 1 and results[0]['d'] == '2'

# In test_filter_expression.py::test_filter_expression_and_projection_expression
# we check that when selecting specific attributes with ProjectionExpression,
# we can still filter on unselected attributes. Let's check this also in the
# separate code path of an LSI with ProjectionType=KEYS_ONLY which selects
# a non-projected (non-key) attribute. In such a case we should be able to
# filter on a key attribute, but return only another, non-key, attribute.
def test_lsi_keys_only_filter_expression_and_projection_expression(test_table_lsi_keys_only):
    p = random_string()
    items = [{'p': p, 'c': str(i), 'b': str(i), 'd': str(i)} for i in range(5)]
    with test_table_lsi_keys_only.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Filter on 'c' (a key attribute, projected into KEYS_ONLY LSI) but
    # project only 'd' (a non-projected, non-key attribute requiring a
    # base-table fetch). The result should contain only the 'd' attribute
    # of the item whose 'c' matches the filter.
    results = full_query(test_table_lsi_keys_only, IndexName='hello',
        KeyConditionExpression='p = :p',
        FilterExpression='c = :c',
        ProjectionExpression='d',
        ExpressionAttributeValues={':p': p, ':c': '2'})
    assert len(results) == 1 and results[0] == {'d': '2'}

# Same as test_lsi_filter_on_non_projected_attribute but for
# ProjectionType=INCLUDE. The INCLUDE list contains 'e' but not 'd', so
# filtering on 'd' should be rejected, while filtering on 'e' (or key
# attributes) should work.
@pytest.mark.xfail(reason="Issue #5036 - ProjectionType=INCLUDE not yet supported")
def test_lsi_include_filter_on_non_projected_attribute(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}, {'AttributeName': 'c', 'KeyType': 'RANGE'}],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
            {'AttributeName': 'c', 'AttributeType': 'S'},
            {'AttributeName': 'b', 'AttributeType': 'S'},
        ],
        LocalSecondaryIndexes=[{
            'IndexName': 'hello',
            'KeySchema': [
                {'AttributeName': 'p', 'KeyType': 'HASH'},
                {'AttributeName': 'b', 'KeyType': 'RANGE'},
            ],
            'Projection': {'ProjectionType': 'INCLUDE', 'NonKeyAttributes': ['e']},
        }]) as table:
        p = random_string()
        items = [{'p': p, 'c': str(i), 'b': str(i), 'd': str(i), 'e': str(i)} for i in range(5)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        # Filtering on 'd' (not in the INCLUDE list) is not allowed.
        with pytest.raises(ClientError, match='ValidationException.*hello.*does not project.*filter.*d'):
            full_query(table, IndexName='hello',
                KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
                FilterExpression='d = :d',
                ExpressionAttributeValues={':d': '2'})
        # Filtering on 'e' (included in the projection) must be allowed.
        results = full_query(table, IndexName='hello',
            KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
            FilterExpression='e = :e',
            ExpressionAttributeValues={':e': '2'})
        assert len(results) == 1 and results[0]['e'] == '2'

# Check that querying a KEYS_ONLY LSI uses base-table reads only when needed:
#  - Selecting projected (key) columns directly reads from the LSI view without
#    touching the base table (efficient path).
#  - Selecting a non-projected column (Select=SPECIFIC_ATTRIBUTES with 'd')
#    must fall back to a base-table point read per item (inefficient path).
#  - Select=ALL_ATTRIBUTES also triggers per-item base-table reads.
# To check whether base-table reads happened, the test uses the metric
# scylla_alternator_table_lsi_reads_from_base_table (using the per-table
# metric makes this test safe even if there is concurrent activity on the
# same Scylla instance).
# This test is Scylla-specific, so it is skipped on AWS.
def test_lsi_query_select_efficiency(test_table_lsi_keys_only, metrics):
    p = random_string()
    items = [{'p': p, 'c': str(i), 'b': str(i), 'd': str(i)} for i in range(5)]
    with test_table_lsi_keys_only.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    per_table_metric = 'scylla_alternator_table_lsi_reads_from_base_table'
    table_labels = {'cf': test_table_lsi_keys_only.name}
    # Query with default Select (ALL_PROJECTED_ATTRIBUTES): reads only from
    # the LSI view - no base-table reads should happen.
    before = get_metric(metrics, per_table_metric, table_labels)
    full_query(test_table_lsi_keys_only, IndexName='hello',
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    after = get_metric(metrics, per_table_metric, table_labels)
    assert after == before
    # Same with explicit Select=ALL_PROJECTED_ATTRIBUTES.
    before = after
    full_query(test_table_lsi_keys_only, IndexName='hello',
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        Select='ALL_PROJECTED_ATTRIBUTES')
    after = get_metric(metrics, per_table_metric, table_labels)
    assert after == before
    # Query with Select=SPECIFIC_ATTRIBUTES for key columns (p, c and b) only.
    # Also no base-table reads needed since key columns are projected.
    before = get_metric(metrics, per_table_metric, table_labels)
    full_query(test_table_lsi_keys_only, IndexName='hello',
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        Select='SPECIFIC_ATTRIBUTES', AttributesToGet=['p', 'c', 'b'])
    after = get_metric(metrics, per_table_metric, table_labels)
    assert after == before
    # Query with Select=SPECIFIC_ATTRIBUTES for a non-projected column 'd':
    # must do one base-table read per item.
    before = get_metric(metrics, per_table_metric, table_labels)
    results = full_query(test_table_lsi_keys_only, IndexName='hello',
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        Select='SPECIFIC_ATTRIBUTES', AttributesToGet=['d'])
    after = get_metric(metrics, per_table_metric, table_labels)
    assert len(results) == len(items)
    assert after - before == len(items)
    # Sanity check: confirm that only the requested attribute 'd' is returned
    # (not even the key columns are returned).
    assert all(set(r.keys()) == {'d'} for r in results)
    # Query with Select=ALL_ATTRIBUTES: also does one base-table read per item.
    # Also use this opportunity to check that global (not per-table) metric
    # scylla_alternator_lsi_reads_from_base_table is updated as well.
    before = get_metric(metrics, per_table_metric, table_labels)
    global_metric = 'scylla_alternator_lsi_reads_from_base_table'
    before_global = get_metric(metrics, global_metric)
    results = full_query(test_table_lsi_keys_only, IndexName='hello',
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        Select='ALL_ATTRIBUTES')
    after = get_metric(metrics, per_table_metric, table_labels)
    after_global = get_metric(metrics, global_metric)
    assert len(results) == len(items)
    assert after - before == len(items)
    # The global metric may have increased by more than len(items) due to
    # concurrent activity, but must have increased by at least len(items).
    assert after_global - before_global >= len(items)

# Check that by default (Select=ALL_PROJECTED_ATTRIBUTES), only projected
# attributes are extracted
def test_lsi_get_all_projected_attributes(test_table_lsi_keys_only):
    items1 = [{'p': random_string(), 'c': random_string(), 'b': random_string(), 'd': random_string()} for i in range(10)]
    p1, b1, d1 = items1[0]['p'], items1[0]['b'], items1[0]['d']
    p2, b2, d2 = random_string(), random_string(), random_string()
    items2 = [{'p': p2, 'c': p2, 'b': b2, 'd': d2}]
    items = items1 + items2
    with test_table_lsi_keys_only.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # The default Select is ALL_PROJECTED_ATTRIBUTES, which returns only the
    # projected attributes, which are the key attributes - p, c and b. The
    # attribute d is not projected and should not appear.
    expected_items = [{'p': i['p'], 'c': i['c'],'b': i['b']} for i in items if i['p'] == p1 and i['b'] == b1]
    assert_index_query(test_table_lsi_keys_only, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p1], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b1], 'ComparisonOperator': 'EQ'}})

# Test the "Select" parameter of a Query on a LSI. We have in test_query.py
# a test 'test_query_select' for this parameter on a query of a normal (base)
# table, but for GSI and LSI the ALL_PROJECTED_ATTRIBUTES is additionally
# allowed (and in fact is the default), and we want to test it.
@pytest.mark.parametrize('projection_type', [
    'KEYS_ONLY',
    pytest.param('INCLUDE', marks=pytest.mark.xfail(reason="Issue #5036 - ProjectionType=INCLUDE not yet supported")),
])
def test_lsi_query_select(dynamodb, projection_type):
    projection = {'ProjectionType': projection_type}
    if projection_type == 'INCLUDE':
        projection['NonKeyAttributes'] = ['a']
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
                    { 'AttributeName': 'b', 'AttributeType': 'S' },
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'b', 'KeyType': 'RANGE' }
                ],
                'Projection': projection,
            }
        ]) as table:
        items = [{'p': random_string(), 'c': random_string(), 'b': random_string(), 'a': random_string(), 'x': random_string()} for i in range(10)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        p = items[0]['p']
        b = items[0]['b']
        # The default Select is ALL_PROJECTED_ATTRIBUTES, which returns only
        # the projected attributes. For INCLUDE: p, c, b, a (but not x).
        # For KEYS_ONLY: only the key attributes p, c, b.
        if projection_type == 'INCLUDE':
            expected_items = [{'p': z['p'], 'c': z['c'], 'b': z['b'], 'a': z['a']} for z in items if z['b'] == b]
        else:
            expected_items = [{'p': z['p'], 'c': z['c'], 'b': z['b']} for z in items if z['b'] == b]
        assert_index_query(table, 'hello', expected_items,
            KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                           'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
        assert_index_query(table, 'hello', expected_items,
            Select='ALL_PROJECTED_ATTRIBUTES',
            KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                           'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
        # Unlike in GSI, in LSI Select=ALL_ATTRIBUTES *is* allowed even
        # when only a subset of the attributes being projected:
        expected_items = [z for z in items if z['b'] == b]
        assert_index_query(table, 'hello', expected_items,
            Select='ALL_ATTRIBUTES',
            KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                           'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
        # Also in LSI, SPECIFIC_ATTRIBUTES (with AttributesToGet /
        # ProjectionExpression) is allowed for any attribute, projected
        # or not projected. Let's try 'a' (projected in INCLUDE, not in
        # KEYS_ONLY) and 'x' (not projected in either case):
        expected_items = [{'a': z['a'], 'x': z['x']} for z in items if z['b'] == b]
        assert_index_query(table, 'hello', expected_items,
            Select='SPECIFIC_ATTRIBUTES',
            AttributesToGet=['a', 'x'],
            KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                           'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
        # Select=COUNT is also allowed, and as expected returns no content.
        assert not 'Items' in table.query(ConsistentRead=False,
            IndexName='hello',
            Select='COUNT',
            KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                           'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})

# Check that strongly consistent reads are allowed for LSI
def test_lsi_consistent_read(test_table_lsi_1):
    items1 = [{'p': random_string(), 'c': random_string(), 'b': random_string()} for i in range(10)]
    p1, b1 = items1[0]['p'], items1[0]['b']
    p2, b2 = random_string(), random_string()
    items2 = [{'p': p2, 'c': p2, 'b': b2}]
    items = items1 + items2
    with test_table_lsi_1.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    expected_items = [i for i in items if i['p'] == p1 and i['b'] == b1]
    assert_index_query(test_table_lsi_1, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p1], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b1], 'ComparisonOperator': 'EQ'}})
    expected_items = [i for i in items if i['p'] == p2 and i['b'] == b2]
    assert_index_query(test_table_lsi_1, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p2], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b2], 'ComparisonOperator': 'EQ'}})

# A table with both gsi and lsi present
@pytest.fixture(scope="module")
def test_table_lsi_gsi(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
                    { 'AttributeName': 'x1', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello_g1',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'x1', 'KeyType': 'RANGE' }
                ],
                'Projection': { 'ProjectionType': 'KEYS_ONLY' }
            }
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'hello_l1',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'x1', 'KeyType': 'RANGE' }
                ],
                'Projection': { 'ProjectionType': 'KEYS_ONLY' }
            }
        ])
    yield table
    table.delete()

# Test that GSI and LSI can coexist, even if they're identical
def test_lsi_and_gsi(test_table_lsi_gsi):
    desc = test_table_lsi_gsi.meta.client.describe_table(TableName=test_table_lsi_gsi.name)
    assert 'Table' in desc
    assert 'LocalSecondaryIndexes' in desc['Table']
    assert 'GlobalSecondaryIndexes' in desc['Table']
    lsis = desc['Table']['LocalSecondaryIndexes']
    gsis = desc['Table']['GlobalSecondaryIndexes']
    assert(sorted([lsi['IndexName'] for lsi in lsis]) == ['hello_l1'])
    assert(sorted([gsi['IndexName'] for gsi in gsis]) == ['hello_g1'])

    items = [{'p': random_string(), 'c': random_string(), 'x1': random_string()} for i in range(17)]
    p1, x1 = items[0]['p'], items[0]['x1']
    with test_table_lsi_gsi.batch_writer() as batch:
        for item in items:
            batch.put_item(item)

    for index in ['hello_g1', 'hello_l1']:
        expected_items = [i for i in items if i['p'] == p1 and i['x1'] == x1]
        retrying_assert_index_query(test_table_lsi_gsi, index, expected_items,
            KeyConditions={'p': {'AttributeValueList': [p1], 'ComparisonOperator': 'EQ'},
                           'x1': {'AttributeValueList': [x1], 'ComparisonOperator': 'EQ'}})

# This test is a version of test_filter_expression_and_projection_expression
# from test_filter_expression, which involves a Query which projects only
# one column but filters on another one, and the point is to verify that
# the implementation got also the filtered column (for the filtering to work)
# but did not return it with the results. This version does the same, except
# that either the filtered column, or the projected column, is an LSI key.
# In our implementation, LSI keys are implemented differently from ordinary
# attributes - they are real Scylla columns and not just items in the
# ":attrs" map - so this test checks that our implementation of the filtering
# and projection (and their combination) did not mess up this special case.
# This test reproduces issue #6951.
def test_lsi_filter_expression_and_projection_expression(test_table_lsi_1):
    p = random_string()
    test_table_lsi_1.put_item(Item={'p': p, 'c': 'hi', 'b': 'dog', 'y': 'cat'})
    test_table_lsi_1.put_item(Item={'p': p, 'c': 'yo', 'b': 'mouse', 'y': 'horse'})
    # Case 1: b (the LSI key) is in filter but not in projection:
    got_items = full_query(test_table_lsi_1,
        KeyConditionExpression='p=:p',
        FilterExpression='b=:b',
        ProjectionExpression='y',
        ExpressionAttributeValues={':p': p, ':b': 'mouse'})
    assert(got_items == [{'y': 'horse'}])
    # Case 2: b (the LSI key) is in the projection, but not the filter:
    got_items = full_query(test_table_lsi_1,
        KeyConditionExpression='p=:p',
        FilterExpression='y=:y',
        ProjectionExpression='b',
        ExpressionAttributeValues={':p': p, ':y': 'cat'})
    assert(got_items == [{'b': 'dog'}])

# We tested above that a table can have both an LSI and a GSI.
# Although Alternator makes a distinction in how it stores the two types of
# indexes, they cannot have the same name - because if they are created with
# the same name, only one will be usable (the index is chosen via the
# IndexName request attribute, which doesn't say if it's an LSI or GSI).
# DynamoDB reports: "One or more parameter values were invalid:
# Duplicate index name: samename"
# Reproduces issue #10789.
def test_lsi_and_gsi_same_name(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*Duplicate'):
        table = create_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
                    { 'AttributeName': 'x1', 'AttributeType': 'S' },
            ],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'samename',
                    'KeySchema': [
                        { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'x1', 'KeyType': 'RANGE' }
                    ],
                    'Projection': { 'ProjectionType': 'KEYS_ONLY' }
                }
            ],
            LocalSecondaryIndexes=[
                {   'IndexName': 'samename',
                    'KeySchema': [
                        { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'x1', 'KeyType': 'RANGE' }
                    ],
                    'Projection': { 'ProjectionType': 'KEYS_ONLY' }
                }
            ])
        table.delete()

# Test that creating multiple LSIs with the same key schema but different names
# is allowed.
def test_lsi_identical_indexes_with_different_names(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'c', 'AttributeType': 'S' },
            { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'index1',
                'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'x', 'KeyType': 'RANGE' }],
                'Projection': { 'ProjectionType': 'ALL' }
            },
            {   'IndexName': 'index2',
                'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'x', 'KeyType': 'RANGE' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ]):
        pass

# Test that the LSI table can be addressed in Scylla's REST API (obviously,
# since this test is for the REST API, it is Scylla-only and can't be run on
# DynamoDB).
# At the time this test was written, the LSI's name has a "!" in it, so this
# test reproduces a bug in URL decoding (#5883). But the goal of this test
# isn't to insist that a table backing an LSI must have a specific name,
# but rather that whatever name it does have - it can be addressed.
def test_lsi_name_rest_api(test_table_lsi_1, rest_api):
    # See that the LSI is listed in list of tables. It will be a table
    # whose CQL name contains the Alternator table's name, and the
    # LSI's name ('hello'). As of this writing, it will actually be
    # alternator_<name>:<name>!:<lsi> - but the test doesn't enshrine this.
    resp = requests.get(f'{rest_api}/column_family/name')
    resp.raise_for_status()
    lsi_rest_name = None
    for name in resp.json():
        if test_table_lsi_1.name in name and 'hello' in name:
            lsi_rest_name = name
            break
    assert lsi_rest_name
    # Attempt to run a request on this LSI's table name "lsi_rest_name".
    # We'll use the compaction_strategy request here, but if for some
    # reason in the future we decide to drop that request, any other
    # request will be fine.
    resp = requests.get(f'{rest_api}/column_family/compaction_strategy/{lsi_rest_name}')
    resp.raise_for_status()
    # Let's make things difficult for the server by URL encoding the
    # lsi_rest_name - exposing issue #5883.
    encoded_lsi_rest_name = requests.utils.quote(lsi_rest_name)
    resp = requests.get(f'{rest_api}/column_family/compaction_strategy/{encoded_lsi_rest_name}')
    resp.raise_for_status()

# Test that when a table has an LSI, then if the indexed attribute is
# missing, the item is added to the base table but not the index.
def test_lsi_missing_attribute(test_table_lsi_1):
    p1 = random_string()
    c1 = random_string()
    b1 = random_string()
    p2 = random_string()
    c2 = random_string()
    test_table_lsi_1.put_item(Item={'p':  p1, 'c': c1, 'b': b1})
    test_table_lsi_1.put_item(Item={'p':  p2, 'c': c2})  # missing b

    # Both items are now in the base table:
    assert test_table_lsi_1.get_item(Key={'p': p1, 'c': c1}, ConsistentRead=True)['Item'] == {'p': p1, 'c': c1, 'b': b1}
    assert test_table_lsi_1.get_item(Key={'p': p2, 'c': c2}, ConsistentRead=True)['Item'] == {'p': p2, 'c': c2}

    # But only the first item is in the index: The first item can be found
    # using a Query, and a scan of the index won't find the second item.
    # Note: with eventually consistent read, we can't really be sure that
    # the second item will "never" appear in the index. We do that read last,
    # so if we had a bug and such item did appear, hopefully we had enough
    # time for the bug to become visible. At least sometimes.
    assert_index_query(test_table_lsi_1, 'hello', [{'p': p1, 'c': c1, 'b': b1}],
        KeyConditions={
            'p': {'AttributeValueList': [p1], 'ComparisonOperator': 'EQ'},
            'b': {'AttributeValueList': [b1], 'ComparisonOperator': 'EQ'},
        })
    assert not any([i['p'] == p2 and i['c'] == c2 for i in full_scan(test_table_lsi_1, ConsistentRead=False, IndexName='hello')])

# The wrong type attributes tests check if a table with an LSI on a string
# attribute rejects operations setting the attribute to values of other type.
def test_lsi_wrong_type_attribute_put(test_table_lsi_1):
    # PutItem with wrong type for 'b' is rejected, item isn't created even
    # in the base table.
    p = random_string()
    c = random_string()
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        test_table_lsi_1.put_item(Item={'p':  p, 'c': c, 'b': 3})
    assert not 'Item' in test_table_lsi_1.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)

def test_lsi_wrong_type_attribute_update(test_table_lsi_1):
    # An UpdateItem with wrong type for 'b' is also rejected, but naturally
    # if the item already existed, it remains as it was.
    p = random_string()
    c = random_string()
    b = random_string()
    test_table_lsi_1.put_item(Item={'p':  p, 'c': c, 'b': b})
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        test_table_lsi_1.update_item(Key={'p':  p, 'c': c}, AttributeUpdates={'b': {'Value': 3, 'Action': 'PUT'}})
    assert test_table_lsi_1.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c, 'b': b}

# Since an LSI key b cannot be a map or an array, in particular updates to
# nested attributes like b.y or b[1] are not legal.
def test_lsi_wrong_type_attribute_update_nested(test_table_lsi_1):
    p = random_string()
    c = random_string()
    b = random_string()
    test_table_lsi_1.put_item(Item={'p':  p, 'c': c, 'b': b})
    # Here we try to write a map into the LSI key column b.
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        test_table_lsi_1.update_item(Key={'p': p, 'c': c}, UpdateExpression='SET b = :val1',
            ExpressionAttributeValues={':val1': {'a': 3, 'b': 4}})
    # Here we try to set b.y for the LSI key column b. Here DynamoDB and
    # Alternator produce different error messages - but both make sense.
    # DynamoDB says "Key attributes must be scalars; list random access '[]'
    # and map # lookup '.' are not allowed: IndexKey: b", while Alternator
    # complains that "document paths not valid for this item: b.y".
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_lsi_1.update_item(Key={'p': p, 'c': c}, UpdateExpression='SET b.y = :val1',
            ExpressionAttributeValues={':val1': 3})

def test_lsi_wrong_type_attribute_batchwrite(test_table_lsi_1):
    # BatchWriteItem with wrong type for 'b' is rejected, item isn't created
    # even in the base table.
    p = random_string()
    c = random_string()
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        with test_table_lsi_1.batch_writer() as batch:
            batch.put_item({'p':  p, 'c': c, 'b': 3})
    assert not 'Item' in test_table_lsi_1.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)

def test_lsi_wrong_type_attribute_batch(test_table_lsi_1):
    # In a BatchWriteItem, if any update is forbidden, the entire batch is
    # rejected, and none of the updates happen at all.
    p = [random_string() for _ in range(3)]
    c = [random_string() for _ in range(3)]
    items = [{'p': p[0], 'c': c[0], 'b': random_string()},
             {'p': p[1], 'c': c[0], 'b': 3},
             {'p': p[2], 'c': c[0], 'b': random_string()}]
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        with test_table_lsi_1.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
    for p, c in zip(p, c):
        assert not 'Item' in test_table_lsi_1.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)

# Utility function for creating a new table (whose name is chosen by
# unique_table_name()) with an LSI with the given name. If creation was
# successful, the table is deleted. Useful for testing which LSI names work.
def create_lsi(dynamodb, index_name):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }, { 'AttributeName': 'c', 'AttributeType': 'S' }],
        LocalSecondaryIndexes=[
            {   'IndexName': index_name,
                'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ]) as table:
        # Verify that the LSI wasn't just ignored
        assert 'LocalSecondaryIndexes' in table.meta.client.describe_table(TableName=table.name)['Table']

# Index names with 255 characters are allowed in Dynamo. In Scylla, the
# limit is different - the sum of both table and index length plus an extra 2
# cannot exceed 222 characters.
# (compare test_create_and_delete_table_255/222() and test_gsi_very_long_name*).
@pytest.mark.xfail(reason="Alternator limits table name length + LSI name length to 220")
def test_lsi_very_long_name_255(dynamodb):
    create_lsi(dynamodb, 'n' * 255)
def test_lsi_very_long_name_256(dynamodb):
    with pytest.raises(ClientError, match='ValidationException'):
        create_lsi(dynamodb, 'n' * 256)
def test_lsi_very_long_name_222(dynamodb, scylla_only):
    # If we subtract from 222 the table's name length (we assume that
    # unique_table_name() always returns the same length) and an extra 2,
    # this is how long the LSI's name may be:
    max = 222 - len(unique_table_name()) - 2
    # This max length should work:
    create_lsi(dynamodb, 'n' * max)
    # But a name one byte longer should fail:
    with pytest.raises(ClientError, match='ValidationException.*total length'):
        create_lsi(dynamodb, 'n' * (max+1))

# This test validates that PutItem replaces the entire item, including the
# attribute 'b' used in the LSI key. The new item won't have 'b', so it should
# be removed from the index.
def test_lsi_put_overwrites_lsi_column(test_table_lsi_1):
    p = random_string()
    c = random_string()
    b = random_string()
    key = {'p': p, 'c': c}
    item = {**key, 'b': b}

    # Create an item with the LSI key column 'b'.
    test_table_lsi_1.put_item(Item=item)
    assert test_table_lsi_1.get_item(Key=key, ConsistentRead=True)['Item'] == item
    # The item should be added to the index.
    assert_index_query(test_table_lsi_1, 'hello', [item],
        KeyConditions={
            'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})

    # Replace the item with an empty item. This should delete 'b'.
    test_table_lsi_1.put_item(Item=key)
    assert test_table_lsi_1.get_item(Key=key, ConsistentRead=True)['Item'] == key
    # Validate that PutItem also removed the item from the LSI index.
    assert_index_query(test_table_lsi_1, 'hello', [],
        KeyConditions={
            'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})

def test_lsi_update_modifies_index(test_table_lsi_1):
    p = random_string()
    c = random_string()
    b1 = random_string()
    b2 = random_string()
    key = {'p': p, 'c': c}
    item = {**key, 'b': b1}

    # Create an item with the LSI key column 'b' set to b1.
    test_table_lsi_1.put_item(Item=item)
    assert test_table_lsi_1.get_item(Key=key, ConsistentRead=True)['Item'] == item
    assert_index_query(test_table_lsi_1, 'hello', [item],
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    # Set b to b2 instead of b1.
    test_table_lsi_1.update_item(Key=key, AttributeUpdates={'b': {'Value': b2, 'Action': 'PUT'}})
    assert test_table_lsi_1.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'b': b2}
    # Validate that the item is no longer in the index under b1, but under b2.
    assert_index_query(test_table_lsi_1, 'hello', [],
        KeyConditions={
            'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'b': {'AttributeValueList': [b1], 'ComparisonOperator': 'EQ'}})
    assert_index_query(test_table_lsi_1, 'hello', [{'p': p, 'c': c, 'b': b2}],
        KeyConditions={
            'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'b': {'AttributeValueList': [b2], 'ComparisonOperator': 'EQ'}})

def test_lsi_delete_modifies_index(test_table_lsi_1):
    p = random_string()
    key = {'p': p, 'c': random_string()}
    item = {**key, 'b': random_string()}

    # Create an item with the LSI key column 'b'.
    test_table_lsi_1.put_item(Item=item)
    assert test_table_lsi_1.get_item(Key=key, ConsistentRead=True)['Item'] == item
    # The item should be added to the index.
    assert_index_query(test_table_lsi_1, 'hello', [item],
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    # Delete the item.
    test_table_lsi_1.delete_item(Key=key)
    assert not 'Item' in test_table_lsi_1.get_item(Key=key, ConsistentRead=True)
    # Validate that the item is no longer in the index.
    assert_index_query(test_table_lsi_1, 'hello', [],
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})

# This test verifies that DescribeTable shows the correct user-requested LSI
# key even when Alternator had to add to the underlying materialized view an
# "extra" clustering key (because Scylla's MV requires each base key column
# to also be a key column in the view). It serves as a regression test for
# issue #5320.
# This is the LSI version of test_gsi.py::test_gsi_describe_table_schema_all
# but the LSI test has far fewer options than the GSI test because:
#     * We know the base table must have both hash and sort key (in the test
#       test_lsi_wrong_no_sort_key() we verified that we can't add an LSI
#       to a base table which has just a hash key).
#     * The LSI must have the same hash key as the base table.
# These constraints leave us just *two* options: The LSI either has the same
# sort key as the base (not a very interesting case, but valid), or the LSI's
# sort key is a non-key attribute in the base. So this test just needs to
# create one base table with two LSIs to cover all options.
def test_lsi_describe_table_schema_all(dynamodb):
    # If we are to use an LSI the base table must have both hash key and sort
    # key. Let's call them 'a', 'b':
    base_keys = ['a', 'b']
    # The LSI key must have the same hash key as the base ('a'), must
    # have a range key, and that range key can either be 'b' or not-'b'
    # (for which we take 'x'). So we only have these two options for what
    # the LSI key might be:
    lsi_keys_options = [['a', 'b'], ['a', 'x']]
    # Create a base table with base_keys and the two LSIs with the
    # LSI key options we collected in lsi_keys_options
    key_schema=[ { 'AttributeName': base_keys[0], 'KeyType': 'HASH' },
                 { 'AttributeName': base_keys[1], 'KeyType': 'RANGE' } ]
    attribute_definitions = [ {'AttributeName': attr, 'AttributeType': 'S' } for attr in (base_keys + ['x']) ]
    lsis = []
    for i, lsi_keys in enumerate(lsi_keys_options):
        lsi_key_schema=[ { 'AttributeName': lsi_keys[0], 'KeyType': 'HASH' },
                         { 'AttributeName': lsi_keys[1], 'KeyType': 'RANGE' } ]
        lsis.append({ 'IndexName': f'index{i}',
                      'KeySchema': lsi_key_schema,
                      'Projection': { 'ProjectionType': 'ALL' } })
    with new_test_table(dynamodb,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        LocalSecondaryIndexes=lsis) as table:
        # Check that DescribeTable shows the table and its LSIs correctly:
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        assert got['KeySchema'] == key_schema
        got_lsis = got['LocalSecondaryIndexes']
        # We want to compare got_lsis to the original lsis, but got_lsis may
        # have extra attributes that DescribeTable added beyond what was
        # present in the origin table creation. So let's leave in got_lsis
        # only the columns that were present in lsis[0].
        got_lsis = [ {k: v for k, v in got_lsi.items() if k in lsis[0]} for got_lsi in got_lsis ]
        # Use multiset to compare ignoring order
        assert multiset(got_lsis) == multiset(lsis)

# Test that after creating an LSI with different ProjectionType options
# (ALL, KEYS_ONLY, INCLUDE), DescribeTable is able to show the correct
# ProjectionType and the LSI. For ProjectionType=INCLUDE, it should also show
# the correct NonKeyAttributes created with the LSI.
@pytest.mark.parametrize('projection_type', [
    'ALL',
    'KEYS_ONLY',
    pytest.param('INCLUDE', marks=pytest.mark.xfail(reason='Issue #5036 - ProjectionType=INCLUDE not yet supported')),
])
def test_lsi_projection_type_describe(dynamodb, projection_type):
    projection = {'ProjectionType': projection_type}
    non_key_attrs = ['extra', 'hello']
    if projection_type == 'INCLUDE':
        projection['NonKeyAttributes'] = non_key_attrs
    with new_test_table(dynamodb,
            KeySchema=[
                {'AttributeName': 'p', 'KeyType': 'HASH'},
                {'AttributeName': 'c', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'c', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'},
            ],
            LocalSecondaryIndexes=[{
                'IndexName': 'lsi',
                'KeySchema': [
                    {'AttributeName': 'p', 'KeyType': 'HASH'},
                    {'AttributeName': 'x', 'KeyType': 'RANGE'},
                ],
                'Projection': projection,
            }]) as table:
        desc = table.meta.client.describe_table(TableName=table.name)
        lsi_list = desc['Table']['LocalSecondaryIndexes']
        assert len(lsi_list) == 1
        assert lsi_list[0]['Projection']['ProjectionType'] == projection_type
        if projection_type == 'INCLUDE':
            assert sorted(lsi_list[0]['Projection']['NonKeyAttributes']) == sorted(non_key_attrs)
