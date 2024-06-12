# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests of LSI (Local Secondary Indexes)
#
# Note that many of these tests are slower than usual, because many of them
# need to create new tables and/or new LSIs of different types, operations
# which are extremely slow in DynamoDB, often taking minutes (!).

import time

import pytest
import requests
from botocore.exceptions import ClientError

from test.alternator.util import create_test_table, new_test_table, random_string, full_scan, full_query, multiset


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

def test_lsi_describe(test_table_lsi_4):
    desc = test_table_lsi_4.meta.client.describe_table(TableName=test_table_lsi_4.name)
    assert 'Table' in desc
    assert 'LocalSecondaryIndexes' in desc['Table']
    lsis = desc['Table']['LocalSecondaryIndexes']
    assert(sorted([lsi['IndexName'] for lsi in lsis]) == ['hello_x1', 'hello_x2', 'hello_x3', 'hello_x4'])
    for lsi in lsis:
        assert lsi['IndexArn'] == desc['Table']['TableArn'] + '/index/' + lsi['IndexName']
        assert lsi['Projection'] == {'ProjectionType': 'ALL'}

# In addition to the basic listing of an LSI in DescribeTable tested above,
# in this test we check additional fields that should appear in each LSI's
# description.
# Note that whereas GSIs also have IndexStatus and ProvisionedThroughput
# fields, LSIs do not. IndexStatus is not needed because LSIs cannot be
# added after the base table is created, and ProvisionedThroughput isn't
# needed because an LSI shares its provisioning with the base table.
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
    assert not 'IndexStatus' in lsi
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

# Check that by default (Select=ALL_PROJECTED_ATTRIBUTES), only projected
# attributes are extracted
@pytest.mark.xfail(reason="LSI in alternator currently only implement full projections")
def test_lsi_get_all_projected_attributes(test_table_lsi_keys_only):
    items1 = [{'p': random_string(), 'c': random_string(), 'b': random_string(), 'd': random_string()} for i in range(10)]
    p1, b1, d1 = items1[0]['p'], items1[0]['b'], items1[0]['d']
    p2, b2, d2 = random_string(), random_string(), random_string()
    items2 = [{'p': p2, 'c': p2, 'b': b2, 'd': d2}]
    items = items1 + items2
    with test_table_lsi_keys_only.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    expected_items = [{'p': i['p'], 'c': i['c'],'b': i['b']} for i in items if i['p'] == p1 and i['b'] == b1]
    assert_index_query(test_table_lsi_keys_only, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p1], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b1], 'ComparisonOperator': 'EQ'}})

# Test the "Select" parameter of a Query on a LSI. We have in test_query.py
# a test 'test_query_select' for this parameter on a query of a normal (base)
# table, but for GSI and LSI the ALL_PROJECTED_ATTRIBUTES is additionally
# allowed (and in fact is the default), and we want to test it.
@pytest.mark.xfail(reason="Projection and Select not supported yet. Issue #5036, #5058")
def test_lsi_query_select(dynamodb):
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
                'Projection': { 'ProjectionType': 'INCLUDE',
                                'NonKeyAttributes': ['a'] }
            }
        ]) as table:
        items = [{'p': random_string(), 'c': random_string(), 'b': random_string(), 'a': random_string(), 'x': random_string()} for i in range(10)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        p = items[0]['p']
        b = items[0]['b']
        # Although in LSI all attributes are available (as we'll check
        # below) the default Select is ALL_PROJECTED_ATTRIBUTES, and
        # returns just the projected attributes (in this case all key
        # attributes in either base or LSI, and 'a' - but not 'x'):
        expected_items = [{'p': z['p'], 'c': z['c'], 'b': z['b'], 'a': z['a']} for z in items if z['b'] == b]
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
        # or not projected. Let's try 'a' (projected) and 'x' (not projected):
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
    p1, c1, x1 = items[0]['p'], items[0]['c'], items[0]['x1']
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
