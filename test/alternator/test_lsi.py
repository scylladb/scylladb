# Copyright 2019-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# Tests of LSI (Local Secondary Indexes)
#
# Note that many of these tests are slower than usual, because many of them
# need to create new tables and/or new LSIs of different types, operations
# which are extremely slow in DynamoDB, often taking minutes (!).

import pytest
import time
from botocore.exceptions import ClientError, ParamValidationError
from util import create_test_table, random_string, full_scan, full_query, multiset, list_tables

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

# Checks that providing a hash key different than the base table is not allowed,
# and so is providing duplicated keys or no sort key at all
def test_lsi_wrong(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*'):
        table = create_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
            AttributeDefinitions=[
                        { 'AttributeName': 'p', 'AttributeType': 'S' },
                        { 'AttributeName': 'a', 'AttributeType': 'S' },
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
    with pytest.raises(ClientError, match='ValidationException.*'):
        table = create_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
            AttributeDefinitions=[
                        { 'AttributeName': 'p', 'AttributeType': 'S' },
                        { 'AttributeName': 'a', 'AttributeType': 'S' },
                        { 'AttributeName': 'b', 'AttributeType': 'S' }
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
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
            AttributeDefinitions=[
                        { 'AttributeName': 'p', 'AttributeType': 'S' },
                        { 'AttributeName': 'a', 'AttributeType': 'S' },
                        { 'AttributeName': 'b', 'AttributeType': 'S' }
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
    # TODO: check projection and key params
    # TODO: check also ProvisionedThroughput, IndexArn

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

# Check that only projected attributes can be extracted
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
