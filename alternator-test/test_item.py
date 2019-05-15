# Tests for the CRUD item operations: PutItem, GetItem, UpdateItem, DeleteItem

import random
import string

import pytest
from botocore.exceptions import ClientError
from decimal import Decimal

def random_string(len=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(len))

# Basic test for creating a new item with a random name, and reading it back
# with strong consistency.
# Only the string type is used for keys and attributes. None of the various
# optional PutItem features (Expected, ReturnValues, ReturnConsumedCapacity,
# ReturnItemCollectionMetrics, ConditionalOperator, ConditionExpression,
# ExpressionAttributeNames, ExpressionAttributeValues) are used, and
# for GetItem strong consistency is requested as well as all attributes,
# but no other optional features (AttributesToGet, ReturnConsumedCapacity,
# ProjectionExpression, ExpressionAttributeNames)
def test_basic_string_put_and_get(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    test_table.put_item(Item={'p': p, 'c': c, 'attribute': val, 'another': val2})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item['p'] == p
    assert item['c'] == c
    assert item['attribute'] == val
    assert item['another'] == val2

# Test ensuring that items inserted by a batched statement can be properly extracted
# via GetItem
def test_basic_batch_write_item(dynamodb, test_table):
    count = 7

    with test_table.batch_writer() as batch:
        for i in range(count):
            batch.put_item(Item={
                'p': "batch{}".format(i),
                'c': "batch_ck{}".format(i),
                'attribute': str(i),
                'another': 'xyz'
            })

    for i in range(count):
        item = test_table.get_item(Key={'p': "batch{}".format(i), 'c': "batch_ck{}".format(i)}, ConsistentRead=True)['Item']
        print(item)
        assert item['p'] == "batch{}".format(i)
        assert item['c'] == "batch_ck{}".format(i)
        assert item['attribute'] == str(i)
        assert item['another'] == 'xyz' 

# Similar to test_basic_string_put_and_get, just uses UpdateItem instead of
# PutItem. Because the item does not yet exist, it should work the same.
def test_basic_string_update_and_get(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'attribute': {'Value': val, 'Action': 'PUT'}, 'another': {'Value': val2, 'Action': 'PUT'}})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item['p'] == p
    assert item['c'] == c
    assert item['attribute'] == val
    assert item['another'] == val2

# Test put_item and get_item of various types for the *attributes*,
# including both scalars as well as nested documents, lists and sets.
# The full list of types tested here:
#    number, boolean, bytes, null, list, map, string set, number set,
#    binary set.
# The keys are still strings.
# Note that only top-level attributes are written and read in this test -
# this test does not attempt to modify *nested* attributes.
# See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html
# on how to pass these various types to Boto3's put_item().
def test_put_and_get_attribute_types(test_table):
    key = {'p': random_string(), 'c': random_string()}
    test_items = [
        Decimal("12.345"),
        42,
        True,
        False,
        b'xyz',
        None,
        ['hello', 'world', 42],
        {'hello': 'world', 'life': 42},
        {'hello': {'test': 'hi', 'hello': True, 'list': [1, 2, 'hi']}},
        set(['hello', 'world', 'hi']),
        set([1, 42, Decimal("3.14")]),
        set([b'xyz', b'hi']),
    ]
    item = { str(i) : test_items[i] for i in range(len(test_items)) }
    item.update(key)
    test_table.put_item(Item=item)
    got_item = test_table.get_item(Key=key, ConsistentRead=True)['Item']
    assert item == got_item

# Test error handling of UpdateItem passed a bad "Action" field.
def test_update_bad_action(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'attribute': {'Value': val, 'Action': 'NONEXISTENT'}})

# A more elaborate UpdateItem test, updating different attributes at different
# times. Includes PUT and DELETE operations.
def test_basic_string_more_update(test_table):
    p = random_string()
    c = random_string()
    val1 = random_string()
    val2 = random_string()
    val3 = random_string()
    val4 = random_string()
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'a3': {'Value': val1, 'Action': 'PUT'}})
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'a1': {'Value': val1, 'Action': 'PUT'}})
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'a2': {'Value': val2, 'Action': 'PUT'}})
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'a1': {'Value': val3, 'Action': 'PUT'}})
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'a3': {'Action': 'DELETE'}})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item['p'] == p
    assert item['c'] == c
    assert item['a1'] == val3
    assert item['a2'] == val2
    assert not 'a3' in item

# Test that item operations on a non-existant table name fail with correct
# error code.
def test_item_operations_nonexistent_table(dynamodb):
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        dynamodb.meta.client.put_item(TableName='non_existent_table',
            Item={'a':{'S':'b'}})
