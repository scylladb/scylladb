# Tests for the CRUD item operations: PutItem, GetItem, UpdateItem, DeleteItem

import random
import string

import pytest
from botocore.exceptions import ClientError

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

# Test that item operations on a non-existant table name fail with correct
# error code.
def test_item_operations_nonexistent_table(dynamodb):
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        dynamodb.meta.client.put_item(TableName='non_existent_table',
            Item={'a':{'S':'b'}})
