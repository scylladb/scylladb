# Tests for the CRUD item operations: PutItem, GetItem, UpdateItem, DeleteItem

import random
import string

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
    test_table.put_item(Item={'p': p, 'c': c, 'attribute': val})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item['p'] == p
    assert item['c'] == c
    assert item['attribute'] == val
