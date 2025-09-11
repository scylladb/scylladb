# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for the ReturnConsumedCapacity header

import pytest
from botocore.exceptions import ClientError
from test.alternator.util import random_string, random_bytes, new_test_table, scylla_config_temporary
import decimal
from decimal import Decimal
KB = 1024

# A basic test that gets an item from a table with and without consistency
# the simple get item validate that when reading a short item
# from a a table we will get 1 RCU for persistent read and 0.5
# for non persistent read.
def test_simple_get_item(test_table_sb):
    p = random_string()
    val = random_string()
    c = random_bytes()
    test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val}, ReturnConsumedCapacity='TOTAL')

    response = test_table_sb.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 1 == response['ConsumedCapacity']["CapacityUnits"]

    response = test_table_sb.get_item(Key={'p': p, 'c': c}, ConsistentRead=False, ReturnConsumedCapacity='TOTAL')
    assert 0.5 == response['ConsumedCapacity']["CapacityUnits"]

# A test that validates that an invalid ReturnConsumedCapacity throw an exception
def test_invalid_consumed_capacity_type(test_table_sb):
    p = random_string()
    val = random_string()
    c = random_bytes()
    test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val})
    with pytest.raises(ClientError):
        response = test_table_sb.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='DUMMY')

# A missing Item, count as zero length item which require 1 or 0.5 RCU depends on the consistency
def test_missing_get_item(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    consumed_capacity = response['ConsumedCapacity']
    assert 1 == consumed_capacity["CapacityUnits"]

    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=False, ReturnConsumedCapacity='TOTAL')
    consumed_capacity = response['ConsumedCapacity']
    assert 0.5 == consumed_capacity["CapacityUnits"]

# the RCU is calculated based on 4KB block size.
# the result should be the same regardless if we return the entire object
# or just part of it
# The test validate that both the attributes and the values are part of the
# limit calculation
def test_long_get(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    combined_keys = "pcattanother" # Takes all the keys and make one single string out of them
    total_length = len(p) + len(c) + len(val) + len(combined_keys)
    val2 = 'a' * (4 * KB - total_length)  # val2 is a string that makes the total message length equals to 4KB

    test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2})
    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 1 == response['ConsumedCapacity']["CapacityUnits"]

    test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2 + 'a'})  # Adding one byte, the total size is 1 byte more than 4KB
    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 2 == response['ConsumedCapacity']["CapacityUnits"]

    response = test_table.get_item(Key={'p': p, 'c': c}, ProjectionExpression='p, c, att', # Asking for part of the document, we still expect the same results
                                   ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 2 == response['ConsumedCapacity']["CapacityUnits"]

    response = test_table.get_item(Key={'p': p, 'c': c}, ProjectionExpression='p, c, att', ConsistentRead=False, ReturnConsumedCapacity='TOTAL')
    assert 1 == response['ConsumedCapacity']["CapacityUnits"]

    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    test_table.put_item(Item={'p': p, 'c': c, 'att': val, ('a' * (4 * KB)): val2}) # This is a case when the key name is relatively large
    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    consumed_capacity = response['ConsumedCapacity']
    assert 2 == consumed_capacity["CapacityUnits"]

# the simple put item validate that when writing a short item
# to a table we will get 1 WCU
def test_simple_put_item(test_table_sb):
    p = random_string()
    val = random_string()
    c = random_bytes()
    response = test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 1 == response['ConsumedCapacity']["CapacityUnits"]

# WCU is calculated based on 1KB block size.
# The test validate that both the attributes and the values are part of the
# limit calculation
def test_long_put(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    combined_keys = "pcattanother" # Takes all the keys and make one single string out of them
    total_length = len(p) + len(c) + len(val) + len(combined_keys)

    val2 = 'a' * (KB - total_length)  # val2 is a string that makes the total message length equals to 1KB
    response = test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 1 == response['ConsumedCapacity']["CapacityUnits"]

    response = test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2 + 'a'}, ReturnConsumedCapacity='TOTAL')  # Adding one byte, the total size is 1 byte more than 1KB
    assert 2 == response['ConsumedCapacity']["CapacityUnits"]

    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    response = test_table.put_item(Item={'p': p, 'c': c, 'att': val, ('a' * KB): val2}, ReturnConsumedCapacity='TOTAL')
    consumed_capacity = response['ConsumedCapacity']
    assert 2 == consumed_capacity["CapacityUnits"]

# WCU is calculated based on 1KB block size.
# The test validate that when calculating the WCU of a put item
# over an existing item, the longer of the items is the
# limit calculation
# By default, Alternator does not perform a read-before-write.
# To force it to, we need to request the old value.
def test_short_after_long_put(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    combined_keys = "pcattanother" # Takes all the keys and make one single string out of them

    val2 = 'a' * (2*KB)  # val2 is a string that makes the total message length larger than 2KB
    response = test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 3 == response['ConsumedCapacity']["CapacityUnits"]

    response = test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': 'a'}, ReturnConsumedCapacity='TOTAL', ReturnValues='ALL_OLD')  # Perform a put_item with total length < 1KB
    assert 3 == response['ConsumedCapacity']["CapacityUnits"]

# This test validate that attribute names that are longer than one byte
# (alternator tests default naming) still calculate WCU correctly.
def test_long_put_varied_key(dynamodb):
    with new_test_table(dynamodb,
                        KeySchema=[ { 'AttributeName': 'p123', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c4567', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p123', 'AttributeType': 'S' },
                    { 'AttributeName': 'c4567', 'AttributeType': 'S' },
        ]) as table:
        p = random_string()
        c = random_string()
        val = random_string()
        combined_keys = "p123c4567attanother" # Takes all the keys and make one single string out of them
        total_length = len(p) + len(c) + len(val) + len(combined_keys)

        val2 = 'a' * (KB - total_length)  # val2 is a string that makes the total message length equals to 1KB
        response = table.put_item(Item={'p123': p, 'c4567': c, 'att': val, 'another': val2}, ReturnConsumedCapacity='TOTAL')
        assert 'ConsumedCapacity' in response
        assert 1 == response['ConsumedCapacity']["CapacityUnits"]

        response = table.put_item(Item={'p123': p, 'c4567': c, 'att': val, 'another': val2 + 'a'}, ReturnConsumedCapacity='TOTAL')  # Adding one byte, the total size is 1 byte more than 1KB
        assert 2 == response['ConsumedCapacity']["CapacityUnits"]

# this helper function mimic how DynamoDB calculate a number size
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
# `The size of a number is approximately (number of UTF-8-encoded bytes of attribute name) + (1 byte per two significant digits) + (1 byte)`
def num_length(n):
    s = str(n)
    l = len(s)
    if '-' in s:
        l += 1
    l += 1
    l /= 2
    l += 1
    return int(l)

# This test validate that numbers length are calculated correctly.
# There are two difficulties, one, DynamoDB number calculation is approximate
# and two, Alternator and DynamodDB uses slightly different encoding.
# We assume that close proximity is ok.
# The way the test work, it performs two put_item operations.
# One that would be 5 bytes shorter than the approximation of 1KB
# and one that is 5 bytes longer than that approximation.
# this makes the test pass both on DynamoDB and Alternator
def test_number_magnitude_key(test_table_sn):
    p = random_string()
    for n in [Decimal("3.14"),
                Decimal("3"),
                Decimal("3143846.26433832795028841"),
                Decimal("31415926535897932384626433832795028841e30")]:
        for num in [-n, n]:
            x = random_string()
            combined_keys = "pcaval2" # Takes all the keys and make one single string out of them
            total_length = len(p) + len(x) + num_length(num) + len(combined_keys)
            val2 = 'a' * (KB - total_length - 5)  # val2 is a string that makes the total message length equals to 1KB minus 5 bytes
            response = test_table_sn.put_item(Item={'p': p, 'c': num, 'a': x, 'val2': val2}, ReturnConsumedCapacity='TOTAL')
            assert 1 == response.get('ConsumedCapacity')["CapacityUnits"]

            # The total message length will now be equals to 1KB plus 5 bytes
            response = test_table_sn.put_item(Item={'p': p, 'c': num, 'a': x, 'val2': val2 + 'a'*10}, ReturnConsumedCapacity='TOTAL')
            assert 2 == response.get('ConsumedCapacity')["CapacityUnits"]

# The simple delete item validates that when deleting a short item from a table
# we will get 1 WCU
def test_simple_delete_item(test_table_sb):
    p = random_string()
    val = random_string()
    c = random_bytes()
    test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val}, ReturnConsumedCapacity='TOTAL')
    response = test_table_sb.delete_item(Key={'p': p, 'c': c}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 1 == response['ConsumedCapacity']["CapacityUnits"]

# The delete missing item validates that when deleting a missing item
# we will get 1 WCU
def test_delete_missing_item(test_table_sb):
    p = random_string()
    val = random_string()
    c = random_bytes()
    response = test_table_sb.delete_item(Key={'p': p, 'c': c}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 1 == response['ConsumedCapacity']["CapacityUnits"]

# Validates that when the old value is returned the WCU takes
# Its size into account in the WCU calculation.
# WCU is calculated based on 1KB block size.
# The test uses Return value so that the API
# would take the previous item length into account
def test_long_delete(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    combined_keys = "pcattanother" # Takes all the keys and make one single string out of them
    total_length = len(p) + len(c) + len(val) + len(combined_keys)

    val2 = 'a' * (1 + 2*KB - total_length)  # val2 is a string that makes the total message length equals to 2KB+1
    test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2}, ReturnConsumedCapacity='TOTAL')
    response = test_table.delete_item(Key={'p': p, 'c': c}, ReturnConsumedCapacity='TOTAL', ReturnValues='ALL_OLD')
    assert 3 == response['ConsumedCapacity']["CapacityUnits"]

# The simple update item validates that when updating a short item in a table
# we will get 1 WCU
def test_simple_update_item(test_table_sb):
    p = random_string()
    val = random_string()
    val1 = random_string()
    c = random_bytes()
    test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val})
    response = test_table_sb.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET att = :val1',
        ExpressionAttributeValues={':val1': val1}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 1 == response['ConsumedCapacity']["CapacityUnits"]


# The simple update missing item validates that when trying to update non-exist item
# we will get 1 WCU
def test_simple_update_missing_item(test_table_sb):
    p = random_string()
    val1 = random_string()
    c = random_bytes()
    response = test_table_sb.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET att = :val1',
        ExpressionAttributeValues={':val1': val1}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 1 == response['ConsumedCapacity']["CapacityUnits"]

# The test validates the length of the values passed to update is taking into account
# when calculating the WCU
@pytest.mark.parametrize("total_length,expected_wcu", [(2*KB + 1, 3), (2*KB, 2)])
def test_update_item_long_attr(test_table_sb, total_length, expected_wcu):
    p = random_string()
    val = random_string()
    c = random_bytes()
    test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val}, ReturnConsumedCapacity='TOTAL')
    combined_keys = "pcatt" # Takes all the keys and make one single string out of them
    key_length = len(p) + len(c) + len(combined_keys)

    val1 = 'a' * (total_length - key_length)  # val1 pads the total message length to total_length
    response = test_table_sb.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET att = :val1',
        ExpressionAttributeValues={':val1': val1}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert expected_wcu == response['ConsumedCapacity']["CapacityUnits"]

# Small numbers seem to take 5 B.
@pytest.mark.parametrize("total_length,expected_wcu", [(2*KB + 1, 3), (2*KB, 2)])
def test_update_item_long_attr_and_number(test_table_s, total_length, expected_wcu):
    NUM_LEN = 5
    p = random_string()
    val = random_string()
    test_table_s.put_item(Item={'p': p, 'att': val}, ReturnConsumedCapacity='TOTAL')
    combined_keys = "pattnum" # Takes all the keys and make one single string out of them
    key_length = len(p) + len(combined_keys)

    val1 = 'a' * (total_length - key_length - NUM_LEN)  # val1 pads the total message length to total_length
    response = test_table_s.update_item(Key={'p': p},
        UpdateExpression='SET num = :num, att = :val1',
        ExpressionAttributeValues={':num': 1, ':val1': val1}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert expected_wcu == response['ConsumedCapacity']["CapacityUnits"]

# Consumed WCU is the maximum of the item's size before the update, and the
# item's size after the update. This test verifies that an existing field 'a'
# and a new field 'b' are included in the new item's size.
#
# Withouht forced read-before-write, only the updated parameters contribute to
# the item's size.
@pytest.mark.parametrize('force_rbw,expected_wcu', [('true', 8), ('false', 3)])
def test_update_item_update_expression_considers_old_and_new(dynamodb, test_table_sb, force_rbw, expected_wcu):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', force_rbw):
        p = random_string()
        c = random_bytes()
        test_table_sb.put_item(Item={'p': p, 'c': c, 'a': 'a' * 5 * KB})

        response = test_table_sb.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET b = :b_value',
            ExpressionAttributeValues={':b_value': 'b' * 2 * KB},
            ReturnConsumedCapacity='TOTAL')

        assert 'ConsumedCapacity' in response
        assert expected_wcu == response['ConsumedCapacity']["CapacityUnits"]

def test_update_item_larger_override_considers_new_item_only(dynamodb, test_table_sb):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'true'):
        p = random_string()
        c = random_bytes()
        test_table_sb.put_item(Item={'p': p, 'c': c, 'a': 'a'})

        response = test_table_sb.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET a = :a_value',
            ExpressionAttributeValues={':a_value': 'a' * 4 * KB},
            ReturnConsumedCapacity='TOTAL')

        assert 'ConsumedCapacity' in response
        assert 5 == response['ConsumedCapacity']["CapacityUnits"]

def test_update_item_smaller_override_considers_old_item_only(dynamodb, test_table_sb):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'true'):
        p = random_string()
        c = random_bytes()
        test_table_sb.put_item(Item={'p': p, 'c': c, 'a': 'a' * 4 * KB})

        response = test_table_sb.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET a = :a_value',
            ExpressionAttributeValues={':a_value': 'a' * 2 * KB},
            ReturnConsumedCapacity='TOTAL')

        assert 'ConsumedCapacity' in response
        assert 5 == response['ConsumedCapacity']["CapacityUnits"]

@pytest.mark.parametrize('force_rbw,expected_wcu', [('true', 8), ('false', 3)])
def test_update_item_attribute_updates(dynamodb, test_table_s, force_rbw, expected_wcu):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', force_rbw):
        p = random_string()
        test_table_s.put_item(Item={'p': p, 'b': 'b' * 5 * KB})

        response = test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 'a' * 2 * KB, 'Action': 'PUT'}},
            ReturnConsumedCapacity='TOTAL')

        assert test_table_s.get_item(Key={'p': p})['Item'] == {'p': p, 'a': 'a' * 2 * KB, 'b': 'b' * 5 * KB}
        assert 'ConsumedCapacity' in response
        assert expected_wcu == response['ConsumedCapacity']["CapacityUnits"]

# Validates that when the old value is returned the WCU takes
# Its size into account in the WCU calculation.
# WCU is calculated based on 1KB block size.
# The test uses Return value so that the API
# would take the previous item length into account
def test_long_update(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    combined_keys = "pcattanother" # Takes all the keys and make one single string out of them
    total_length = len(p) + len(c) + len(val) + len(combined_keys)

    val2 = 'a' * (1 + 2*KB - total_length)  # val2 is a string that makes the total message length equals to 2KB+1
    test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2})
    val1 = 'a' # we replace the long string of val2 with a short string
    response = test_table.update_item(Key={'p': p, 'c': c}, UpdateExpression='SET another = :val1',
        ExpressionAttributeValues={':val1': val1},
        ReturnConsumedCapacity='TOTAL', ReturnValues='ALL_OLD')
    assert 3 == response['ConsumedCapacity']["CapacityUnits"]

# A simple batch getItem test
# This test validates that when two items are fetched from the same table using BatchGetItem,
# the ReturnConsumedCapacity field reflects the sum of independent RCU calculations for each item.
# Consistency is defined per table in the BatchGetItem request, so both items share the same
# consistency setting. The test ensures that RCU is calculated independently for each item
# according to that setting, and the total consumed capacity is their sum.
def test_simple_batch_get_items(test_table_sb):
    p1 = random_string()
    val = random_string()
    c1 = random_bytes()
    test_table_sb.put_item(Item={'p': p1, 'c': c1})

    p2 = random_string()
    c2 = random_bytes()
    test_table_sb.put_item(Item={'p': p2, 'c': c2})

    response = test_table_sb.meta.client.batch_get_item(RequestItems = {
            test_table_sb.name: {'Keys': [{'p': p1, 'c': c1}, {'p': p2, 'c': c2}], 'ConsistentRead': True}}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 'TableName' in response['ConsumedCapacity'][0]
    assert response['ConsumedCapacity'][0]['TableName'] == test_table_sb.name
    assert 2 == response['ConsumedCapacity'][0]['CapacityUnits']

# Validate that when getting a batch of requests
# From multiple tables we get an RCU for each of the tables
# We also validate that the eventual consistency return half the units
def test_multi_table_batch_get_items(test_table_s, test_table):
    keys1 = []
    for i in range(5):
        p = random_string()
        test_table_s.put_item(Item={'p': p})
        keys1.append({'p': p})
    keys2 = []
    for i in range(3):
        p = random_string()
        c = random_string()
        test_table.put_item(Item={'p': p, 'c': c}, ReturnConsumedCapacity='TOTAL')
        keys2.append({'p': p, 'c': c})

    response = test_table.meta.client.batch_get_item(RequestItems = {
            test_table_s.name: {'Keys': keys1, 'ConsistentRead': True},
            test_table.name: {'Keys': keys2, 'ConsistentRead': False}}, ReturnConsumedCapacity='TOTAL')
    for cc in response['ConsumedCapacity']:
        if cc['TableName'] == test_table_s.name:
            assert cc["CapacityUnits"] == 5
        else:
            assert cc['TableName'] == test_table.name
            assert cc["CapacityUnits"] == 1.5

# A simple batch write item test
# This test validates that when two items are inserted into the same table using BatchWriteItem,
# the ReturnConsumedCapacity field reflects the sum of independent WCU calculations for each item.
# The test ensures that WCU is calculated independently for each item,
# and that the total consumed capacity is the sum of both.
def test_simple_batch_write_item(test_table_s):
    p1 = random_string()
    p2 = random_string()
    response = test_table_s.meta.client.batch_write_item(RequestItems = {
        test_table_s.name: [{'PutRequest': {'Item': {'p': p1, 'a': 'hi'}}}, {'PutRequest': {'Item': {'p': p2, 'a': 'hi'}}}]
    }, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 'TableName' in response['ConsumedCapacity'][0]
    assert response['ConsumedCapacity'][0]['TableName'] == test_table_s.name
    assert 2 == response['ConsumedCapacity'][0]['CapacityUnits']


# Validate that when updating a batch of requests
# across multiple tables, we get a WCU for each table.
# Also validate that delete operations are counted as 1 WCU.
def test_multi_table_batch_write_item(test_table_s, test_table):
    p = random_string()
    c = random_string()
    test_table.put_item(Item={'p': p, 'c': c})

    table_s_items = [{'PutRequest': {'Item': {'p':  random_string(), 'a': 'hi'}}} for i in range(3)]
    table_s_items.append({'PutRequest': {'Item': {'p':  random_string(), 'a': 'a' * KB}}})
    table_items = [{'PutRequest': {'Item': {'p':  random_string(), 'c': random_string()}}}, {'DeleteRequest': {'Key': {'p':  p, 'c': c}}}]
    response = test_table_s.meta.client.batch_write_item(RequestItems = {
        test_table_s.name: table_s_items,
        test_table.name: table_items
    }, ReturnConsumedCapacity='TOTAL')
    for cc in response['ConsumedCapacity']:
        if cc['TableName'] == test_table_s.name:
            assert cc["CapacityUnits"] == 5
        else:
            assert cc['TableName'] == test_table.name
            assert cc["CapacityUnits"] == 2

# batch_write_item has no option to return the previous value
# This test uses alternator_force_read_before_write to
# calculate WCU real values
def test_batch_write_item_with_true_values(test_table, cql):
    p = random_string()
    c = random_string()
    test_table.put_item(Item={'p': p, 'c': c, 'a': 'a' * KB}) # Item with more than 1KB
    p1 = random_string()
    c1 = random_string()
    test_table.put_item(Item={'p': p1, 'c': c1, 'a': 'a' * KB}) # Item with more than 1KB
    cql.execute("UPDATE system.config set value = 'true' WHERE name = 'alternator_force_read_before_write'")
    table_items = [{'PutRequest': {'Item': {'p': p, 'c': c , 'a': 'hi'}}}, {'DeleteRequest': {'Key': {'p':  p1, 'c': c1}}}]
    response = test_table.meta.client.batch_write_item(RequestItems = {
        test_table.name: table_items
    }, ReturnConsumedCapacity='TOTAL')
    cql.execute("UPDATE system.config set value = 'false' WHERE name = 'alternator_force_read_before_write'")
    assert response['ConsumedCapacity'][0]["CapacityUnits"] == 4
