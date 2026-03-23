# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for the ReturnConsumedCapacity header

import pytest
from botocore.exceptions import ClientError
from test.alternator.util import random_string, random_bytes, new_test_table
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
        test_table_sb.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='DUMMY')

# A missing Item, count as zero length item which require 1 or 0.5 RCU depends on the consistency
def test_missing_get_item(test_table):
    p = random_string()
    c = random_string()
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
def test_update_item_long_attr(test_table_sb):
    p = random_string()
    val = random_string()
    c = random_bytes()
    test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val}, ReturnConsumedCapacity='TOTAL')
    combined_keys = "pcatt" # Takes all the keys and make one single string out of them
    total_length = len(p) + len(c) + len(combined_keys)

    val1 = 'a' * (2*KB + 1 - total_length) # val1 is a string that makes the total message length equals 2KB +1
    response = test_table_sb.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET att = :val1',
        ExpressionAttributeValues={':val1': val1}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 3 == response['ConsumedCapacity']["CapacityUnits"]

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

# This test reproduces a bug where the consumed capacity was divided by 16 MB,
# instead of 4 KB. The general formula for RCU per item is the same as for
# GetItem, namely:
#
# CEIL(ItemSizeInBytes / 4096) * (1 if strong consistency, 0.5 if eventual
# consistency)
#
# The RCU is calculated for each item individually, and the results are summed
# for the total cost of the BatchGetItem. In this case, the larger item is
# rounded up to 68KB, giving 17 RCUs, and the smaller item to 20KB, which
# results in 5 RCUs, making the total consumed capacity for this operation
# 22 RCUs.
def test_batch_get_items_large(test_table_sb):
    p1 = random_string()
    c1 = random_bytes()
    test_table_sb.put_item(Item={'p': p1, 'c': c1, 'a': 'a' * 64 * KB})

    p2 = random_string()
    c2 = random_bytes()
    test_table_sb.put_item(Item={'p': p2, 'c': c2, 'a': 'a' * 16 * KB})

    response = test_table_sb.meta.client.batch_get_item(RequestItems = {
        test_table_sb.name: {'Keys': [{'p': p1, 'c': c1}, {'p': p2, 'c': c2}], 'ConsistentRead': True}}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 'TableName' in response['ConsumedCapacity'][0]
    assert response['ConsumedCapacity'][0]['TableName'] == test_table_sb.name
    assert 22 == response['ConsumedCapacity'][0]['CapacityUnits']

# Helper function to generate item_count items and batch write them to the
# table. Returns the list of generated items.
def prepare_items(table, item_factory, item_count=10):
    items = []
    with table.batch_writer() as writer:
        for i in range(item_count):
            item = item_factory(i)
            items.append(item)
            writer.put_item(Item=item)
    return items

# This test verifies if querying two tables, each containing multiple ~30 byte
# items, reports the RCU correctly. A single item should consume 1 RCU, because
# the items' sizes are rounded up separately to 1 KB (ConsistentReads), and
# RCU should be reported per table. A variant of test_batch_get_items_large.
def test_batch_get_items_many_small(test_table_s, test_table_sb):
    # Each item should be about 30 bytes.
    items_sb = prepare_items(test_table_sb, lambda i: {'p': f'item_{i}_' + random_string(), 'c': random_bytes()})
    items_s = prepare_items(test_table_s, lambda i: {'p': f'item_{i}_' + random_string()})

    response = test_table_sb.meta.client.batch_get_item(RequestItems = {
        test_table_sb.name: {'Keys': items_sb, 'ConsistentRead': True},
        test_table_s.name: {'Keys': items_s, 'ConsistentRead': True},
    }, ReturnConsumedCapacity='TOTAL')

    assert 'ConsumedCapacity' in response
    assert len(response['ConsumedCapacity']) == 2
    expected_tables = {test_table_sb.name, test_table_s.name}
    for consumption_per_table in response['ConsumedCapacity']:
        assert 'TableName' in consumption_per_table
        assert consumption_per_table['CapacityUnits'] == 10, f"Table {consumption_per_table['TableName']} reported {consumption_per_table['CapacityUnits']} RCUs, expected 10"
        assert consumption_per_table['TableName'] in expected_tables
        expected_tables.remove(consumption_per_table['TableName'])
    assert not expected_tables

# This test verifies if querying a single partition reports the RCU correctly.
# This test is similar to test_batch_get_items_many_small.
def test_batch_get_items_many_small_single_partition(test_table_sb):
    # Each item should be about 20 bytes.
    pk = random_string()
    items_sb = prepare_items(test_table_sb, lambda _: {'p': pk, 'c': random_bytes()})

    response = test_table_sb.meta.client.batch_get_item(RequestItems = {
        test_table_sb.name: {'Keys': items_sb, 'ConsistentRead': True},
    }, ReturnConsumedCapacity='TOTAL')

    assert 'ConsumedCapacity' in response
    assert 'TableName' in response['ConsumedCapacity'][0]
    assert response['ConsumedCapacity'][0]['TableName'] == test_table_sb.name
    assert 10 == response['ConsumedCapacity'][0]['CapacityUnits']

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

# Test that a Query operation with ReturnConsumedCapacity='TOTAL' returns
# the correct ConsumedCapacity field in the response
@pytest.mark.xfail(reason="issue #5027 - Query doesn't report consumed capacity")
@pytest.mark.parametrize("consistent_read", [True, False])
def test_query_consumed_capacity(test_table_ss, consistent_read):
    p = random_string()
    c = random_string()
    test_table_ss.put_item(Item={'p': p, 'c': c})
    response = test_table_ss.query(
        KeyConditionExpression='p = :pk',
        ExpressionAttributeValues={':pk': p},
        ConsistentRead=consistent_read,
        ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    assert 'TableName' in response['ConsumedCapacity']
    assert response['ConsumedCapacity']['TableName'] == test_table_ss.name
    # This is just a tiny item, so it should consume 1 RCU for a consistent
    # read, 0.5 RCU for an eventually consistent read
    expected_rcu = 1 if consistent_read else 0.5
    assert expected_rcu == response['ConsumedCapacity']['CapacityUnits']

# Test that a Scan operation with ReturnConsumedCapacity='TOTAL' returns
# the correct ConsumedCapacity field in the response.
@pytest.mark.xfail(reason="issue #5027 - Scan doesn't report consumed capacity")
def test_scan_consumed_capacity(dynamodb):
    # For Scan to report a known consumed capacity, we unfortunately
    # can't use a shared table filled with data by other tests, and need
    # to create a new table for this test.
    with new_test_table(dynamodb,
            KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' } ],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            ) as table:
        p = random_string()
        table.put_item(Item={'p': p, 'animal': 'cat'})
        for consistent_read in [True, False]:
            response = table.scan(
                ConsistentRead=consistent_read,
                ReturnConsumedCapacity='TOTAL')
            assert response['Count'] == 1
            assert 'ConsumedCapacity' in response
            assert 'TableName' in response['ConsumedCapacity']
            assert response['ConsumedCapacity']['TableName'] == table.name
            # With just one tiny item, we would have expected to get 1 RCU for
            # a consistent read, 0.5 RCU for an eventually consistent read.
            # However, it turns out that for an unknown reason, DynamoDB's
            # implementation actually returns 4 RCU for consistent read, 2 for
            # eventually consistent read, and nobody knows why (see discussion
            # in https://stackoverflow.com/questions/70121087). We may want to
            # deviate from DynamoDB here and return the expected 1/0.5 RCU,
            # but for now, this test expects the DynamoDB behavior.
            expected_rcu = 4.0 if consistent_read else 2.0
            assert expected_rcu == response['ConsumedCapacity']['CapacityUnits']

# Test that for a table that does not have GSI or LSI, setting
# ReturnConsumedCapacity to INDEXES is allowed and is almost the same as
# setting it to TOTAL, except we also get another copy of the consumed
# capacity in a "Table" field.
# This test doesn't check again what the ConsumedCapacity value is, just that
# the request succeeds and sets ConsumedCapacity for the total and the table
# and doesn't set it for any index.
@pytest.mark.xfail(reason="issue #29138 - ReturnConsumedCapacity=INDEXES not supported")
def test_return_consumed_capacity_indexes_no_indexes(test_table_s):
    p = random_string()
    def check_consumed_capacity(consumed_capacity):
        assert 'TableName' in consumed_capacity
        assert consumed_capacity['TableName'] == test_table_s.name
        capacity = consumed_capacity['CapacityUnits']
        assert capacity > 0
        # With "INDEXES", we expect to see in "Table" another copy of the total
        # capacity, and since there are no indexes, we don't expect to see
        # "GlobalSecondaryIndexes" or "LocalSecondaryIndexes" in the response.
        assert 'Table' in consumed_capacity
        assert 'CapacityUnits' in consumed_capacity['Table']
        assert capacity == consumed_capacity['Table']['CapacityUnits']
        assert 'GlobalSecondaryIndexes' not in consumed_capacity
        assert 'LocalSecondaryIndexes' not in consumed_capacity
    # GetItem
    response = test_table_s.get_item(Key={'p': p}, ReturnConsumedCapacity='INDEXES')
    check_consumed_capacity(response['ConsumedCapacity'])
    # PutItem
    response = test_table_s.put_item(Item={'p': p}, ReturnConsumedCapacity='INDEXES')
    check_consumed_capacity(response['ConsumedCapacity'])
    # UpdateItem
    response = test_table_s.update_item(Key={'p': p}, UpdateExpression='SET a = :val',
        ExpressionAttributeValues={':val': 'hi'}, ReturnConsumedCapacity='INDEXES')
    check_consumed_capacity(response['ConsumedCapacity'])
    # DeleteItem
    response = test_table_s.delete_item(Key={'p': p}, ReturnConsumedCapacity='INDEXES')
    check_consumed_capacity(response['ConsumedCapacity'])
    # Query
    response = test_table_s.query(KeyConditionExpression='p = :pk',
        ExpressionAttributeValues={':pk': p}, ReturnConsumedCapacity='INDEXES')
    check_consumed_capacity(response['ConsumedCapacity'])
    # Scan
    response = test_table_s.scan(ReturnConsumedCapacity='INDEXES')
    check_consumed_capacity(response['ConsumedCapacity'])

    # Batches may read or write to multiples tables, so they always return a
    # list of ConsumedCapacity, one per table, instead of a single
    # ConsumedCapacity. Hence the "[0]" to access the first table's consumed
    # capacity.

    # BatchGetItem
    response = test_table_s.meta.client.batch_get_item(RequestItems = {
        test_table_s.name: {'Keys': [{'p': p}]}}, ReturnConsumedCapacity='INDEXES')
    assert len(response['ConsumedCapacity']) == 1
    check_consumed_capacity(response['ConsumedCapacity'][0])
    # BatchWriteItem
    response = test_table_s.meta.client.batch_write_item(RequestItems = {
        test_table_s.name: [{'PutRequest': {'Item': {'p': p, 'a': 'hi'}}}]}, ReturnConsumedCapacity='INDEXES')
    assert len(response['ConsumedCapacity']) == 1
    check_consumed_capacity(response['ConsumedCapacity'][0])

# This is a complete test for ReturnConsumedCapacity=INDEXES, in a table with
# indexes (GSI and LSI). It is supposed to set consumed capacity per-index in
# addition to the table and total capacity. But there are different operations
# that set different combinations of table/index capacity, and we want to make
# sure that each operation sets the right ones. In this test we just check
# that the right fields are set in the response, and that the total capacity is
# the sum of the table and index capacities, without checking the actual values
@pytest.mark.xfail(reason="issue #29138 - ReturnConsumedCapacity=INDEXES not supported")
def test_return_consumed_capacity_indexes_with_indexes(dynamodb):
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
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'},
                }
            ],
            LocalSecondaryIndexes=[
                {
                    'IndexName': 'lsi',
                    'KeySchema': [
                        {'AttributeName': 'p', 'KeyType': 'HASH'},
                        {'AttributeName': 'x', 'KeyType': 'RANGE'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                }
            ],
    ) as table:
        p = random_string()
        c = random_string()
        x = random_string()

        def check_consumed_capacity(consumed_capacity, uses_indexes, only_gsi=False, only_lsi=False):
            assert 'TableName' in consumed_capacity
            assert consumed_capacity['TableName'] == table.name
            total = consumed_capacity['CapacityUnits']
            assert total > 0
            # The 'Table' field should reflect the base table's own portion
            table_cap = 0
            if not only_gsi and not only_lsi:
                assert 'Table' in consumed_capacity
                table_cap = consumed_capacity['Table']['CapacityUnits']
                assert table_cap > 0
            gsi_cap = 0
            lsi_cap = 0
            if uses_indexes:
                # The GSI should appear in GlobalSecondaryIndexes
                if not only_lsi:
                    assert 'GlobalSecondaryIndexes' in consumed_capacity
                    gsi_cap = consumed_capacity['GlobalSecondaryIndexes']['gsi']['CapacityUnits']
                    assert gsi_cap > 0
                # The LSI should appear in LocalSecondaryIndexes
                if not only_gsi:
                    assert 'LocalSecondaryIndexes' in consumed_capacity
                    lsi_cap = consumed_capacity['LocalSecondaryIndexes']['lsi']['CapacityUnits']
                    assert lsi_cap > 0
            # The total should equal the sum of table + GSI + LSI capacities
            assert total == table_cap + gsi_cap + lsi_cap

        # GetItem (doesn't use index capacity)
        response = table.get_item(Key={'p': p, 'c': c}, ReturnConsumedCapacity='INDEXES')
        check_consumed_capacity(response['ConsumedCapacity'], uses_indexes=False)
        # PutItem (uses index capacity)
        response = table.put_item(Item={'p': p, 'c': c, 'x': x}, ReturnConsumedCapacity='INDEXES')
        check_consumed_capacity(response['ConsumedCapacity'], uses_indexes=True)
        # UpdateItem (uses index capacity)
        response = table.update_item(Key={'p': p, 'c': c}, UpdateExpression='SET a = :val',
            ExpressionAttributeValues={':val': 'hi'}, ReturnConsumedCapacity='INDEXES')
        check_consumed_capacity(response['ConsumedCapacity'], uses_indexes=True)
        # DeleteItem (uses index capacity)
        response = table.delete_item(Key={'p': p, 'c': c}, ReturnConsumedCapacity='INDEXES')
        check_consumed_capacity(response['ConsumedCapacity'], uses_indexes=True)
        # Query (doesn't use index capacity)
        response = table.query(KeyConditionExpression='p = :pk and c = :ck',
            ExpressionAttributeValues={':pk': p, ':ck': c}, ReturnConsumedCapacity='INDEXES')
        check_consumed_capacity(response['ConsumedCapacity'], uses_indexes=False)
        # Scan (doesn't use index capacity)
        response = table.scan(ReturnConsumedCapacity='INDEXES')
        check_consumed_capacity(response['ConsumedCapacity'], uses_indexes=False)

        # Batches may read or write to multiples tables, so they always return a
        # list of ConsumedCapacity, one per table, instead of a single
        # ConsumedCapacity. Hence the "[0]" to access the first table's consumed
        # capacity.

        # BatchGetItem (doesn't use index capacity)
        response = table.meta.client.batch_get_item(RequestItems = {
            table.name: {'Keys': [{'p': p, 'c': c}]}}, ReturnConsumedCapacity='INDEXES')
        assert len(response['ConsumedCapacity']) == 1
        check_consumed_capacity(response['ConsumedCapacity'][0], uses_indexes=False)
        # BatchWriteItem (uses index capacity)
        response = table.meta.client.batch_write_item(RequestItems = {
            table.name: [{'PutRequest': {'Item': {'p': p, 'c': c, 'x': x, 'a': 'hi'}}}]}, ReturnConsumedCapacity='INDEXES')
        assert len(response['ConsumedCapacity']) == 1
        check_consumed_capacity(response['ConsumedCapacity'][0], uses_indexes=True)

        # In the special case of a Query from an index, we only get index capacity,
        # not table capacity. Try read from both GSI and LSI.
        response = table.query(IndexName='gsi',
            KeyConditionExpression='x = :x',
            ExpressionAttributeValues={':x': x}, ReturnConsumedCapacity='INDEXES')
        check_consumed_capacity(response['ConsumedCapacity'], uses_indexes=True, only_gsi=True)
        response = table.query(IndexName='lsi',
            KeyConditionExpression='p = :p and x = :x',
            ExpressionAttributeValues={':p': p, ':x': x}, ReturnConsumedCapacity='INDEXES')
        check_consumed_capacity(response['ConsumedCapacity'], uses_indexes=True, only_lsi=True)

        # There is a special case for writes where the index's key attribute (in
        # test, "x" is in the key of both indexes) is missing from the item, the
        # indexes' capacity is not used at all). However, this depends strongly on
        # the situation: PutItem of a new item that didn't exist, without an "x"
        # attribute, doesn't use index capacity, but if the item did already exist
        # then PutItem will need to delete the old item from the index, so does
        # use Index capacity!
        # case 1: old item with p,c,x already exists so needs to be deleted from
        # the index, so uses_indexes=True
        response = table.put_item(Item={'p': p, 'c': c}, ReturnConsumedCapacity='INDEXES')
        check_consumed_capacity(response['ConsumedCapacity'], uses_indexes=True)
        # case 2: old item now doesn't have a "x" value, and new one doesn't
        # either, so no need to update the index, so uses_indexes=False
        response = table.put_item(Item={'p': p, 'c': c, 'animal': 'dog'}, ReturnConsumedCapacity='INDEXES')
        check_consumed_capacity(response['ConsumedCapacity'], uses_indexes=False)
