# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for the ReturnConsumedCapacity header

import pytest
from botocore.exceptions import ClientError
from decimal import Decimal
from test.alternator.util import random_string, random_bytes

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
