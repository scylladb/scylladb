# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for the ReturnConsumedCapacity header

import pytest
from botocore.exceptions import ClientError
from decimal import Decimal
from test.alternator.util import random_string, random_bytes

# A basic test that gets an item from a table with and without consistency
KB = 1024

def test_basic_get(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    test_table.put_item(Item={'p': p, 'c': c, 'attribute': val, 'another': val2})
    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    item = response['Item']
    assert item == {'p': p, 'c': c, 'attribute': val, 'another': val2}
    consumed_capacity = response.get('ConsumedCapacity')
    assert 'ConsumedCapacity' in response
    capacity_unit = consumed_capacity["CapacityUnits"]
    assert 1 == consumed_capacity["CapacityUnits"]

    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=False, ReturnConsumedCapacity='TOTAL')
    consumed_capacity = response.get('ConsumedCapacity')
    assert 0.5 == consumed_capacity["CapacityUnits"]

# the RCU is calculated by multiply of 4KB.
# the result should be the same regardless if we return the entire object
# or just part of it
# The test validate that both the attributes and the values are part of the
# limit calculation

def test_long_get(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    total_length = len(p) + len(c) + len(val) + len("pcattanother")
    val2 = 'a' * (4 * KB - total_length) # val2 length pass the total length to 4KB

    test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2})
    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
    assert 1 == consumed_capacity["CapacityUnits"]

    test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2 + 'a'}) # val2 now passes 4KB
    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
    assert 2 == consumed_capacity["CapacityUnits"]

    response = test_table.get_item(Key={'p': p, 'c': c}, ProjectionExpression='p, c, att',
                                   ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    consumed_capacity = response.get('ConsumedCapacity')
    assert 2 == consumed_capacity["CapacityUnits"]

    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=False, ReturnConsumedCapacity='TOTAL')
    consumed_capacity = response.get('ConsumedCapacity')
    assert 1 == consumed_capacity["CapacityUnits"]

    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    test_table.put_item(Item={'p': p, 'c': c, 'att': val, ('a' * (4 * KB)): val2})
    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    consumed_capacity = response.get('ConsumedCapacity')
    assert 2 == consumed_capacity["CapacityUnits"]