# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for the ReturnConsumedCapacity header

import pytest
from botocore.exceptions import ClientError
import decimal
from decimal import Decimal
import boto3.dynamodb.types
from test.alternator.util import random_string, random_bytes, new_test_table

# A basic test that gets an item from a table with and without consistency
KB = 1024
boto3.dynamodb.types.DYNAMODB_CONTEXT = decimal.Context(prec=100)

def test_basic_put_get(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    put_response = test_table.put_item(Item={'p': p, 'c': c, 'attribute': val, 'another': val2}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in put_response
    consumed_capacity = put_response.get('ConsumedCapacity')
    capacity_unit = consumed_capacity["CapacityUnits"]
    assert 1 == consumed_capacity["CapacityUnits"]

    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    item = response['Item']
    assert item == {'p': p, 'c': c, 'attribute': val, 'another': val2}
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
    capacity_unit = consumed_capacity["CapacityUnits"]
    assert 1 == consumed_capacity["CapacityUnits"]

    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=False, ReturnConsumedCapacity='TOTAL')
    consumed_capacity = response.get('ConsumedCapacity')
    assert 0.5 == consumed_capacity["CapacityUnits"]

# A missing Item, count as zero length item which require 1 or 0.5 RCU depends on the consistency
def test_missing_get_item(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
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
    val2 = 'a' * (4 * KB - total_length)  # val2 length pass the total length to 4KB

    test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2})
    response = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
    assert 1 == consumed_capacity["CapacityUnits"]

    test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2 + 'a'})  # val2 now passes 4KB
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


def test_long_put(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    total_length = len(p) + len(c) + len(val) + len("pcattanother")

    val2 = 'a' * (KB - total_length)  # val2 length pass the total length to KB
    response = test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
    assert 1 == consumed_capacity["CapacityUnits"]

    response = test_table.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2 + 'a'}, ReturnConsumedCapacity='TOTAL')  # val2 now passes 1KB
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
    assert 2 == consumed_capacity["CapacityUnits"]

    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    response = test_table.put_item(Item={'p': p, 'c': c, 'att': val, ('a' * KB): val2}, ReturnConsumedCapacity='TOTAL')
    consumed_capacity = response.get('ConsumedCapacity')
    assert 2 == consumed_capacity["CapacityUnits"]


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
        total_length = len(p) + len(c) + len(val) + len("p123c4567attanother")

        val2 = 'a' * (KB - total_length)  # val2 length pass the total length to KB
        response = table.put_item(Item={'p123': p, 'c4567': c, 'att': val, 'another': val2}, ReturnConsumedCapacity='TOTAL')
        assert 'ConsumedCapacity' in response
        consumed_capacity = response.get('ConsumedCapacity')
        assert 1 == consumed_capacity["CapacityUnits"]

        response = table.put_item(Item={'p123': p, 'c4567': c, 'att': val, 'another': val2 + 'a'}, ReturnConsumedCapacity='TOTAL')  # val2 now passes 1KB
        assert 'ConsumedCapacity' in response
        consumed_capacity = response.get('ConsumedCapacity')
        assert 2 == consumed_capacity["CapacityUnits"]


def test_simple_put_item_bytes(test_table_sb):
    p = random_string()
    val = random_string()
    c = random_bytes()
    total_length = len(p) + len(val) + len(c) + len("pcattanother")

    val2 = 'a' * (KB - total_length)  # val2 length pass the total length to KB
    response = test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
    assert 1 == consumed_capacity["CapacityUnits"]
    response = test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2 + 'a'}, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
    assert 2 == consumed_capacity["CapacityUnits"]


def test_simple_get_item_bytes(test_table_sb):
    p = random_string()
    val = random_string()
    c = random_bytes()
    total_length = len(p) + len(val) + len(c) + len("pcattanother")

    val2 = 'a' * (4 * KB - total_length)  # val2 length pass the total length to KB
    test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2}, ReturnConsumedCapacity='TOTAL')
    response = test_table_sb.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
    assert 1 == consumed_capacity["CapacityUnits"]
    test_table_sb.put_item(Item={'p': p, 'c': c, 'att': val, 'another': val2 + 'a'}, ReturnConsumedCapacity='TOTAL')
    response = test_table_sb.get_item(Key={'p': p, 'c': c}, ConsistentRead=True, ReturnConsumedCapacity='TOTAL')
    assert 'ConsumedCapacity' in response
    consumed_capacity = response.get('ConsumedCapacity')
    assert 2 == consumed_capacity["CapacityUnits"]

def num_length(n):
    s = str(n)
    l = len(s)
    if '-' in s:
        l += 1
    l += 1
    l /= 2
    l += 1
    return int(l)

def test_number_magnitude_key(test_table_sn):
    p = random_string()
    # Legal magnitudes are allowed:
    for n in [Decimal("3.14"),
                Decimal("3"),
                Decimal("3143846.26433832795028841"),
                Decimal("31415926535897932384626433832795028841e30")]:
        for num in [-n, n]:
            x = random_string()
            total_length = len(p) + len(x) + num_length(num) + len("pcaval2")+20
            val2 = 'a' * (KB - total_length)  # val2 length pass the total length to KB
            response = test_table_sn.put_item(Item={'p': p, 'c': num, 'a': x, 'val2': val2}, ReturnConsumedCapacity='TOTAL')

            val2 = 'a' * (KB - total_length)  # val2 length pass the total length to KB
            response = test_table_sn.put_item(Item={'p': p, 'c': num, 'a': x, 'val2': val2}, ReturnConsumedCapacity='TOTAL')
            assert 1 == response.get('ConsumedCapacity')["CapacityUnits"]

            response = test_table_sn.put_item(Item={'p': p, 'c': num, 'a': x, 'val2': val2+'a'*40}, ReturnConsumedCapacity='TOTAL')
            assert 2 == response.get('ConsumedCapacity')["CapacityUnits"]

