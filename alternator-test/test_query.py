# -*- coding: utf-8 -*-
# Tests for the Query operation

import random
import string

import pytest
from botocore.exceptions import ClientError

# Utility function for fetching the entire results of a query into an array of items
def full_query(table, **kwargs):
    response = table.query(**kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items

def random_string(len=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(len))
def random_bytes(len=10):
    return bytearray(random.getrandbits(8) for _ in xrange(len))

def set_of_frozen_elements(list_of_dicts):
    return {frozenset(item.items()) for item in list_of_dicts}

# Test that scanning works fine with in-stock paginator
def test_query_basic_restrictions(dynamodb, filled_test_table):
    test_table, items = filled_test_table
    paginator = dynamodb.meta.client.get_paginator('query')

    # EQ
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'}
        }):
        got_items += page['Items']
    print(got_items)
    assert set_of_frozen_elements([item for item in items if item['p'] == 'long']) == set_of_frozen_elements(got_items)

    # LT
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['12'], 'ComparisonOperator': 'LT'}
        }):
        got_items += page['Items']
    print(got_items)
    assert set_of_frozen_elements([item for item in items if item['p'] == 'long' and item['c'] < '12']) == set_of_frozen_elements(got_items)

    # LE
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['14'], 'ComparisonOperator': 'LE'}
        }):
        got_items += page['Items']
    print(got_items)
    assert set_of_frozen_elements([item for item in items if item['p'] == 'long' and item['c'] <= '14']) == set_of_frozen_elements(got_items)

    # GT
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['15'], 'ComparisonOperator': 'GT'}
        }):
        got_items += page['Items']
    print(got_items)
    assert set_of_frozen_elements([item for item in items if item['p'] == 'long' and item['c'] > '15']) == set_of_frozen_elements(got_items)

    # GE
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['14'], 'ComparisonOperator': 'GE'}
        }):
        got_items += page['Items']
    print(got_items)
    assert set_of_frozen_elements([item for item in items if item['p'] == 'long' and item['c'] >= '14']) == set_of_frozen_elements(got_items)

    # BETWEEN
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['155', '164'], 'ComparisonOperator': 'BETWEEN'}
        }):
        got_items += page['Items']
    print(got_items)
    assert set_of_frozen_elements([item for item in items if item['p'] == 'long' and item['c'] >= '155' and item['c'] <= '164']) == set_of_frozen_elements(got_items)

    # BEGINS_WITH
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['11'], 'ComparisonOperator': 'BEGINS_WITH'}
        }):
        print([item for item in items if item['p'] == 'long' and item['c'].startswith('11')])
        got_items += page['Items']
    print(got_items)
    assert set_of_frozen_elements([item for item in items if item['p'] == 'long' and item['c'].startswith('11')]) == set_of_frozen_elements(got_items)

def test_begins_with(dynamodb, test_table):
    paginator = dynamodb.meta.client.get_paginator('query')
    items = [{'p': 'unorthodox_chars', 'c': sort_key, 'str': 'a'} for sort_key in [u'ÿÿÿ', u'cÿbÿ', u'cÿbÿÿabg'] ]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)

    # TODO(sarna): Once bytes type is supported, /xFF character should be tested
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['unorthodox_chars'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': [u'ÿÿ'], 'ComparisonOperator': 'BEGINS_WITH'}
        }):
        got_items += page['Items']
    print(got_items)
    assert sorted([d['c'] for d in got_items]) == sorted([d['c'] for d in items if d['c'].startswith(u'ÿÿ')])

    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['unorthodox_chars'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': [u'cÿbÿ'], 'ComparisonOperator': 'BEGINS_WITH'}
        }):
        got_items += page['Items']
    print(got_items)
    assert sorted([d['c'] for d in got_items]) == sorted([d['c'] for d in items if d['c'].startswith(u'cÿbÿ')])

# Items returned by Query should be sorted by the sort key. The following
# tests verify that this is indeed the case, for the three allowed key types:
# strings, binary, and numbers. These tests test not just the Query operation,
# but inherently that the sort-key sorting works.
def test_query_sort_order_string(test_table):
    # Insert a lot of random items in one new partition:
    # str(i) has a non-obvious sort order (e.g., "100" comes before "2") so is a nice test.
    p = random_string()
    items = [{'p': p, 'c': str(i)} for i in range(128)]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    assert len(items) == len(got_items)
    # Extract just the sort key ("c") from the items
    sort_keys = [x['c'] for x in items]
    got_sort_keys = [x['c'] for x in got_items]
    # Verify that got_sort_keys are already sorted (in string order)
    assert sorted(got_sort_keys) == got_sort_keys
    # Verify that got_sort_keys are a sorted version of the expected sort_keys
    assert sorted(sort_keys) == got_sort_keys
def test_query_sort_order_bytes(test_table_sb):
    # Insert a lot of random items in one new partition:
    # We arbitrarily use random_bytes with a random length.
    p = random_string()
    items = [{'p': p, 'c': random_bytes(10)} for i in range(128)]
    with test_table_sb.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    got_items = full_query(test_table_sb, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    assert len(items) == len(got_items)
    sort_keys = [x['c'] for x in items]
    got_sort_keys = [x['c'] for x in got_items]
    # Boto3's "Binary" objects are sorted as if bytes are signed integers.
    # This isn't the order that DynamoDB itself uses (byte 0 should be first,
    # not byte -128). Sorting the byte array ".value" works.
    assert sorted(got_sort_keys, key=lambda x: x.value) == got_sort_keys
    assert sorted(sort_keys) == got_sort_keys
# TODO: add number key version of this test: test_query_sort_order_number(test_table_sn)
