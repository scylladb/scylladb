# Tests for the Query operation

import random
import string

import pytest
from botocore.exceptions import ClientError

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
