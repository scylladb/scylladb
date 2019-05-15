# Tests for the Scan operation

import random
import string

import pytest
from botocore.exceptions import ClientError

# Utility function for scanning the entire table into an array of items
def full_scan(table, **kwargs):
    response = table.scan(**kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items

def set_of_frozen_elements(list_of_dicts):
    return {frozenset(item.items()) for item in list_of_dicts}

# Test that scanning works fine with/without pagination
def test_scan_basic(filled_test_table):
    test_table, items = filled_test_table
    for limit in [None,1,2,4,33,50,100,9007,16*1024*1024]:
        pos = None
        got_items = []
        while True:
            if limit:
                response = test_table.scan(Limit=limit, ExclusiveStartKey=pos) if pos else test_table.scan(Limit=limit)
                assert len(response['Items']) <= limit
            else:
                response = test_table.scan(ExclusiveStartKey=pos) if pos else test_table.scan()
            pos = response.get('LastEvaluatedKey', None)
            got_items += response['Items']
            if not pos:
                break

        assert len(items) == len(got_items)
        assert set_of_frozen_elements(items) == set_of_frozen_elements(got_items)

def test_scan_with_paginator(dynamodb, filled_test_table):
    test_table, items = filled_test_table
    paginator = dynamodb.meta.client.get_paginator('scan')

    got_items = []
    for page in paginator.paginate(TableName=test_table.name):
        got_items += page['Items']

    assert len(items) == len(got_items)
    assert set_of_frozen_elements(items) == set_of_frozen_elements(got_items)

    for page_size in [1, 17, 1234]:
        got_items = []
        for page in paginator.paginate(TableName=test_table.name, PaginationConfig={'PageSize': page_size}):
            got_items += page['Items']

    assert len(items) == len(got_items)
    assert set_of_frozen_elements(items) == set_of_frozen_elements(got_items)

# Although partitions are scanned in seemingly-random order, inside a
# partition items must be returned by Scan sorted in sort-key order.
# This test verifies this, for string sort key. We'll need separate
# tests for the other sort-key types (number and binary)
def test_scan_sort_order_string(filled_test_table):
    test_table, items = filled_test_table
    got_items = full_scan(test_table)
    assert len(items) == len(got_items)
    # Extract just the sort key ("c") from the partition "long"
    items_long = [x['c'] for x in items if x['p'] == 'long']
    got_items_long = [x['c'] for x in got_items if x['p'] == 'long']
    # Verify that got_items_long are already sorted (in string order)
    assert sorted(got_items_long) == got_items_long
    # Verify that got_items_long are a sorted version of the expected items_long
    assert sorted(items_long) == got_items_long

# Test Scan with the AttributesToGet parameter. Result should include the
# selected attributes only - if one wants the key attributes as well, one
# needs to select them explicitly. When no key attributes are selected,
# some items may have *none* of the selected attributes. Those items are
# returned too, as empty items - they are not outright missing.
def test_scan_attributes_to_get(dynamodb, filled_test_table):
    table, items = filled_test_table
    for wanted in [ ['another'],       # only non-key attributes (one item doesn't have it!)
                    ['c', 'another'],  # a key attribute (sort key) and non-key
                    ['p', 'c'],        # entire key
                    ['nonexistent']    # none of the items have this attribute!
                   ]:
        print(wanted)
        got_items = full_scan(table, AttributesToGet=wanted)
        expected_items = [{k: x[k] for k in wanted if k in x} for x in items]
        assert sorted(expected_items) == sorted(got_items)
