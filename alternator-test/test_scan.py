# Tests for the Scan operation

import random
import string

import pytest
from botocore.exceptions import ClientError

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
