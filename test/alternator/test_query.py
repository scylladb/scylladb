# -*- coding: utf-8 -*-
# Copyright 2019 ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# Tests for the Query operation

import random
import pytest
from botocore.exceptions import ClientError, ParamValidationError
from decimal import Decimal
from util import random_string, random_bytes, full_query, multiset
from boto3.dynamodb.conditions import Key, Attr

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
    assert multiset([item for item in items if item['p'] == 'long']) == multiset(got_items)

    # LT
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['12'], 'ComparisonOperator': 'LT'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] < '12']) == multiset(got_items)

    # LE
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['14'], 'ComparisonOperator': 'LE'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] <= '14']) == multiset(got_items)

    # GT
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['15'], 'ComparisonOperator': 'GT'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] > '15']) == multiset(got_items)

    # GE
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['14'], 'ComparisonOperator': 'GE'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] >= '14']) == multiset(got_items)

    # BETWEEN
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['155', '164'], 'ComparisonOperator': 'BETWEEN'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] >= '155' and item['c'] <= '164']) == multiset(got_items)

    # BEGINS_WITH
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['11'], 'ComparisonOperator': 'BEGINS_WITH'}
        }):
        print([item for item in items if item['p'] == 'long' and item['c'].startswith('11')])
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'].startswith('11')]) == multiset(got_items)

def test_query_nonexistent_table(dynamodb):
    client = dynamodb.meta.client
    with pytest.raises(ClientError, match="ResourceNotFoundException"):
        client.query(TableName="i_do_not_exist", KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['11'], 'ComparisonOperator': 'BEGINS_WITH'}
        })

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

def test_begins_with_wrong_type(dynamodb, test_table_sn):
    paginator = dynamodb.meta.client.get_paginator('query')
    with pytest.raises(ClientError, match='ValidationException'):
        for page in paginator.paginate(TableName=test_table_sn.name, KeyConditions={
                'p' : {'AttributeValueList': ['unorthodox_chars'], 'ComparisonOperator': 'EQ'},
                'c' : {'AttributeValueList': [17], 'ComparisonOperator': 'BEGINS_WITH'}
                }):
            pass

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
def test_query_sort_order_number(test_table_sn):
    # This is a list of numbers, sorted in correct order, and each suitable
    # for accurate representation by Alternator's number type.
    numbers = [
        Decimal("-2e10"),
        Decimal("-7.1e2"),
        Decimal("-4.1"),
        Decimal("-0.1"),
        Decimal("-1e-5"),
        Decimal("0"),
        Decimal("2e-5"),
        Decimal("0.15"),
        Decimal("1"),
        Decimal("1.00000000000000000000000001"),
        Decimal("3.14159"),
        Decimal("3.1415926535897932384626433832795028841"),
        Decimal("31.4"),
        Decimal("1.4e10"),
    ]
    # Insert these numbers, in random order, into one partition:
    p = random_string()
    items = [{'p': p, 'c': num} for num in random.sample(numbers, len(numbers))]
    with test_table_sn.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Finally, verify that we get back exactly the same numbers (with identical
    # precision), and in their original sorted order.
    got_items = full_query(test_table_sn, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == numbers

def test_query_filtering_attributes_equality(filled_test_table):
    test_table, items = filled_test_table

    query_filter = {
        "attribute" : {
            "AttributeValueList" : [ "xxxx" ],
            "ComparisonOperator": "EQ"
        }
    }
    got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'}}, QueryFilter=query_filter)
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['attribute'] == 'xxxx']) == multiset(got_items)

    query_filter = {
        "attribute" : {
            "AttributeValueList" : [ "xxxx" ],
            "ComparisonOperator": "EQ"
        },
        "another" : {
            "AttributeValueList" : [ "yy" ],
            "ComparisonOperator": "EQ"
        }
    }

    got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'}}, QueryFilter=query_filter)
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['attribute'] == 'xxxx' and item['another'] == 'yy']) == multiset(got_items)

# Test that FilterExpression works as expected
@pytest.mark.xfail(reason="FilterExpression not supported yet")
def test_query_filter_expression(filled_test_table):
    test_table, items = filled_test_table

    got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'}}, FilterExpression=Attr("attribute").eq("xxxx"))
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['attribute'] == 'xxxx']) == multiset(got_items)

    got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'}}, FilterExpression=Attr("attribute").eq("xxxx") & Attr("another").eq("yy"))
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['attribute'] == 'xxxx' and item['another'] == 'yy']) == multiset(got_items)

# QueryFilter can only contain non-key attributes in order to be compatible
def test_query_filtering_key_equality(filled_test_table):
    test_table, items = filled_test_table

    with pytest.raises(ClientError, match='ValidationException'):
        query_filter = {
            "c" : {
                "AttributeValueList" : [ "5" ],
                "ComparisonOperator": "EQ"
            }
        }
        got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'}}, QueryFilter=query_filter)
        print(got_items)

    with pytest.raises(ClientError, match='ValidationException'):
        query_filter = {
            "attribute" : {
                "AttributeValueList" : [ "x" ],
                "ComparisonOperator": "EQ"
            },
            "p" : {
                "AttributeValueList" : [ "5" ],
                "ComparisonOperator": "EQ"
            }
        }
        got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'}}, QueryFilter=query_filter)
        print(got_items)

# Test Query with the AttributesToGet parameter. Result should include the
# selected attributes only - if one wants the key attributes as well, one
# needs to select them explicitly. When no key attributes are selected,
# some items may have *none* of the selected attributes. Those items are
# returned too, as empty items - they are not outright missing.
def test_query_attributes_to_get(dynamodb, test_table):
    p = random_string()
    items = [{'p': p, 'c': str(i), 'a': str(i*10), 'b': str(i*100) } for i in range(10)]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    for wanted in [ ['a'],             # only non-key attributes
                    ['c', 'a'],        # a key attribute (sort key) and non-key
                    ['p', 'c'],        # entire key
                    ['nonexistent']    # none of the items have this attribute!
                   ]:
        got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, AttributesToGet=wanted)
        expected_items = [{k: x[k] for k in wanted if k in x} for x in items]
        assert multiset(expected_items) == multiset(got_items)

# Test that in a table with both hash key and sort key, which keys we can
# Query by: We can Query by the hash key, by a combination of both hash and
# sort keys, but *cannot* query by just the sort key, and obviously not
# by any non-key column.
def test_query_which_key(test_table):
    p = random_string()
    c = random_string()
    p2 = random_string()
    c2 = random_string()
    item1 = {'p': p, 'c': c}
    item2 = {'p': p, 'c': c2}
    item3 = {'p': p2, 'c': c}
    for i in [item1, item2, item3]:
        test_table.put_item(Item=i)
    # Query by hash key only:
    got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    expected_items = [item1, item2]
    assert multiset(expected_items) == multiset(got_items)
    # Query by hash key *and* sort key (this is basically a GetItem):
    got_items = full_query(test_table, KeyConditions={
        'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}
    })
    expected_items = [item1]
    assert multiset(expected_items) == multiset(got_items)
    # Query by sort key alone is not allowed. DynamoDB reports:
    # "Query condition missed key schema element: p".
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(test_table, KeyConditions={
            'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}
        })
    # Query by a non-key isn't allowed, for the same reason - that the
    # actual hash key (p) is missing in the query:
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(test_table, KeyConditions={
            'z': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}
        })
    # If we try both p and a non-key we get a complaint that the sort
    # key is missing: "Query condition missed key schema element: c"
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(test_table, KeyConditions={
            'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'z': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}
        })
    # If we try p, c and another key, we get an error that
    # "Conditions can be of length 1 or 2 only".
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(test_table, KeyConditions={
            'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'},
            'z': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}
        })

# Test the "Select" parameter of Query. The default Select mode,
# ALL_ATTRIBUTES, returns items with all their attributes. Other modes
# allow returning just specific attributes or just counting the results
# without returning items at all.
@pytest.mark.xfail(reason="Select not supported yet")
def test_query_select(test_table_sn):
    numbers = [Decimal(i) for i in range(10)]
    # Insert these numbers, in random order, into one partition:
    p = random_string()
    items = [{'p': p, 'c': num, 'x': num} for num in random.sample(numbers, len(numbers))]
    with test_table_sn.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Verify that we get back the numbers in their sorted order. By default,
    # query returns all attributes:
    got_items = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})['Items']
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == numbers
    got_x_attributes = [x['x'] for x in got_items]
    assert got_x_attributes == numbers
    # Select=ALL_ATTRIBUTES does exactly the same as the default - return
    # all attributes:
    got_items = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='ALL_ATTRIBUTES')['Items']
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == numbers
    got_x_attributes = [x['x'] for x in got_items]
    assert got_x_attributes == numbers
    # Select=ALL_PROJECTED_ATTRIBUTES is not allowed on a base table (it
    # is just for indexes, when IndexName is specified)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='ALL_PROJECTED_ATTRIBUTES')
    # Select=SPECIFIC_ATTRIBUTES requires that either a AttributesToGet
    # or ProjectionExpression appears, but then really does nothing:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='SPECIFIC_ATTRIBUTES')
    got_items = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='SPECIFIC_ATTRIBUTES', AttributesToGet=['x'])['Items']
    expected_items = [{'x': i} for i in numbers]
    assert got_items == expected_items
    got_items = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='x')['Items']
    assert got_items == expected_items
    # Select=COUNT just returns a count - not any items
    got = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='COUNT')
    assert got['Count'] == len(numbers)
    assert not 'Items' in got
    # Check again that we also get a count - not just with Select=COUNT,
    # but without Select=COUNT we also get the items:
    got = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    assert got['Count'] == len(numbers)
    assert 'Items' in got
    # Select with some unknown string generates a validation exception:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='UNKNOWN')

# Test that the "Limit" parameter can be used to return only some of the
# items in a single partition. The items returned are the first in the
# sorted order.
def test_query_limit(test_table_sn):
    numbers = [Decimal(i) for i in range(10)]
    # Insert these numbers, in random order, into one partition:
    p = random_string()
    items = [{'p': p, 'c': num} for num in random.sample(numbers, len(numbers))]
    with test_table_sn.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Verify that we get back the numbers in their sorted order.
    # First, no Limit so we should get all numbers (we have few of them, so
    # it all fits in the default 1MB limitation)
    got_items = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})['Items']
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == numbers
    # Now try a few different Limit values, and verify that the query
    # returns exactly the first Limit sorted numbers.
    for limit in [1, 2, 3, 7, 10, 17, 100, 10000]:
        got_items = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Limit=limit)['Items']
        assert len(got_items) == min(limit, len(numbers))
        got_sort_keys = [x['c'] for x in got_items]
        assert got_sort_keys == numbers[0:limit]
    # Unfortunately, the boto3 library forbids a Limit of 0 on its own,
    # before even sending a request, so we can't test how the server responds.
    with pytest.raises(ParamValidationError):
        test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Limit=0)

# In test_query_limit we tested just that Limit allows to stop the result
# after right right number of items. Here we test that such a stopped result
# can be resumed, via the LastEvaluatedKey/ExclusiveStartKey paging mechanism.
def test_query_limit_paging(test_table_sn):
    numbers = [Decimal(i) for i in range(20)]
    # Insert these numbers, in random order, into one partition:
    p = random_string()
    items = [{'p': p, 'c': num} for num in random.sample(numbers, len(numbers))]
    with test_table_sn.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Verify that full_query() returns all these numbers, in sorted order.
    # full_query() will do a query with the given limit, and resume it again
    # and again until the last page.
    for limit in [1, 2, 3, 7, 10, 17, 100, 10000]:
        got_items = full_query(test_table_sn, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Limit=limit)
        got_sort_keys = [x['c'] for x in got_items]
        assert got_sort_keys == numbers

# Test that the ScanIndexForward parameter works, and can be used to
# return items sorted in reverse order. Combining this with Limit can
# be used to return the last items instead of the first items of the
# partition.
def test_query_reverse(test_table_sn):
    numbers = [Decimal(i) for i in range(20)]
    # Insert these numbers, in random order, into one partition:
    p = random_string()
    items = [{'p': p, 'c': num} for num in random.sample(numbers, len(numbers))]
    with test_table_sn.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Verify that we get back the numbers in their sorted order or reverse
    # order, depending on the ScanIndexForward parameter being True or False.
    # First, no Limit so we should get all numbers (we have few of them, so
    # it all fits in the default 1MB limitation)
    reversed_numbers = list(reversed(numbers))
    got_items = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, ScanIndexForward=True)['Items']
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == numbers
    got_items = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, ScanIndexForward=False)['Items']
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == reversed_numbers
    # Now try a few different Limit values, and verify that the query
    # returns exactly the first Limit sorted numbers - in regular or
    # reverse order, depending on ScanIndexForward.
    for limit in [1, 2, 3, 7, 10, 17, 100, 10000]:
        got_items = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Limit=limit, ScanIndexForward=True)['Items']
        assert len(got_items) == min(limit, len(numbers))
        got_sort_keys = [x['c'] for x in got_items]
        assert got_sort_keys == numbers[0:limit]
        got_items = test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Limit=limit, ScanIndexForward=False)['Items']
        assert len(got_items) == min(limit, len(numbers))
        got_sort_keys = [x['c'] for x in got_items]
        assert got_sort_keys == reversed_numbers[0:limit]

# Test that paging also works properly with reverse order
# (ScanIndexForward=false), i.e., reverse-order queries can be resumed
def test_query_reverse_paging(test_table_sn):
    numbers = [Decimal(i) for i in range(20)]
    # Insert these numbers, in random order, into one partition:
    p = random_string()
    items = [{'p': p, 'c': num} for num in random.sample(numbers, len(numbers))]
    with test_table_sn.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    reversed_numbers = list(reversed(numbers))
    # Verify that with ScanIndexForward=False, full_query() returns all
    # these numbers in reversed sorted order - getting pages of Limit items
    # at a time and resuming the query.
    for limit in [1, 2, 3, 7, 10, 17, 100, 10000]:
        got_items = full_query(test_table_sn, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, ScanIndexForward=False, Limit=limit)
        got_sort_keys = [x['c'] for x in got_items]
        assert got_sort_keys == reversed_numbers

# A query without a KeyConditions or KeyConditionExpress is, or an empty
# one, is obviously not allowed:
def test_query_missing_key(test_table):
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(test_table, KeyConditions={})
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(test_table)
