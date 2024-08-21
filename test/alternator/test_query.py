# -*- coding: utf-8 -*-
# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for the Query operation
# Some of the Query features are tested in separate files:
#   * test_key_conditions.py: the KeyConditions parameter.
#   * test_key_condition_expression.py: the KeyConditionExpression parameter.
#   * test_filter_expression.py: the FilterExpression parameter.
#   * test_query_filter.py: the QueryFilter parameter.

import operator
import random
from decimal import Decimal

import pytest
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from test.alternator.util import random_string, random_bytes, full_query, multiset

python_compare_op_dict = {"LE": operator.le, "LT": operator.lt, "GE": operator.ge, "GT": operator.gt}

def test_query_nonexistent_table(dynamodb):
    client = dynamodb.meta.client
    with pytest.raises(ClientError, match="ResourceNotFoundException"):
        client.query(TableName="i_do_not_exist", KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['11'], 'ComparisonOperator': 'BEGINS_WITH'}
        })

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

# Note: this is a very partial check for the QueryFilter feature. See
# test_query_filter.py for much more exhaustive tests for this feature.
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
def test_query_filter_expression(filled_test_table):
    test_table, items = filled_test_table

    got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'}}, FilterExpression=Attr("attribute").eq("xxxx"))
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['attribute'] == 'xxxx']) == multiset(got_items)

    got_items = full_query(test_table, KeyConditions={'p': {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'}}, FilterExpression=Attr("attribute").eq("xxxx") & Attr("another").eq("yy"))
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['attribute'] == 'xxxx' and item['another'] == 'yy']) == multiset(got_items)


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

# Verify that it is forbidden to ask for an empty AttributesToGet
# Reproduces issue #10332.
def test_query_attributes_to_get_empty(dynamodb, test_table):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(test_table, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, AttributesToGet=[])

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
    got_items = test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})['Items']
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == numbers
    got_x_attributes = [x['x'] for x in got_items]
    assert got_x_attributes == numbers
    # Select=ALL_ATTRIBUTES does exactly the same as the default - return
    # all attributes:
    got_items = test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='ALL_ATTRIBUTES')['Items']
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == numbers
    got_x_attributes = [x['x'] for x in got_items]
    assert got_x_attributes == numbers
    # Select=ALL_PROJECTED_ATTRIBUTES is not allowed on a base table (it
    # is just for indexes, when IndexName is specified)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='ALL_PROJECTED_ATTRIBUTES')
    # Select=SPECIFIC_ATTRIBUTES requires that either a AttributesToGet
    # or ProjectionExpression appears, but then really does nothing:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='SPECIFIC_ATTRIBUTES')
    got_items = test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='SPECIFIC_ATTRIBUTES', AttributesToGet=['x'])['Items']
    expected_items = [{'x': i} for i in numbers]
    assert got_items == expected_items
    got_items = test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='x')['Items']
    assert got_items == expected_items
    # Select=COUNT just returns a count - not any items
    got = test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='COUNT')
    assert got['Count'] == len(numbers)
    assert not 'Items' in got
    # Check again that we also get a count - not just with Select=COUNT,
    # but without Select=COUNT we also get the items:
    got = test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    assert got['Count'] == len(numbers)
    assert 'Items' in got
    # Select with some unknown string generates a validation exception:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='UNKNOWN')
    # The Select value is case sensitive - "COUNT" works (checked above),
    # but "count" doesn't:
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='count')
    # If either AttributesToGet or ProjectionExpression appear in the query,
    # only Select=SPECIFIC_ATTRIBUTES (or nothing) is allowed - other Select
    # settings contradict the AttributesToGet or ProjectionExpression, and
    # therefore forbidden:
    with pytest.raises(ClientError, match='ValidationException.*AttributesToGet'):
        test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='ALL_ATTRIBUTES', AttributesToGet=['x'])
    with pytest.raises(ClientError, match='ValidationException.*AttributesToGet'):
        test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='COUNT', AttributesToGet=['x'])
    with pytest.raises(ClientError, match='ValidationException.*ProjectionExpression'):
        test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='ALL_ATTRIBUTES', ProjectionExpression='x')
    with pytest.raises(ClientError, match='ValidationException.*ProjectionExpression'):
        test_table_sn.query(KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Select='COUNT', ProjectionExpression='x')

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
    got_items = test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})['Items']
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == numbers
    # Now try a few different Limit values, and verify that the query
    # returns exactly the first Limit sorted numbers.
    for limit in [1, 2, 3, 7, 10, 17, 100, 10000]:
        got_items = test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Limit=limit)['Items']
        assert len(got_items) == min(limit, len(numbers))
        got_sort_keys = [x['c'] for x in got_items]
        assert got_sort_keys == numbers[0:limit]
    # Limit 0 is not allowed:
    with pytest.raises(ClientError, match='ValidationException.*[lL]imit'):
        test_table_sn.query(ConsistentRead=True, KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}, Limit=0)

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

# Although ExclusiveStartKey is usually used for paging through a long
# partition by setting it to the previous page's LastEvaluatedKey, a user
# may also use ExclusiveStartKey to skip directly to the middle of a
# partition, without having paged through anything earlier. Moreover,
# ExclusiveStartKey doesn't even have to be one of the actual keys in the
# partition. This test verifies that this works.
# Additionally, because the previous tests only passed the value of
# LastEvaluatedKey into ExclusiveStartKey, they couldn't tell whether the
# format of the "cookie" is the correct one - any opaque cookie would have
# worked. So this test also demonstrates that ExclusiveStartKey with a
# specific format actually works - because users can use this format directly.
def test_query_exclusivestartkey(test_table_sn):
    # Insert the numbers 0, 2, 4, ... 38 into one partition. We insert the
    # items in random order, but of course as sort keys they will be sorted.
    numbers = [i*2 for i in range(20)]
    p = random_string()
    items = [{'p': p, 'c': num} for num in random.sample(numbers, len(numbers))]
    with test_table_sn.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Query with ExclusiveStartKey set to different numbers, and verify we
    # get the expected results. In particular we want to check that the
    # result is *exclusive* of the given key (if ExclusiveStartKey=0,
    # the first result is 2, not 0), and that it's fine for ExclusiveStartKey
    # to not be an existing key (-3, 17 and 80), and that it's fine that we
    # have less than the Limit remaining items (34 and 80).
    limit = 5
    for start in [-3, 0, 8, 17, 34, 80]:
        expected_sort_keys = [x for x in numbers if x > start][:limit]
        # The ExclusiveStartKey option must indicate both partition key and
        # sort key. Note that the Python driver further converts this map
        # into the correct format for the request (including the key types).
        exclusivestartkey = { 'p': p, 'c': start }
        got_items = test_table_sn.query(
            KeyConditions={'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
            ExclusiveStartKey= { 'p': p, 'c': start },
            Limit=limit)['Items']
        got_sort_keys = [x['c'] for x in got_items]
        assert expected_sort_keys == got_sort_keys

# Test that the ScanIndexForward parameter works, and can be used to
# return items sorted in reverse order. Combining this with Limit can
# be used to return the last items instead of the first items of the
# partition.
@pytest.mark.parametrize("sort_key_op", [None, 'LT', 'LE', 'GT', 'GE'])
def test_query_reverse(sort_key_op, test_table_sn):
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
    key_condition = {'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}
    if sort_key_op:
        c_bound = random.randint(1, 19)
        key_condition['c'] = {'AttributeValueList': [c_bound], 'ComparisonOperator': sort_key_op}
        op = lambda x: python_compare_op_dict[sort_key_op](x, c_bound)
    else:
        op = lambda x: True

    for scan_index_forward in [True, False]:
        got_items = test_table_sn.query(ConsistentRead=True, KeyConditions=key_condition, ScanIndexForward=scan_index_forward)['Items']
        got_sort_keys = [x['c'] for x in got_items]
        assert got_sort_keys == list(filter(op, numbers[:: 1 if scan_index_forward else -1]))

        for limit in [1, 2, 3, 7, 10, 17, 100, 10000]:
            got_items = test_table_sn.query(ConsistentRead=True, KeyConditions=key_condition, Limit=limit, ScanIndexForward=scan_index_forward)['Items']
            got_sort_keys = [x['c'] for x in got_items]
            assert got_sort_keys == list(filter(op, numbers[:: 1 if scan_index_forward else -1]))[:limit]


# Test that paging also works properly with reverse order
# (ScanIndexForward=false), i.e., reverse-order queries can be resumed
@pytest.mark.parametrize("sort_key_op", [None, 'LT', 'LE', 'GT', 'GE'])
def test_query_reverse_paging(sort_key_op, test_table_sn):
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
    key_condition = {'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}
    if sort_key_op:
        c_bound = random.randint(1, 19)
        key_condition['c'] = {'AttributeValueList': [c_bound], 'ComparisonOperator': sort_key_op}
        op = lambda x: python_compare_op_dict[sort_key_op](x, c_bound)
    else:
        op = lambda x: True

    for limit in [1, 2, 3, 7, 10, 17, 100, 10000]:
        got_items = full_query(test_table_sn, KeyConditions=key_condition, ScanIndexForward=False, Limit=limit)
        got_sort_keys = [x['c'] for x in got_items]
        assert got_sort_keys == list(filter(op, reversed_numbers))

# Test that a reverse query also works for long partitions. This test
# reproduces #7586, where reverse queries had to read the entire partition
# so were limited to 100 MB (max_memory_for_unlimited_query_hard_limit).
# This is a relatively slow test (its setup of a 100 MB partition takes
# several seconds), so we mark it with "veryslow" - so it's not run unless
# the "--runveryslow" option is passed to pytest.
@pytest.mark.veryslow
def test_query_reverse_long(test_table_sn):
    # Insert many big strings into one partition sized over 100MB:
    p = random_string()
    str = 'x' * 10240
    N = 10000
    with test_table_sn.batch_writer() as batch:
        for i in range(N):
            batch.put_item({'p': p, 'c': i, 's': str})

    # Query one page of a specific length (Limit=50) in reverse order.
    # We should get the requested number of items, starting from the last
    # item (N-1), in reversed order:
    got_items = test_table_sn.query(KeyConditions={
        'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        ScanIndexForward=False,
        Limit=50,
        ConsistentRead=True)['Items']
    assert len(got_items) == 50
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == list(reversed(range(N-50, N)))

    # A similar limited and reversed query - with an explicit starting
    # point (2345) instead of the end:
    start = 2345
    got_items = test_table_sn.query(KeyConditions={
        'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        ExclusiveStartKey={'p': p, 'c': start},
        ScanIndexForward=False, Limit=50, ConsistentRead=True)['Items']
    assert len(got_items) == 50
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == list(reversed(range(start-50, start)))

    # Even if a Limit is *not* specified, queries have some built-in size
    # limit (around 1MB) - a query should never return the entire 100MB
    # partition in one response. One of the development versions had a bug
    # here - normal (unreversed) queries were limited to 1MB, but reversed
    # queries returned the entire 100MB.
    # First check this with regular (unreversed) order:
    got_items = test_table_sn.query(KeyConditions={
        'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        ConsistentRead=True)['Items']
    n = len(got_items)
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == list(range(n))
    assert n < N  # we don't how big n should be, but definitely not N!
    # And in reverse order:
    got_items = test_table_sn.query(KeyConditions={
        'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        ScanIndexForward=False,
        ConsistentRead=True)['Items']
    n = len(got_items)
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == list(reversed(range(N-n, N)))
    assert n < N

# The above test_query_reverse_long() tests a very long (over 100MB)
# partition, so is very slow especially over a slow network. The following
# is a much smaller subset of the above test that reproduces issue #9487:
# When doing a reverse query without "Limit" on a partition of 2MB, a result
# page should nevertheless be limited to 1MB of data and not return the
# entire 2MB in one page.
def test_query_reverse_longish(test_table_sn):
    # Insert a 2MB partition
    p = random_string()
    str = 'x' * 10240
    N = 200
    with test_table_sn.batch_writer() as batch:
        for i in range(N):
            batch.put_item({'p': p, 'c': i, 's': str})

    # Query one page of a specific length (Limit=50) in reverse order.
    # We should get the requested number of items, starting from the last
    # item (N-1), in reversed order:
    got_items = test_table_sn.query(KeyConditions={
        'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        ScanIndexForward=False,
        Limit=50,
        ConsistentRead=True)['Items']
    assert len(got_items) == 50
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == list(reversed(range(N-50, N)))

    # A similar limited and reversed query - with an explicit starting
    # point (2345) instead of the end:
    start = 147
    got_items = test_table_sn.query(KeyConditions={
        'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        ExclusiveStartKey={'p': p, 'c': start},
        ScanIndexForward=False, Limit=50, ConsistentRead=True)['Items']
    assert len(got_items) == 50
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == list(reversed(range(start-50, start)))

    # Even if a Limit is *not* specified, queries have some built-in size
    # limit (around 1MB) - a query should never return the entire 2MB
    # partition in one response. One of the development versions had a bug
    # here - normal (unreversed) queries were limited to 1MB, but reversed
    # queries returned the entire 100MB.
    # First check this with regular (unreversed) order:
    got_items = test_table_sn.query(KeyConditions={
        'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        ConsistentRead=True)['Items']
    n = len(got_items)
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == list(range(n))
    assert n < N  # we don't how big n should be, but definitely not N!
    # And in reverse order:
    got_items = test_table_sn.query(KeyConditions={
        'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        ScanIndexForward=False,
        ConsistentRead=True)['Items']
    n = len(got_items)
    got_sort_keys = [x['c'] for x in got_items]
    assert got_sort_keys == list(reversed(range(N-n, N)))
    assert n < N

# A query without a KeyConditions or KeyConditionExpress is, or an empty
# one, is obviously not allowed:
def test_query_missing_key(test_table):
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(test_table, KeyConditions={})
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(test_table)

# The paging tests above used a numeric sort key. Let's now also test paging
# with a bytes sort key. We already have above a test that bytes sort keys
# work and are sorted correctly (test_query_sort_order_bytes), but the
# following test adds a check that *paging* works correctly for such keys.
# We used to have a bug in this (issue #7768) - the returned LastEvaluatedKey
# was incorrectly formatted, breaking the boto3's parsing of the response.
# Note we only check the case of bytes *sort* keys in this test. For bytes
# *partition* keys, see test_scan_paging_bytes().
def test_query_paging_bytes(test_table_sb):
    p = random_string()
    items = [{'p': p, 'c': random_bytes()} for i in range(10)]
    with test_table_sb.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Deliberately pass Limit=1 to enforce paging even though we have
    # just 10 items in the partition.
    got_items = full_query(test_table_sb, Limit=1,
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    got_sort_keys = [x['c'] for x in got_items]
    expected_sort_keys = sorted(x['c'] for x in items)
    assert got_sort_keys == expected_sort_keys

# Similar for test for string clustering keys
def test_query_paging_string(test_table_ss):
    p = random_string()
    items = [{'p': p, 'c': random_string()} for i in range(10)]
    with test_table_ss.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    got_items = full_query(test_table_ss, Limit=1,
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    got_sort_keys = [x['c'] for x in got_items]
    expected_sort_keys = sorted(x['c'] for x in items)
    assert got_sort_keys == expected_sort_keys

# The following test reproduces #17995: A Query returning a large page
# composed of many small rows, which causes a lot of processing work for
# outputting the result, and the risk (before #17995 is fixed) to stall.
# To see the stall, an option like '--blocked-reactor-notify-ms', '5'
# must be added to Scylla in test/alternator/run, as the default stall
# threshold is higher than the one that this test produces.
# Because this test is slow (takes several seconds to build the large
# partition) and can't fail or even log a stall without different
# configuration, we skip it by default, using the "veryslow" mark.
# Remove this mark, and set the --block-reactor-notify-ms option, to run
# this test.
@pytest.mark.veryslow
def test_query_large_page_small_rows(test_table_sn):
    p = random_string()
    # Experimentally, Scylla considers the rows we insert below (which each
    # have a 10-byte string partition key with a one byte name, a numeric
    # clustering key with a one byte name, and no other data) as being 32
    # bytes in size, and returns 32772 of these rows in one (nominally) 1MB
    # page of Query. So if the partition has just 30,000 rows, it will be
    # returned entirely in one page.
    N = 30_000
    with test_table_sn.batch_writer() as batch:
        for i in range(N):
            batch.put_item({'p': p, 'c': i})

    got_items = test_table_sn.query(KeyConditions={
        'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}},
        ConsistentRead=True)['Items']
    n = len(got_items)
    assert n == N
