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

# Tests for the Scan operation

import pytest
from botocore.exceptions import ClientError
from util import random_string, full_scan, full_scan_and_count, multiset
from boto3.dynamodb.conditions import Attr

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
        assert multiset(items) == multiset(got_items)

def test_scan_nonexistent_table(dynamodb):
    client = dynamodb.meta.client
    with pytest.raises(ClientError, match="ResourceNotFoundException"):
        client.scan(TableName="i_do_not_exist")

def test_scan_with_paginator(dynamodb, filled_test_table):
    test_table, items = filled_test_table
    paginator = dynamodb.meta.client.get_paginator('scan')

    got_items = []
    for page in paginator.paginate(TableName=test_table.name):
        got_items += page['Items']

    assert len(items) == len(got_items)
    assert multiset(items) == multiset(got_items)

    for page_size in [1, 17, 1234]:
        got_items = []
        for page in paginator.paginate(TableName=test_table.name, PaginationConfig={'PageSize': page_size}):
            got_items += page['Items']

    assert len(items) == len(got_items)
    assert multiset(items) == multiset(got_items)

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
        assert multiset(expected_items) == multiset(got_items)

def test_scan_with_attribute_equality_filtering(dynamodb, filled_test_table):
    table, items = filled_test_table
    scan_filter = {
        "attribute" : {
            "AttributeValueList" : [ "xxxxx" ],
            "ComparisonOperator": "EQ"
        }
    }

    got_items = full_scan(table, ScanFilter=scan_filter)
    expected_items = [item for item in items if "attribute" in item.keys() and item["attribute"] == "xxxxx" ]
    assert multiset(expected_items) == multiset(got_items)

    scan_filter = {
        "another" : {
            "AttributeValueList" : [ "y" ],
            "ComparisonOperator": "EQ"
        },
        "attribute" : {
            "AttributeValueList" : [ "xxxxx" ],
            "ComparisonOperator": "EQ"
        }
    }

    got_items = full_scan(table, ScanFilter=scan_filter)
    expected_items = [item for item in items if "attribute" in item.keys() and item["attribute"] == "xxxxx" and item["another"] == "y" ]
    assert multiset(expected_items) == multiset(got_items)

# Test that FilterExpression works as expected
@pytest.mark.xfail(reason="FilterExpression not supported yet")
def test_scan_filter_expression(filled_test_table):
    test_table, items = filled_test_table

    got_items = full_scan(test_table, FilterExpression=Attr("attribute").eq("xxxx"))
    print(got_items)
    assert multiset([item for item in items if 'attribute' in item.keys() and item['attribute'] == 'xxxx']) == multiset(got_items)

    got_items = full_scan(test_table, FilterExpression=Attr("attribute").eq("xxxx") & Attr("another").eq("yy"))
    print(got_items)
    assert multiset([item for item in items if 'attribute' in item.keys() and 'another' in item.keys() and item['attribute'] == 'xxxx' and item['another'] == 'yy']) == multiset(got_items)

def test_scan_with_key_equality_filtering(dynamodb, filled_test_table):
    table, items = filled_test_table
    scan_filter_p = {
        "p" : {
            "AttributeValueList" : [ "7" ],
            "ComparisonOperator": "EQ"
        }
    }
    scan_filter_c = {
        "c" : {
            "AttributeValueList" : [ "9" ],
            "ComparisonOperator": "EQ"
        }
    }
    scan_filter_p_and_attribute = {
        "p" : {
            "AttributeValueList" : [ "7" ],
            "ComparisonOperator": "EQ"
        },
        "attribute" : {
            "AttributeValueList" : [ "x"*7 ],
            "ComparisonOperator": "EQ"
        }
    }
    scan_filter_c_and_another = {
        "c" : {
            "AttributeValueList" : [ "9" ],
            "ComparisonOperator": "EQ"
        },
        "another" : {
            "AttributeValueList" : [ "y"*16 ],
            "ComparisonOperator": "EQ"
        }
    }

    # Filtering on the hash key
    got_items = full_scan(table, ScanFilter=scan_filter_p)
    expected_items = [item for item in items if "p" in item.keys() and item["p"] == "7" ]
    assert multiset(expected_items) == multiset(got_items)

    # Filtering on the sort key
    got_items = full_scan(table, ScanFilter=scan_filter_c)
    expected_items = [item for item in items if "c" in item.keys() and item["c"] == "9"]
    assert multiset(expected_items) == multiset(got_items)

    # Filtering on the hash key and an attribute
    got_items = full_scan(table, ScanFilter=scan_filter_p_and_attribute)
    expected_items = [item for item in items if "p" in item.keys() and "another" in item.keys() and item["p"] == "7" and item["another"] == "y"*16]
    assert multiset(expected_items) == multiset(got_items)

    # Filtering on the sort key and an attribute
    got_items = full_scan(table, ScanFilter=scan_filter_c_and_another)
    expected_items = [item for item in items if "c" in item.keys() and "another" in item.keys() and item["c"] == "9" and item["another"] == "y"*16]
    assert multiset(expected_items) == multiset(got_items)

# Test the "Select" parameter of Scan. The default Select mode,
# ALL_ATTRIBUTES, returns items with all their attributes. Other modes
# allow returning just specific attributes or just counting the results
# without returning items at all.
@pytest.mark.xfail(reason="Select not supported yet")
def test_scan_select(filled_test_table):
    test_table, items = filled_test_table
    got_items = full_scan(test_table)
    # By default, a scan returns all the items, with all their attributes:
    # query returns all attributes:
    got_items = full_scan(test_table)
    assert multiset(items) == multiset(got_items)
    # Select=ALL_ATTRIBUTES does exactly the same as the default - return
    # all attributes:
    got_items = full_scan(test_table, Select='ALL_ATTRIBUTES')
    assert multiset(items) == multiset(got_items)
    # Select=ALL_PROJECTED_ATTRIBUTES is not allowed on a base table (it
    # is just for indexes, when IndexName is specified)
    with pytest.raises(ClientError, match='ValidationException'):
        full_scan(test_table, Select='ALL_PROJECTED_ATTRIBUTES')
    # Select=SPECIFIC_ATTRIBUTES requires that either a AttributesToGet
    # or ProjectionExpression appears, but then really does nothing beyond
    # what AttributesToGet and ProjectionExpression already do:
    with pytest.raises(ClientError, match='ValidationException'):
        full_scan(test_table, Select='SPECIFIC_ATTRIBUTES')
    wanted = ['c', 'another']
    got_items = full_scan(test_table, Select='SPECIFIC_ATTRIBUTES', AttributesToGet=wanted)
    expected_items = [{k: x[k] for k in wanted if k in x} for x in items]
    assert multiset(expected_items) == multiset(got_items)
    got_items = full_scan(test_table, Select='SPECIFIC_ATTRIBUTES', ProjectionExpression=','.join(wanted))
    assert multiset(expected_items) == multiset(got_items)
    # Select=COUNT just returns a count - not any items
    (got_count, got_items) = full_scan_and_count(test_table, Select='COUNT')
    assert got_count == len(items)
    assert got_items == []
    # Check that we also get a count in regular scans - not just with
    # Select=COUNT, but without Select=COUNT we both items and count:
    (got_count, got_items) = full_scan_and_count(test_table)
    assert got_count == len(items)
    assert multiset(items) == multiset(got_items)
    # Select with some unknown string generates a validation exception:
    with pytest.raises(ClientError, match='ValidationException'):
        full_scan(test_table, Select='UNKNOWN')

# Test parallel scan, i.e., the Segments and TotalSegments options.
# In the following test we check that these parameters allow splitting
# a scan into multiple parts, and that these parts are in fact disjoint,
# and their union is the entire contents of the table. We do not actually
# try to run these queries in *parallel* in this test.
def test_scan_parallel(filled_test_table):
    test_table, items = filled_test_table
    for nsegments in [1, 2, 17]:
        print('Testing TotalSegments={}'.format(nsegments))
        got_items = []
        for segment in range(nsegments):
            got_items.extend(full_scan(test_table, TotalSegments=nsegments, Segment=segment))
        # The following comparison verifies that each of the expected item
        # in items was returned in one - and just one - of the segments.
        assert multiset(items) == multiset(got_items)

# Test correct handling of incorrect parallel scan parameters.
# Most of the corner cases (like TotalSegments=0) are validated
# by boto3 itself, but some checks can still be performed.
def test_scan_parallel_incorrect(filled_test_table):
    test_table, items = filled_test_table
    with pytest.raises(ClientError, match='ValidationException.*Segment'):
        full_scan(test_table, TotalSegments=1000001, Segment=0)
    for segment in [7, 9]:
        with pytest.raises(ClientError, match='ValidationException.*Segment'):
            full_scan(test_table, TotalSegments=5, Segment=segment)
