# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for the Scan operation

import pytest
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from test.alternator.util import random_bytes, full_scan, full_scan_and_count, multiset, new_test_table


# Test that scanning works fine with/without pagination
def test_scan_basic(filled_test_table):
    test_table, items = filled_test_table
    for limit in [None,1,2,4,33,50,100,9007,16*1024*1024]:
        pos = None
        got_items = []
        while True:
            if limit:
                response = test_table.scan(Limit=limit, ConsistentRead=True, ExclusiveStartKey=pos) if pos else test_table.scan(Limit=limit, ConsistentRead=True)
                assert len(response['Items']) <= limit
            else:
                response = test_table.scan(ExclusiveStartKey=pos, ConsistentRead=True) if pos else test_table.scan(ConsistentRead=True)
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
    # If either AttributesToGet or ProjectionExpression appear, only
    # Select=SPECIFIC_ATTRIBUTES (or nothing) is allowed - other Select
    # settings contradict the AttributesToGet or ProjectionExpression, and
    # therefore forbidden:
    with pytest.raises(ClientError, match='ValidationException.*AttributesToGet'):
        full_scan(test_table, Select='ALL_ATTRIBUTES', AttributesToGet=['x'])
    with pytest.raises(ClientError, match='ValidationException.*AttributesToGet'):
        full_scan(test_table, Select='COUNT', AttributesToGet=['x'])
    with pytest.raises(ClientError, match='ValidationException.*ProjectionExpression'):
        full_scan(test_table, Select='ALL_ATTRIBUTES', ProjectionExpression='x')
    with pytest.raises(ClientError, match='ValidationException.*ProjectionExpression'):
        full_scan(test_table, Select='COUNT', ProjectionExpression='x')

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

# ExclusiveStartKey must lie within the segment when using Segment/TotalSegment.
def test_scan_parallel_with_exclusive_start_key(filled_test_table):
    test_table, items = filled_test_table
    with pytest.raises(ClientError, match='ValidationException.*Exclusive'):
        full_scan(test_table, TotalSegments=1000000, Segment=0, ExclusiveStartKey={'p': '0', 'c': '0'})

# We used to have a bug with formatting of LastEvaluatedKey in the response
# of Query and Scan with bytes keys (issue #7768). In test_query_paging_byte()
# (test_query.py) we tested the case of bytes *sort* keys. In the following
# test we check bytes *partition* keys.
def test_scan_paging_bytes(test_table_b):
    # We will not Scan the entire table - we have no idea what it contains.
    # But we don't need to scan the entire table - we just need the table
    # to contain at least two items, and then Scan it with Limit=1 and stop
    # after one page. Before #7768 was fixed, the test failed when the
    # LastEvaluatedKey in the response could not be parsed.
    items = [{'p': random_bytes()}, {'p': random_bytes()}]
    with test_table_b.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    response = test_table_b.scan(ConsistentRead=True, Limit=1)
    assert 'LastEvaluatedKey' in response

# A fixture to read query_tombstone_page_limit from Scylla's configuration.
# A test using this fixture will be skipped if the test is not running
# against Scylla.
@pytest.fixture(scope="session")
def query_tombstone_page_limit(dynamodb, scylla_only):
    config_table = dynamodb.Table('.scylla.alternator.system.config')
    return int(config_table.query(
            KeyConditionExpression='#key=:val',
            ExpressionAttributeNames={'#key': 'name'},
            ExpressionAttributeValues={':val': 'query_tombstone_page_limit'}
        )['Items'][0]['value'])

# The following two tests reproduced issue #7933, where a scan encounters a
# long string of row/partition tombstones, and because it is expected to
# return a page in a constant amount of time, it should stop in the middle
# of long that string of tombstones.
# Before #7933 was fixed, there was no official threshold number of
# tombstones over which we know that paging must stop a page, so we could
# only demonstrate the bug asymptotically: If we have two live items with
# N consecutive tombstones between then, we know that there must be a large
# enough such N for which fetching the first page should return just the
# first live item - not both live items. If we try with arbitrarily large N,
# and the time to read the single page grows as O(N) but still always get
# two results - this is a bug.
# But now #7933 is fixed, and we do have an official threshold - configured
# by "query_tombstone_page_limit" and defaulting to 10,000. As soon as our
# test has more than this number of tombstones, we know the page must stop.
# So we can test that this indeed happens - without needing to reach huge
# sizes.
#
# In the first test, we create a single long partition with just two
# live items and a long contiguous string of row tombstones between them.
# A Scan of this partition should be able to stop in the middle of this
# string and *not* return both live items in a single page - because
# returning both live items takes an unbounded amount of time (proportional
# to the number of tombstones), and retrieving a single page must take a
# bounded amount of time. Reproduces issue #7933.
#
# This test is marked "scylla_only" because it doesn't really test something
# that *has* to happen - there is no reason in the DynamoDB API why reading
# a partition with a lot of deleted data must take a long time, or that if
# we do, that it will stop after a known number of tombstones. But we do
# expect this in Scylla's tombstone-based implementation - and in that
# implementation, and only in that implementation, we can require the scan
# to stop early, and also that we know when- after "query_tombstone_page_limit".
def test_scan_long_row_tombstone_string(dynamodb, query_tombstone_page_limit):
    N = query_tombstone_page_limit + 10
    with new_test_table(dynamodb,
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' },
                   { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'N' },
                               { 'AttributeName': 'c', 'AttributeType': 'N' }]
        ) as table:
        # Create two items with c=0 and c=N, and N-2 deletions between them.
        # Although the deleted items never existed, Scylla is forced to keep
        # tombstones for them.
        with table.batch_writer() as batch:
            batch.put_item(Item={ 'p': 1, 'c': 0 })
            for i in range(1, N-1):
                batch.delete_item(Key={ 'p': 1, 'c': i })
            batch.put_item(Item={ 'p': 1, 'c': N })
        response = table.scan()

        found = len(response['Items'])
        assert found == 1

        # Let's finish the scan for as many pages as it takes (some of them
        # may be empty!), and confirm we eventually got the two results.
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            found += len(response['Items'])
        assert found == 2

# A similar test to the above, but here we have a table with two partitions
# separated by a long string of partition tombstones. Again, the Scan should
# be able to stop in the middle of that string, and not return both partitions
# in a single page.
def test_scan_long_partition_tombstone_string(dynamodb, query_tombstone_page_limit):
    # Unfortunately, unlike strings of row-tombstones which end a page after
    # exactly query_tombstone_page_limit, in the case of partition tombstones
    # the limit applies to separate vnode subscans, so we need to have
    # significantly more before the split. Experimentally "* 8" works here
    # with test/alternator/run, but may need to be changed in the future.
    N = int(query_tombstone_page_limit * 8)
    with new_test_table(dynamodb,
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'N' }],
        # This test does a lot of writes, so let's do them without LWT
        # to make the test less slow.
        Tags=[{'Key': 'system:write_isolation', 'Value': 'forbid_rmw'}]
        ) as table:
        # We want to have two live partitions with a lot of partition
        # tombstones between them. But the hash function is pseudo-random
        # so we don't know which partitions would be the first and last.
        # As a workaround, let's begin by writing just 100 partitions, then
        # read them back and keep the first and last one of those live. If
        # the hash function is random enough, 99% of the partitions we'll
        # write later will fall between those two partitions.
        with table.batch_writer() as batch:
            for i in range(100):
                batch.put_item(Item={ 'p': i })
        r = table.scan()
        first = r['Items'][0]['p']
        while 'LastEvaluatedKey' in r:
            r = table.scan(ExclusiveStartKey=r['LastEvaluatedKey'])
        last = r['Items'][-1]['p']
        print("first", first, "last", last)
        # Now write all N items - all except "first" and "last" are deletions
        with table.batch_writer() as batch:
            for i in range(N):
                if i == first or i == last:
                    batch.put_item(Item={ 'p': i })
                else:
                    batch.delete_item(Key={ 'p': i })
        response = table.scan()

        found = len(response['Items'])
        assert found == 1

        # Let's finish the scan for as many pages as it takes (some of them
        # may be empty!), and confirm we eventually got the two results.
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            found += len(response['Items'])
        assert found == 2

# Verify that even if no "Limit" is specified for a Scan, the size of a
# single returned page is still limited. DynamoDB specifies it should be
# limited to 1 MB. In Alternator the limit is close to 1 MB, but it turns
# out (see issue #10327) that for small tables the page size can grow up
# to 3 MB. The following test accepts this as ok. Note that for larger tables,
# the page size goes back to being closer to 1 MB.
#
# This test is for Scan paging on a table with many small partitions. We have
# a separate test for a Query over a single long partition with many rows -
# test_query.py::test_query_reverse_longish (the test's name suggests it
# checks reverse queries, but it also checks the unreversed unlimited query).
# For single-partition scans, the page size is more exactly 1 MB.
def test_scan_paging_missing_limit(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[
                { 'AttributeName': 'p', 'AttributeType': 'N' }]) as table:
        # Insert a 6 MB of data in multiple smaller partitions.
        # Because of issue #10327 when the table is *small* Alternator may
        # return significantly more than 1 MB - sometimes even 4 MB. This
        # is why we need to use 6 MB of data here and 2 MB is not enough.
        str = 'x' * 10240
        N = 600
        with table.batch_writer() as batch:
            for i in range(N):
                batch.put_item({'p': i, 's': str})
        n = len(table.scan(ConsistentRead=True)['Items'])
        # we don't know how big n should be (hopefully around 100)
        # but definitely not N.
        assert n < N
