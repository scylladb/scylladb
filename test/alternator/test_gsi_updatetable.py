# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests of for UpdateTable's GlobalSecondaryIndexUpdates option for modifying
# the GSIs (Global Secondary Indexes) on an existing table: Adding a GSI to
# an existing table, removing a GSI from a table, and updating an existing GSI.
# This feature was issue #11567, so all tests in this file reproduce
# various cases of that issue.

import pytest
import time
from botocore.exceptions import ClientError
from .util import random_string, full_scan, full_query, multiset, \
    new_test_table
from .test_gsi import assert_index_query

# UpdateTable for creating a GSI is an asynchronous operation. The table's
# TableStatus changes from ACTIVE to UPDATING for a short while, and then
# goes back to ACTIVE, but the new GSI's IndexStatus appears as CREATING,
# until eventually (in Amazon DynamoDB - it tests a *long* time...) it
# becomes ACTIVE. During the CREATING phase, at some point the Backfilling
# attribute also appears, until it eventually disappears. We need to wait
# until all three markers indicate completion.
# Unfortunately, while boto3 has a client.get_waiter('table_exists') to
# wait for a table to exists, there is no such function to wait for an
# index to come up, so we need to code it ourselves.
def wait_for_gsi(table, gsi_name):
    start_time = time.time()
    # The timeout needs to be long because on Amazon DynamoDB, even on a
    # a tiny table, it sometimes takes minutes.
    while time.time() < start_time + 600:
        desc = table.meta.client.describe_table(TableName=table.name)
        table_status = desc['Table']['TableStatus']
        if table_status != 'ACTIVE':
            time.sleep(0.1)
            continue
        index_desc = [x for x in desc['Table']['GlobalSecondaryIndexes'] if x['IndexName'] == gsi_name]
        assert len(index_desc) == 1
        index_status = index_desc[0]['IndexStatus']
        if index_status != 'ACTIVE':
            time.sleep(0.1)
            continue
        # When the index is ACTIVE, this must be after backfilling completed
        assert not 'Backfilling' in index_desc[0]
        return
    raise AssertionError("wait_for_gsi did not complete")

# Similarly to how wait_for_gsi() waits for a GSI to finish adding,
# this function waits for a GSI to be finally deleted.
def wait_for_gsi_gone(table, gsi_name):
    start_time = time.time()
    while time.time() < start_time + 600:
        desc = table.meta.client.describe_table(TableName=table.name)
        table_status = desc['Table']['TableStatus']
        if table_status != 'ACTIVE':
            time.sleep(0.1)
            continue
        if 'GlobalSecondaryIndexes' in desc['Table']:
            index_desc = [x for x in desc['Table']['GlobalSecondaryIndexes'] if x['IndexName'] == gsi_name]
            if len(index_desc) != 0:
                index_status = index_desc[0]['IndexStatus']
                time.sleep(0.1)
                continue
        return
    raise AssertionError("wait_for_gsi_gone did not complete")

# All tests in test_gsi.py involved creating a new table with a GSI up-front.
# This test will be about creating a base table *without* a GSI, putting data
# in it, and then adding a GSI with the UpdateTable operation. This starts
# a backfilling stage - where data is copied to the index - and when this
# stage is done, the index is usable. Items whose indexed column contains
# the wrong type are silently ignored and not added to the index. We also
# check that after adding the GSI, it is no longer possible to add more
# items with wrong types to the base table.
@pytest.mark.xfail(reason="issue #11567")
def test_gsi_backfill(dynamodb):
    # First create, and fill, a table without GSI. The items in items1
    # will have the appropriate string type for 'x' and will later get
    # indexed. Items in item2 have no value for 'x', and in intems in
    # items3 'x' is in not a string; So the items in items2 and items3
    # will be missing in the index that we'll create later.
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
        items1 = [{'p': random_string(), 'x': random_string(), 'y': random_string()} for i in range(10)]
        items2 = [{'p': random_string(), 'y': random_string()} for i in range(10)]
        items3 = [{'p': random_string(), 'x': i} for i in range(10)]
        items = items1 + items2 + items3
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        assert multiset(items) == multiset(full_scan(table))
        # Now use UpdateTable to create the GSI
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'S' }],
            GlobalSecondaryIndexUpdates=[ {  'Create':
                {  'IndexName': 'hello',
                    'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }}])
        # update_table is an asynchronous operation. We need to wait until it
        # finishes and the table is backfilled.
        wait_for_gsi(table, 'hello')
        # As explained above, only items in items1 got copied to the gsi,
        # so this is what a full table scan on the GSI 'hello' should return.
        # Note that we don't need to retry the reads here (i.e., use the
        # assert_index_scan() function) because after we waited for
        # backfilling to complete, we know all the pre-existing data is
        # already in the index.
        assert multiset(items1) == multiset(full_scan(table, ConsistentRead=False, IndexName='hello'))
        # We can also use Query on the new GSI, to search on the attribute x:
        assert multiset([items1[3]]) == multiset(full_query(table,
            ConsistentRead=False, IndexName='hello',
            KeyConditions={'x': {'AttributeValueList': [items1[3]['x']], 'ComparisonOperator': 'EQ'}}))
        # Because the GSI now exists, we are no longer allowed to add to the
        # base table items with a wrong type for x (like we were able to add
        # earlier - see items3). But if x is missing (as in items2), we
        # *are* allowed to add the item and it appears in the base table
        # (but the view table doesn't change).
        p = random_string()
        y = random_string()
        table.put_item(Item={'p': p, 'y': y})
        assert table.get_item(Key={'p':  p}, ConsistentRead=True)['Item'] == {'p': p, 'y': y}
        with pytest.raises(ClientError, match='ValidationException.*mismatch'):
            table.put_item(Item={'p': random_string(), 'x': 3})

        # Let's also test that we cannot add another index with the same name
        # that already exists
        with pytest.raises(ClientError, match='ValidationException.*already exists'):
            dynamodb.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=[{ 'AttributeName': 'y', 'AttributeType': 'S' }],
                GlobalSecondaryIndexUpdates=[ {  'Create':
                    {  'IndexName': 'hello',
                        'KeySchema': [{ 'AttributeName': 'y', 'KeyType': 'HASH' }],
                        'Projection': { 'ProjectionType': 'ALL' }
                    }}])

# Test deleting an existing GSI using UpdateTable
@pytest.mark.xfail(reason="issue #11567")
def test_gsi_delete(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'x', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ]) as table:
        items = [{'p': random_string(), 'x': random_string()} for i in range(10)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        # So far, we have the GSI for "x" and can use it:
        assert_index_query(table, 'hello', [items[3]],
            KeyConditions={'x': {'AttributeValueList': [items[3]['x']], 'ComparisonOperator': 'EQ'}})
        # Now use UpdateTable to delete the GSI for "x"
        dynamodb.meta.client.update_table(TableName=table.name,
            GlobalSecondaryIndexUpdates=[{  'Delete':
                { 'IndexName': 'hello' } }])
        # update_table() is an asynchronous operation. We need to wait until
        # it finishes and the GSI is removed.
        wait_for_gsi_gone(table, 'hello')
        # Now index is gone. We cannot query using it.
        with pytest.raises(ClientError, match='ValidationException.*hello'):
            full_query(table, ConsistentRead=False, IndexName='hello',
                KeyConditions={'x': {'AttributeValueList': [items[3]['x']], 'ComparisonOperator': 'EQ'}})
        # When we had a GSI on x with type S, we weren't allowed to insert
        # items with a number for x. Now, after dropping this GSI, we should
        # be able to insert any type for x:
        p = random_string()
        table.put_item(Item={'p': p, 'x': 7})
        assert table.get_item(Key={'p':  p}, ConsistentRead=True)['Item'] == {'p': p, 'x': 7}

# As noted in test_gsi.py's test_gsi_empty_value(), setting an indexed string
# column to an empty string is rejected, since keys (including GSI keys) are
# not allowed to be empty strings or binary blobs.
# However, empty strings *are* legal for ordinary non-indexed attributes, so
# if the user adds a GSI to an existing table with pre-existing data, it might
# contain empty string values for the indexed keys. Such values should be
# skipped while filling the GSI - even if Scylla actually capable of
# representing such empty view keys (see issue #9375).
# Reproduces issue #9424.
@pytest.mark.xfail(reason="issue #9424")
def test_gsi_backfill_empty_string(dynamodb):
    # First create, and fill, a table without GSI:
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' },
                                   { 'AttributeName': 'c', 'AttributeType': 'S' } ]) as table:
        p1 = random_string()
        p2 = random_string()
        c = random_string()
        # Create two items, one has an empty-string "x" attribute, the other
        # is a non-empty string.
        table.put_item(Item={'p': p1, 'c': c, 'x': 'hello'})
        table.put_item(Item={'p': p2, 'c': c, 'x': ''})
        # Now use UpdateTable to create two GSIs. In one of them "x" will be
        # the partition key, and in the other "x" will be a sort key.
        # DynamoDB limits the number of indexes that can be added in one
        # UpdateTable command to just one, so we need to do it in two separate
        # commands and wait for each to complete.
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'S' }],
            GlobalSecondaryIndexUpdates=[
                { 'Create': { 'IndexName': 'index1',
                              'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                              'Projection': { 'ProjectionType': 'ALL' }}
                }
            ])
        wait_for_gsi(table, 'index1')
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'S' },
                                  { 'AttributeName': 'c', 'AttributeType': 'S' }],
            GlobalSecondaryIndexUpdates=[
                { 'Create': { 'IndexName': 'index2',
                              'KeySchema': [{ 'AttributeName': 'c', 'KeyType': 'HASH' },
                                            { 'AttributeName': 'x', 'KeyType': 'RANGE' }],
                              'Projection': { 'ProjectionType': 'ALL' }}
                }
            ])
        wait_for_gsi(table, 'index2')
        # Verify that the items with the empty-string x are missing from both
        # GSIs, so only the one item with x != '' should appear in both.
        # Note that we don't need to retry the reads here (i.e., use the
        # assert_index_scan() or assert_index_query() functions) because after
        # we waited for backfilling to complete, we know all the pre-existing
        # data is already in the index.
        assert [{'p': p1, 'c': c, 'x': 'hello'}] == full_scan(table, ConsistentRead=False, IndexName='index1')
        assert [{'p': p1, 'c': c, 'x': 'hello'}] == full_scan(table, ConsistentRead=False, IndexName='index2')

# Trying to create two different GSIs with different types for the same key is
# NOT allowed. The reason is that DynamoDB wants to insist that future writes
# to this attribute must have the declared type - and it can't insist on two
# different types at the same time.
# We have two versions of this test: One in test_gsi.py where the conflict
# happens during the table creation, and one here where the second GSI is
# added after the table already exists with the first GSI.
# Reproduces #13870.
@pytest.mark.xfail(reason="issue #11567")
def test_gsi_key_type_conflict_on_update(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'xyz', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'index1',
                'KeySchema': [{ 'AttributeName': 'xyz', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ]) as table:
        # Now use UpdateTable to create a second GSI, with a different
        # type for attribute xyz. DynamoDB gives a lengthy error message:
        #   "One or more parameter values were invalid: Attributes cannot be
        #    redefined. Please check that your attribute has the same type as
        #    previously defined.
        #    Existing schema: Schema:[SchemaElement: key{xyz:S:HASH}]
        #    New schema: Schema:[SchemaElement: key{xyz:N:HASH}]"
        with pytest.raises(ClientError, match='ValidationException.*redefined'):
            dynamodb.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=[{ 'AttributeName': 'xyz', 'AttributeType': 'N' }],
                GlobalSecondaryIndexUpdates=[ {  'Create':
                    {  'IndexName': 'index2',
                        'KeySchema': [{ 'AttributeName': 'xyz', 'KeyType': 'HASH' }],
                        'Projection': { 'ProjectionType': 'ALL' }
                    }}])
