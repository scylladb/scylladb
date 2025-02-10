# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests of for UpdateTable's GlobalSecondaryIndexUpdates option for modifying
# the GSIs (Global Secondary Indexes) on an existing table: Adding a GSI to
# an existing table, removing a GSI from a table, and updating an existing GSI.
# This feature was issue #11567, so all tests in this file reproduce
# various cases of that issue.

import pytest
import time
from botocore.exceptions import ClientError
from .util import random_string, full_scan, full_query, multiset, \
    new_test_table, wait_for_gsi, wait_for_gsi_gone
from .test_gsi import assert_index_query

# All tests in test_gsi.py involved creating a new table with a GSI up-front.
# This test will be about creating a base table *without* a GSI, putting data
# in it, and then adding a GSI with the UpdateTable operation. This starts
# a backfilling stage - where data is copied to the index - and when this
# stage is done, the index is usable. Items whose indexed column contains
# the wrong type are silently ignored and not added to the index. We also
# check that after adding the GSI, it is no longer possible to add more
# items with wrong types to the base table.
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

# Another test similar to the above test_gsi_backfill(), but here we add
# a GSI to a table that already has an LSI with the same key column, and
# check that the new GSI works. In Alternator's implementation, the LSI key
# column will become a real column in the schema, and the GSI needs to use
# that instead of the usual computed column.
def test_gsi_backfill_with_lsi(dynamodb):
    # First create, and fill, a table with an LSI but without GSI.
    with new_test_table(dynamodb,
            KeySchema=[
                # Must have both hash key and range key to allow LSI creation
                { 'AttributeName': 'p', 'KeyType': 'HASH' },
                { 'AttributeName': 'c', 'KeyType': 'RANGE' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'p', 'AttributeType': 'S' },
                { 'AttributeName': 'c', 'AttributeType': 'S' },
                { 'AttributeName': 'x', 'AttributeType': 'S' },
            ],
            LocalSecondaryIndexes=[
                {   'IndexName': 'lsi',
                    'KeySchema': [
                        { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'x', 'KeyType': 'RANGE' },
                    ],
                    'Projection': { 'ProjectionType': 'ALL' }
                }
            ]) as table:
        items = [{'p': random_string(), 'c': random_string(), 'x': random_string(), 'y': random_string()} for i in range(10)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        assert multiset(items) == multiset(full_scan(table))
        # Now use UpdateTable to create the GSI
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'S' }],
            GlobalSecondaryIndexUpdates=[ {  'Create':
                {   'IndexName': 'gsi',
                    'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }}])
        wait_for_gsi(table, 'gsi')
        # Check that the GSI got backfilled as expected.
        assert multiset(items) == multiset(full_scan(table, ConsistentRead=False, IndexName='gsi'))
        # Let's also test that we cannot add a GSI with the same name as an
        # already existing LSI (see test_lsi.py::test_lsi_and_gsi_same_same
        # for an explanation why this is so)
        with pytest.raises(ClientError, match='ValidationException.*already exists'):
            dynamodb.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=[{ 'AttributeName': 'y', 'AttributeType': 'S' }],
                GlobalSecondaryIndexUpdates=[ {  'Create':
                    {  'IndexName': 'lsi',
                        'KeySchema': [{ 'AttributeName': 'y', 'KeyType': 'HASH' }],
                        'Projection': { 'ProjectionType': 'ALL' }
                    }}])
        # When we created the GSI above we had to specify in
        # AttributeDefinitons the type of "x", even though DynamoDB already
        # knows it (because it's already a base key column). Let's verify we
        # aren't allowed to skip this AttributeDefinitions, even if it's
        # redundant:
        with pytest.raises(ClientError, match='ValidationException.*AttributeDefinitions'):
            dynamodb.meta.client.update_table(TableName=table.name,
                GlobalSecondaryIndexUpdates=[ {  'Create':
                    {   'IndexName': 'gsi2',
                        'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                        'Projection': { 'ProjectionType': 'ALL' }
                    }}])

# Another test similar to the above test_gsi_backfill_with_lsi() where we
# checked the case of a new GSI key being a real column because it was an
# LSI key. In this test the GSI key is a real column because it was a
# key column of the base table itself.
def test_gsi_backfill_with_real_column(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[
                { 'AttributeName': 'p', 'KeyType': 'HASH' },
                { 'AttributeName': 'c', 'KeyType': 'RANGE' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'p', 'AttributeType': 'S' },
                { 'AttributeName': 'c', 'AttributeType': 'S' },
            ]) as table:
        items = [{'p': random_string(), 'c': random_string(), 'x': random_string(), 'y': random_string()} for i in range(10)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        assert multiset(items) == multiset(full_scan(table))
        # Now use UpdateTable to create the GSI, with the key "c", already
        # a key column in the base table
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{ 'AttributeName': 'c', 'AttributeType': 'S' }],
            GlobalSecondaryIndexUpdates=[ {  'Create':
                {   'IndexName': 'gsi',
                    'KeySchema': [{ 'AttributeName': 'c', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }}])
        wait_for_gsi(table, 'gsi')
        assert multiset(items) == multiset(full_scan(table, ConsistentRead=False, IndexName='gsi'))

# Test deleting an existing GSI using UpdateTable
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

# Another test for deleting a GSI using UpdateTable, similar to the previous
# test test_gsi_delete() but in this test we *also* have an LSI whose range
# key is the same attribute used by the GSI. It should be legal to delete the
# GSI, but after the deletion the restriction of the type of the column is
# still enforced because it is still an LSI key. In Alternator's
# implementation this happens because the LSI key column was - and remains -
# a real column in the schema.
def test_gsi_delete_with_lsi(dynamodb):
    # A table whose non-key column "x" serves as a range key in an LSI,
    # and partition key in a GSI.
    with new_test_table(dynamodb,
        KeySchema=[
            # Must have both hash key and range key to allow LSI creation
            { 'AttributeName': 'p', 'KeyType': 'HASH' },
            { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'c', 'AttributeType': 'S' },
            { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'lsi',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'x', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'gsi',
                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ]) as table:
        # We shouldn't need to wait for a GSI created together with the
        # table, but let's do it anyway to work around bug #9059 (which
        # isn't what this test is trying to reproduce).
        wait_for_gsi(table, 'gsi')
        items = [{'p': random_string(), 'c': random_string(), 'x': random_string()} for i in range(10)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        # So far, we have the GSI for "x" and can use it:
        assert_index_query(table, 'gsi', [items[3]],
            KeyConditions={'x': {'AttributeValueList': [items[3]['x']], 'ComparisonOperator': 'EQ'}})
        # As a sanity check, see we can't create a GSI called 'gsi' because
        # this name is already taken.
        with pytest.raises(ClientError, match='ValidationException.*already exists'):
            dynamodb.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'S' }],
                GlobalSecondaryIndexUpdates=[{ 'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }}}])
        # Now use UpdateTable to delete the GSI for "x"
        dynamodb.meta.client.update_table(TableName=table.name,
            GlobalSecondaryIndexUpdates=[{ 'Delete': { 'IndexName': 'gsi' } }])
        # update_table is an asynchronous operation. We need to wait until it
        # finishes and the GSI is removed.
        wait_for_gsi_gone(table, 'gsi')
        # Now index is gone. We can no longer query using it.
        with pytest.raises(ClientError, match='ValidationException.*gsi'):
            full_query(table, ConsistentRead=False, IndexName='gsi',
                KeyConditions={'x': {'AttributeValueList': [items[3]['x']], 'ComparisonOperator': 'EQ'}})
        # The attribute "x" is still a LSI key of type S, so we still aren't
        # allowed to insert items with a number for x.
        with pytest.raises(ClientError, match='ValidationException.*mismatch'):
            table.put_item(Item={'p': random_string(), 'c': random_string(), 'x': 7})
        # Of course, we can't delete the same GSI again after it's deleted
        with pytest.raises(ClientError, match='ResourceNotFoundException'):
            dynamodb.meta.client.update_table(TableName=table.name,
                GlobalSecondaryIndexUpdates=[{ 'Delete': { 'IndexName': 'gsi' } }])
        # A GSI can be deleted, but an LSI can't. Alternator and DynamoDB
        # give different errors in this case - Alternator gives a
        # ResourceNotFoundException since no such GSI exists, but DynamoDB
        # gives a ValidationException saying it knows it's an index but
        # it's not a GSI. I think this difference is acceptable.
        with pytest.raises(ClientError, match='ResourceNotFoundException|ValidationException'):
            dynamodb.meta.client.update_table(TableName=table.name,
                GlobalSecondaryIndexUpdates=[{ 'Delete': { 'IndexName': 'lsi' } }])

# In the previous tests we performed a UpdateTable GSI Create or Delete
# operation on a table set up by CreateTable. In this test we try several
# of these operations in sequence, to check we can add more than one GSI,
# delete a GSI that we just added, recreate a GSI that we just deleted, etc.
def test_gsi_creates_and_deletes(dynamodb):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]
    }
    create_gsi = lambda name, key: {
        'AttributeDefinitions': [{ 'AttributeName': key, 'AttributeType': 'S' }],
        'GlobalSecondaryIndexUpdates': [
            {  'Create':
                {  'IndexName': name,
                    'KeySchema': [{ 'AttributeName': key, 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }
            }
        ]}
    delete_gsi = lambda name: {
        'GlobalSecondaryIndexUpdates': [
            {  'Delete': { 'IndexName': name } }
        ]}
    with new_test_table(dynamodb, **schema) as table:
        items = [{'p': random_string(), 'x': random_string(), 'y': random_string()} for i in range(10)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        # Create indexes gsi1 and gsi2 for "x" and for "y" respectively,
        # in two separate UpdateTable requests:
        dynamodb.meta.client.update_table(
            TableName=table.name, **create_gsi('gsi1', 'x'))
        wait_for_gsi(table, 'gsi1')
        dynamodb.meta.client.update_table(
            TableName=table.name, **create_gsi('gsi2', 'y'))
        wait_for_gsi(table, 'gsi2')
        # We have the index for "x" and "y" and can use it. We don't
        # need a retry loop (like assert_index_query()) because we waited
        # for the view build to complete.
        assert [items[3]] == full_query(table,
            ConsistentRead=False, IndexName='gsi1',
            KeyConditions={'x': {'AttributeValueList': [items[3]['x']], 'ComparisonOperator': 'EQ'}})
        assert [items[3]] == full_query(table,
            ConsistentRead=False, IndexName='gsi2',
            KeyConditions={'y': {'AttributeValueList': [items[3]['y']], 'ComparisonOperator': 'EQ'}})
        # Now delete the GSI for "x" that we added above:
        dynamodb.meta.client.update_table(
            TableName=table.name, **delete_gsi('gsi1'))
        wait_for_gsi_gone(table, 'gsi1')
        # Now index for x is gone. We cannot query using it, but index for y
        # is still there:
        with pytest.raises(ClientError, match='ValidationException.*gsi1'):
            full_query(table, ConsistentRead=False, IndexName='gsi1',
                KeyConditions={'x': {'AttributeValueList': [items[3]['x']], 'ComparisonOperator': 'EQ'}})
        assert [items[3]] == full_query(table,
            ConsistentRead=False, IndexName='gsi2',
            KeyConditions={'y': {'AttributeValueList': [items[3]['y']], 'ComparisonOperator': 'EQ'}})
        # Finally, re-add an index gsi1 for "x", and see it begins to work
        # again:
        dynamodb.meta.client.update_table(
            TableName=table.name, **create_gsi('gsi1', 'x'))
        wait_for_gsi(table, 'gsi1')
        assert [items[3]] == full_query(table,
            ConsistentRead=False, IndexName='gsi1',
            KeyConditions={'x': {'AttributeValueList': [items[3]['x']], 'ComparisonOperator': 'EQ'}})

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

@pytest.fixture(scope="module")
def table1(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[
                { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[
                { 'AttributeName': 'p', 'AttributeType': 'S' },
            ]) as table:
        yield table

# An empty update_table() call, without any parameters changed, is not allowed.
def test_updatetable_empty(dynamodb, table1):
    with pytest.raises(ClientError, match='ValidationException.*UpdateTable'):
        dynamodb.meta.client.update_table(TableName=table1.name)
    # An empty GlobalSecondaryIndexUpdates array is the same as no parameter
    # at all:
    with pytest.raises(ClientError, match='ValidationException.*UpdateTable'):
        dynamodb.meta.client.update_table(TableName=table1.name,
            GlobalSecondaryIndexUpdates=[])

# Test various invalid cases of UpdateTable's GlobalSecondaryIndexUpdates.
def test_gsi_updatetable_errors(dynamodb, table1):
    client = dynamodb.meta.client

    # Each operation in the GlobalSecondaryIndexUpdates array must contain
    # some operation - Create, Update, or Delete. It can't be an empty map.
    with pytest.raises(ClientError, match='ValidationException.*GlobalSecondaryIndexUpdate'):
        dynamodb.meta.client.update_table(TableName=table1.name,
            GlobalSecondaryIndexUpdates=[{}])

    # Allowed operations in GlobalSecondaryIndexUpdates are Create, Update
    # and Delete. An unsupported operation like "Dog" should result in a
    # validation error.

    # Unfortunately, botocore through its service-description file
    # botocore/data/dynamodb/2012-08-10/service-2.json knows which
    # operations are valid and fails to serialize the parameter to "Dog"
    # so let's monkey-patch botocore's internal service model to allow
    # the "Dog" to behave like "Create", to allow the request to be sent
    # to the server - and let the server reject this invalid operation.
    service_model = client.meta.service_model
    client.meta.service_model._instance_cache = {} # clear cached shapes
    shape_resolver = service_model._shape_resolver
    shape = shape_resolver._shape_map['GlobalSecondaryIndexUpdate']
    shape['members']['Dog'] = shape['members']['Create']

    with pytest.raises(ClientError, match='ValidationException.*GlobalSecondaryIndexUpdate'):
        client.update_table(TableName=table1.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'N' }],
            GlobalSecondaryIndexUpdates=[ {  'Dog':
            {  'IndexName': 'ind',
                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }}])

    # A single map in the GlobalSecondaryIndexUpdates array can't have both
    # Create and Delete entries, for example:
    with pytest.raises(ClientError, match='ValidationException.*one'):
        client.update_table(TableName=table1.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'N' }],
            GlobalSecondaryIndexUpdates=[
                {  'Create': {  'IndexName': 'ind',
                                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                                'Projection': { 'ProjectionType': 'ALL' } },
                   'Delete': {  'IndexName': 'xyz' }
                }])

    # GlobalSecondaryIndexUpdates can also have more than one seaprate map
    # in the array, each supposedly indicating a different operation, but
    # creating more than one GSI in the same UpdateTable is NOT allowed.
    # DynamoDB throws a LimitExceededException:
    with pytest.raises(ClientError, match='LimitExceededException'):
        client.update_table(TableName=table1.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'N' }],
            GlobalSecondaryIndexUpdates=[
                {  'Create': {  'IndexName': 'ind1',
                                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                                'Projection': { 'ProjectionType': 'ALL' } }
                },
                {  'Create': {  'IndexName': 'ind2',
                                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                            'Projection': { 'ProjectionType': 'ALL' } }
                },
            ])

    # Similarly, can't delete two GSIs in one UpdateTable operation:
    with pytest.raises(ClientError, match='LimitExceededException'):
        client.update_table(TableName=table1.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'N' }],
            GlobalSecondaryIndexUpdates=[
                { 'Delete': {  'IndexName': 'ind1' } },
                { 'Delete': {  'IndexName': 'ind2' } }
            ])

    # Similarly, can't delete a GSI and create another one:
    with pytest.raises(ClientError, match='LimitExceededException'):
        client.update_table(TableName=table1.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'N' }],
            GlobalSecondaryIndexUpdates=[
                {  'Create': {  'IndexName': 'ind1',
                                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                                'Projection': { 'ProjectionType': 'ALL' } }
                },
                { 'Delete': {  'IndexName': 'ind2' } }
            ])

# Whereas CreateTable rejects spurious entries in AttributeDefinitions
# (entries which aren't used as a key of the table or any GSI or LSI),
# as tested in test_table.py::test_create_table_spurious_attribute_definitions
# it turns out that in UpdateTable when creating a GSI, spurious attribute
# definitions are *not* detected in DynamoDB, and silently ignored.
# In Alternator, we decided to detect this case anyway - it can help users
# notice problems (see #19784). So because we differ from DynamoDB on this,
# this test is marked scylla_only.
def test_gsi_updatetable_spurious_attribute_definitions(table1, scylla_only):
    with pytest.raises(ClientError, match='ValidationException.*AttributeDefinitions'):
        table1.meta.client.update_table(TableName=table1.name,
            AttributeDefinitions=[
                { 'AttributeName': 'x', 'AttributeType': 'S' },
                { 'AttributeName': 'y', 'AttributeType': 'S' }],
            GlobalSecondaryIndexUpdates=[ {  'Create':
                {   'IndexName': 'gsi2',
                    'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }}])
        # Just in case the update_table didn't fail as expected...
        wait_for_gsi(table1, 'gsi2')

# Check that attempting to delete a GSI that doesn't exist results in
# the expected ResourceNotFoundException.
def test_updatetable_delete_missing_gsi(dynamodb, table1):
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        dynamodb.meta.client.update_table(TableName=table1.name,
            GlobalSecondaryIndexUpdates=[{  'Delete':
                { 'IndexName': 'nonexistent' } }])

# Whereas DynamoDB allows attribute values to reach a generous length (they
# are only limited by the item's size limit, 400 KB), an attribute which is
# a *key* has much stricter limits - 2048 bytes for a partition key, 1024
# bytes for a sort key. This means that if a table has a GSI or LSI and
# one of the attributes serves as a key in that GSI and LSI, DynamoDB
# limits its length. In the tests test_gsi.py::test_gsi_limit_* we verified
# that attempts to write an oversized value to an attribute which is a
# GSI key are rejected. Here we test what happens when adding a GSI to
# a table with pre-existing data, which already includes items with oversized
# values for the key attribute. These items can't be "rejected" - they
# are already in the base table - but should be skipped while filling the
# GSI. What we don't want to happen is to see the view building hang,
# as described in issue #8627 and #10347.
# The first test here, test_gsi_backfill_oversized_key(), doesn't check the
# specific limits of 2048 and 1024 bytes, it only checks that an item with
# a 65 KB attribute (above Scylla's internal limitations for keys) are
# cleanly skipped and don't cause view build hangs. The following test
# test_gsi_backfill_key_limits will check the specific limits.
def test_gsi_backfill_oversized_key(dynamodb):
    # First create, and fill, a table without GSI:
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' },
                                   { 'AttributeName': 'c', 'AttributeType': 'S' } ]) as table:
        p1 = random_string()
        p2 = random_string()
        c = random_string()
        # Create two items, one has a small "x" attribute, the other has
        # a 65 KB "x" attribute.
        table.put_item(Item={'p': p1, 'c': c, 'x': 'hello'})
        table.put_item(Item={'p': p2, 'c': c, 'x': 'a'*66500})
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
        # Verify that the items with the oversized x are missing from both
        # GSIs, so only the one item with x = hello should appear in both.
        # Note that we don't need to retry the reads here (i.e., use the
        # assert_index_scan() or assert_index_query() functions) because after
        # we waited for backfilling to complete, we know all the pre-existing
        # data is already in the index.
        assert [{'p': p1, 'c': c, 'x': 'hello'}] == full_scan(table, ConsistentRead=False, IndexName='index1')
        assert [{'p': p1, 'c': c, 'x': 'hello'}] == full_scan(table, ConsistentRead=False, IndexName='index2')

# The previous test, test_gsi_backfill_oversized_key(), checked that a
# grossly oversized GSI key attribute (over Scylla's internal key limit
# of 64 KB) doesn't hang the view building process. This test verifies
# more specifically that DynamoDB's documented limits - 2048 bytes for
# a GSI partition key and 1024 for a GSI sort key - are implemented. An
# item that has an attribute longer than that should simply be skipped
# during view building.
# Reproduces issue #10347.
@pytest.mark.xfail(reason="issue #10347: key length limits not enforced")
def test_gsi_backfill_key_limits(dynamodb):
    # First create, and fill, a table without GSI:
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' },
                                   { 'AttributeName': 'c', 'AttributeType': 'S' } ]) as table:
        # Create four items, with 'x' attribute sizes of 1024, 1025, 2048
        # and 2049. Only one item (1024) has x suitable for a sort key,
        # and three (1024, 1025 and 2048) have length suitable for a partition
        # key. The unsuitable items will be missing from the indexes.
        lengths = [1024, 1025, 2048, 2049]
        p = [random_string() for length in lengths]
        x = ['a'*length for length in lengths]
        c = random_string()
        for i in range(len(lengths)):
            table.put_item(Item={'p': p[i], 'c': c, 'x': x[i]})
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
        # Verify that the items with the oversized x are missing from both
        # GSIs. For index1 (x is a partition key, limited to 2048 bytes)
        # items 0,1,2 should appear, for index2 (x is a sort key, limited
        # to 1024 bytes), only item 0 should appear.
        assert multiset([{'p': p[i], 'c': c, 'x': x[i]} for i in range(3)]) == multiset(full_scan(table, ConsistentRead=False, IndexName='index1'))
        assert [{'p': p[0], 'c': c, 'x': x[0]}] == full_scan(table, ConsistentRead=False, IndexName='index2')
