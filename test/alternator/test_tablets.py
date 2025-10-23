# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for the Scylla-only "tablets" feature.
#
# Ideally, tablets are just an implementation detail (replacing the
# old vnodes), that the DynamoDB API user would not even be aware
# of. So there should be very few, if any, tests in this file.
# However, temporarily - while the tablets feature is only partially
# working and turned off by default (see issue #21989) - it is useful
# to have here a few tests that clarify the situation and how to
# override it. Most of these tests, or perhaps even this entire file,
# will probably go away eventually.

import pytest
import boto3
from botocore.exceptions import ClientError

from .util import new_test_table, wait_for_gsi, random_string, full_scan, full_query, multiset

# All tests in this file are scylla-only
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

# Utility function for checking if a given table is using tablets
# or not. We rely on some knowledge of Alternator internals:
# 1. For table with name X, Scylla creates a keyspace called alternator_X
# 2. We can read a CQL system table using the ".scylla.alternator." prefix.
def uses_tablets(dynamodb, table):
    info = dynamodb.Table('.scylla.alternator.system_schema.scylla_keyspaces')
    try:
        response = info.query(
            KeyConditions={'keyspace_name': {
                    'AttributeValueList': ['alternator_'+table.name],
                    'ComparisonOperator':  'EQ'}})
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        # The internal Scylla table doesn't even exist, either this isn't
        # Scylla or it's older Scylla and doesn't use tablets.
        return False
    if not 'Items' in response or not response['Items']:
        return False
    if 'initial_tablets' in response['Items'][0] and response['Items'][0]['initial_tablets']:
        return True
    return False

# Right now, new Alternator tables are created *without* tablets.
# This test should be changed if this default ever changes.
def test_default_tablets(dynamodb):
    schema = {
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]}
    with new_test_table(dynamodb, **schema) as table:
        # Change this assertion if Alternator's default changes!
        assert not uses_tablets(dynamodb, table)

# Tests for the initial_tablets tag. Currently, it is considered
# experimental, and named "experimental:initial_tablets", but perhaps
# in the future it will graduate out of experimental status and
# the prefix will be replaced by "system:".
initial_tablets_tag = 'experimental:initial_tablets'

# Check that a table created with a number as initial_tablets will use 
# tablets. Different numbers have different meanings (0 asked to use
# default number, any other number overrides the default) but they
# all enable tablets.
def test_initial_tablets_number(dynamodb):
    for value in ['0', '4']:
        schema = {
            'Tags': [{'Key': initial_tablets_tag, 'Value': value}],
            'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
            'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]}
        with new_test_table(dynamodb, **schema) as table:
            assert uses_tablets(dynamodb, table)

# Check that a table created with a non-number (e.g., the string "none")
# as initial_tablets, will not use tablets.
def test_initial_tablets_number(dynamodb):
    schema = {
        'Tags': [{'Key': initial_tablets_tag, 'Value': 'none'}],
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]}
    with new_test_table(dynamodb, **schema) as table:
        assert not uses_tablets(dynamodb, table)

# Before Alternator Streams is supported with tablets (#23838), let's verify
# that enabling Streams results in an orderly error. This test should be
# deleted when #23838 is fixed.
def test_streams_enable_error_with_tablets(dynamodb):
    # Test attempting to create a table already with streams
    with pytest.raises(ClientError, match='ValidationException.*tablets'):
        with new_test_table(dynamodb,
            Tags=[{'Key': initial_tablets_tag, 'Value': '4'}],
            StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'},
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
            AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
            pass
    # Test attempting to add a stream to an existing table
    with new_test_table(dynamodb,
        Tags=[{'Key': initial_tablets_tag, 'Value': '4'}],
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
        with pytest.raises(ClientError, match='ValidationException.*tablets'):
            table.update(StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'});

# For a while (see #18068) it was possible to create an Alternator table with
# tablets enabled and choose LWT for write isolation (always_use_lwt)
# but the writes themselves failed. This test verifies that this is no longer
# the case, and the LWT writes succeed even when tables are used.
def test_alternator_tablets_and_lwt(dynamodb):
    schema = {
        'Tags': [
            {'Key': initial_tablets_tag, 'Value': '0'},
            {'Key': 'system:write_isolation', 'Value': 'always_use_lwt'}],
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]}
    with new_test_table(dynamodb, **schema) as table:
        assert uses_tablets(dynamodb, table)
        # This put_item() failed before #18068 was fixed:
        table.put_item(Item={'p': 'hello'})
        assert table.get_item(Key={'p': 'hello'}, ConsistentRead=True)['Item'] == {'p': 'hello'}

# An Alternator table created tablets and with a write isolation
# mode that doesn't use LWT ("forbid_rmw") works normally, even
# before #18068 is fixed.
def test_alternator_tablets_without_lwt(dynamodb):
    schema = {
        'Tags': [
            {'Key': initial_tablets_tag, 'Value': '0'},
            {'Key': 'system:write_isolation', 'Value': 'forbid_rmw'}],
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        'AttributeDefinitions': [ { 'AttributeName': 'p', 'AttributeType': 'S' }]}
    with new_test_table(dynamodb, **schema) as table:
        assert uses_tablets(dynamodb, table)
        table.put_item(Item={'p': 'hello'})
        assert table.get_item(Key={'p': 'hello'})['Item'] == {'p': 'hello'}

# Reproduces scylladb/scylladb#26615
def test_gsi_with_tablets(dynamodb):
    schema = {
        'Tags': [{'Key': initial_tablets_tag, 'Value': '4'}],
        'KeySchema': [ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        'AttributeDefinitions': [
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ],
        'GlobalSecondaryIndexes': [
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'c', 'KeyType': 'HASH' },
                    { 'AttributeName': 'p', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
    }
    with new_test_table(dynamodb, **schema) as table:
        desc = table.meta.client.describe_table(TableName=table.name)
        table_status = desc['Table']['TableStatus']
        assert table_status == 'ACTIVE'

        index_desc = [x for x in desc['Table']['GlobalSecondaryIndexes'] if x['IndexName'] == 'hello']
        assert len(index_desc) == 1
        index_status = index_desc[0]['IndexStatus']
        assert index_status == 'ACTIVE'

        # When the index is ACTIVE, this must be after backfilling completed
        assert not 'Backfilling' in index_desc[0]

# This test is copy of alternator/test_gsi_backfill.py::test_gsi_backfill but with enabled tablets.
def test_gsi_backfill_with_tablets(dynamodb):
    # First create, and fill, a table without GSI. The items in items1
    # will have the appropriate string type for 'x' and will later get
    # indexed. Items in item2 have no value for 'x', and in items in
    # items3 'x' is not a string; So the items in items2 and items3
    # will be missing in the index that we'll create later.
    with new_test_table(dynamodb,
        Tags=[{'Key': initial_tablets_tag, 'Value': '4'}],
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
