# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the DynamoDB Global Tables feature.
# See https://github.com/scylladb/scylladb/issues/5062
#
# DynamoDB Global Tables allow a table to be replicated across multiple AWS
# regions. The original 2017.11.29 (v1) API is legacy and no longer supported
# by AWS - we verify that the relevant API calls return an error as expected.
# The current 2019.11.21 (v2) API uses UpdateTable with ReplicaUpdates to add
# or remove replicas, and is what Alternator should implement.

import boto3
import botocore.client
import pytest
import requests
import time
from contextlib import contextmanager
from urllib.parse import urlparse
from botocore.exceptions import ClientError

from .util import new_test_table, unique_table_name, random_string
from test.pylib.skip_types import skip_env

# Timeout (in seconds) used by retry loops in tests that wait for replication
# between regions. Centralized here so it can be adjusted easily.
REPLICATION_TIMEOUT = 30
# Timeout (in seconds) used by retry loops in tests that wait for enabling
# or removing a replica to take effect. This can take a very long time in
# DynamoDB.
ACTIVE_TIMEOUT = 300

# Fixture for a DynamoDB API connection to the first region - an alias for
# our regular "dynamodb" fixture. When running against AWS DynamoDB, this
# fixture connects to whatever AWS region is configured as the default in
# ~/.aws. When running against Scylla, it connects to a node in one of the
# DCs.
@pytest.fixture(scope='module')
def region1(dynamodb):
    yield dynamodb

# Fixture for a second region, pointing at a node in a different DC than
# region1. On AWS it picks a different region; on Alternator it uses one
# the nodes in the cluster that is in a different DC, skipping the test if
# there is only one DC in the cluster.
@pytest.fixture(scope='module')
def region2(request, dynamodb, get_valid_alternator_role):
    if request.config.getoption('aws'):
        # On AWS DynamoDB, we have a list of two or more regions KNOWN_REGIONS.
        # One of them must be NOT the same region as our connection is already
        # using so pick that region and connect to it.
        KNOWN_REGIONS = ['us-west-1', 'us-west-2']
        region1_name = dynamodb.meta.client.meta.region_name
        other_region = next(r for r in KNOWN_REGIONS if r != region1_name)
        res = boto3.resource('dynamodb', region_name=other_region)
    else:
        # On Alternator pick one the other nodes that is on a different DC from
        # this own, and connect to it. Skip the test if there is just one DC.
        url = dynamodb.meta.client._endpoint.host
        parsed = urlparse(url)
        my_dc = dynamodb.Table('.scylla.alternator.system.local').scan(
            AttributesToGet=['data_center'])['Items'][0]['data_center']
        # Find the name of another DC by scanning system.peers, then use
        # /localnodes?dc=<other_dc> to get a connectable address for it.
        peers = dynamodb.Table('.scylla.alternator.system.peers').scan(
            AttributesToGet=['data_center'])['Items']
        other_dc = next((p['data_center'] for p in peers
                         if p['data_center'] != my_dc), None)
        if other_dc is None:
            skip_env('Test requires at least two DCs in the Scylla cluster')
        port_suffix = f':{parsed.port}' if parsed.port is not None else ''
        other_node = requests.get(url + f'/localnodes?dc={other_dc}', verify=False).json()[0]
        other_url = f'{parsed.scheme}://{other_node}{port_suffix}'
        user, secret = get_valid_alternator_role(other_url)
        config = botocore.client.Config(
            parameter_validation=False,
            retries={'max_attempts': 0},
            read_timeout=300,
        )
        res = boto3.resource('dynamodb',
            endpoint_url=other_url,
            verify=False,
            region_name=other_dc,
            aws_access_key_id=user,
            aws_secret_access_key=secret,
            config=config)
    yield res
    res.meta.client.close()

# Before testing global tables, let's test non-global, "regional" tables.
# Test that we're allowed to create two tables with the same name in two
# different regions, and they will be independent of each other.
@pytest.mark.xfail(reason="Issue #5062")
def test_independent_regional_tables(region1, region2):
    table_name = unique_table_name()
    schema = dict(
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    )
    # Note that we create both tables with the same name table_name! Just on
    # two different regions (region1 and region2).
    with new_test_table(region1, name=table_name, **schema) as table1:
        with new_test_table(region2, name=table_name, **schema) as table2:
            # Write a different value to the same key in each region's table:
            table1.put_item(Item={'p': 'x', 'region': 1})
            table2.put_item(Item={'p': 'x', 'region': 2})
            # Check we can read back the different values:
            assert table1.get_item(Key={'p': 'x'})['Item'] == {'p': 'x', 'region': 1}
            assert table2.get_item(Key={'p': 'x'})['Item'] == {'p': 'x', 'region': 2}

####### Tests for Global Tables v1 API (the original API) #######

# It turns out that the original Global Tables API (from 2017) is deprecated,
# and no longer supported by AWS for new global tables. So all we can - and
# should - test is how it gives errors on all the operations. Alternator
# should give the same error types on these requests.
# The error returned by DynamoDB is "DynamoDB global tables version 2017.11.29
# is not supported. We recommend using DynamoDB global tables version
# 2019.11.21, instead of version 2017.11.29 (Legacy)."

# Test that CreateGlobalTable (the 2017 legacy API) is no longer supported.
# We create identically-named tables in two regions (both with streams enabled
# as required by the Global Tables legacy API) and then attempt to join them
# into a global table with CreateGlobalTable - which should fail mentioning
# that the 2017 version is not supported.
@pytest.mark.xfail(reason="Issue #5062")
def test_not_supported_create_global_table(region1, region2):
    table_name = unique_table_name()
    schema = dict(
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
        # Global Tables v1 requires streams with NEW_AND_OLD_IMAGES.
        StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
    )
    with new_test_table(region1, name=table_name, **schema) as table1:
        with new_test_table(region2, name=table_name, **schema) as table2:
            with pytest.raises(ClientError, match='ValidationException.*2017'):
                client = region1.meta.client
                region2_name = region2.meta.client.meta.region_name
                client.create_global_table(
                    GlobalTableName=table_name,
                    ReplicationGroup=[{'RegionName': region2_name}]
                )

# UpdateGlobalTable is old, 2017 legacy API, which is no longer supported.
# But there isn't a special error that the API isn't supported - instead
# any name you give can't be a 2017 global table (which we can no longer
# create), so we get: "Global table with name '<name>' does not exist."
# Below we have additional tests (test_v1_*_on_v2_global_table) that verify
# that even even when given a v2 global table name, these v1 APIs still return
# the same "Global table with name '<name>' does not exist." error.
@pytest.mark.xfail(reason="Issue #5062")
def test_not_supported_update_global_table(region1):
    with pytest.raises(ClientError, match='GlobalTableNotFoundException.*Global table.*does not exist'):
        region1.meta.client.update_global_table(
            GlobalTableName=unique_table_name(),
            # RegionName doesn't matter, we'll still get the "does not exist" error.
            ReplicaUpdates=[{'Create': {'RegionName': 'us-east-2'}}],
        )

# DescribeGlobalTable is old, 2017 legacy API, which is no longer supported.
# Like UpdateGlobalTable, it returns GlobalTableNotFoundException since there
# are no v1 global tables: "Global table with name '<name>' does not exist."
@pytest.mark.xfail(reason="Issue #5062")
def test_not_supported_describe_global_table(region1):
    with pytest.raises(ClientError, match='GlobalTableNotFoundException.*Global table.*does not exist'):
        region1.meta.client.describe_global_table(GlobalTableName=unique_table_name())

# DescribeGlobalTableSettings is old, 2017 legacy API, which is no longer
# supported. Like UpdateGlobalTable, it returns GlobalTableNotFoundException
# since there are no v1 global tables: "Global table with name '<name>' does
# not exist."
@pytest.mark.xfail(reason="Issue #5062")
def test_not_supported_describe_global_table_settings(region1):
    with pytest.raises(ClientError, match='GlobalTableNotFoundException.*Global table.*does not exist'):
        region1.meta.client.describe_global_table_settings(GlobalTableName=unique_table_name())

# UpdateGlobalTableSettings is old, 2017 legacy API, which is no longer
# supported. Like UpdateGlobalTable, it returns GlobalTableNotFoundException
# since there are no v1 global tables: "Global table with name '<name>' does
# not exist."
@pytest.mark.xfail(reason="Issue #5062")
def test_not_supported_update_global_table_settings(region1):
    with pytest.raises(ClientError, match='GlobalTableNotFoundException.*Global table.*does not exist'):
        region1.meta.client.update_global_table_settings(
            GlobalTableName=unique_table_name(),
            GlobalTableBillingMode='PAY_PER_REQUEST',
        )

# Test that ListGlobalTables (the 2017 legacy API) returns an empty list.
# Unlike the other v1 operations, ListGlobalTables doesn't return an error -
# it just returns an empty list since there are no v1 global tables to list.
# Presumably, legacy DynamoDB customers they may still have some old v1 global
# tables created when this was still allowed - and those would be listed by
# ListGlobalTables. But for new users (and for Alternator), the list should
# always be empty.
@pytest.mark.xfail(reason="Issue #5062")
def test_list_global_tables_empty(region1):
    response = region1.meta.client.list_global_tables()
    assert 'GlobalTables' in response
    assert response['GlobalTables'] == []

####### Tests for Global Tables v2 API (introduced November 2019) #######

# Wait until the given table's status is ACTIVE (not UPDATING or CREATING).
# Used after update_table() calls that don't immediately take effect.
def wait_for_active(table):
    deadline = time.monotonic() + ACTIVE_TIMEOUT
    while True:
        desc = table.meta.client.describe_table(TableName=table.name)['Table']
        if desc['TableStatus'] == 'ACTIVE':
            break
        if time.monotonic() >= deadline:
            pytest.fail(f'Table {table.name} did not become ACTIVE within {ACTIVE_TIMEOUT} seconds')
        time.sleep(1)

# Context manager for a Global Tables v2 global table. Adds a replica of the
# given table in 'region', and removes it when done. Unlike the old v1 API,
# there is no need to pre-create a table in the replica region, nor to enable
# streams - the replica table is created and deleted automatically.
@contextmanager
def add_region(table, region):
    if not isinstance(region, str):
        region = region.meta.client.meta.region_name
    client = table.meta.client
    client.update_table(
        TableName=table.name,
        ReplicaUpdates=[{'Create': {'RegionName': region}}],
    )
    try:
        # Wait for the replica to become ACTIVE
        deadline = time.monotonic() + ACTIVE_TIMEOUT
        while True:
            desc = client.describe_table(TableName=table.name)['Table']
            replicas = desc.get('Replicas', [])
            replica = next((r for r in replicas if r['RegionName'] == region), None)
            if replica and replica['ReplicaStatus'] == 'ACTIVE':
                break

            if time.monotonic() >= deadline:
                pytest.fail(f'Replica in {region} did not become ACTIVE within {ACTIVE_TIMEOUT} seconds')
            time.sleep(1)
        # Just in case, wait for the table to become ACTIVE as well, saying
        # that the UpdateTable operation is complete.
        wait_for_active(table)
        yield
    finally:
        client.update_table(
            TableName=table.name,
            ReplicaUpdates=[{'Delete': {'RegionName': region}}],
        )
        # Wait for the replica to be fully removed before returning, so that
        # the caller can delete the table.
        deadline = time.monotonic() + ACTIVE_TIMEOUT
        while True:
            desc = client.describe_table(TableName=table.name)['Table']
            replicas = desc.get('Replicas', [])
            if not any(r['RegionName'] == region for r in replicas):
                break
            if time.monotonic() >= deadline:
                pytest.fail(f'Replica in {region} was not removed within {ACTIVE_TIMEOUT} seconds')
            time.sleep(1)
        wait_for_active(table)

# Test the error when trying to add a table to a region it's already in.
# A ValidationException is returned in this case, with a strange message:
# "Cannot add or delete the local region through ReplicaUpdates.
# Use CreateTable, DeleteTable, or UpdateTable as required."
@pytest.mark.xfail(reason="Issue #5062")
def test_add_region_already_exists(region1):
    schema = dict(
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    )
    with new_test_table(region1, **schema) as table:
        region1_name = region1.meta.client.meta.region_name
        with pytest.raises(ClientError, match='ValidationException.*local region'):
            table.meta.client.update_table(
                TableName=table.name,
                ReplicaUpdates=[{'Create': {'RegionName': region1_name}}],
            )

# A module-scoped fixture for a shared global table replicated in region1
# and region2. Shared across all tests in this module that need it.
@pytest.fixture(scope='module')
def global_table(region1, region2):
    schema = dict(
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    )
    with new_test_table(region1, **schema) as table1:
        with add_region(table1, region2):
            table2 = region2.Table(table1.name)
            yield table1, table2

# A simple test that we can create a global table using the v2 (2019) API:
# create a table in region1, then add a replica in region2 using UpdateTable
# with ReplicaUpdates Create.
@pytest.mark.xfail(reason="Issue #5062")
def test_create_global_table(region1, region2, global_table):
    table1, table2 = global_table
    # We expect that the Replicas list should  contain exactly region1 and
    # region2, both ACTIVE. But very surprisingly, and without any,
    # documentation, in practice we saw that consistently (but not always)
    # Replicas only lists the "other" regions - i.e., only region2. So we
    # allow the replicas list to contain either both region1 and region2, or
    # just region 2.
    desc = table1.meta.client.describe_table(TableName=table1.name)['Table']
    assert 'Replicas' in desc
    replics_regions = {r['RegionName'] for r in desc['Replicas']}
    region1_name = region1.meta.client.meta.region_name
    region2_name = region2.meta.client.meta.region_name
    assert replics_regions == {region1_name, region2_name} or replics_regions == {region2_name}
    assert all(r['ReplicaStatus'] == 'ACTIVE' for r in desc['Replicas'])

# Test that a global table gets NEW_AND_OLD_IMAGES streaming enabled
# automatically, even though we never enabled it explicitly.
@pytest.mark.xfail(reason="Issue #5062")
def test_global_table_streaming(global_table):
    table1, table2 = global_table
    desc = table1.meta.client.describe_table(TableName=table1.name)['Table']
    stream_spec = desc.get('StreamSpecification', {})
    assert stream_spec.get('StreamEnabled') == True
    assert stream_spec.get('StreamViewType') == 'NEW_AND_OLD_IMAGES'

# Test that the streaming of a global table cannot be disabled. This also
# means that its StreamViewType cannot be changed - it's never possible in
# DynamoDB to change a StreamViewType of a stream without disabling it first.
@pytest.mark.xfail(reason="Issue #5062")
def test_global_table_streaming_cannot_be_changed(global_table):
    table1, table2 = global_table
    client = table1.meta.client
    with pytest.raises(ClientError, match='ValidationException.*not allowed.*Global Table'):
        client.update_table(
            TableName=table1.name,
            StreamSpecification={'StreamEnabled': False},
        )
    with pytest.raises(ClientError, match='ValidationException.*already has an enabled stream'):
        client.update_table(
            TableName=table1.name,
            StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'},
        )

# Above we tested that making a table global always enables a stream with
# NEW_AND_OLD_IMAGES, which cannot be changed or disabled. This makes it
# reasonable to *guess* that DynamoDB Global Tables actually requires
# NEW_AND_OLD_IMAGES streams. But the surprising fact that the following test
# proves that it does NOT: If you already have a stream with just KEYS_ONLY,
# you can make the table global, and the stream remains KEYS_ONLY and is NOT
# upgraded to NEW_AND_OLD_IMAGES. At least not visibly: we don't know if
# DynamoDB really doesn't need images (forcing it to read the items from the
# table when replicating), or maybe it does need them so internally puts new
# and old images in its implementation of the stream - but hides them from
# the API. In any case we confirm that as far as the user-facing API is
# concerned, the stream really does remain KEYS_ONLY - and the user can
# only see keys in the stream.
@pytest.mark.xfail(reason="Issue #5062")
def test_global_table_keys_only_is_enough(region1, region2, dynamodbstreams):
    schema = dict(
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
        StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'},
    )
    with new_test_table(region1, **schema) as table:
        # Verify that the stream was enabled with KEYS_ONLY:
        desc = table.meta.client.describe_table(TableName=table.name)['Table']
        assert 'StreamSpecification' in desc
        assert desc['StreamSpecification'].get('StreamEnabled') == True
        assert desc['StreamSpecification'].get('StreamViewType') == 'KEYS_ONLY'
        with add_region(table, region2):
            # Verify that creating the global table did not change the stream
            # type:
            desc = table.meta.client.describe_table(TableName=table.name)['Table']
            assert 'StreamSpecification' in desc
            assert desc['StreamSpecification'].get('StreamEnabled') == True
            assert desc['StreamSpecification'].get('StreamViewType') == 'KEYS_ONLY'
            # Verify that despite the stream being just KEYS_ONLY, the global
            # table replication still works - if we write an item to region1 we
            # can (eventually) find it in region2:
            table2 = region2.Table(table.name)
            p = random_string()
            table.put_item(Item={'p': p, 'written_in': 'region1'})
            deadline = time.monotonic() + REPLICATION_TIMEOUT
            while time.monotonic() < deadline:
                response = table2.get_item(Key={'p': p})
                if 'Item' in response:
                    assert response['Item'] == {'p': p, 'written_in': 'region1'}
                    break
                time.sleep(0.5)
            else:
                pytest.fail(f'Item written in region1 was not replicated to region2 within {REPLICATION_TIMEOUT} seconds')
            # Verify that (at least as far as the Streams API can see) the
            # stream is really KEYS_ONLY - we can only see keys, not old or
            # new images in it.
            arn = desc['LatestStreamArn']
            iters = []
            response = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
            while True:
                for shard in response['Shards']:
                    iters.append(dynamodbstreams.get_shard_iterator(
                        StreamArn=arn, ShardId=shard['ShardId'],
                        ShardIteratorType='TRIM_HORIZON')['ShardIterator'])
                last = response.get('LastEvaluatedShardId')
                if not last:
                    break
                response = dynamodbstreams.describe_stream(
                    StreamArn=arn, ExclusiveStartShardId=last)['StreamDescription']
            deadline = time.monotonic() + REPLICATION_TIMEOUT
            while time.monotonic() < deadline:
                new_iters = []
                for it in iters:
                    r = dynamodbstreams.get_records(ShardIterator=it)
                    if 'NextShardIterator' in r:
                        new_iters.append(r['NextShardIterator'])
                    for record in r.get('Records', []):
                        event = record['dynamodb']
                        if (record['eventName'] == 'INSERT' and
                                event.get('Keys', {}).get('p', {}).get('S') == p):
                            # Make sure the stream record really doesn't have
                            # old or new images.
                            assert 'OldImage' not in event
                            assert 'NewImage' not in event
                            # Test done successfully.
                            return
                iters = new_iters if new_iters else iters
                time.sleep(0.5)
            pytest.fail(f'Expected INSERT stream record not found within {REPLICATION_TIMEOUT} seconds')

# Basic test that a global table is replicated between regions: write an
# item in region1 and check we can eventually read it from region2.
@pytest.mark.xfail(reason="Issue #5062")
def test_global_table_replication(global_table):
    table1, table2 = global_table
    p = random_string()
    table1.put_item(Item={'p': p, 'written_in': 'region1'})
    deadline = time.monotonic() + REPLICATION_TIMEOUT
    while time.monotonic() < deadline:
        response = table2.get_item(Key={'p': p})
        if 'Item' in response:
            assert response['Item'] == {'p': p, 'written_in': 'region1'}
            return
        time.sleep(0.5)
    pytest.fail(f'Item written in region1 was not replicated to region2 within {REPLICATION_TIMEOUT} seconds')

# Above we tested (test_global_table_streaming) that a global table always has
# NEW_AND_OLD_IMAGES streaming enabled. Let's verify it really does enable
# streaming - and we can actually read this stream and see old and new images.
# We can do this with the shared global_table fixture, we don't need a separate
# global table for this test.
@pytest.mark.xfail(reason="Issue #5062")
def test_global_table_stream_is_real(global_table, dynamodbstreams):
    table1, table2 = global_table
    # Get the stream ARN.
    desc = table1.meta.client.describe_table(TableName=table1.name)['Table']
    arn = desc['LatestStreamArn']
    # Get LATEST iterators for all shards before writing. Because this is a
    # shared table used by previous tests, it is possible we'll see some older
    # events, but this is fine - we'll use a random key and only look for it
    # in the stream.
    iters = []
    response = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
    while True:
        for shard in response['Shards']:
            iters.append(dynamodbstreams.get_shard_iterator(
                StreamArn=arn, ShardId=shard['ShardId'],
                ShardIteratorType='LATEST')['ShardIterator'])
        last = response.get('LastEvaluatedShardId')
        if not last:
            break
        response = dynamodbstreams.describe_stream(
            StreamArn=arn, ExclusiveStartShardId=last)['StreamDescription']
    # Write an item and then update it. The update should produce a MODIFY
    # event with both OldImage and NewImage (due to NEW_AND_OLD_IMAGES).
    p = random_string()
    table1.put_item(Item={'p': p, 'x': 1})
    table1.update_item(Key={'p': p},
        UpdateExpression='SET x = :v', ExpressionAttributeValues={':v': 2})
    # Poll all shards until we find the MODIFY record for our key, and
    # verify it contains the correct OldImage and NewImage.
    deadline = time.monotonic() + REPLICATION_TIMEOUT
    while time.monotonic() < deadline:
        new_iters = []
        for it in iters:
            r = dynamodbstreams.get_records(ShardIterator=it)
            if 'NextShardIterator' in r:
                new_iters.append(r['NextShardIterator'])
            for record in r.get('Records', []):
                event = record['dynamodb']
                if (record['eventName'] == 'MODIFY' and
                        event.get('Keys', {}).get('p', {}).get('S') == p):
                    assert 'OldImage' in event
                    assert 'NewImage' in event
                    assert event['OldImage']['p']['S'] == p
                    assert event['OldImage']['x']['N'] == '1'
                    assert event['NewImage']['p']['S'] == p
                    assert event['NewImage']['x']['N'] == '2'
                    return
        iters = new_iters if new_iters else iters
        time.sleep(0.5)
    pytest.fail(f'Expected MODIFY stream record with OldImage and NewImage not found within {REPLICATION_TIMEOUT} seconds')

# Test that a regular non-global table (e.g., test_table_s) doesn't have
# Replicas or GlobalTableVersion fields in DescribeTable.
def test_nonglobal_table_has_no_global_fields(test_table_s):
    desc = test_table_s.meta.client.describe_table(TableName=test_table_s.name)['Table']
    assert 'Replicas' not in desc
    assert 'GlobalTableVersion' not in desc

# Test that in a global table (we can use the shared global_table fixture),
# DescribeTable returns the expected GlobalTableVersion and Replicas fields.
@pytest.mark.xfail(reason="Issue #5062")
def test_describe_global_table(region1, region2, global_table):
    table1, table2 = global_table
    desc = table1.meta.client.describe_table(TableName=table1.name)['Table']
    # A v2 global table should report GlobalTableVersion = '2019.11.21'.
    assert 'GlobalTableVersion' in desc
    assert desc['GlobalTableVersion'] == '2019.11.21'
    # The Replicas list should contain both regions with ACTIVE status.
    # It usually does. But for an unknown reason, sometimes it only contains
    # region2. This is weird and undocumented, and I assume it's just a bug
    # in DynamoDB... For now we'll allow for both possibilities.
    region1_name = region1.meta.client.meta.region_name
    region2_name = region2.meta.client.meta.region_name
    assert 'Replicas' in desc
    replicas = {r['RegionName'] for r in desc['Replicas']}
    assert replicas == {region1_name, region2_name} or replicas == {region2_name}
    assert all(r['ReplicaStatus'] == 'ACTIVE' for r in desc['Replicas'])

# Test that if we create a global table on region1 and region2, and then remove
# the replica from region2, the table that is now in just a single region
# (region1) reverts to being a normal, non-global table. However, it does
# NOT disable streaming! This is rather surprising, since the user never asked
# for this streaming explicitly, and it was only added - automatically - when
# we enabled the global table. But we test that we can disable this streaming -
# which wasn't possible while the table was global.
@pytest.mark.xfail(reason="Issue #5062")
def test_global_table_reverts_to_nonglobal_after_removing_replica(region1, region2):
    schema = dict(
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    )
    with new_test_table(region1, **schema) as table:
        # Make it a global table by adding region2 as a replica, then
        # immediately remove it. add_region() handles both on entry and exit.
        with add_region(table, region2):
            # While the table is global, DescribeTable says it has Replicas
            # and GlobalTableVersion, and streaming is enabled:
            desc = table.meta.client.describe_table(TableName=table.name)['Table']
            assert 'GlobalTableVersion' in desc
            assert desc['GlobalTableVersion'] == '2019.11.21'
            region1_name = region1.meta.client.meta.region_name
            region2_name = region2.meta.client.meta.region_name
            replicas = desc.get('Replicas', [])
            replicas_regions = {r['RegionName'] for r in replicas}
            # We expect {region1_name, region2_name} but for an unknown reason,
            # as also noted in test_create_global_table, sometimes we only get
            # {region2_name} - which is weird and undocumented.
            assert replicas_regions == {region1_name, region2_name} or replicas_regions == {region2_name}
            assert all(r['ReplicaStatus'] == 'ACTIVE' for r in replicas)
            assert 'StreamSpecification' in desc
            stream_spec = desc['StreamSpecification']
            assert stream_spec['StreamEnabled'] == True
            assert stream_spec['StreamViewType'] == 'NEW_AND_OLD_IMAGES'

        # After add_region() exits, the second replica is removed. At that point,
        # the table has gone back to being a normal, non-global, table. It should
        # no longer have Replicas or GlobalTableVersion:
        desc = table.meta.client.describe_table(TableName=table.name)['Table']
        assert 'Replicas' not in desc
        assert 'GlobalTableVersion' not in desc
        # However, the stream is NOT removed, it remains on with
        # NEW_AND_OLD_IMAGES - which is a bit surprising given that the user
        # never wanted to explicitly enabled it, but this is what DynamoDB
        # does.
        assert 'StreamSpecification' in desc
        assert desc['StreamSpecification']['StreamEnabled'] == True
        assert desc['StreamSpecification']['StreamViewType'] == 'NEW_AND_OLD_IMAGES'

        # At this point, it is allowed to disable the stream - which wasn't
        # allowed when the table was global. The stream will remain readable
        # for 24 more hours, as a historical "DISABLED" stream.
        table.meta.client.update_table(
            TableName=table.name,
            StreamSpecification={'StreamEnabled': False},
        )
        wait_for_active(table)
        desc = table.meta.client.describe_table(TableName=table.name)['Table']
        assert 'StreamSpecification' not in desc
        assert 'LatestStreamArn' in desc

# Test that the legacy 2017 APIs that take a GlobalTableName parameter do
# NOT work on a v2 (2019) global table:

# Test that the legacy 2017 API "UpdateGlobalTable" cannot be used on a v2
# global table - even though a table with that name exists as a v2 global
# table, the v1 API doesn't see it and returns GlobalTableNotFoundException.
@pytest.mark.xfail(reason="Issue #5062")
def test_v1_update_global_table_on_v2_global_table(global_table):
    table1, table2 = global_table
    region2_name = table2.meta.client.meta.region_name
    with pytest.raises(ClientError, match='GlobalTableNotFoundException'):
        table1.meta.client.update_global_table(
            GlobalTableName=table1.name,
            ReplicaUpdates=[{'Delete': {'RegionName': region2_name}}],
        )

# Test that the legacy 2017 API "DescribeGlobalTable" cannot be used on a v2
# global table - even though a table with that name exists as a v2 global
# table, the v1 API doesn't see it and returns GlobalTableNotFoundException.
@pytest.mark.xfail(reason="Issue #5062")
def test_v1_describe_global_table_on_v2_global_table(global_table):
    table1, table2 = global_table
    with pytest.raises(ClientError, match='GlobalTableNotFoundException'):
        table1.meta.client.describe_global_table(
            GlobalTableName=table1.name,
        )

# Test that the legacy 2017 API "DescribeGlobalTableSettings" cannot be used
# on a v2 global table - returns GlobalTableNotFoundException.
@pytest.mark.xfail(reason="Issue #5062")
def test_v1_describe_global_table_settings_on_v2_global_table(global_table):
    table1, table2 = global_table
    with pytest.raises(ClientError, match='GlobalTableNotFoundException'):
        table1.meta.client.describe_global_table_settings(
            GlobalTableName=table1.name,
        )

# Test that the legacy 2017 API "UpdateGlobalTableSettings" cannot be used
# on a v2 global table - returns GlobalTableNotFoundException.
@pytest.mark.xfail(reason="Issue #5062")
def test_v1_update_global_table_settings_on_v2_global_table(global_table):
    table1, table2 = global_table
    with pytest.raises(ClientError, match='GlobalTableNotFoundException'):
        table1.meta.client.update_global_table_settings(
            GlobalTableName=table1.name,
            GlobalTableBillingMode='PAY_PER_REQUEST',
        )

# Test that the legacy 2017 API "ListGlobalTables" does not list v2 global
# tables - the v2 global table is invisible to the v1 API. We can assume
# the resulting list of global tables is completely empty - we'll never
# run this test on an account that happens to have ancient v1 global tables
# unrelated to this test.
@pytest.mark.xfail(reason="Issue #5062")
def test_v1_list_global_tables_on_v2_global_table(region1, global_table):
    response = region1.meta.client.list_global_tables()
    assert 'GlobalTables' in response
    assert response['GlobalTables'] == []
