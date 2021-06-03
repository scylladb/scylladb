# Copyright 2020-present ScyllaDB
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

# Tests for stream operations: ListStreams, DescribeStream, GetShardIterator,
# GetRecords.

import pytest
import time
import urllib.request

from botocore.exceptions import ClientError
from util import list_tables, test_table_name, create_test_table, random_string, freeze
from contextlib import contextmanager
from urllib.error import URLError
from boto3.dynamodb.types import TypeDeserializer

stream_types = [ 'OLD_IMAGE', 'NEW_IMAGE', 'KEYS_ONLY', 'NEW_AND_OLD_IMAGES']

def disable_stream(dynamodbstreams, table):
    table.update(StreamSpecification={'StreamEnabled': False});
    # Wait for the stream to really be disabled. A table may have multiple
    # historic streams - we need all of them to become DISABLED. One of
    # them (the current one) may remain DISABLING for some time.
    exp = time.process_time() + 60
    while time.process_time() < exp:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        disabled = True
        for stream in streams['Streams']:
            desc = dynamodbstreams.describe_stream(StreamArn=stream['StreamArn'])['StreamDescription']
            if desc['StreamStatus'] != 'DISABLED':
                disabled = False
                break
        if disabled:
            print('disabled stream on {}'.format(table.name))
            return
        time.sleep(0.5)
    pytest.fail("timed out")
            
# Cannot use fixtures. Because real dynamodb cannot _remove_ a stream
# once created. It can only expire 24h later. So reusing test_table for 
# example works great for scylla/local testing, but once we run against
# actual aws instances, we get lingering streams, and worse: cannot 
# create new ones to replace it, because we have overlapping types etc. 
# 
# So we have to create and delete a table per test. And not run this 
# test to often against aws.  
@contextmanager
def create_stream_test_table(dynamodb, StreamViewType=None):
    spec = { 'StreamEnabled': False }
    if StreamViewType != None:
        spec = {'StreamEnabled': True, 'StreamViewType': StreamViewType}
    table = create_test_table(dynamodb, StreamSpecification=spec,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ])
    try:
        yield table
    finally:
        print(f"Deleting table {table.name}")
        while True:
            try:
                table.delete()
                break
            except ClientError as ce:
                # if the table has a stream currently being created we cannot
                # delete the table immediately. Again, only with real dynamo
                if ce.response['Error']['Code'] == 'ResourceInUseException':
                    print('Could not delete table yet. Sleeping 5s.')
                    time.sleep(5)
                    continue;
                raise

def wait_for_active_stream(dynamodbstreams, table, timeout=60):
    exp = time.process_time() + timeout
    while time.process_time() < exp:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        for stream in streams['Streams']:
            arn = stream['StreamArn']
            if arn == None:
                continue
            desc = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
            if not 'StreamStatus' in desc or desc.get('StreamStatus') == 'ENABLED':
                return (arn, stream['StreamLabel']);
        # real dynamo takes some time until a stream is usable
        print("Stream not available. Sleep 5s...")
        time.sleep(5)
    assert False

# Local java dynamodb server version behaves differently from 
# the "real" one. Most importantly, it does not verify a number of 
# parameters, and consequently does not throw when called with borked 
# args. This will try to check if we are in fact running against 
# this test server, and if so, just raise the error here and be done 
# with it. All this just so we can run through the tests on 
# aws, scylla and local. 
def is_local_java(dynamodbstreams):
    # no good way to check, but local server runs on a Jetty, 
    # so check for that. 
    url = dynamodbstreams.meta.endpoint_url
    try: 
        urllib.request.urlopen(url)
    except URLError as e:
        if hasattr(e, 'info'):
            return e.info()['Server'].startswith('Jetty')
    return False

def ensure_java_server(dynamodbstreams, error='ValidationException'):
    # no good way to check, but local server has a "shell" builtin, 
    # so check for that. 
    if is_local_java(dynamodbstreams):
        if error != None:
            raise ClientError({'Error': { 'Code' : error }}, '')
        return
    assert False

def test_list_streams_create(dynamodb, dynamodbstreams):
    for type in stream_types:
        with create_stream_test_table(dynamodb, StreamViewType=type) as table:
            wait_for_active_stream(dynamodbstreams, table)

def test_list_streams_alter(dynamodb, dynamodbstreams):
    for type in stream_types:
        with create_stream_test_table(dynamodb, StreamViewType=None) as table:
            res = table.update(StreamSpecification={'StreamEnabled': True, 'StreamViewType': type});
            wait_for_active_stream(dynamodbstreams, table)

def test_list_streams_paged(dynamodb, dynamodbstreams):
    # There is no reason to run this test for all stream types - we have
    # other tests for creating tables with all stream types, and for using
    # them. This one is only about list_streams.
    for type in stream_types[0:1]:
        with create_stream_test_table(dynamodb, StreamViewType=type) as table1:
            with create_stream_test_table(dynamodb, StreamViewType=type) as table2:
                wait_for_active_stream(dynamodbstreams, table1)
                wait_for_active_stream(dynamodbstreams, table2)
                streams = dynamodbstreams.list_streams(Limit=1)
                assert streams
                assert streams.get('Streams')
                assert streams.get('LastEvaluatedStreamArn')
                tables = [ table1.name, table2.name ]
                while True:
                    for s in streams['Streams']:
                        name = s['TableName']
                        if name in tables: tables.remove(name)
                    if not tables:
                        break
                    streams = dynamodbstreams.list_streams(Limit=1, ExclusiveStartStreamArn=streams['LastEvaluatedStreamArn'])


@pytest.mark.skip(reason="Python driver validates Limit, so trying to test it is pointless")
def test_list_streams_zero_limit(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        with pytest.raises(ClientError, match='ValidationException'):
            wait_for_active_stream(dynamodbstreams, table)
            dynamodbstreams.list_streams(Limit=0)

def test_create_streams_wrong_type(dynamodb, dynamodbstreams, test_table):
    with pytest.raises(ClientError, match='ValidationException'):
        # should throw
        test_table.update(StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'Fisk'});
        # just in case the test fails, disable stream again
        test_table.update(StreamSpecification={'StreamEnabled': False});

def test_list_streams_empty(dynamodb, dynamodbstreams, test_table):
    streams = dynamodbstreams.list_streams(TableName=test_table.name)
    assert 'Streams' in streams
    assert not streams['Streams'] # empty

def test_list_streams_with_nonexistent_last_stream(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        with pytest.raises(ClientError, match='ValidationException'):
            streams = dynamodbstreams.list_streams(TableName=table.name, ExclusiveStartStreamArn='kossaapaaasdafsdaasdasdasdasasdasfadfadfasdasdas')
            assert 'Streams' in streams
            assert not streams['Streams'] # empty
            # local java dynamodb does _not_ raise validation error for 
            # malformed stream arn here. verify 
            ensure_java_server(dynamodbstreams)

def test_describe_stream(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        desc = dynamodbstreams.describe_stream(StreamArn=arn)
        assert desc;
        assert desc.get('StreamDescription')
        assert desc['StreamDescription']['StreamArn'] == arn
        assert desc['StreamDescription']['StreamStatus'] != 'DISABLED'
        assert desc['StreamDescription']['StreamViewType'] == 'KEYS_ONLY'
        assert desc['StreamDescription']['TableName'] == table.name
        assert desc['StreamDescription'].get('Shards')
        assert desc['StreamDescription']['Shards'][0].get('ShardId')
        assert desc['StreamDescription']['Shards'][0].get('SequenceNumberRange')
        assert desc['StreamDescription']['Shards'][0]['SequenceNumberRange'].get('StartingSequenceNumber')
        # We don't know what the sequence number is supposed to be, but the
        # DynamoDB documentation requires that it contains only numeric
        # characters and some libraries rely on this. Reproduces issue #7158:
        assert desc['StreamDescription']['Shards'][0]['SequenceNumberRange']['StartingSequenceNumber'].isdecimal()

@pytest.mark.xfail(reason="alternator does not have creation time on streams")
def test_describe_stream_create_time(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        desc = dynamodbstreams.describe_stream(StreamArn=arn)
        assert desc;
        assert desc.get('StreamDescription')
        # note these are non-required attributes
        assert 'CreationRequestDateTime' in desc['StreamDescription']

def test_describe_nonexistent_stream(dynamodb, dynamodbstreams):
    with pytest.raises(ClientError, match='ResourceNotFoundException' if is_local_java(dynamodbstreams) else 'ValidationException'):
        streams = dynamodbstreams.describe_stream(StreamArn='sdfadfsdfnlfkajakfgjalksfgklasjklasdjfklasdfasdfgasf')

def test_describe_stream_with_nonexistent_last_shard(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        try:
            desc = dynamodbstreams.describe_stream(StreamArn=arn, ExclusiveStartShardId='zzzzzzzzzzzzzzzzzzzzzzzzsfasdgagadfadfgagkjsdfsfsdjfjks')
            assert not desc['StreamDescription']['Shards']
        except:
            # local java throws here. real does not. 
            ensure_java_server(dynamodbstreams, error=None)

def test_get_shard_iterator(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        desc = dynamodbstreams.describe_stream(StreamArn=arn)

        shard_id = desc['StreamDescription']['Shards'][0]['ShardId'];
        seq = desc['StreamDescription']['Shards'][0]['SequenceNumberRange']['StartingSequenceNumber'];

        # spec says shard_id must be 65 chars or less
        assert len(shard_id) <= 65

        for type in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']: 
            iter = dynamodbstreams.get_shard_iterator(
                StreamArn=arn, ShardId=shard_id, ShardIteratorType=type, SequenceNumber=seq
            )
            assert iter.get('ShardIterator')

        for type in ['TRIM_HORIZON', 'LATEST']: 
            iter = dynamodbstreams.get_shard_iterator(
                StreamArn=arn, ShardId=shard_id, ShardIteratorType=type
            )
            assert iter.get('ShardIterator')

        for type in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']: 
            # must have seq in these modes
            with pytest.raises(ClientError, match='ValidationException'):
                iter = dynamodbstreams.get_shard_iterator(
                    StreamArn=arn, ShardId=shard_id, ShardIteratorType=type
                )

        for type in ['TRIM_HORIZON', 'LATEST']: 
            # should not set "seq" in these modes
            with pytest.raises(ClientError, match='ValidationException'):
                dynamodbstreams.get_shard_iterator(
                    StreamArn=arn, ShardId=shard_id, ShardIteratorType=type, SequenceNumber=seq
                )

        # bad arn
        with pytest.raises(ClientError, match='ValidationException'):
            iter = dynamodbstreams.get_shard_iterator(
                StreamArn='sdfadsfsdfsdgdfsgsfdabadfbabdadsfsdfsdfsdfsdfsdfsdfdfdssdffbdfdf', ShardId=shard_id, ShardIteratorType=type, SequenceNumber=seq
            )
        # bad shard id  
        with pytest.raises(ClientError, match='ResourceNotFoundException'):
            dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId='semprinidfaasdasfsdvacsdcfsdsvsdvsdvsdvsdvsdv', 
                ShardIteratorType='LATEST'
                )
        # bad iter type
        with pytest.raises(ClientError, match='ValidationException'):
            dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, 
                ShardIteratorType='bulle', SequenceNumber=seq
                )
        # bad seq 
        with pytest.raises(ClientError, match='ValidationException'):
            dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, 
                ShardIteratorType='LATEST', SequenceNumber='sdfsafglldfngjdafnasdflgnaldklkafdsgklnasdlv'
                )

def test_get_shard_iterator_for_nonexistent_stream(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        (arn, label) = wait_for_active_stream(dynamodbstreams, table)
        desc = dynamodbstreams.describe_stream(StreamArn=arn)
        shards = desc['StreamDescription']['Shards']
        with pytest.raises(ClientError, match='ResourceNotFoundException' if is_local_java(dynamodbstreams) else 'ValidationException'):
            dynamodbstreams.get_shard_iterator(
                    StreamArn='sdfadfsddafgdafsgjnadflkgnalngalsdfnlkasnlkasdfasdfasf', ShardId=shards[0]['ShardId'], ShardIteratorType='LATEST'
                )

def test_get_shard_iterator_for_nonexistent_shard(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        with pytest.raises(ClientError, match='ResourceNotFoundException'):
            dynamodbstreams.get_shard_iterator(
                    StreamArn=arn, ShardId='adfasdasdasdasdasdasdasdasdasasdasd', ShardIteratorType='LATEST'
                )

def test_get_records(dynamodb, dynamodbstreams):
    # TODO: add tests for storage/transactionable variations and global/local index
    with create_stream_test_table(dynamodb, StreamViewType='NEW_AND_OLD_IMAGES') as table:
        (arn, label) = wait_for_active_stream(dynamodbstreams, table)

        p = 'piglet'
        c = 'ninja'
        val = 'lucifers'
        val2 = 'flowers'
        table.put_item(Item={'p': p, 'c': c, 'a1': val, 'a2': val2})

        nval = 'semprini'
        nval2 = 'nudge'
        table.update_item(Key={'p': p, 'c': c}, 
            AttributeUpdates={ 'a1': {'Value': nval, 'Action': 'PUT'},
                'a2': {'Value': nval2, 'Action': 'PUT'}
            })

        has_insert = False

        # in truth, we should sleep already here, since at least scylla
        # will not be able to produce any stream content until 
        # ~30s after insert/update (confidence iterval)
        # but it is useful to see a working null-iteration as well, so 
        # lets go already.
        while True:
            desc = dynamodbstreams.describe_stream(StreamArn=arn)
            iterators = []

            while True:
                shards = desc['StreamDescription']['Shards']

                for shard in shards:
                    shard_id = shard['ShardId']
                    start = shard['SequenceNumberRange']['StartingSequenceNumber']
                    iter = dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='AT_SEQUENCE_NUMBER',SequenceNumber=start)['ShardIterator']
                    iterators.append(iter)

                last_shard = desc["StreamDescription"].get("LastEvaluatedShardId")
                if not last_shard:
                    break

                desc = dynamodbstreams.describe_stream(StreamArn=arn, ExclusiveStartShardId=last_shard)

            next_iterators = []
            while iterators:
                iter = iterators.pop(0)
                response = dynamodbstreams.get_records(ShardIterator=iter, Limit=1000)
                if 'NextShardIterator' in response:
                    next_iterators.append(response['NextShardIterator'])

                records = response.get('Records')
                # print("Query {} -> {}".format(iter, records))
                if records:
                    for record in records:
                        # print("Record: {}".format(record))
                        type = record['eventName']
                        dynamodb = record['dynamodb']
                        keys = dynamodb['Keys']
                        
                        assert keys.get('p')
                        assert keys.get('c')
                        assert keys['p'].get('S')
                        assert keys['p']['S'] == p
                        assert keys['c'].get('S')
                        assert keys['c']['S'] == c

                        if type == 'MODIFY' or type == 'INSERT':
                            assert dynamodb.get('NewImage')
                            newimage = dynamodb['NewImage'];
                            assert newimage.get('a1')
                            assert newimage.get('a2')

                        if type == 'INSERT' or (type == 'MODIFY' and not has_insert):
                            assert newimage['a1']['S'] == val
                            assert newimage['a2']['S'] == val2
                            has_insert = True
                            continue
                        if type == 'MODIFY':
                            assert has_insert
                            assert newimage['a1']['S'] == nval
                            assert newimage['a2']['S'] == nval2
                            assert dynamodb.get('OldImage')
                            oldimage = dynamodb['OldImage'];
                            assert oldimage.get('a1')
                            assert oldimage.get('a2')
                            assert oldimage['a1']['S'] == val
                            assert oldimage['a2']['S'] == val2
                            return
            print("Not done. Sleep 10s...")
            time.sleep(10)
            iterators = next_iterators

def test_get_records_nonexistent_iterator(dynamodbstreams):
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodbstreams.get_records(ShardIterator='sdfsdfsgagaddafgagasgasgasdfasdfasdfasdfasdgasdasdgasdg', Limit=1000)

##############################################################################

# Fixtures for creating a table with a stream enabled with one of the allowed
# StreamViewType settings (KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES).
# Unfortunately changing the StreamViewType setting of an existing stream is
# not allowed (see test_streams_change_type), and while removing and re-adding
# a stream is posssible, it is very slow. So we create four different fixtures
# with the four different StreamViewType settings for these four fixtures.
#
# It turns out that DynamoDB makes reusing the same table in different tests
# very difficult, because when we request a "LATEST" iterator we sometimes
# miss the immediately following write (this issue doesn't happen in
# ALternator, just in DynamoDB - presumably LATEST adds some time slack?)
# So all the fixtures we create below have scope="function", meaning that a
# separate table is created for each of the tests using these fixtures. This
# slows the tests down a bit, but not by much (about 0.05 seconds per test).
# It is still worthwhile to use a fixture rather than to create a table
# explicitly - it is convenient, safe (the table gets deleted automatically)
# and if in the future we can work around the DynamoDB problem, we can return
# these fixtures to module scope.

def create_table_ss(dynamodb, dynamodbstreams, type):
    table = create_test_table(dynamodb,
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }, { 'AttributeName': 'c', 'AttributeType': 'S' }],
        StreamSpecification={ 'StreamEnabled': True, 'StreamViewType': type })
    (arn, label) = wait_for_active_stream(dynamodbstreams, table, timeout=60)
    yield table, arn
    table.delete()

@pytest.fixture(scope="function")
def test_table_ss_keys_only(dynamodb, dynamodbstreams):
    yield from create_table_ss(dynamodb, dynamodbstreams, 'KEYS_ONLY')

@pytest.fixture(scope="function")
def test_table_ss_new_image(dynamodb, dynamodbstreams):
    yield from create_table_ss(dynamodb, dynamodbstreams, 'NEW_IMAGE')

@pytest.fixture(scope="function")
def test_table_ss_old_image(dynamodb, dynamodbstreams):
    yield from create_table_ss(dynamodb, dynamodbstreams, 'OLD_IMAGE')

@pytest.fixture(scope="function")
def test_table_ss_new_and_old_images(dynamodb, dynamodbstreams):
    yield from create_table_ss(dynamodb, dynamodbstreams, 'NEW_AND_OLD_IMAGES')

# Test that it is, sadly, not allowed to use UpdateTable on a table which
# already has a stream enabled to change that stream's StreamViewType.
# Currently, Alternator does allow this (see issue #6939), so the test is
# marked xfail.
@pytest.mark.xfail(reason="Alternator allows changing StreamViewType - see issue #6939")
def test_streams_change_type(test_table_ss_keys_only):
    table, arn = test_table_ss_keys_only
    with pytest.raises(ClientError, match='ValidationException.*already'):
        table.update(StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'OLD_IMAGE'});
        # If the above change succeeded (because of issue #6939), switch it back :-)
        table.update(StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'});

# Utility function for listing all the shards of the given stream arn.
# Implemented by multiple calls to DescribeStream, possibly several pages
# until all the shards are returned. The return of this function should be
# cached - it is potentially slow, and DynamoDB documentation even states
# DescribeStream may only be called at a maximum rate of 10 times per second.
# list_shards() only returns the shard IDs, not the information about the
# shards' sequence number range, which is also returned by DescribeStream.
def list_shards(dynamodbstreams, arn):
    # By default DescribeStream lists a limit of 100 shards. For faster
    # tests we reduced the number of shards in the testing setup to
    # 32 (16 vnodes x 2 cpus), see issue #6979, so to still exercise this
    # paging feature, lets use a limit of 10.
    limit = 10
    response = dynamodbstreams.describe_stream(StreamArn=arn, Limit=limit)['StreamDescription']
    assert len(response['Shards']) <= limit
    shards = [x['ShardId'] for x in response['Shards']]
    while 'LastEvaluatedShardId' in response:
        # 7409 kinesis ignores LastEvaluatedShardId and just looks at last shard
        assert shards[-1] == response['LastEvaluatedShardId']
        response = dynamodbstreams.describe_stream(StreamArn=arn, Limit=limit,
            ExclusiveStartShardId=response['LastEvaluatedShardId'])['StreamDescription']
        assert len(response['Shards']) <= limit
        shards.extend([x['ShardId'] for x in response['Shards']])

    print('Number of shards in stream: {}'.format(len(shards)))
    assert len(set(shards)) == len(shards)
    # 7409 - kinesis required shards to be in lexical order.
    # verify.
    assert shards == sorted(shards)

    # special test: ensure we get nothing more if we ask for starting at the last
    # of the last
    response = dynamodbstreams.describe_stream(StreamArn=arn,
        ExclusiveStartShardId=shards[-1])['StreamDescription']
    assert len(response['Shards']) == 0

    return shards

# Utility function for getting shard iterators starting at "LATEST" for
# all the shards of the given stream arn.
def latest_iterators(dynamodbstreams, arn):
    iterators = []
    for shard_id in list_shards(dynamodbstreams, arn):
        iterators.append(dynamodbstreams.get_shard_iterator(StreamArn=arn,
            ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator'])
    assert len(set(iterators)) == len(iterators)
    return iterators

# Similar to latest_iterators(), just also returns the shard id which produced
# each iterator.
def shards_and_latest_iterators(dynamodbstreams, arn):
    shards_and_iterators = []
    for shard_id in list_shards(dynamodbstreams, arn):
        shards_and_iterators.append((shard_id, dynamodbstreams.get_shard_iterator(StreamArn=arn,
            ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator']))
    return shards_and_iterators

# Utility function for fetching more content from the stream (given its
# array of iterators) into an "output" array. Call repeatedly to get more
# content - the function returns a new array of iterators which should be
# used to replace the input list of iterators.
# Note that the order of the updates is guaranteed for the same partition,
# but cannot be trusted for *different* partitions.
def fetch_more(dynamodbstreams, iterators, output):
    new_iterators = []
    for iter in iterators:
        response = dynamodbstreams.get_records(ShardIterator=iter)
        if 'NextShardIterator' in response:
            new_iterators.append(response['NextShardIterator'])
        output.extend(response['Records'])
    assert len(set(new_iterators)) == len(new_iterators)
    return new_iterators

# Utility function for comparing "output" as fetched by fetch_more(), to a list
# expected_events, each of which looks like:
#   [type, keys, old_image, new_image]
# where type is REMOVE, INSERT or MODIFY.
# The "mode" parameter specifies which StreamViewType mode (KEYS_ONLY,
# OLD_IMAGE, NEW_IMAGE, NEW_AND_OLD_IMAGES) was supposedly used to generate
# "output". This mode dictates what we can compare - e.g., in KEYS_ONLY mode
# the compare_events() function ignores the the old and new image in
# expected_events.
# compare_events() throws an exception immediately if it sees an unexpected
# event, but if some of the expected events are just missing in the "output",
# it only returns false - suggesting maybe the caller needs to try again
# later - maybe more output is coming.
# Note that the order of events is only guaranteed (and therefore compared)
# inside a single partition.
def compare_events(expected_events, output, mode):
    # The order of expected_events is only meaningful inside a partition, so
    # let's convert it into a map indexed by partition key.
    expected_events_map = {}
    for event in expected_events:
        expected_type, expected_key, expected_old_image, expected_new_image = event
        # For simplicity, we actually use the entire key, not just the partiton
        # key. We only lose a bit of testing power we didn't plan to test anyway
        # (that events for different items in the same partition are ordered).
        key = freeze(expected_key)
        if not key in expected_events_map:
            expected_events_map[key] = []
        expected_events_map[key].append(event)
    # Iterate over the events in output. An event for a certain key needs to
    # be the *first* remaining event for this key in expected_events_map (and
    # then we remove this matched even from expected_events_map)
    for event in output:
        # In DynamoDB, eventSource is 'aws:dynamodb'. We decided to set it to
        # a *different* value - 'scylladb:alternator'. Issue #6931.
        assert 'eventSource' in event
        # Alternator is missing "awsRegion", which makes little sense for it
        # (although maybe we should have provided the DC name). Issue #6931.
        #assert 'awsRegion' in event
        # Alternator is also missing the "eventVersion" entry. Issue #6931.
        #assert 'eventVersion' in event
        # Check that eventID appears, but can't check much on what it is.
        assert 'eventID' in event
        op = event['eventName']
        record = event['dynamodb']
        # record['Keys'] is "serialized" JSON, ({'S', 'thestring'}), so we
        # want to deserialize it to match our expected_events content.
        deserializer = TypeDeserializer()
        key = {x:deserializer.deserialize(y) for (x,y) in record['Keys'].items()}
        expected_type, expected_key, expected_old_image, expected_new_image = expected_events_map[freeze(key)].pop(0)
        if expected_type != '*': # hack to allow a caller to not test op, to bypass issue #6918.
            assert op == expected_type
        assert record['StreamViewType'] == mode
        # We don't know what ApproximateCreationDateTime should be, but we do
        # know it needs to be a timestamp - there is conflicting documentation
        # in what format (ISO 8601?). In any case, boto3 parses this timestamp
        # for us, so we can't check it here, beyond checking it exists.
        assert 'ApproximateCreationDateTime' in record
        # We don't know what SequenceNumber is supposed to be, but the DynamoDB
        # documentation requires that it contains only numeric characters and
        # some libraries rely on this. This reproduces issue #7158:
        assert 'SequenceNumber' in record
        assert record['SequenceNumber'].isdecimal()
        # Alternator doesn't set the SizeBytes member. Issue #6931.
        #assert 'SizeBytes' in record
        if mode == 'KEYS_ONLY':
            assert not 'NewImage' in record
            assert not 'OldImage' in record
        elif mode == 'NEW_IMAGE':
            assert not 'OldImage' in record
            if expected_new_image == None:
                assert not 'NewImage' in record
            else:
                new_image = {x:deserializer.deserialize(y) for (x,y) in record['NewImage'].items()}
                assert expected_new_image == new_image
        elif mode == 'OLD_IMAGE':
            assert not 'NewImage' in record
            if expected_old_image == None:
                assert not 'OldImage' in record
                pass
            else:
                old_image = {x:deserializer.deserialize(y) for (x,y) in record['OldImage'].items()}
                assert expected_old_image == old_image
        elif mode == 'NEW_AND_OLD_IMAGES':
            if expected_new_image == None:
                assert not 'NewImage' in record
            else:
                new_image = {x:deserializer.deserialize(y) for (x,y) in record['NewImage'].items()}
                assert expected_new_image == new_image
            if expected_old_image == None:
                assert not 'OldImage' in record
            else:
                old_image = {x:deserializer.deserialize(y) for (x,y) in record['OldImage'].items()}
                assert expected_old_image == old_image
        else:
            pytest.fail('cannot happen')
    # After the above loop, expected_events_map should remain empty arrays.
    # If it isn't, one of the expected events did not yet happen. Return False.
    for entry in expected_events_map.values():
        if len(entry) > 0:
            return False
    return True

# Convenience funtion used to implement several tests below. It runs a given
# function "updatefunc" which is supposed to do some updates to the table
# and also return an expected_events list. do_test() then fetches the streams
# data and compares it to the expected_events using compare_events().
def do_test(test_table_ss_stream, dynamodbstreams, updatefunc, mode, p = random_string(), c = random_string()):
    table, arn = test_table_ss_stream
    iterators = latest_iterators(dynamodbstreams, arn)
    expected_events = updatefunc(table, p, c)
    # Updating the stream is asynchronous. Moreover, it may even be delayed
    # artificially (see Alternator's alternator_streams_time_window_s config).
    # So if compare_events() reports the stream data is missing some of the
    # expected events (by returning False), we need to retry it for some time.
    # Note that compare_events() throws if the stream data contains *wrong*
    # (unexpected) data, so even failing tests usually finish reasonably
    # fast - depending on the alternator_streams_time_window_s parameter.
    # This is optimization is important to keep *failing* tests reasonably
    # fast and not have to wait until the following arbitrary timeout.
    timeout = time.time() + 20
    output = []
    while time.time() < timeout:
        iterators = fetch_more(dynamodbstreams, iterators, output)
        print("after fetch_more number expected_events={}, output={}".format(len(expected_events), len(output)))
        if compare_events(expected_events, output, mode):
            # success!
            return
        time.sleep(0.5)
    # If we're still here, the last compare_events returned false.
    pytest.fail('missing events in output: {}'.format(output))

# Test a single PutItem of a new item. Should result in a single INSERT
# event. Currently fails because in Alternator, PutItem - which generates a
# tombstone to *replace* an item - generates REMOVE+MODIFY (issue #6930).
@pytest.mark.xfail(reason="Currently fails - see issue #6930")
def test_streams_putitem_keys_only(test_table_ss_keys_only, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.put_item(Item={'p': p, 'c': c, 'x': 2})
        events.append(['INSERT', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
        return events
    do_test(test_table_ss_keys_only, dynamodbstreams, do_updates, 'KEYS_ONLY')

# Test a single UpdateItem. Should result in a single INSERT event.
# Currently fails because Alternator generates a MODIFY event even though
# this is a new item (issue #6918).
@pytest.mark.xfail(reason="Currently fails - see issue #6918")
def test_streams_updateitem_keys_only(test_table_ss_keys_only, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 2})
        events.append(['INSERT', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
        return events
    do_test(test_table_ss_keys_only, dynamodbstreams, do_updates, 'KEYS_ONLY')

# This is exactly the same test as test_streams_updateitem_keys_only except
# we don't verify the type of even we find (MODIFY or INSERT). It allows us
# to have at least one good GetRecords test until solving issue #6918.
# When we do solve that issue, this test should be removed.
def test_streams_updateitem_keys_only_2(test_table_ss_keys_only, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 2})
        events.append(['*', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
        return events
    do_test(test_table_ss_keys_only, dynamodbstreams, do_updates, 'KEYS_ONLY')

# Test OLD_IMAGE using UpdateItem. Verify that the OLD_IMAGE indeed includes,
# as needed, the entire old item and not just the modified columns.
# Reproduces issue #6935
def test_streams_updateitem_old_image(test_table_ss_old_image, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 2})
        # We use here '*' instead of the really expected 'INSERT' to avoid
        # checking again the same Alternator bug already checked by
        # test_streams_updateitem_keys_only (issue #6918).
        # Note: The "None" as OldImage here tests that the OldImage should be
        # missing because the item didn't exist. This reproduces issue #6933.
        events.append(['*', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET y = :val1', ExpressionAttributeValues={':val1': 3})
        events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'x': 2}, {'p': p, 'c': c, 'x': 2, 'y': 3}])
        return events
    do_test(test_table_ss_old_image, dynamodbstreams, do_updates, 'OLD_IMAGE')

# Above we verified that if an item did not previously exist, the OLD_IMAGE
# would be missing, but if the item did previously exist, OLD_IMAGE should
# be present and must include the key. Here we confirm the special case the
# latter case - the case of a pre-existing *empty* item, which has just the
# key - in this case since the item did exist, OLD_IMAGE should be returned -
# and include just the key. This is a special case of reproducing #6935 -
# the first patch for this issue failed in this special case.
def test_streams_updateitem_old_image_empty_item(test_table_ss_old_image, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        # Create an *empty* item, with nothing except a key:
        table.update_item(Key={'p': p, 'c': c})
        events.append(['*', {'p': p, 'c': c}, None, {'p': p, 'c': c}])
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET y = :val1', ExpressionAttributeValues={':val1': 3})
        # Note that OLD_IMAGE should be present and be the empty item,
        # with just a key, not entirely missing.
        events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c}, {'p': p, 'c': c, 'y': 3}])
        return events
    do_test(test_table_ss_old_image, dynamodbstreams, do_updates, 'OLD_IMAGE')

# Test that OLD_IMAGE indeed includes the entire old item and not just the
# modified attributes, in the special case of attributes which are a key of
# a secondary index.
# The unique thing about this case is that as currently implemented,
# secondary-index key attributes are real Scylla columns, contrasting with
# other attributes which are just elements of a map. And our CDC
# implementation treats those cases differently - when a map is modified
# the preimage includes the entire content of the map, but for regular
# columns they are only included in the preimage if they change.
# Currently fails in Alternator because the item's key is missing in
# OldImage (#6935) and the LSI key is also missing (#7030).
@pytest.fixture(scope="function")
def test_table_ss_old_image_and_lsi(dynamodb, dynamodbstreams):
    table = create_test_table(dynamodb,
        KeySchema=[
            {'AttributeName': 'p', 'KeyType': 'HASH'},
            {'AttributeName': 'c', 'KeyType': 'RANGE'}],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'c', 'AttributeType': 'S' },
            { 'AttributeName': 'k', 'AttributeType': 'S' }],
        LocalSecondaryIndexes=[{
            'IndexName': 'index',
            'KeySchema': [
                {'AttributeName': 'p', 'KeyType': 'HASH'},
                {'AttributeName': 'k', 'KeyType': 'RANGE'}],
            'Projection': { 'ProjectionType': 'ALL' }
        }],
        StreamSpecification={ 'StreamEnabled': True, 'StreamViewType': 'OLD_IMAGE' })
    (arn, label) = wait_for_active_stream(dynamodbstreams, table, timeout=60)
    yield table, arn
    table.delete()

def test_streams_updateitem_old_image_lsi(test_table_ss_old_image_and_lsi, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET x = :x, k = :k',
            ExpressionAttributeValues={':x': 2, ':k': 'dog'})
        # We use here '*' instead of the really expected 'INSERT' to avoid
        # checking again the same Alternator bug already checked by
        # test_streams_updateitem_keys_only (issue #6918).
        events.append(['*', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2, 'k': 'dog'}])
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET y = :y', ExpressionAttributeValues={':y': 3})
        # In issue #7030, the 'k' value was missing from the OldImage.
        events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'x': 2, 'k': 'dog'}, {'p': p, 'c': c, 'x': 2, 'k': 'dog', 'y': 3}])
        return events
    do_test(test_table_ss_old_image_and_lsi, dynamodbstreams, do_updates, 'OLD_IMAGE')

# Tests similar to the above tests for OLD_IMAGE, just for NEW_IMAGE mode.
# Verify that the NEW_IMAGE includes the entire old item (including the key),
# that deleting the item results in a missing NEW_IMAGE, and that setting the
# item to be empty has a different result - a NEW_IMAGE with just a key.
# Reproduces issue #7107.
def test_streams_new_image(test_table_ss_new_image, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 2})
        # Confirm that when adding one attribute "x", the NewImage contains both
        # the new x, and the key columns (p and c).
        # We use here '*' instead of 'INSERT' to avoid testing issue #6918 here.
        events.append(['*', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
        # Confirm that when adding just attribute "y", the NewImage will contain
        # all the attributes, including the old "x":
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET y = :val1', ExpressionAttributeValues={':val1': 3})
        events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'x': 2}, {'p': p, 'c': c, 'x': 2, 'y': 3}])
        # Confirm that when deleting the columns x and y, the NewImage becomes
        # empty - but still exists and contains the key columns,
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='REMOVE x, y')
        events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'x': 2, 'y': 3}, {'p': p, 'c': c}])
        # Confirm that deleting the item results in a missing NewImage:
        table.delete_item(Key={'p': p, 'c': c})
        events.append(['REMOVE', {'p': p, 'c': c}, {'p': p, 'c': c}, None])
        return events
    do_test(test_table_ss_new_image, dynamodbstreams, do_updates, 'NEW_IMAGE')

# Test similar to the above test for NEW_IMAGE corner cases, but here for
# NEW_AND_OLD_IMAGES mode.
# Although it is likely that if both OLD_IMAGE and NEW_IMAGE work correctly then
# so will the combined NEW_AND_OLD_IMAGES mode, it is still possible that the
# implementation of the combined mode has unique bugs, so it is worth testing
# it separately.
# Reproduces issue #7107.
def test_streams_new_and_old_images(test_table_ss_new_and_old_images, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 2})
        # The item doesn't yet exist, so OldImage is missing.
        # We use here '*' instead of 'INSERT' to avoid testing issue #6918 here.
        events.append(['*', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
        # Confirm that when adding just attribute "y", the NewImage will contain
        # all the attributes, including the old "x". Also, OldImage contains the
        # key attributes, not just "x":
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET y = :val1', ExpressionAttributeValues={':val1': 3})
        events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'x': 2}, {'p': p, 'c': c, 'x': 2, 'y': 3}])
        # Confirm that when deleting the attributes x and y, the NewImage becomes
        # empty - but still exists and contains the key attributes:
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='REMOVE x, y')
        events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'x': 2, 'y': 3}, {'p': p, 'c': c}])
        # Confirm that when adding an attribute z to the empty item, OldItem is
        # not missing - it just contains only the key attributes:
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET z = :val1', ExpressionAttributeValues={':val1': 4})
        events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c}, {'p': p, 'c': c, 'z': 4}])
        # Confirm that deleting the item results in a missing NewImage:
        table.delete_item(Key={'p': p, 'c': c})
        events.append(['REMOVE', {'p': p, 'c': c}, {'p': p, 'c': c, 'z': 4}, None])
        return events
    do_test(test_table_ss_new_and_old_images, dynamodbstreams, do_updates, 'NEW_AND_OLD_IMAGES')

# Test that when a stream shard has no data to read, GetRecords returns an
# empty Records array - not a missing one. Reproduces issue #6926.
def test_streams_no_records(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    # Get just one shard - any shard - and its LATEST iterator. Because it's
    # LATEST, there will be no data to read from this iterator.
    shard = dynamodbstreams.describe_stream(StreamArn=arn, Limit=1)['StreamDescription']['Shards'][0]
    shard_id = shard['ShardId']
    iter = dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator']
    response = dynamodbstreams.get_records(ShardIterator=iter)
    assert 'NextShardIterator' in response or 'EndingSequenceNumber' in shard['SequenceNumberRange']
    assert 'Records' in response
    # We expect Records to be empty - there is no data at the LATEST iterator.
    assert response['Records'] == []

# Test that after fetching the last result from a shard, we don't get it
# yet again. Reproduces issue #6942.
def test_streams_last_result(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    iterators = latest_iterators(dynamodbstreams, arn)
    # Do an UpdateItem operation that is expected to leave one event in the
    # stream.
    table.update_item(Key={'p': random_string(), 'c': random_string()},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 5})
    # Eventually (we may need to retry this for a while), *one* of the
    # stream shards will return one event:
    timeout = time.time() + 15
    while time.time() < timeout:
        for iter in iterators:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            if 'Records' in response and response['Records'] != []:
                # Found the shard with the data! Test that it only has
                # one event and that if we try to read again, we don't
                # get more data (this was issue #6942).
                assert len(response['Records']) == 1
                assert 'NextShardIterator' in response
                response = dynamodbstreams.get_records(ShardIterator=response['NextShardIterator'])
                assert response['Records'] == []
                return
        time.sleep(0.5)
    pytest.fail("timed out")

# In test_streams_last_result above we tested that there is no further events
# after reading the only one. In this test we verify that if we *do* perform
# another change on the same key, we do get another event and it happens on the
# *same* shard.
def test_streams_another_result(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    iterators = latest_iterators(dynamodbstreams, arn)
    # Do an UpdateItem operation that is expected to leave one event in the
    # stream.
    p = random_string()
    c = random_string()
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 5})
    # Eventually, *one* of the stream shards will return one event:
    timeout = time.time() + 15
    while time.time() < timeout:
        for iter in iterators:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            if 'Records' in response and response['Records'] != []:
                # Finally found the shard reporting the changes to this key
                assert len(response['Records']) == 1
                assert response['Records'][0]['dynamodb']['Keys']== {'p': {'S': p}, 'c': {'S': c}}
                assert 'NextShardIterator' in response
                iter = response['NextShardIterator']
                # Found the shard with the data. It only has one event so if
                # we try to read again, we find nothing (this is the same as
                # what test_streams_last_result tests).
                response = dynamodbstreams.get_records(ShardIterator=iter)
                assert response['Records'] == []
                assert 'NextShardIterator' in response
                iter = response['NextShardIterator']
                # Do another UpdateItem operation to the same key, so it is
                # expected to write to the *same* shard:
                table.update_item(Key={'p': p, 'c': c},
                    UpdateExpression='SET x = :val2', ExpressionAttributeValues={':val2': 7})
                # Again we may need to wait for the event to appear on the stream:
                timeout = time.time() + 15
                while time.time() < timeout:
                    response = dynamodbstreams.get_records(ShardIterator=iter)
                    if 'Records' in response and response['Records'] != []:
                        assert len(response['Records']) == 1
                        assert response['Records'][0]['dynamodb']['Keys']== {'p': {'S': p}, 'c': {'S': c}}
                        assert 'NextShardIterator' in response
                        # The test is done, successfully.
                        return
                    time.sleep(0.5)
                pytest.fail("timed out")
        time.sleep(0.5)
    pytest.fail("timed out")

# Test the SequenceNumber attribute returned for stream events, and the
# "AT_SEQUENCE_NUMBER" iterator that can be used to re-read from the same
# event again given its saved "sequence number".
def test_streams_at_sequence_number(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    shards_and_iterators = shards_and_latest_iterators(dynamodbstreams, arn)
    # Do an UpdateItem operation that is expected to leave one event in the
    # stream.
    p = random_string()
    c = random_string()
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 5})
    # Eventually, *one* of the stream shards will return the one event:
    timeout = time.time() + 15
    while time.time() < timeout:
        for (shard_id, iter) in shards_and_iterators:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            if 'Records' in response and response['Records'] != []:
                # Finally found the shard reporting the changes to this key:
                assert len(response['Records']) == 1
                assert response['Records'][0]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                assert 'NextShardIterator' in response
                sequence_number = response['Records'][0]['dynamodb']['SequenceNumber']
                # Found the shard with the data. It only has one event so if
                # we try to read again, we find nothing (this is the same as
                # what test_streams_last_result tests).
                iter = response['NextShardIterator']
                response = dynamodbstreams.get_records(ShardIterator=iter)
                assert response['Records'] == []
                assert 'NextShardIterator' in response
                # If we use the SequenceNumber of the first event to create an
                # AT_SEQUENCE_NUMBER iterator, we can read the same event again.
                # We don't need a loop and a timeout, because this event is already
                # available.
                iter = dynamodbstreams.get_shard_iterator(StreamArn=arn,
                    ShardId=shard_id, ShardIteratorType='AT_SEQUENCE_NUMBER',
                    SequenceNumber=sequence_number)['ShardIterator']
                response = dynamodbstreams.get_records(ShardIterator=iter)
                assert 'Records' in response
                assert len(response['Records']) == 1
                assert response['Records'][0]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                assert response['Records'][0]['dynamodb']['SequenceNumber'] == sequence_number
                return
        time.sleep(0.5)
    pytest.fail("timed out")

# Test the SequenceNumber attribute returned for stream events, and the
# "AFTER_SEQUENCE_NUMBER" iterator that can be used to re-read *after* the same
# event again given its saved "sequence number".
def test_streams_after_sequence_number(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    shards_and_iterators = shards_and_latest_iterators(dynamodbstreams, arn)
    # Do two UpdateItem operations to the same key, that are expected to leave
    # two events in the stream.
    p = random_string()
    c = random_string()
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 3})
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 5})
    # Eventually, *one* of the stream shards will return the two events:
    timeout = time.time() + 15
    while time.time() < timeout:
        for (shard_id, iter) in shards_and_iterators:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            if 'Records' in response and len(response['Records']) == 2:
                assert response['Records'][0]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                assert response['Records'][1]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                sequence_number_1 = response['Records'][0]['dynamodb']['SequenceNumber']
                sequence_number_2 = response['Records'][1]['dynamodb']['SequenceNumber']

                # #7424 - AWS sdk assumes sequence numbers can be compared
                # as bigints, and are monotonically growing.
                assert int(sequence_number_1) < int(sequence_number_2)

                # If we use the SequenceNumber of the first event to create an
                # AFTER_SEQUENCE_NUMBER iterator, we can read the second event
                # (only) again. We don't need a loop and a timeout, because this
                # event is already available.
                iter = dynamodbstreams.get_shard_iterator(StreamArn=arn,
                    ShardId=shard_id, ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                    SequenceNumber=sequence_number_1)['ShardIterator']
                response = dynamodbstreams.get_records(ShardIterator=iter)
                assert 'Records' in response
                assert len(response['Records']) == 1
                assert response['Records'][0]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                assert response['Records'][0]['dynamodb']['SequenceNumber'] == sequence_number_2
                return
        time.sleep(0.5)
    pytest.fail("timed out")

# Test the "TRIM_HORIZON" iterator, which can be used to re-read *all* the
# previously-read events of the stream shard again.
# NOTE: This test relies on the test_table_ss_keys_only fixture giving us a
# brand new stream, with no old events saved from other tests. If we ever
# change this, we should change this test to use a different fixture.
def test_streams_trim_horizon(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    shards_and_iterators = shards_and_latest_iterators(dynamodbstreams, arn)
    # Do two UpdateItem operations to the same key, that are expected to leave
    # two events in the stream.
    p = random_string()
    c = random_string()
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 3})
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 5})
    # Eventually, *one* of the stream shards will return the two events:
    timeout = time.time() + 15
    while time.time() < timeout:
        for (shard_id, iter) in shards_and_iterators:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            if 'Records' in response and len(response['Records']) == 2:
                assert response['Records'][0]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                assert response['Records'][1]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                sequence_number_1 = response['Records'][0]['dynamodb']['SequenceNumber']
                sequence_number_2 = response['Records'][1]['dynamodb']['SequenceNumber']
                # If we use the TRIM_HORIZON iterator, we should receive the
                # same two events again, in the same order.
                # Note that we assume that the fixture gave us a brand new
                # stream, with no old events saved from other tests. If we
                # couldn't assume this, this test would need to become much
                # more complex, and would need to read from this shard until
                # we find the two events we are looking for.
                iter = dynamodbstreams.get_shard_iterator(StreamArn=arn,
                    ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')['ShardIterator']
                response = dynamodbstreams.get_records(ShardIterator=iter)
                assert 'Records' in response
                assert len(response['Records']) == 2
                assert response['Records'][0]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                assert response['Records'][1]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                assert response['Records'][0]['dynamodb']['SequenceNumber'] == sequence_number_1
                assert response['Records'][1]['dynamodb']['SequenceNumber'] == sequence_number_2
                return
        time.sleep(0.5)
    pytest.fail("timed out")

# Test the StartingSequenceNumber information returned by DescribeStream.
# The DynamoDB documentation explains that StartingSequenceNumber is
# "The first sequence number for the stream records contained within a shard"
# which suggests that is should be the sequence number of the first event
# that can actually be read from the shard. However, in Alternator the
# StartingSequenceNumber may be *before* any of the events on the shard -
# it is the time that the shard was created. In Alternator, it is possible
# that no event appeared on this shard for some time after it was created,
# and it is also possible that after more than 24 hours, some of the original
# items that existed in the shard have expired. Nevertheless, we believe
# that the important thing is that reading a shard starting at
# StartingSequenceNumber will result in reading all the available items -
# similar to how TRIM_HORIZON works. This is what the following test verifies.
def test_streams_starting_sequence_number(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    # Do two UpdateItem operations to the same key, that are expected to leave
    # two events in the stream.
    p = random_string()
    c = random_string()
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 3})
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 5})
    # Get for all the stream shards the iterator starting at the shard's
    # StartingSequenceNumber:
    response = dynamodbstreams.describe_stream(StreamArn=arn)
    shards = response['StreamDescription']['Shards']
    while 'LastEvaluatedShardId' in response:
        response = dynamodbstreams.describe_stream(StreamArn=arn,
            ExclusiveStartShardId=response['LastEvaluatedShardId'])
        shards.extend(response['StreamDescription']['Shards'])
    iterators = []
    for shard in shards:
        shard_id = shard['ShardId']
        start = shard['SequenceNumberRange']['StartingSequenceNumber']
        assert start.isdecimal()
        iterators.append(dynamodbstreams.get_shard_iterator(StreamArn=arn,
            ShardId=shard_id, ShardIteratorType='AT_SEQUENCE_NUMBER',
            SequenceNumber=start)['ShardIterator'])

    # Eventually, *one* of the stream shards will return the two events:
    timeout = time.time() + 15
    while time.time() < timeout:
        for iter in iterators:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            if 'Records' in response and len(response['Records']) == 2:
                assert response['Records'][0]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                assert response['Records'][1]['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                return
        time.sleep(0.5)
    pytest.fail("timed out")

# Above we tested some specific operations in small tests aimed to reproduce
# a specific bug, in the following tests we do a all the different operations,
# PutItem, DeleteItem, BatchWriteItem and UpdateItem, and check the resulting
# stream for correctness.
# The following tests focus on mulitple operations on the *same* item. Those
# should appear in the stream in the correct order.
def do_updates_1(table, p, c):
    events = []
    # a first put_item appears as an INSERT event. Note also empty old_image.
    table.put_item(Item={'p': p, 'c': c, 'x': 2})
    events.append(['INSERT', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
    # a second put_item of the *same* key and same value, doesn't appear in the log at all!
    table.put_item(Item={'p': p, 'c': c, 'x': 2})
    # a second put_item of the *same* key and different value, appears as a MODIFY event
    table.put_item(Item={'p': p, 'c': c, 'y': 3})
    events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'x': 2}, {'p': p, 'c': c, 'y': 3}])
    # deleting an item appears as a REMOVE event. Note no new_image at all, but there is an old_image.
    table.delete_item(Key={'p': p, 'c': c})
    events.append(['REMOVE', {'p': p, 'c': c}, {'p': p, 'c': c, 'y': 3}, None])
    # deleting a non-existant item doesn't appear in the log at all.
    table.delete_item(Key={'p': p, 'c': c})
    # If update_item creates an item, the event is INSERT as well.
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET b = :val1',
        ExpressionAttributeValues={':val1': 4})
    events.append(['INSERT', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'b': 4}])
    # If update_item modifies the item, note how old and new image includes both old and new columns
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET x = :val1',
        ExpressionAttributeValues={':val1': 5})
    events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'b': 4}, {'p': p, 'c': c, 'b': 4, 'x': 5}])
    # TODO: incredibly, if we uncomment the "REMOVE b" update below, it will be
    # completely missing from the DynamoDB stream - the test continues to
    # pass even though we didn't add another expected event, and even though
    # the preimage in the following expected event includes this "b" we will
    # remove. I couldn't reproduce this apparant DynamoDB bug in a smaller test.
    #table.update_item(Key={'p': p, 'c': c}, UpdateExpression='REMOVE b')
    # Test BatchWriteItem as well. This modifies the item, so will be a MODIFY event.
    table.meta.client.batch_write_item(RequestItems = {table.name: [{'PutRequest': {'Item': {'p': p, 'c': c, 'x': 5}}}]})
    events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'b': 4, 'x': 5}, {'p': p, 'c': c, 'x': 5}])
    return events

@pytest.mark.xfail(reason="Currently fails - because of multiple issues listed above")
def test_streams_1_keys_only(test_table_ss_keys_only, dynamodbstreams):
    do_test(test_table_ss_keys_only, dynamodbstreams, do_updates_1, 'KEYS_ONLY')

@pytest.mark.xfail(reason="Currently fails - because of multiple issues listed above")
def test_streams_1_new_image(test_table_ss_new_image, dynamodbstreams):
    do_test(test_table_ss_new_image, dynamodbstreams, do_updates_1, 'NEW_IMAGE')

@pytest.mark.xfail(reason="Currently fails - because of multiple issues listed above")
def test_streams_1_old_image(test_table_ss_old_image, dynamodbstreams):
    do_test(test_table_ss_old_image, dynamodbstreams, do_updates_1, 'OLD_IMAGE')

@pytest.mark.xfail(reason="Currently fails - because of multiple issues listed above")
def test_streams_1_new_and_old_images(test_table_ss_new_and_old_images, dynamodbstreams):
    do_test(test_table_ss_new_and_old_images, dynamodbstreams, do_updates_1, 'NEW_AND_OLD_IMAGES')

# A fixture which creates a test table with a stream enabled, and returns a
# bunch of interesting information collected from the CreateTable response.
# This fixture is module-scoped - it can be shared by multiple tests below,
# because we are not going to actually use or change this stream, we will
# just do multiple tests on its setup.
@pytest.fixture(scope="module")
def test_table_stream_with_result(dynamodb, dynamodbstreams):
    tablename = test_table_name()
    result = dynamodb.meta.client.create_table(TableName=tablename,
        BillingMode='PAY_PER_REQUEST',
        StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'},
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ])
    waiter = dynamodb.meta.client.get_waiter('table_exists')
    waiter.config.delay = 1
    waiter.config.max_attempts = 200
    waiter.wait(TableName=tablename)
    table = dynamodb.Table(tablename)
    yield result, table
    while True:
        try:
            table.delete()
            return
        except ClientError as ce:
            # if the table has a stream currently being created we cannot
            # delete the table immediately. Again, only with real dynamo
            if ce.response['Error']['Code'] == 'ResourceInUseException':
                print('Could not delete table yet. Sleeping 5s.')
                time.sleep(5)
                continue;
            raise

# Wait for a table to return to "ACTIVE" status. We need to call this after
# doing an UpdateTable to a table - because before this wait finishes we are
# not allowed to update the same table again or delete it.
def wait_for_status_active(table):
    start_time = time.time()
    for i in range(60):
        desc = table.meta.client.describe_table(TableName=table.name)
        if desc['Table']['TableStatus'] == 'ACTIVE':
            return
        time.sleep(1)
    pytest.fail("wait_for_status_active() timed out")

# Test that in a table with Streams enabled, LatestStreamArn is returned
# by CreateTable, DescribeTable and UpdateTable, and is the same ARN as
# returned by ListStreams. Reproduces issue #7157.
def test_latest_stream_arn(test_table_stream_with_result, dynamodbstreams):
    (result, table) = test_table_stream_with_result
    assert 'LatestStreamArn' in result['TableDescription']
    arn_in_create_table = result['TableDescription']['LatestStreamArn']
    # Check that ListStreams returns the same stream ARN as returned
    # by the original CreateTable
    (arn_in_list_streams, label) = wait_for_active_stream(dynamodbstreams, table)
    assert arn_in_create_table == arn_in_list_streams
    # Check that DescribeTable also includes the same LatestStreamArn:
    desc = table.meta.client.describe_table(TableName=table.name)['Table']
    assert 'LatestStreamArn' in desc
    assert desc['LatestStreamArn'] == arn_in_create_table
    # Check that UpdateTable also includes the same LatestStreamArn.
    # The "update" changes nothing (it just sets BillingMode to what it was).
    desc = table.meta.client.update_table(TableName=table.name,
            BillingMode='PAY_PER_REQUEST')['TableDescription']
    assert desc['LatestStreamArn'] == arn_in_create_table
    wait_for_status_active(table)

# Test that in a table with Streams enabled, LatestStreamLabel is returned
# by CreateTable, DescribeTable and UpdateTable, and is the same "label" as
# returned by ListStreams. Reproduces issue #7162.
def test_latest_stream_label(test_table_stream_with_result, dynamodbstreams):
    (result, table) = test_table_stream_with_result
    assert 'LatestStreamLabel' in result['TableDescription']
    label_in_create_table = result['TableDescription']['LatestStreamLabel']
    # Check that ListStreams returns the same stream label as returned
    # by the original CreateTable
    (arn, label) = wait_for_active_stream(dynamodbstreams, table)
    assert label_in_create_table == label
    # Check that DescribeTable also includes the same LatestStreamLabel:
    desc = table.meta.client.describe_table(TableName=table.name)['Table']
    assert 'LatestStreamLabel' in desc
    assert desc['LatestStreamLabel'] == label_in_create_table
    # Check that UpdateTable also includes the same LatestStreamLabel.
    # The "update" changes nothing (it just sets BillingMode to what it was).
    desc = table.meta.client.update_table(TableName=table.name,
            BillingMode='PAY_PER_REQUEST')['TableDescription']
    assert desc['LatestStreamLabel'] == label_in_create_table
    wait_for_status_active(table)

# Test that in a table with Streams enabled, StreamSpecification is returned
# by CreateTable, DescribeTable and UpdateTable. Reproduces issue #7163.
def test_stream_specification(test_table_stream_with_result, dynamodbstreams):
    # StreamSpecification as set in test_table_stream_with_result:
    stream_specification = {'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'}
    (result, table) = test_table_stream_with_result
    assert 'StreamSpecification' in result['TableDescription']
    assert stream_specification == result['TableDescription']['StreamSpecification']
    # Check that DescribeTable also includes the same StreamSpecification:
    desc = table.meta.client.describe_table(TableName=table.name)['Table']
    assert 'StreamSpecification' in desc
    assert stream_specification == desc['StreamSpecification']
    # Check that UpdateTable also includes the same StreamSpecification.
    # The "update" changes nothing (it just sets BillingMode to what it was).
    desc = table.meta.client.update_table(TableName=table.name,
            BillingMode='PAY_PER_REQUEST')['TableDescription']
    assert stream_specification == desc['StreamSpecification']
    wait_for_status_active(table)

# The following test checks the behavior of *closed* shards.
# We achieve a closed shard by disabling the stream - the DynamoDB
# documentation states that "If you disable a stream, any shards that are
# still open will be closed. The data in the stream will continue to be
# readable for 24 hours". In the test we verify that indeed, after a shard
# is closed, it is still readable with GetRecords (reproduces issue #7239).
# Moreover, upon reaching the end of data in the shard, the NextShardIterator
# attribute should say that the end was reached. The DynamoDB documentation
# says that NextShardIterator should be "set to null" in this case - but it
# is not clear what "null" means in this context: Should NextShardIterator
# be missing? Or a "null" JSON type? Or an empty string? This test verifies
# that the right answer is that NextShardIterator should be *missing*
# (reproduces issue #7237).
@pytest.mark.xfail(reason="disabled stream is deleted - issue #7239")
def test_streams_closed_read(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    shards_and_iterators = shards_and_latest_iterators(dynamodbstreams, arn)
    # Do an UpdateItem operation that is expected to leave one event in the
    # stream.
    table.update_item(Key={'p': random_string(), 'c': random_string()},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 5})
    # Disable streaming for this table. Note that the test_table_ss_keys_only
    # fixture has "function" scope so it is fine to ruin table, it will not
    # be used in other tests.
    disable_stream(dynamodbstreams, table)

    # Even after streaming is disabled for the table, we can still read
    # from the earlier stream (it is guaranteed to work for 24 hours).
    # The iterators we got earlier should still be fully usable, and
    # eventually *one* of the stream shards will return one event:
    timeout = time.time() + 15
    while time.time() < timeout:
        for (shard_id, iter) in shards_and_iterators:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            if 'Records' in response and response['Records'] != []:
                # Found the shard with the data! Test that it only has
                # one event. NextShardIterator should either be missing now,
                # indicating that it is a closed shard (DynamoDB does this),
                # or, it may (and currently does in Alternator) return another
                # and reading from *that* iterator should then tell us that
                # we reached the end of the shard (i.e., zero results and
                # missing NextShardIterator).
                assert len(response['Records']) == 1
                if 'NextShardIterator' in response:
                    response = dynamodbstreams.get_records(ShardIterator=response['NextShardIterator'])
                    assert len(response['Records']) == 0
                    assert not 'NextShardIterator' in response
                # Until now we verified that we can read the closed shard
                # using an old iterator. Let's test now that the closed
                # shard id is also still valid, and a new iterator can be
                # created for it, and the old data can be read from it:
                iter = dynamodbstreams.get_shard_iterator(StreamArn=arn,
                    ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')['ShardIterator']
                response = dynamodbstreams.get_records(ShardIterator=iter)
                assert len(response['Records']) == 1
                return
        time.sleep(0.5)
    pytest.fail("timed out")

# In the above test (test_streams_closed_read) we used a disabled stream as
# a means to generate a closed shard, and tested the behavior of that closed
# shard. In the following test, we do more extensive testing on the the
# behavior of a disabled stream and verify that it is sill usable (for 24
# hours), reproducing issue #7239: The disabled stream's ARN should still be
# listed for the table, this ARN should continue to work, listing the
# stream's shards should give an indication that they are all closed - but
# all these shards should still be readable.
@pytest.mark.xfail(reason="disabled stream is deleted - issue #7239")
def test_streams_disabled_stream(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    iterators = latest_iterators(dynamodbstreams, arn)
    # Do an UpdateItem operation that is expected to leave one event in the
    # stream.
    table.update_item(Key={'p': random_string(), 'c': random_string()},
        UpdateExpression='SET x = :x', ExpressionAttributeValues={':x': 5})

    # Wait for this one update to become available in the stream before we
    # disable the stream. Otherwise, theoretically (although unlikely in
    # practice) we may disable the stream before the update was saved to it.
    timeout = time.time() + 15
    found = False
    while time.time() < timeout and not found:
        for iter in iterators:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            if 'Records' in response and len(response['Records']) > 0:
                found = True
                break
        time.sleep(0.5)
    assert found

    # Disable streaming for this table. Note that the test_table_ss_keys_only
    # fixture has "function" scope so it is fine to ruin table, it will not
    # be used in other tests.
    disable_stream(dynamodbstreams, table)

    # Check that the stream ARN which we previously got for the disabled
    # stream is still listed by ListStreams
    arns = [stream['StreamArn'] for stream in dynamodbstreams.list_streams(TableName=table.name)['Streams']]
    assert arn in arns

    # DescribeStream on the disabled stream still works and lists its shards.
    # All these shards are listed as being closed (i.e., should have
    # EndingSequenceNumber). The basic details of the stream (e.g., the view
    # type) are available and the status of the stream is DISABLED.
    response = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
    assert response['StreamStatus'] == 'DISABLED'
    assert response['StreamViewType'] == 'KEYS_ONLY'
    assert response['TableName'] == table.name
    shards_info = response['Shards']
    while 'LastEvaluatedShardId' in response:
        response = dynamodbstreams.describe_stream(StreamArn=arn, ExclusiveStartShardId=response['LastEvaluatedShardId'])['StreamDescription']
        assert response['StreamStatus'] == 'DISABLED'
        assert response['StreamViewType'] == 'KEYS_ONLY'
        assert response['TableName'] == table.name
        shards_info.extend(response['Shards'])
    print('Number of shards in stream: {}'.format(len(shards_info)))
    for shard in shards_info:
        assert 'EndingSequenceNumber' in shard['SequenceNumberRange']
        assert shard['SequenceNumberRange']['EndingSequenceNumber'].isdecimal()

    # We can get TRIM_HORIZON iterators for all these shards, to read all
    # the old data they still have (this data should be saved for 24 hours
    # after the stream was disabled)
    iterators = []
    for shard in shards_info:
        iterators.append(dynamodbstreams.get_shard_iterator(StreamArn=arn,
            ShardId=shard['ShardId'], ShardIteratorType='TRIM_HORIZON')['ShardIterator'])

    # We can read the one change we did in one of these iterators. The data
    # should be available immediately - no need for retries with timeout.
    nrecords = 0
    for iter in iterators:
        response = dynamodbstreams.get_records(ShardIterator=iter)
        if 'Records' in response:
            nrecords += len(response['Records'])
        # The shard is closed, so NextShardIterator should either be missing
        # now,  indicating that it is a closed shard (DynamoDB does this),
        # or, it may (and currently does in Alternator) return an iterator
        # and reading from *that* iterator should then tell us that
        # we reached the end of the shard (i.e., zero results and
        # missing NextShardIterator).
        if 'NextShardIterator' in response:
            response = dynamodbstreams.get_records(ShardIterator=response['NextShardIterator'])
            assert len(response['Records']) == 0
            assert not 'NextShardIterator' in response
    assert nrecords == 1

# TODO: tests on multiple partitions
# TODO: write a test that disabling the stream and re-enabling it works, but
#   requires the user to wait for the first stream to become DISABLED before
#   creating the new one. Then ListStreams should return the two streams,
#   one DISABLED and one ENABLED? I'm not sure we want or can do this in
#   Alternator.
# TODO: Can we test shard splitting? (shard splitting
#   requires the user to - periodically or following shards ending - to call
#   DescribeStream again. We don't do this in any of our tests.
