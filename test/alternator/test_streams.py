# Copyright 2020 ScyllaDB
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
from util import list_tables, test_table_name, create_test_table, random_string
from contextlib import contextmanager
from urllib.error import URLError

stream_types = [ 'OLD_IMAGE', 'NEW_IMAGE', 'KEYS_ONLY', 'NEW_AND_OLD_IMAGES']

def disable_stream(dynamodbstreams, table):
    while True:
        try:
            table.update(StreamSpecification={'StreamEnabled': False});
            while True:
                streams = dynamodbstreams.list_streams(TableName=table.name)
                if not streams.get('Streams'):
                    return
                # when running against real dynamodb, modifying stream 
                # state has a delay. need to wait for propagation. 
                print("Stream(s) lingering. Sleep 10s...")
                time.sleep(10)
        except ClientError as ce:
            # again, real dynamo has periods when state cannot yet
            # be modified. 
            if ce.response['Error']['Code'] == 'ResourceInUseException':
                print("Stream(s) in use. Sleep 10s...")
                time.sleep(10)
                continue
            raise
            
#
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
    yield table
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

def wait_for_active_stream(dynamodbstreams, table, timeout=60):
    exp = time.process_time() + timeout
    while time.process_time() < exp:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        for stream in streams['Streams']:
            desc = dynamodbstreams.describe_stream(StreamArn=stream['StreamArn'])['StreamDescription']
            if not 'StreamStatus' in desc or desc.get('StreamStatus') == 'ENABLED':
                arn = stream['StreamArn']
                if arn != None:
                    return arn;
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
    for type in stream_types:
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

@pytest.mark.xfail(reason="alternator does not have creation time or label for streams")
def test_describe_stream(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        desc = dynamodbstreams.describe_stream(StreamArn=arn)
        assert desc;
        assert desc.get('StreamDescription')
        # note these are non-required attributes
        assert 'StreamLabel' in desc['StreamDescription']
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
        arn = wait_for_active_stream(dynamodbstreams, table)
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
        arn = wait_for_active_stream(dynamodbstreams, table)

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
                next = response['NextShardIterator']

                if next != '':
                    next_iterators.append(next)

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
