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

# Tests for stream operations: ListStreams, DescribeStream, GetShardIterator.

import pytest
from botocore.exceptions import ClientError
from util import list_tables, test_table_name, create_test_table, random_string

stream_types = [ 'OLD_IMAGE', 'NEW_IMAGE', 'KEYS_ONLY', 'NEW_AND_OLD_IMAGES']

def test_list_streams(dynamodb, dynamostreams):
    for type in stream_types:
        table = create_test_table(dynamodb,
            StreamSpecification={'StreamEnabled': True, 'StreamViewType': type},
            KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]);

        streams = dynamostreams.list_streams(TableName=table.name)

        assert streams
        assert streams.get('LastEvaluatedStreamArn')
        assert streams.get('Streams')
                    
        table.delete();

def test_describe_stream(dynamodb, dynamostreams):
    table = create_test_table(dynamodb,
        StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'},
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]);

    streams = dynamostreams.list_streams(TableName=table.name)
    arn = streams['Streams'][0]['StreamArn'];

    desc = dynamostreams.describe_stream(StreamArn=arn)

    print(desc)

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

def test_get_shard_iterator(dynamodb, dynamostreams):
    table = create_test_table(dynamodb,
        StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'},
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]);

    streams = dynamostreams.list_streams(TableName=table.name)
    arn = streams['Streams'][0]['StreamArn'];
    desc = dynamostreams.describe_stream(StreamArn=arn)

    shard_id = desc['StreamDescription']['Shards'][0]['ShardId'];
    seq = desc['StreamDescription']['Shards'][0]['SequenceNumberRange']['StartingSequenceNumber'];

    for type in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER', 'TRIM_HORIZON', 'LATEST']: 
        iter = dynamostreams.get_shard_iterator(
            StreamArn=arn, ShardId=shard_id, ShardIteratorType=type, SequenceNumber=seq
        )
        assert iter.get('ShardIterator')
