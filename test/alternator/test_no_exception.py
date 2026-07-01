# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Test that common error scenarios are propagated internally in Alternator's without
# throwing exceptions. Throwing exceptions is significantly slower than returning a
# value and can cause throughput to collapse:
#  1. A node is slightly overloaded which result in some timeouts
#  2. These timeouts are implemented with exceptions, which is slow, causing the
#      node to become even more overloaded.
#  3. The even more overloaded node results in even more timeouts, and more exceptions.
#
# So in this test we check some scenarios, like timeouts, which in the past caused exception
# throwing in Alternator's implementation, and we want to verify no longer throw exceptions.
#
# All tests here are scylla_only: The question of whether an exception was thrown inside the
# implementation has no parallel in DynamoDB, and also these tests rely on two Alternator-only
# features:
# 1. The Alternator-specific `scylla_reactor_cpp_exceptions` metric to count C++ exceptions thrown inside the implementation.
# 2. The Alternator-specific "injections" that allow us to simulate timeouts.

import pytest
from botocore.exceptions import ClientError

from test.alternator.test_streams import create_stream_test_table, wait_for_active_stream
from test.alternator.test_metrics import check_increases_metric_exact, metrics
from test.alternator.util import multiset, new_test_table, random_string, scylla_inject_error

# Test for checking that returning partial results as UnprocessedKeys
# is properly handled. This test relies on error injection available
# only in Scylla compiled with appropriate flags (present in dev/debug/sanitize
# modes) and is skipped otherwise.
def test_batch_get_item_partial_no_exception_thrown(scylla_only, dynamodb, rest_api, test_table_sn, metrics):
    p = random_string()
    content = random_string()
    # prepare "count" rows in "partitions" partitions
    count = 10
    partitions = 3
    with test_table_sn.batch_writer() as batch:
        for i in range(count):
            batch.put_item(Item={
                'p': p + str(i % partitions), 'c': i, 'content': content})

    # We trigger read_timeout_exception in batch_get_item (when item is being read) here - 
    # not thrown, no stack unwinding, see SCYLLADB-2960 - which uses different code path
    # that test_batch_get_item_partial (which throws an exception).
    # The return value should be properly created and returned to the user (including UnprocessedKeys),
    # no exception should be thrown and no internal server error should be returned to the user.
    responses = []
    to_read = { test_table_sn.name: {'Keys': [{'p': p + str(c % partitions), 'c': c} for c in range(count)], 'ConsistentRead': True } }
    with scylla_inject_error(rest_api, 'alternator_query_result_timeout', one_shot=True, parameters={ 'table_name': test_table_sn.name }):
        with check_increases_metric_exact(metrics, 'scylla_reactor_cpp_exceptions', [[0, None]]):
            some_keys_were_unprocessed = False
            while to_read:
                reply = test_table_sn.meta.client.batch_get_item(RequestItems = to_read)
                assert 'UnprocessedKeys' in reply
                to_read = reply['UnprocessedKeys']
                # The UnprocessedKeys should not only list the keys, it should
                # also copy the additional parameters used in the original read.
                # In this example this was "ConsistentRead".
                for tbl in to_read:
                    assert 'ConsistentRead' in to_read[tbl]
                some_keys_were_unprocessed = some_keys_were_unprocessed or len(to_read) > 0
                print("Left to read:", to_read)
                assert 'Responses' in reply
                assert test_table_sn.name in reply['Responses']
                responses.extend(reply['Responses'][test_table_sn.name])
            assert multiset(responses) == multiset(
                [{'p': p + str(i % partitions), 'c': i, 'content': content} for i in range(count)])
            assert some_keys_were_unprocessed

# Test that if the batch read failure is total, i.e. all read requests
# failed, it's reported as an error and not as a regular response with
# UnprocessedKeys set to all given keys.
def test_batch_get_item_full_failure_no_exception_thrown(scylla_only, dynamodb, rest_api, test_table_sn, metrics):
    p = random_string()
    content = random_string()
    count = 10
    with test_table_sn.batch_writer() as batch:
        for i in range(count):
            batch.put_item(Item={
                'p': p, 'c': i, 'content': content})
    # We trigger read_timeout_exception in batch_get_item (when item is being read) here - 
    # not thrown, no stack unwinding, see SCYLLADB-2960 - which uses different code path
    # that test_batch_get_item_partial (which throws an exception).
    # The return value should be properly created and returned to the user (including UnprocessedKeys)
    # and no internal server error should be returned to the user.
    to_read = { test_table_sn.name: {'Keys': [{'p': p, 'c': c} for c in range(count)], 'ConsistentRead': True } }
    with scylla_inject_error(rest_api, 'alternator_query_result_timeout', one_shot=True, parameters={ 'table_name': test_table_sn.name }):
        with check_increases_metric_exact(metrics, 'scylla_reactor_cpp_exceptions', [[0, None]]):
            with pytest.raises(ClientError, match="InternalServerError.*Alternator's error injection: timeout"):
                test_table_sn.meta.client.batch_get_item(RequestItems = to_read)

# Verify that timeout exception is properly returned to the user when the error is passed by value (instead of being thrown).
# We try `get_item` path here - uses query_result directly.
# Regression test for SCYLLADB-2960
def test_getitem_timeout_is_propagated_to_user_no_exception(dynamodb, test_table, rest_api, metrics):
    p = random_string()
    c = random_string()
    with check_increases_metric_exact(metrics, 'scylla_reactor_cpp_exceptions', [[0, None]]):
        with pytest.raises(ClientError, match="InternalServerError.*Alternator's error injection: timeout"):
            with scylla_inject_error(rest_api, 'alternator_query_result_timeout', one_shot=True, parameters={ 'table_name': test_table.name }):
                test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)

# Verify that timeout exception is properly returned to the user when error is passed by value (instead of being thrown).
# We try `put_item` path here, only in `unsafe_rmw` mode
# Regression test for SCYLLADB-2960
def test_putitem_timeout_is_propagated_to_user_no_exception(dynamodb, rest_api, metrics):
    p = random_string()
    c = random_string()

    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ],
        Tags=[{'Key': 'system:write_isolation', 'Value': 'unsafe_rmw'}]) as table:
        with check_increases_metric_exact(metrics, 'scylla_reactor_cpp_exceptions', [[0, None]]):
            with pytest.raises(ClientError, match="InternalServerError.*Alternator's error injection: timeout"):
                with scylla_inject_error(rest_api, 'alternator_query_result_timeout', one_shot=True, parameters={ 'table_name': table.name }):
                    # We don't care about exact expected value - timeout error happens before the expected value is checked.
                    table.put_item(Item={'p': p, 'c': c}, Expected={'a': {'Value': 1}})

# Test that timeout error injected and passed by value (instead of being thrown) is correctly handled by get_records.
# This is a regression test for SCYLLADB-2960.
def test_get_records_timeout_error_no_exception(dynamodb, dynamodbstreams, scylla_only, rest_api, metrics):
    with create_stream_test_table(dynamodb, StreamViewType='NEW_AND_OLD_IMAGES') as table:
        (arn, label) = wait_for_active_stream(dynamodbstreams, table)
        shard = dynamodbstreams.describe_stream(StreamArn=arn, Limit=1)['StreamDescription']['Shards'][0]
        shard_id = shard['ShardId']
        iter = dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator']

        with pytest.raises(ClientError, match="InternalServerError.*Alternator's error injection: timeout"):
            with check_increases_metric_exact(metrics, 'scylla_reactor_cpp_exceptions', [[0, None]]):
                # We need a `_scylla_cdc_log` appended to a table, because query_result is used for readign from CDC table, which name is
                # table_name + '_scylla_cdc_log'.
                with scylla_inject_error(rest_api, 'alternator_query_result_timeout', one_shot=True, parameters={ 'table_name': table.name + '_scylla_cdc_log' }):
                    dynamodbstreams.get_records(ShardIterator=iter)

# Test that DescribeTable returns an InternalServerError with a timeout message if the is_view_built query times out and
# the error is passed by value (instead of being thrown).
# Regression test for SCYLLADB-2960
def test_describe_table_with_timeout_in_is_view_built_no_exception(dynamodb, rest_api, metrics):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'},
                   {'AttributeName': 'c', 'KeyType': 'RANGE'}],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' },
                               { 'AttributeName': 'c', 'AttributeType': 'S' },
                               { 'AttributeName': 'x', 'AttributeType': 'S' }],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }],
        LocalSecondaryIndexes=[
            {   'IndexName': 'hithere',
                'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                              {'AttributeName': 'x', 'KeyType': 'RANGE'}],
                'Projection': { 'ProjectionType': 'ALL' }
            }]
    ) as table:
        with scylla_inject_error(rest_api, 'alternator_query_result_timeout', one_shot=True, parameters={ 'table_name': 'view_build_status_v2' }):
            with check_increases_metric_exact(metrics, 'scylla_reactor_cpp_exceptions', [[0, None]]):
                with pytest.raises(ClientError, match='InternalServerError.*timeout'):
                    table.meta.client.describe_table(TableName=table.name)['Table']

