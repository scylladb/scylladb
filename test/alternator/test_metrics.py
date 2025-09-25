# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

##############################################################################
# Tests for Scylla's metrics (see docs/dev/metrics.md) for Alternator
# queries. Reproduces issues #9406 and #17615, where although metrics were
# implemented for Alternator requests, they were missing for some operations
# (BatchGetItem).
# In the tests here we attempt to ensure that the metrics continue to work
# for the relevant operations as the code evolves.
#
# Note that all tests in this file test Scylla-specific features, and are
# "skipped" when not running against Scylla, or when unable to retrieve
# metrics through out-of-band HTTP requests to Scylla's Prometheus port (9180).
#
# IMPORTANT: we do not want these tests to assume that are not running in
# parallel with any other tests or workload - because such an assumption
# would limit our test deployment options in the future. NOT making this
# assumption means that these tests can't check that a certain operation
# increases a certain counter by exactly 1 - because other concurrent
# operations might increase it further! So our test can only check that the
# counter increases.
##############################################################################

import re
import time
from bisect import bisect_left
from contextlib import contextmanager
from functools import lru_cache

import pytest
import requests
from botocore.exceptions import ClientError

from test.alternator.test_manual_requests import get_signed_request
from test.alternator.test_cql_rbac import new_dynamodb, new_role
from test.alternator.util import random_string, new_test_table, is_aws, scylla_config_read, scylla_config_temporary

# Fixture for checking if we are able to test Scylla metrics. Scylla metrics
# are not available on AWS (of course), but may also not be available for
# Scylla if for some reason we have only access to the Alternator protocol
# port but no access to the metrics port (9180).
# If metrics are *not* available, tests using this fixture will be skipped.
# Tests using this fixture may call get_metrics(metrics).
@pytest.fixture(scope="module")
def metrics(dynamodb):
    if is_aws(dynamodb):
        pytest.skip('Scylla-only feature not supported by AWS')
    url = dynamodb.meta.client._endpoint.host
    # The Prometheus API is on port 9180, and always http, not https.
    url = re.sub(r':[0-9]+(/|$)', ':9180', url)
    url = re.sub(r'^https:', 'http:', url)
    url = url + '/metrics'
    resp = requests.get(url)
    if resp.status_code != 200:
        pytest.skip('Metrics port 9180 is not available')
    yield url

# Utility function for fetching all metrics from Scylla, using an HTTP request
# to port 9180. The response format is defined by the Prometheus protocol.
# get_metrics() needs the test to use the "metrics" fixture.
def get_metrics(metrics):
    response = requests.get(metrics)
    assert response.status_code == 200
    return response.text

# Utility function for fetching a metric with a given name and optionally a
# given sub-metric label (which should be a name-value map). If multiple
# matches are found, they are summed - this is useful for summing up the
# counts from multiple shards.
def get_metric(metrics, name, requested_labels=None, the_metrics=None):
    if not the_metrics:
        the_metrics = get_metrics(metrics)
    total = 0.0
    lines = re.compile('^'+name+'{.*$', re.MULTILINE)
    for match in re.findall(lines, the_metrics):
        a = match.split()
        metric = a[0]
        val = float(a[1])
        # Check if match also matches the requested labels
        if requested_labels:
            # we know metric begins with name{ and ends with } - the labels
            # are what we have between those
            got_labels = metric[len(name)+1:-1].split(',')
            # Check that every one of the requested labels is in got_labels:
            for k, v in requested_labels.items():
                if not f'{k}="{v}"' in got_labels:
                    # No match for requested label, skip this metric (python
                    # doesn't have "continue 2" so let's just set val to 0...
                    val = 0
                    break
        total += float(val)
    return total

# context manager for checking that a certain piece of code increases each
# of the specified metrics. Helps reduce the amount of code duplication
# below.
@contextmanager
def check_increases_metric(metrics, metric_names, requested_labels=None):
    the_metrics = get_metrics(metrics)
    saved_metrics = { x: get_metric(metrics, x, requested_labels, the_metrics) for x in metric_names }
    yield
    the_metrics = get_metrics(metrics)
    for n in metric_names:
        assert saved_metrics[n] < get_metric(metrics, n, requested_labels, the_metrics), f"Metric '{n}' with labels {requested_labels} did not increase"

@contextmanager
def check_increases_metric_exact(metrics, metric_name, value_and_labels):
    the_metrics = get_metrics(metrics)
    saved_metric = [get_metric(metrics, metric_name, vl[1], the_metrics) for vl in value_and_labels]
    yield
    the_metrics = get_metrics(metrics)
    for (expected_increase, labels), base_value in zip(value_and_labels, saved_metric):
        actual_increase = get_metric(metrics, metric_name, labels, the_metrics) - base_value
        assert actual_increase == expected_increase, (
            f"Metric '{metric_name}' with labels {labels} did not increase by an exact value. "
            f"Initial value: {base_value}. "
            f"Expected increase: {expected_increase}. "
            f"Actual increase: {actual_increase}."
        )

@contextmanager
def check_increases_operation(metrics, operation_names, metric_name = 'scylla_alternator_operation', expected_value=None):
    the_metrics = get_metrics(metrics)
    saved_metrics = { x: get_metric(metrics, metric_name, {'op': x}, the_metrics) for x in operation_names }
    yield
    the_metrics = get_metrics(metrics)
    for op in operation_names:
        if expected_value:
            assert expected_value == get_metric(metrics, metric_name, {'op': op}, the_metrics) - saved_metrics[op]
        else:
            assert saved_metrics[op] < get_metric(metrics, metric_name, {'op': op}, the_metrics)

@contextmanager
def check_table_increases_operation(metrics, operation_names, table, metric_name = 'scylla_alternator_table_operation', expected_value=None):
    the_metrics = get_metrics(metrics)
    saved_metrics = { x: get_metric(metrics, metric_name, {'op': x, 'cf': table}, the_metrics) for x in operation_names }
    yield
    the_metrics = get_metrics(metrics)
    for op in operation_names:
        if expected_value:
            assert expected_value == get_metric(metrics, metric_name, {'op': op, 'cf': table}, the_metrics) - saved_metrics[op]
        else:
            assert saved_metrics[op] < get_metric(metrics, metric_name, {'op': op, 'cf': table}, the_metrics)

@lru_cache
def histogram_bucket_offsets(size, ratio=1.2):
    """
    Generate bucket offsets for an estimated histogram.
    
    Args:
        size (int): Number of bucket offsets to generate
        ratio (float): The multiplier for the exponential growth (default is 1.2)
        
    Returns:
        list: List of bucket offset values
    """
    if size == 0:
        return []

    bucket_offsets = [1]
    last = 1
    for _ in range(size - 1):
        current = round(last * ratio)
        if current == last:
            current += 1
        bucket_offsets.append(current)
        last = current
    return bucket_offsets

def bucket_idx(size_in_kb, bucket_count=30, ratio=1.2):
    buckets = histogram_bucket_offsets(bucket_count, ratio)
    return bisect_left(buckets, size_in_kb)

def bucket(size_in_kb, bucket_count=30, ratio=1.2):
    buckets = histogram_bucket_offsets(bucket_count, ratio)
    idx = bucket_idx(size_in_kb, bucket_count, ratio)
    return str(buckets[idx]) + '.000000' if size_in_kb <= buckets[-1] else '+Inf'

def next_bucket(size_in_kb, bucket_count=30, ratio=1.2):
    buckets = histogram_bucket_offsets(bucket_count, ratio)
    idx = bucket_idx(size_in_kb, bucket_count, ratio)
    return str(buckets[idx + 1]) + '.000000' if idx + 1 < bucket_count else None

def prev_bucket(size_in_kb, bucket_count=30, ratio=1.2):
    buckets = histogram_bucket_offsets(bucket_count, ratio)
    idx = bucket_idx(size_in_kb, bucket_count, ratio)
    return str(buckets[idx - 1]) + '.000000' if idx > 0 else None

###### Test for metrics that count DynamoDB API operations:

def test_batch_write_item(test_table_s, metrics):
    with check_increases_operation(metrics, ['BatchWriteItem']):
        test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}}]})

def test_table_batch_write_item(test_table_s, metrics):
    with check_table_increases_operation(metrics, ['BatchWriteItem'], test_table_s.name, expected_value=1):
        test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}}]})

def test_batch_write_item_histogram(test_table_s, metrics):
    with check_increases_metric_exact(metrics, "scylla_alternator_batch_item_count_histogram_bucket", [[0, {'op': 'BatchWriteItem', 'le': '2.000000'}], [1, {'op': 'BatchWriteItem', 'le': '3.000000'}]]):
        test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}}, {'PutRequest': {'Item': {'p': random_string(), 'a': 'hi1'}}}, {'PutRequest': {'Item': {'p': random_string(), 'a': 'hi1'}}}]})
    items = [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}} for i in range(100)]
    items2 = [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}} for i in range(100)]
    with check_increases_metric_exact(metrics, "scylla_alternator_batch_item_count_histogram_bucket", [[0, {'op': 'BatchWriteItem', 'le': '86.000000'}], [2, {'op': 'BatchWriteItem', 'le': '103.000000'}]]):
        test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: items})
        test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: items2})

def test_table_batch_write_item_histogram(test_table_s, metrics):
    with check_increases_metric_exact(metrics, "scylla_alternator_table_batch_item_count_histogram_bucket", [[0, {'op': 'BatchWriteItem', 'le': '2.000000', 'cf': test_table_s.name}], [1, {'op': 'BatchWriteItem', 'le': '3.000000', 'cf': test_table_s.name}]]):
        test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}}, {'PutRequest': {'Item': {'p': random_string(), 'a': 'hi1'}}}, {'PutRequest': {'Item': {'p': random_string(), 'a': 'hi1'}}}]})
    items = [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}} for i in range(100)]
    items2 = [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}} for i in range(100)]
    with check_increases_metric_exact(metrics, "scylla_alternator_table_batch_item_count_histogram_bucket", [[0, {'op': 'BatchWriteItem', 'le': '86.000000', 'cf': test_table_s.name}], [2, {'op': 'BatchWriteItem', 'le': '103.000000', 'cf': test_table_s.name}]]):
        test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: items})
        test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: items2})

def test_batch_get_item_histogram(test_table_s, metrics):
    with check_increases_metric_exact(metrics, "scylla_alternator_batch_item_count_histogram_bucket", [[0, {'op': 'BatchWriteItem', 'le': '2.000000'}], [1, {'op': 'BatchGetItem', 'le': '3.000000'}]]):
        test_table_s.meta.client.batch_get_item(RequestItems = {
            test_table_s.name: {'Keys': [{'p': random_string()}, {'p': random_string()}, {'p': random_string()}], 'ConsistentRead': True}})
    keys = [{'p': random_string()} for i in range(100) ]
    keys2 = [{'p': random_string()} for i in range(100) ]
    with check_increases_metric_exact(metrics, "scylla_alternator_batch_item_count_histogram_bucket", [[0, {'op': 'BatchGetItem', 'le': '86.000000'}], [2, {'op': 'BatchGetItem', 'le': '103.000000'}]]):
        test_table_s.meta.client.batch_get_item(RequestItems = {
            test_table_s.name: {'Keys': keys, 'ConsistentRead': True}})
        test_table_s.meta.client.batch_get_item(RequestItems = {
            test_table_s.name: {'Keys': keys2, 'ConsistentRead': True}})

def test_table_batch_get_item_histogram(test_table_s, metrics):
    with check_increases_metric_exact(metrics, "scylla_alternator_table_batch_item_count_histogram_bucket", [[0, {'op': 'BatchWriteItem', 'le': '2.000000', 'cf': test_table_s.name}], [1, {'op': 'BatchGetItem', 'le': '3.000000', 'cf': test_table_s.name}]]):
        test_table_s.meta.client.batch_get_item(RequestItems = {
            test_table_s.name: {'Keys': [{'p': random_string()}, {'p': random_string()}, {'p': random_string()}], 'ConsistentRead': True}})
    keys = [{'p': random_string()} for i in range(100) ]
    keys2 = [{'p': random_string()} for i in range(100) ]
    with check_increases_metric_exact(metrics, "scylla_alternator_table_batch_item_count_histogram_bucket", [[0, {'op': 'BatchGetItem', 'le': '86.000000', 'cf': test_table_s.name}], [2, {'op': 'BatchGetItem', 'le': '103.000000', 'cf': test_table_s.name}]]):
        test_table_s.meta.client.batch_get_item(RequestItems = {
            test_table_s.name: {'Keys': keys, 'ConsistentRead': True}})
        test_table_s.meta.client.batch_get_item(RequestItems = {
            test_table_s.name: {'Keys': keys2, 'ConsistentRead': True}})

# Reproduces issue #9406:
def test_batch_get_item(test_table_s, metrics):
    with check_increases_operation(metrics, ['BatchGetItem']):
        test_table_s.meta.client.batch_get_item(RequestItems = {
            test_table_s.name: {'Keys': [{'p': random_string()}], 'ConsistentRead': True}})

def test_batch_write_item_count(test_table_s, metrics):
    with check_increases_operation(metrics, ['BatchWriteItem'], metric_name='scylla_alternator_batch_item_count', expected_value=2):
        test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}}, {'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}}]})

def test_batch_get_item_count(test_table_s, metrics):
    with check_increases_operation(metrics, ['BatchGetItem'], metric_name='scylla_alternator_batch_item_count', expected_value=2):
        test_table_s.meta.client.batch_get_item(RequestItems = {
            test_table_s.name: {'Keys': [{'p': random_string()}, {'p': random_string()}], 'ConsistentRead': True}})

KB = 1024
def test_rcu(test_table_s, metrics):
    with check_increases_metric_exact(metrics, 'scylla_alternator_rcu_total', [[2, None]]):
        p = random_string()
        val = random_string()
        total_length = len(p) + len(val) + len("pattanother")
        val2 = 'a' * (4 * KB - total_length + 1) # message length 4KB +1

        test_table_s.put_item(Item={'p': p, 'att': val, 'another': val2})
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

def test_wcu(test_table_s, metrics):
    with check_increases_operation(metrics, ['PutItem'], 'scylla_alternator_wcu_total', 3):
        p = random_string()
        val = random_string()
        total_length = len(p) + len(val) + len("pattanother")
        val2 = 'a' * (2 * KB - total_length + 1) # message length 2K + 1
        test_table_s.put_item(Item={'p': p, 'att': val, 'another': val2})

def test_rcu_per_table(test_table_s, metrics):
    with check_increases_metric_exact(metrics, 'scylla_alternator_table_rcu_total', [[2, {'cf': test_table_s.name}]]):
        p = random_string()
        val = random_string()
        total_length = len(p) + len(val) + len("pattanother")
        val2 = 'a' * (4 * KB - total_length + 1) # message length 4KB +1

        test_table_s.put_item(Item={'p': p, 'att': val, 'another': val2})
        test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

def test_wcu_per_table(test_table_s, metrics):
    with check_table_increases_operation(metrics, ['PutItem'], test_table_s.name, 'scylla_alternator_table_wcu_total', 3):
        p = random_string()
        val = random_string()
        total_length = len(p) + len(val) + len("pattanother")
        val2 = 'a' * (2 * KB - total_length + 1) # message length 2K + 1
        test_table_s.put_item(Item={'p': p, 'att': val, 'another': val2})

def test_wcu_batch_write_item(test_table_s, metrics):
    with check_increases_operation(metrics, ['PutItem'], 'scylla_alternator_wcu_total', 3):
        p1 = random_string()
        p2 = random_string()
        response = test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: [{'PutRequest': {'Item': {'p': p1, 'a': 'hi'}}}, {'PutRequest': {'Item': {'p': p2, 'a': 'a' * KB}}}]
        })

# Test counters for CreateTable, DescribeTable, UpdateTable and DeleteTable
def test_table_operations(dynamodb, metrics):
    with check_increases_operation(metrics, ['CreateTable', 'DescribeTable', 'UpdateTable', 'DeleteTable']):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
            AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
            # Run DescribeTable and UpdateTable on the new table to have those
            # counted as well. The DescribeTable call isn't strictly necessary
            # here because new_test_table already does it (to make sure the
            # table exists before returning), but let's not assume it.
            dynamodb.meta.client.describe_table(TableName=table.name)
            dynamodb.meta.client.update_table(TableName=table.name, BillingMode='PAY_PER_REQUEST')

# Test counters for DeleteItem, GetItem, PutItem and UpdateItem:
def test_item_operations(test_table_s, metrics):
    with check_increases_operation(metrics, ['DeleteItem', 'GetItem', 'PutItem', 'UpdateItem']):
        p = random_string()
        test_table_s.put_item(Item={'p': p})
        test_table_s.get_item(Key={'p': p})
        test_table_s.delete_item(Key={'p': p})
        test_table_s.update_item(Key={'p': p})

# Test counters for DeleteItem, GetItem, PutItem and UpdateItem:
def test_table_item_operations(test_table_s, metrics):
    with check_table_increases_operation(metrics, ['DeleteItem', 'GetItem', 'PutItem', 'UpdateItem'], test_table_s.name):
        p = random_string()
        test_table_s.put_item(Item={'p': p})
        test_table_s.get_item(Key={'p': p})
        test_table_s.delete_item(Key={'p': p})
        test_table_s.update_item(Key={'p': p})

# Test counters for Query and Scan:
def test_scan_operations(test_table_s, metrics):
    with check_increases_operation(metrics, ['Query', 'Scan']):
        # We don't care whether test_table_s contains any content when we scan
        # it. We just want the scan to be counted
        test_table_s.scan(Limit=1)
        test_table_s.query(Limit=1, KeyConditionExpression='p=:p',
            ExpressionAttributeValues={':p': 'dog'})

# Test counters for DescribeEndpoints:
def test_describe_endpoints_operations(dynamodb, metrics):
    with check_increases_operation(metrics, ['DescribeEndpoints']):
        dynamodb.meta.client.describe_endpoints()

# Test counters for ListTables:
def test_list_tables_operations(dynamodb, metrics):
    with check_increases_operation(metrics, ['ListTables']):
        dynamodb.meta.client.list_tables(Limit=1)

# Test counters for TagResource, UntagResource, ListTagsOfResource:
def test_tag_operations(test_table_s, metrics):
    with check_increases_operation(metrics, ['TagResource', 'UntagResource', 'ListTagsOfResource']):
        client = test_table_s.meta.client
        arn = client.describe_table(TableName=test_table_s.name)['Table']['TableArn']
        client.list_tags_of_resource(ResourceArn=arn)
        p = random_string()
        client.tag_resource(ResourceArn=arn, Tags=[{'Key': p, 'Value': 'dog'}])
        client.untag_resource(ResourceArn=arn, TagKeys=[p])

# Test counters for DescribeTimeToLive, UpdateTimeToLive:
def test_ttl_operations(test_table_s, metrics):
    with check_increases_operation(metrics, ['DescribeTimeToLive', 'UpdateTimeToLive']):
        client = test_table_s.meta.client
        client.describe_time_to_live(TableName=test_table_s.name)
        # We don't need to enable time-to-live to check that the
        # UpdateTimeToLive operation is counted. It's fine that it fails.
        with pytest.raises(ClientError, match='ValidationException'):
            client.update_time_to_live(TableName=test_table_s.name)

# Test counters for ListStreams, DescribeStream, GetShardIterator, GetRecords:
def test_streams_operations(test_table_s, dynamodbstreams, metrics):
    # Just to verify that these operations are counted, they don't have
    # to pass. So we can use them on a table which does not have
    # streams enabled, and bogus ARNs, ignoring the errors. This makes
    # this test faster than actually testing working streams (which we
    # test thoroughly in test_streams.py).
    with check_increases_operation(metrics, ['ListStreams', 'DescribeStream', 'GetShardIterator', 'GetRecords']):
        dynamodbstreams.list_streams(TableName=test_table_s.name)
        arn=random_string(37)
        with pytest.raises(ClientError, match='ValidationException'):
            dynamodbstreams.describe_stream(StreamArn=arn)
        shard_id=random_string(28)
        with pytest.raises(ClientError, match='ValidationException'):
            dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='LATEST')
        shard_iterator='garbage'
        with pytest.raises(ClientError, match='ValidationException'):
            dynamodbstreams.get_records(ShardIterator=shard_iterator)

# TODO: The following DynamoDB operations are not yet supported by Alternator,
# so we can't increment their metrics so we don't have tests for them:
# CreateBackup, DeleteBackup, DescribeBackup, DescribeContinuousBackups,
# ListBackups, RestoreTableFromBackup, RestoreTableToPointInTime,
# UpdateContinuousBackups, TransactGetItems, TransactWriteItems,
# CreateGlobalTable, DescribeGlobalTable, DescribeGlobalTableSettings,
# ListGlobalTables, UpdateGlobalTable, UpdateGlobalTableSettings,
# DescribeLimits.

###### Test metrics that are latency histograms of DynamoDB API operations:

# Check that an operation sets the desired latency histograms to non-zero.
# We don't know what this latency should be, but we can still check that
# the desired latency metric gets updated.
#
# A latency histogram metric "scylla_alternator_op_latency" is actually three
# separate metrics: scylla_alternator_op_latency_count, ..._sum and ..._bucket.
# As discussed in issue #18847, we cannot assume that the _sum must increase
# on every request (it is possible for it to stay unchanged if the request is
# very short and to sum until now was high). But we can rely on the _count
# to change when the latency is updated, so that's what we'll check here.
# Remember that the goal of this test isn't to verify that the histogram
# metrics code is correct, but to verify that Alternator didn't forget
# to update latencies for one kind of operation (#17616, and compare #9406),
# and to do that checking that ..._count increases for that op is enough.
@contextmanager
def check_sets_latency(metrics, operation_names):
    the_metrics = get_metrics(metrics)
    saved_latency_count = { x: get_metric(metrics, 'scylla_alternator_op_latency_count', {'op': x}, the_metrics) for x in operation_names }
    yield
    the_metrics = get_metrics(metrics)
    for op in operation_names:
        # The total "count" on all shards should strictly increase
        assert saved_latency_count[op] < get_metric(metrics, 'scylla_alternator_op_latency_count', {'op': op}, the_metrics)

# Test latency metrics for PutItem, GetItem, DeleteItem, UpdateItem.
# We can't check what exactly the latency is - just that it gets updated.
def test_item_latency(test_table_s, metrics):
    with check_sets_latency(metrics, ['DeleteItem', 'GetItem', 'PutItem', 'UpdateItem', 'BatchWriteItem', 'BatchGetItem']):
        p = random_string()
        test_table_s.put_item(Item={'p': p})
        test_table_s.get_item(Key={'p': p})
        test_table_s.delete_item(Key={'p': p})
        test_table_s.update_item(Key={'p': p})
        test_table_s.meta.client.batch_write_item(RequestItems = {
            test_table_s.name: [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}}]})
        test_table_s.meta.client.batch_get_item(RequestItems = {
            test_table_s.name: {'Keys': [{'p': random_string()}], 'ConsistentRead': True}})

# Test latency metrics for GetRecords. Other Streams-related operations -
# ListStreams, DescribeStream, and GetShardIterator, have an operation
# count (tested above) but do NOT currently have a latency histogram.
def test_streams_latency(dynamodb, dynamodbstreams, metrics):
    # Whereas the *count* metric for various operations also counts failed
    # operations so we could write this test without a real stream, the
    # latency metrics are only updated for *successful* operations so we
    # need to use a real Alternator Stream in this test.
    with new_test_table(dynamodb,
        # Alternator Streams is expected to fail with tablets due to #23838.
        # To ensure that this test still runs, instead of xfailing it, we
        # temporarily coerce Alternator to avoid using default tablets
        # setting, even if it's available. We do this by using the following
        # tags when creating the table:
        Tags=[{'Key': 'system:initial_tablets', 'Value': 'none'}],
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        StreamSpecification={ 'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'}
        ) as table:
        # Wait for the stream to become active. This should be instantenous
        # in Alternator, so the following loop won't wait
        stream_enabled = False
        start_time = time.time()
        while time.time() < start_time + 60:
            desc = table.meta.client.describe_table(TableName=table.name)['Table']
            if 'LatestStreamArn' in desc:
                arn = desc['LatestStreamArn']
                desc = dynamodbstreams.describe_stream(StreamArn=arn)
                if desc['StreamDescription']['StreamStatus'] == 'ENABLED':
                    stream_enabled = True
                    break
            time.sleep(1)
        assert stream_enabled
        desc = dynamodbstreams.describe_stream(StreamArn=arn)
        shard_id = desc['StreamDescription']['Shards'][0]['ShardId'];
        it = dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator']
        with check_sets_latency(metrics, ['GetRecords']):
            dynamodbstreams.get_records(ShardIterator=it)

###### Test metrics that are item size histograms of DynamoDB API operations which manipulate data:
# Tests for histogram metrics operation_size_kb.

INF = float('inf')

def check_histogram_metric_increases(op, name, metrics, do_test, probes):
    points = [[value, {'op': op, 'le': le}] for value, le in probes]
    metric_bucket = f'scylla_alternator_table_{name}_bucket'
    with check_increases_metric_exact(metrics, metric_bucket, points):
        do_test()

@pytest.mark.parametrize("force_rbw", [True, False])
def test_get_item_size_not_found_doesnt_increase_the_metric(dynamodb, test_table_s, metrics, force_rbw):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', str(force_rbw).lower()):
        def do_test():
            test_table_s.get_item(Key={'p': random_string()})
        check_histogram_metric_increases('GetItem', 'operation_size_kb', metrics, do_test, [(0, bucket(INF))])

@pytest.mark.parametrize("force_rbw", [True, False])
def test_get_item_size_item_falls_into_appropriate_bucket(dynamodb, test_table_s, metrics, force_rbw):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', str(force_rbw).lower()):
        pk = random_string()
        test_table_s.put_item(Item={'p': pk, 'a': 'a' * 7 * KB, 'b': 'b' * 10 * KB})
        def do_test():
            test_table_s.get_item(Key={'p': pk})
        check_histogram_metric_increases('GetItem', 'operation_size_kb', metrics, do_test, [(0, bucket(17)), (1, bucket(17.1)), (1, bucket(INF))])

def test_put_item_many_items_fall_into_appropriate_buckets(test_table_s, metrics):
    def do_test():
        pk = random_string()
        test_table_s.put_item(Item={'p': pk, 'a': 'a' * 1 * KB, 'b': 'b' * 5 * KB})
        test_table_s.put_item(Item={'p': pk, 'a': 'a' * 6 * KB})
        test_table_s.put_item(Item={'p': pk, 'a': 'a' * 401 * KB})
    check_histogram_metric_increases('PutItem', 'operation_size_kb', metrics, do_test, [(0, prev_bucket(6.1)), (2, bucket(6.1)), (2, prev_bucket(401.1)), (3, bucket(401.1)), (3, bucket(INF))])

# Verify that only the new item size is counted in the histogram. The WCU is
# calculated as the maximum of the old and new item sizes, but the histogram
# should log only the new item size.
def test_put_item_increases_metrics_for_new_item_size_only(test_table_s, metrics):
    pk = random_string()
    points = [[value, {'op': 'PutItem', 'le': le}] for value, le in [(0, prev_bucket(6.1)), (1, bucket(6.1)), (1, bucket(INF))]]

    test_table_s.put_item(Item={'p': pk, 'b': 'b' * 3 * KB})
    with check_increases_metric_exact(metrics, 'scylla_alternator_table_operation_size_kb_bucket', points):
        test_table_s.put_item(Item={'p': pk, 'a': 'a' * 6 * KB})

@pytest.mark.parametrize("force_rbw", [True, False])
def test_delete_item_is_zero_for_nonexistent_item(dynamodb, test_table_s, metrics, force_rbw):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', str(force_rbw).lower()):
        def do_test():
            test_table_s.delete_item(Key={'p': random_string()})
        check_histogram_metric_increases('DeleteItem', 'operation_size_kb', metrics, do_test, [(0, bucket(INF))])

@pytest.mark.parametrize("force_rbw", [True, False])
def test_delete_item_many_items_fall_into_appropriate_buckets(dynamodb, test_table_s, metrics, force_rbw):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', str(force_rbw).lower()):
        if force_rbw:
            points = [[value, {'op': 'DeleteItem', 'le': le}] for value, le in [(0, prev_bucket(24.1)), (1, bucket(24.1)), (1, prev_bucket(378.1)), (2, bucket(378.1)), (3, bucket(INF))]]
        else:
            points = [[0, {'op': 'DeleteItem', 'le': bucket(INF)}]]

        # ~378KB, ~24KB, ~401KB
        pks = [random_string() for _ in range(3)]
        test_table_s.put_item(Item={'p': pks[0], 'a': 'a' * 128 * KB, 'b': 'b' * 250 * KB})
        test_table_s.put_item(Item={'p': pks[1], 'a': 'a' * 24 * KB})
        test_table_s.put_item(Item={'p': pks[2], 'a': 'a' * 447 * KB})
        with check_increases_metric_exact(metrics, 'scylla_alternator_table_operation_size_kb_bucket', points):
            for pk in pks:
                test_table_s.delete_item(Key={'p': pk})

# The item does not exist, so only the new item size is counted in the histogram.
@pytest.mark.parametrize("force_rbw", [True, False])
def test_update_item_single_pk_item(dynamodb, test_table_s, metrics, force_rbw):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', str(force_rbw).lower()):
        def do_test():
            test_table_s.update_item(Key={'p': random_string()})
        check_histogram_metric_increases('UpdateItem', 'operation_size_kb', metrics, do_test, [(1, bucket(0.1)), (1, bucket(INF))])

@pytest.mark.xfail(reason="Updates doesn't add up the existing parameters and the new parameters. This issue will be fixed in a next PR.")
def test_update_item_many_items_fall_into_appropriate_buckets(dynamodb, test_table_s, metrics):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'true'):
        pk = random_string()
        test_table_s.put_item(Item={'p': pk, 'a': 'a'})

        def do_test():
            # Update 1: item becomes ~216KB
            test_table_s.update_item(Key={'p': pk}, UpdateExpression="SET b = :b, c = :c", ExpressionAttributeValues={':b': 'b' * 47 * KB, ':c': 'c' * 169 * KB})
            # Update 2: item becomes ~250KB, 34KB + 216KB = 250KB logged
            test_table_s.update_item(Key={'p': pk}, UpdateExpression="SET a = :a", ExpressionAttributeValues={':a': 'a' * 34 * KB})
            # Update 3: item becomes ~550KB, 250KB + 300KB = 550KB logged
            test_table_s.update_item(Key={'p': pk}, UpdateExpression="SET a = :a", ExpressionAttributeValues={':a': 'a' * 300 * KB})
        check_histogram_metric_increases('UpdateItem', 'operation_size_kb', metrics, do_test, [(0, prev_bucket(216.1)), (2, bucket(250.1)), (2, prev_bucket(550.1)), (3, bucket(INF))])

# Verify that only the new item size is counted in the histogram if RBW is
# disabled, and both sizes if it is enabled. The WCU is calculated as the
# maximum of the old and new item sizes.
@pytest.mark.xfail(reason="Updates don't consider the larger of the old item size and the new item size. This will be fixed in a next PR.")
@pytest.mark.parametrize("force_rbw", [True, False])
def test_update_item_increases_metrics_for_new_item_size_only(dynamodb, test_table_s, metrics, force_rbw):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', str(force_rbw).lower()):
        if force_rbw:
            points = [(0, prev_bucket(32.1)), (1, bucket(32.1)), (1, bucket(INF))]
        else:
            points = [(0, prev_bucket(22.1)), (1, bucket(22.1)), (1, bucket(INF))]

        pk = random_string()
        test_table_s.put_item(Item={'p': pk, 'a': 'a' * 10 * KB })
        def do_test():
            test_table_s.update_item(Key={'p': pk}, UpdateExpression="SET a = :a", ExpressionAttributeValues={':a': 'a' * 22 * KB})
        check_histogram_metric_increases('UpdateItem', 'operation_size_kb', metrics, do_test, points)

def test_batch_get_item_size_no_items_increases_zero_interval(test_table_s, metrics):
    def do_test():
        # An item whose size rounds down to 0 KB shouldn't increase any metric.
        test_table_s.meta.client.batch_get_item(RequestItems={
            test_table_s.name: {
                'Keys': [{'p': random_string()}],
                'ConsistentRead': True,
            }
        })
    check_histogram_metric_increases('BatchGetItem', 'operation_size_kb', metrics, do_test, [(0, bucket(INF))])

@pytest.mark.parametrize("force_rbw", [True, False])
def test_batch_get_item_size_256kb_item_falls_into_appropriate_bucket(dynamodb, test_table_s, metrics, force_rbw):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', str(force_rbw).lower()):
        def do_test():
            pk = random_string()
            test_table_s.put_item(Item={'p': pk, 'a': 'a' * 256 * KB})
            test_table_s.meta.client.batch_get_item(RequestItems={
                test_table_s.name: {
                    'Keys': [{'p': pk}],
                    'ConsistentRead': True,
                }
            })
        check_histogram_metric_increases('BatchGetItem', 'operation_size_kb', metrics, do_test, [(0, prev_bucket(256.1)), (1, bucket(256.1)), (1, bucket(INF))])

@pytest.mark.parametrize("force_rbw", [True, False])
def test_batch_get_item_size_many_items_fall_into_appropriate_buckets(dynamodb, test_table_s, metrics, force_rbw):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', str(force_rbw).lower()):
        def do_test():
            pk = random_string()
            pk2 = random_string()
            pk3 = random_string()
            test_table_s.put_item(Item={'p': pk, 'a': 'a' * 2 * KB, 'b': 'b' * 8 * KB})
            test_table_s.put_item(Item={'p': pk3, 'a': 'a' * 7 * KB})
            test_table_s.meta.client.batch_get_item(RequestItems={
                test_table_s.name: {
                    'Keys': [{'p': pk}, {'p': pk2}, {'p': pk3}],
                    'ConsistentRead': True,
                }
            })
        check_histogram_metric_increases('BatchGetItem', 'operation_size_kb', metrics, do_test, [(0, prev_bucket(7.1)), (1, bucket(7.1)),
                                                                                                 (1, prev_bucket(10.1)), (2, bucket(10.1)), (2, bucket(INF))])

def test_batch_write_item_many_putitems_falls_into_appropriate_bucket(dynamodb, test_table_s, metrics):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'true'):
        def do_test():
            items = [
                {'PutRequest': {'Item': {'p': random_string(), 'a': 'a' * 47 * KB, 'b': 'b' * 169 * KB}}},
                {'PutRequest': {'Item': {'p': random_string(), 'a': 'a' * 80 * KB}}},
            ]
            test_table_s.meta.client.batch_write_item(RequestItems={test_table_s.name: items})
        check_histogram_metric_increases('BatchWriteItem', 'operation_size_kb', metrics, do_test, [(0, prev_bucket(80.1)), (1, bucket(80.1)),
                                                                                                   (1, prev_bucket(216.1)), (2, bucket(216.1)), (2, bucket(INF))])

def test_batch_write_item_increases_metrics_for_bigger_item_only(dynamodb, test_table_s, metrics):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'true'):
        pk = random_string()
        probes = [[value, {'op': 'BatchWriteItem', 'le': le}] for value, le in [(0, prev_bucket(250.1)), (1, bucket(250.1)), (1, bucket(INF))]]
        test_table_s.put_item(Item={'p': pk, 'a': 'a', 'b': 'b' * 250 * KB})
        with check_increases_metric_exact(metrics, 'scylla_alternator_table_operation_size_kb_bucket', probes):
            test_table_s.meta.client.batch_write_item(RequestItems={
                test_table_s.name: [{'PutRequest': {'Item': {'p': pk, 'a': 'a' * 128 * KB}}}]
            })

### Test isolation of operation_size_kb metrics between multiple tables:
# The tests check if operations on two different tables don't affect each 
# others' metrics.

def check_table_histogram_metric_increases(op, metrics, do_test, table_name_and_probes):
    points = [[value, {'op': op, 'le': le, 'cf': table_name}] for table_name in table_name_and_probes for value, le in table_name_and_probes[table_name]]
    with check_increases_metric_exact(metrics, 'scylla_alternator_table_operation_size_kb_bucket', points):
        do_test()

def test_get_item_size_separate_tables_track_metrics_independently(test_table_s, test_table_s_2, metrics):
    # Prepare items in both tables
    pk_1 = random_string()
    pk_2 = random_string()
    test_table_s.put_item(Item={'p': pk_1, 'a': 'a' * 12 * KB})  # 12KB item
    test_table_s_2.put_item(Item={'p': pk_2, 'a': 'a' * 35 * KB})  # 35KB item
    
    def do_test():
        # Table1: Get 12KB+ item. Should fall in (12.000000, 14.000000] bucket
        test_table_s.get_item(Key={'p': pk_1})
        # Table2: Get 35KB+ item. Should fall in (35.000000, 42.000000] bucket
        test_table_s_2.get_item(Key={'p': pk_2})

    check_table_histogram_metric_increases('GetItem', metrics, do_test, {
        test_table_s.name: [(0, prev_bucket(12.1)), (1, bucket(12.1)), (1, bucket(INF))],
        test_table_s_2.name: [(0, prev_bucket(35.1)), (1, bucket(35.1)), (1, bucket(INF))],
    })

def test_put_item_size_separate_tables_track_metrics_independently(test_table_s, test_table_s_2, metrics):
    def do_test():
        # Table1: Put 8KB item. Should fall in (8.000000, 10.000000] bucket
        pk_1 = random_string()
        test_table_s.put_item(Item={'p': pk_1, 'a': 'a' * 8 * KB})
        # Table2: Put 50KB item. Should fall in (50.000000, 60.000000] bucket
        pk_2 = random_string()
        test_table_s_2.put_item(Item={'p': pk_2, 'a': 'a' * 50 * KB})

    check_table_histogram_metric_increases('PutItem', metrics, do_test, {
        test_table_s.name: [(0, prev_bucket(8.1)), (1, bucket(8.1)), (1, bucket(INF))],
        test_table_s_2.name: [(0, prev_bucket(50.1)), (1, bucket(50.1)), (1, bucket(INF))],
    })

def test_delete_item_size_separate_tables_track_metrics_independently(dynamodb, test_table_s, test_table_s_2, metrics):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'true'):
        # Prepare items in both tables
        pk_1 = random_string()
        pk_2 = random_string()
        test_table_s.put_item(Item={'p': pk_1, 'a': 'a' * 60 * KB})  # 60KB item
        test_table_s_2.put_item(Item={'p': pk_2, 'a': 'a' * 86 * KB})  # 86KB item
        
        def do_test():
            # Table1: Delete 60KB+ item. Should fall in (60.000000, 72.000000] bucket
            test_table_s.delete_item(Key={'p': pk_1})
            # Table2: Delete 86KB item. Should fall in (86.000000, 103.000000] bucket
            test_table_s_2.delete_item(Key={'p': pk_2})

        check_table_histogram_metric_increases('DeleteItem', metrics, do_test, {
            test_table_s.name: [(0, prev_bucket(60.1)), (1, bucket(60.1)), (1, bucket(INF))],
            test_table_s_2.name: [(0, prev_bucket(86.1)), (1, bucket(86.1)), (1, bucket(INF))],
        })

def test_update_item_size_separate_tables_track_metrics_independently(dynamodb, test_table_s, test_table_s_2, metrics):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'true'):
        # Prepare items in both tables
        pk_1 = random_string()
        pk_2 = random_string()
        test_table_s.put_item(Item={'p': pk_1})
        test_table_s_2.put_item(Item={'p': pk_2})
        
        def do_test():
            # Table1: Update to create 24KB+ item. Should fall in (24.000000, 29.000000] bucket
            test_table_s.update_item(Key={'p': pk_1}, UpdateExpression="SET a = :a", 
                                     ExpressionAttributeValues={':a': 'a' * 24 * KB})
            # Table2: Update to create 72KB+ item. Should fall in (72.000000, 86.000000] bucket
            test_table_s_2.update_item(Key={'p': pk_2}, UpdateExpression="SET b = :b", 
                                       ExpressionAttributeValues={':b': 'b' * 72 * KB})

        check_table_histogram_metric_increases('UpdateItem', metrics, do_test, {
            test_table_s.name: [(0, prev_bucket(24.1)), (1, bucket(24.1)), (1, bucket(INF))],
            test_table_s_2.name: [(0, prev_bucket(72.1)), (1, bucket(72.1)), (1, bucket(INF))],
        })

def test_batch_get_item_size_separate_tables_track_metrics_independently(test_table_s, test_table_s_2, metrics):
    # Prepare items in both tables
    pk_1 = random_string()
    pk_2 = random_string()
    test_table_s.put_item(Item={'p': pk_1, 'a': 'a' * 72 * KB})  # 72KB item
    test_table_s_2.put_item(Item={'p': pk_2, 'a': 'a' * 86 * KB})  # 86KB item
    
    def do_test():
        # Table1: BatchGet 72KB+ item. Should fall in (72.000000, 86.000000] buckets
        test_table_s.meta.client.batch_get_item(RequestItems={
            test_table_s.name: {
                'Keys': [{'p': pk_1}],
                'ConsistentRead': True,
            }
        })
        # Table2: BatchGet 86KB item. Should fall in (86.000000, 103.000000] bucket
        test_table_s_2.meta.client.batch_get_item(RequestItems={
            test_table_s_2.name: {
                'Keys': [{'p': pk_2}],
                'ConsistentRead': True,
            }
        })

    check_table_histogram_metric_increases('BatchGetItem', metrics, do_test, {
        test_table_s.name: [(0, prev_bucket(72.1)), (1, bucket(72.1)), (1, bucket(INF))],
        test_table_s_2.name: [(0, prev_bucket(86.1)), (1, bucket(86.1)), (1, bucket(INF))],
    })

def test_batch_write_item_size_separate_tables_track_metrics_independently(dynamodb, test_table_s, test_table_s_2, metrics):
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'true'):
        def do_test():
            # Table1: BatchWrite 60KB item. Should fall in (60.000000, 72.000000] bucket
            items_1 = [{'PutRequest': {'Item': {'p': random_string(), 'a': 'a' * 60 * KB}}}]
            test_table_s.meta.client.batch_write_item(RequestItems={test_table_s.name: items_1})
            # Table2: BatchWrite 86KB item. Should fall in (86.000000, 103.000000] bucket
            items_2 = [{'PutRequest': {'Item': {'p': random_string(), 'a': 'a' * 86 * KB}}}]
            test_table_s_2.meta.client.batch_write_item(RequestItems={test_table_s_2.name: items_2})

        check_table_histogram_metric_increases('BatchWriteItem', metrics, do_test, {
            test_table_s.name: [(0, prev_bucket(60.1)), (1, bucket(60.1)), (1, bucket(INF))],
            test_table_s_2.name: [(0, prev_bucket(86.1)), (1, bucket(86.1)), (1, bucket(INF))],
        })

###### Test for other metrics, not counting specific DynamoDB API operations:

# Test that unsupported operations operations increment a counter. Instead
# using an actual unsupported operation - which can change in the future as
# Alternator supports more operations - let's use a bogus operation name
# which will never be implemented.
# An unsupported operation also increments the total_operations counter.
def test_unsupported_operation(dynamodb, metrics):
    with check_increases_metric(metrics, ['scylla_alternator_unsupported_operations', 'scylla_alternator_total_operations']):
        req = get_signed_request(dynamodb, 'BoguousOperationName', '{}')
        requests.post(req.url, headers=req.headers, data=req.body, verify=False)

# Test that also supported operations (such as DescribeEndPoints in this
# example) increment the total_operations metric:
def test_total_operations(dynamodb, metrics):
    with check_increases_metric(metrics, ['scylla_alternator_total_operations']):
        dynamodb.meta.client.describe_endpoints()

# A fixture to read alternator-ttl-period-in-seconds from Scylla's
# configuration. If we're testing something which isn't Scylla, or
# this configuration does not exist, skip this test. If the configuration
# isn't low enough (it is more than one second), skip this test unless
# the "--runveryslow" option is used.
@pytest.fixture(scope="module")
def alternator_ttl_period_in_seconds(dynamodb, request):
    # If not running on Scylla, skip the test
    if is_aws(dynamodb):
        pytest.skip('Scylla-only test skipped')
    # In Scylla, we can inspect the configuration via a system table
    # (which is also visible in Alternator)
    period = scylla_config_read(dynamodb, 'alternator_ttl_period_in_seconds')
    if period is None:
        pytest.skip('missing TTL feature, skipping test')
    period = float(period)
    if period > 1 and not request.config.getoption('runveryslow'):
        pytest.skip('need --runveryslow option to run')
    return period

# Test metrics of the background expiration thread run for Alternator's TTL
# feature. The metrics tested in this test are scylla_expiration_scan_passes,
# scylla_expiration_scan_table and scylla_expiration_items_deleted. The
# metric scylla_expiration_secondary_ranges_scanned is not tested in this
# test - testing it requires a multi-node cluster because it counts the
# number of times that this node took over another node's expiration duty.
#
# Unfortunately, to see TTL expiration in action this test may need to wait
# up to the setting of alternator_ttl_period_in_seconds. test/alternator/run
# sets this to 1 second, which becomes the maximum delay of this test, but
# if it is set higher we skip this test unless --runveryslow is enabled.
# This test fails with tablets due to #16567, so to temporarily ensure that
# Alternator TTL is still being tested, we use the following TAGS to
# coerce Alternator to create the test table without tablets.
TAGS = [{'Key': 'system:initial_tablets', 'Value': 'none'}]
def test_ttl_stats(dynamodb, metrics, alternator_ttl_period_in_seconds):
    print(alternator_ttl_period_in_seconds)
    with check_increases_metric(metrics, ['scylla_expiration_scan_passes', 'scylla_expiration_scan_table', 'scylla_expiration_items_deleted']):
        with new_test_table(dynamodb,
            Tags = TAGS,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
            AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
            # Insert one already-expired item, and then enable TTL:
            p0 = random_string()
            table.put_item(Item={'p': p0, 'expiration': int(time.time())-60})
            assert 'Item' in table.get_item(Key={'p': p0})
            client = table.meta.client
            client.update_time_to_live(TableName=table.name,
                TimeToLiveSpecification= {'AttributeName': 'expiration', 'Enabled': True})
            # After alternator_ttl_period_in_seconds, we expect the metrics
            # to have increased. Add some time to that, to account for
            # extremely overloaded test machines.
            start_time = time.time()
            while time.time() < start_time + alternator_ttl_period_in_seconds + 120:
                if not 'Item' in table.get_item(Key={'p': p0}):
                    break
                time.sleep(0.1)
            assert not 'Item' in table.get_item(Key={'p': p0})

# The following tests check the authentication and authorization failure
# counters:
#  * scylla_alternator_authentication_failures
#  * scylla_alternator_authorization_failures
# as well as their interaction with the alternator_enforce_authorization
# and alternator_warn_authorization configuration options:
#
# 1. When alternator_enforce_authorization and alternator_warn_authorization
#    are both set to "false", these two metrics aren't incremented (and
#    operations are allowed).
# 2. When alternator_enforce_authorization is set to false but
#    alternator_warn_authorization is set to true, the two metrics are
#    incremented but the operations are still allowed.
# 3. When alternator_enforce_authorization is set to "true", the two metrics
#    are incremented and the operations are not allowed.
#
# We have several tests here, for several kinds of authentication and
# authorization errors. These are tests for issue #25308.

@contextmanager
def scylla_config_auth_temporary(dynamodb, enforce_auth, warn_auth):
    with scylla_config_temporary(dynamodb, 'alternator_enforce_authorization', 'true' if enforce_auth else 'false'):
        with scylla_config_temporary(dynamodb, 'alternator_warn_authorization', 'true' if warn_auth else 'false'):
            yield

# authentication failure 1: bogus username and secret key
@pytest.mark.parametrize("enforce_auth", [True, False])
@pytest.mark.parametrize("warn_auth", [True, False])
def test_authentication_failure_1(dynamodb, metrics, test_table_s, enforce_auth, warn_auth):
    with scylla_config_auth_temporary(dynamodb, enforce_auth, warn_auth):
        with new_dynamodb(dynamodb, 'bogus_username', 'bogus_secret_key') as d:
            # We don't expect get_item() to find any item, we just care if
            # to see if it experiences an authentication failure, and if
            # it increments the authentication failure metric.
            saved_auth_failures = get_metric(metrics, 'scylla_alternator_authentication_failures')
            tab = d.Table(test_table_s.name)
            try:
                tab.get_item(Key={'p': 'dog'})
                operation_succeeded = True
            except ClientError as e:
                assert 'UnrecognizedClientException' in str(e)
                operation_succeeded = False
            if enforce_auth:
                assert not operation_succeeded
            else:
                assert operation_succeeded
            new_auth_failures = get_metric(metrics, 'scylla_alternator_authentication_failures')
            if warn_auth or enforce_auth:
                # If Alternator has any reason to check the authentication
                # headers (i.e., either warn_auth or enforce_auth is enabled)
                # then it will count the errors.
                assert new_auth_failures == saved_auth_failures + 1
            else:
                # If Alternator has no reason to check the authentication
                # headers (i.e., both warn_auth and enforce_auth are off)
                # it won't check - and won't find or count auth errors.
                assert new_auth_failures == saved_auth_failures

# authentication failure 2: real username, wrong secret key
# Unfortunately, tests that create a new role need to use CQL too.
@pytest.mark.parametrize("enforce_auth", [True, False])
@pytest.mark.parametrize("warn_auth", [True, False])
def test_authentication_failure_2(dynamodb, cql, metrics, test_table_s, enforce_auth, warn_auth):
    with scylla_config_auth_temporary(dynamodb, enforce_auth, warn_auth):
        with new_role(cql) as (role, key):
            with new_dynamodb(dynamodb, role, 'bogus_secret_key') as d:
                saved_auth_failures = get_metric(metrics, 'scylla_alternator_authentication_failures')
                tab = d.Table(test_table_s.name)
                try:
                    tab.get_item(Key={'p': 'dog'})
                    operation_succeeded = True
                except ClientError as e:
                    assert 'UnrecognizedClientException' in str(e)
                    operation_succeeded = False
                if enforce_auth:
                    assert not operation_succeeded
                else:
                    assert operation_succeeded
                new_auth_failures = get_metric(metrics, 'scylla_alternator_authentication_failures')
                if enforce_auth or warn_auth:
                    assert new_auth_failures == saved_auth_failures + 1
                else:
                    assert new_auth_failures == saved_auth_failures

# Authorization failure - a valid user but without permissions to do a
# given operation.
@pytest.mark.parametrize("enforce_auth", [True, False])
@pytest.mark.parametrize("warn_auth", [True, False])
def test_authorization_failure(dynamodb, cql, metrics, test_table_s, enforce_auth, warn_auth):
    with scylla_config_auth_temporary(dynamodb, enforce_auth, warn_auth):
        with new_role(cql) as (role, key):
            with new_dynamodb(dynamodb, role, key) as d:
                saved_auth_failures = get_metric(metrics, 'scylla_alternator_authorization_failures')
                tab = d.Table(test_table_s.name)
                try:
                    # Note that the new role is not a superuser, so should
                    # not have permissions to read from this table created
                    # earlier by the superuser.
                    tab.get_item(Key={'p': 'dog'})
                    operation_succeeded = True
                except ClientError as e:
                    assert 'AccessDeniedException' in str(e)
                    operation_succeeded = False
                if enforce_auth:
                    assert not operation_succeeded
                else:
                    assert operation_succeeded
                new_auth_failures = get_metric(metrics, 'scylla_alternator_authorization_failures')
                if enforce_auth or warn_auth:
                    assert new_auth_failures == saved_auth_failures + 1
                else:
                    assert new_auth_failures == saved_auth_failures

# TODO: there are additional metrics which we don't yet test here. At the
# time of this writing they are:
# reads_before_write, write_using_lwt, shard_bounce_for_lwt,
# requests_blocked_memory, requests_shed
