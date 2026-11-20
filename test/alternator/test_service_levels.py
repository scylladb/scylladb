# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import pytest
import requests
import threading
from botocore.exceptions import ClientError
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
import time

from test.cqlpy.util import config_value_context
from .util import random_string, unique_table_name, metrics, get_metrics, check_increases_metric, new_role, new_dynamodb, \
    new_dynamodb_streams, new_test_table, scylla_inject_error
from .test_streams import create_stream_test_table, wait_for_active_stream

# new_service_level() is a context manager for temporarily creating a new
# service level with a unique name and attaching it to the given role.
# The fixture returns the new service level's name. The optional timeout
# uses the CQL duration syntax, for example '100ms'.
@contextmanager
def new_service_level(cql, role, timeout=None):
    # The service level name is not a table's name but it doesn't matter.
    # Because our unique_table_name() uses (deliberately) a non-lower-case
    # character, the role name has to be quoted in double quotes when used
    # in CQL below.
    sl = unique_table_name()
    options = f' WITH timeout = {timeout}' if timeout else ''
    cql.execute(f'CREATE SERVICE LEVEL "{sl}"{options}')
    cql.execute(f'ATTACH SERVICE LEVEL "{sl}" TO "{role}"')
    try:
        yield sl
    finally:
        cql.execute(f'DROP SERVICE LEVEL "{sl}"')

# The service level controller applies a new service level asynchronously.
# wait_for_service_level() repeats a read of the given item in the session
# "ses" until the read runs in the scheduling group of the service level
# "sl". The read itself can fail, for example on the service level timeout.
def wait_for_service_level(ses, table, key, metrics, sl, timeout=30):
    deadline = time.time() + timeout
    while True:
        try:
            with check_increases_metric(metrics, ['scylla_storage_proxy_coordinator_read_latency_count'],
                                        {'scheduling_group_name': f'sl:{sl}'}):
                try:
                    ses.meta.client.get_item(TableName=table.name, Key=key, ConsistentRead=True)
                except ClientError:
                    pass
            return
        except AssertionError:
            assert time.time() < deadline, f'Service level {sl} did not take effect'
            time.sleep(0.1)

def test_service_level_metrics(test_table, request, dynamodb, cql, metrics):
    print("Please make sure authorization is enforced in your Scylla installation: alternator_enforce_authorization: true")
    p = random_string()
    c = random_string()
    _ = get_metrics(metrics)
    with new_role(cql, superuser=True) as (role, key):
        with new_service_level(cql, role) as sl:
            with new_dynamodb(dynamodb, role, key) as ses:
            # service_level_controler acts asynchronously in a loop so we can fail metric check
            # if it hasn't processed service level update yet. It can take as long as 10 seconds.
                started = time.time()
                timeout = 30
                while True:
                    try:
                        with check_increases_metric(metrics,
                                            ['scylla_storage_proxy_coordinator_write_latency_count'],
                                            {'scheduling_group_name': f'sl:{sl}'}):
                            ses.meta.client.put_item(TableName=test_table.name, Item={'p': p, 'c': c})
                        break # no exception, test passed
                    except:
                        if time.time() - started > timeout:
                            raise
                        else:
                            time.sleep(0.5) # retry

# The tests below check that the timeout of a role's service level bounds the
# reads and writes of items by that role, but not the schema and other
# control-plane operations (SCYLLADB-6038). The role's service level has a
# zero timeout. A write then fails at once. A read of data in memory can end
# before its timeout fires, so the read tests hold the read in the replica.
# The tables have a write isolation tag each, so that the writes use each code
# path: a plain write, LWT, and a read before a write without LWT.
@pytest.fixture(scope='module')
def zero_timeout(dynamodb, dynamodbstreams, cql, metrics):
    schema = {
        'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'}, {'AttributeName': 'c', 'KeyType': 'RANGE'}],
        'AttributeDefinitions': [{'AttributeName': 'p', 'AttributeType': 'S'}, {'AttributeName': 'c', 'AttributeType': 'S'},
                                 {'AttributeName': 'x', 'AttributeType': 'S'}],
        'GlobalSecondaryIndexes': [{'IndexName': 'gsi', 'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                                    'Projection': {'ProjectionType': 'ALL'}}],
    }
    def isolation(mode):
        return [{'Key': 'system:write_isolation', 'Value': mode}]
    with new_test_table(dynamodb, Tags=isolation('only_rmw_uses_lwt'), **schema) as table, \
         new_test_table(dynamodb, Tags=isolation('always_use_lwt'), **schema) as table_lwt, \
         new_test_table(dynamodb, Tags=isolation('unsafe_rmw'), **schema) as table_unsafe_rmw, \
         create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table_stream:
        item = {'p': random_string(), 'c': random_string(), 'x': random_string()}
        # The conditional writes need an existing item to get to their write.
        for t in [table, table_unsafe_rmw]:
            t.put_item(Item=item)
        arn, _ = wait_for_active_stream(dynamodbstreams, table_stream)
        with new_role(cql, superuser=True) as (role, secret):
            with new_service_level(cql, role, timeout='0ms') as sl, \
                 new_dynamodb(dynamodb, role, secret) as ses, \
                 new_dynamodb_streams(dynamodb, role, secret) as streams:
                key = {'p': item['p'], 'c': item['c']}
                wait_for_service_level(ses, table, key, metrics, sl)
                yield SimpleNamespace(client=ses.meta.client, streams=streams, table=table.name, table_lwt=table_lwt.name,
                                      table_unsafe_rmw=table_unsafe_rmw.name, table_stream=table_stream.name, arn=arn,
                                      item=item, key=key)

# held_reads() holds each replica read of the given table until "release"
# seconds pass or the context exits. It returns an event, which is set when
# the release starts. Without a table, it holds nothing and returns None.
@contextmanager
def held_reads(rest_api, table, release):
    if table is None:
        yield None
        return
    released = threading.Event()
    def do_release():
        released.set()
        requests.post(f'{rest_api}/v2/error_injection/injection/replica_query_wait/message')
    with scylla_inject_error(rest_api, 'replica_query_wait', parameters={'table': table}):
        timer = threading.Timer(release, do_release)
        timer.start()
        try:
            yield released
        finally:
            timer.cancel()

def timed_out():
    return pytest.raises(ClientError, match='InternalServerError.*[Tt]imed out')

def get_records(t):
    shard_id = t.streams.describe_stream(StreamArn=t.arn)['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = t.streams.get_shard_iterator(StreamArn=t.arn, ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')['ShardIterator']
    return t.streams.get_records(ShardIterator=shard_iterator)

def create_table_delete_table(t):
    name = unique_table_name()
    t.client.create_table(TableName=name, BillingMode='PAY_PER_REQUEST',
                          KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                          AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}])
    t.client.delete_table(TableName=name)

def tag_resource_untag_resource(t):
    arn = t.client.describe_table(TableName=t.table)['Table']['TableArn']
    t.client.tag_resource(ResourceArn=arn, Tags=[{'Key': 'k', 'Value': 'v'}])
    t.client.untag_resource(ResourceArn=arn, TagKeys=['k'])

# Each read and write of items, through each of its code paths, and the table
# whose replica reads the test holds, if any.
item_operations = {
    'GetItem': (lambda t: t.client.get_item(TableName=t.table, Key=t.key), lambda t: t.table),
    'BatchGetItem': (lambda t: t.client.batch_get_item(RequestItems={t.table: {'Keys': [t.key]}}), lambda t: t.table),
    'Query': (lambda t: t.client.query(TableName=t.table, KeyConditionExpression='p = :p', ExpressionAttributeValues={':p': t.key['p']}),
              lambda t: t.table),
    'QueryGSI': (lambda t: t.client.query(TableName=t.table, IndexName='gsi', KeyConditionExpression='x = :x',
                                          ExpressionAttributeValues={':x': t.item['x']}),
                 lambda t: f'{t.table}:gsi'),
    'Scan': (lambda t: t.client.scan(TableName=t.table), lambda t: t.table),
    'GetRecords': (get_records, lambda t: f'{t.table_stream}_scylla_cdc_log'),
    'PutItem': (lambda t: t.client.put_item(TableName=t.table, Item=t.item), None),
    'UpdateItem': (lambda t: t.client.update_item(TableName=t.table, Key=t.key, UpdateExpression='SET a = :a',
                                                  ExpressionAttributeValues={':a': 1}),
                   None),
    'DeleteItem': (lambda t: t.client.delete_item(TableName=t.table, Key={'p': random_string(), 'c': random_string()}), None),
    'BatchWriteItem': (lambda t: t.client.batch_write_item(RequestItems={t.table: [{'PutRequest': {'Item': t.item}}]}), None),
    'PutItemLWT': (lambda t: t.client.put_item(TableName=t.table, Item=t.item, ConditionExpression='attribute_exists(p)'), None),
    'UpdateItemLWT': (lambda t: t.client.update_item(TableName=t.table, Key=t.key, UpdateExpression='SET a = :a',
                                                     ExpressionAttributeValues={':a': 1}, ReturnValues='ALL_OLD'),
                      None),
    # 8 partitions, so that some writes move to another shard for LWT, except
    # in 1 of 256 runs (the test server has 2 shards).
    'BatchWriteItemLWT': (lambda t: t.client.batch_write_item(RequestItems={t.table_lwt: [
                              {'PutRequest': {'Item': {'p': random_string(), 'c': t.item['c']}}} for _ in range(8)]}),
                          None),
    # A read before a write without LWT: one case holds the read, the other
    # case usually gets to the write, because the read usually ends first.
    'ReadBeforeWriteRead': (lambda t: t.client.put_item(TableName=t.table_unsafe_rmw, Item=t.item,
                                                        ConditionExpression='attribute_exists(p)'),
                            lambda t: t.table_unsafe_rmw),
    'ReadBeforeWriteWrite': (lambda t: t.client.put_item(TableName=t.table_unsafe_rmw, Item=t.item,
                                                         ConditionExpression='attribute_exists(p)'),
                             None),
}

@pytest.mark.parametrize('op', item_operations.keys())
def test_service_level_timeout_items(zero_timeout, rest_api, op):
    operation, held_table = item_operations[op]
    with held_reads(rest_api, held_table and held_table(zero_timeout), release=5) as released:
        with timed_out():
            operation(zero_timeout)
        # The held read must end on its timeout, not on its release.
        assert not (released and released.is_set())

# Each schema and other control-plane operation must succeed despite the
# zero timeout. The second element is the table whose replica reads the test
# holds for longer than the service level timeout, if any: DescribeTable
# reads the build status of a GSI.
control_plane_operations = {
    'CreateTableDeleteTable': (create_table_delete_table, None),
    'DescribeTable': (lambda t: t.client.describe_table(TableName=t.table), 'view_build_status_v2'),
    'UpdateTable': (lambda t: t.client.update_table(TableName=t.table, BillingMode='PAY_PER_REQUEST'), None),
    'ListTables': (lambda t: t.client.list_tables(), None),
    'TagResourceUntagResource': (tag_resource_untag_resource, None),
    'ListTagsOfResource': (lambda t: t.client.list_tags_of_resource(
        ResourceArn=t.client.describe_table(TableName=t.table)['Table']['TableArn']), None),
    'UpdateTimeToLive': (lambda t: t.client.update_time_to_live(TableName=t.table_stream,
        TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True}), None),
    'DescribeTimeToLive': (lambda t: t.client.describe_time_to_live(TableName=t.table_stream), None),
    'DescribeContinuousBackups': (lambda t: t.client.describe_continuous_backups(TableName=t.table), None),
    'DescribeEndpoints': (lambda t: t.client.describe_endpoints(), None),
    'ListStreams': (lambda t: t.streams.list_streams(TableName=t.table_stream), None),
    'DescribeStream': (lambda t: t.streams.describe_stream(StreamArn=t.arn), None),
}

@pytest.mark.parametrize('op', control_plane_operations.keys())
def test_service_level_timeout_control_plane(zero_timeout, rest_api, op):
    operation, held_table = control_plane_operations[op]
    with held_reads(rest_api, held_table, release=1):
        operation(zero_timeout)

# A service level timeout replaces alternator_timeout_in_ms, also when it is
# longer. A service level without a timeout, and a role without a service
# level, keep alternator_timeout_in_ms. The test sets alternator_timeout_in_ms
# through CQL, because Alternator cannot change it back from zero.
@pytest.mark.parametrize('service_level,timeout,fails', [(True, '1h', False), (True, None, True), (False, None, True)],
                         ids=['timeout_1h', 'no_timeout', 'no_service_level'])
def test_service_level_timeout_replaces_alternator_timeout(dynamodb, cql, test_table_s, metrics, service_level, timeout, fails):
    item = {'p': random_string()}
    with new_role(cql, superuser=True) as (role, secret), \
         new_service_level(cql, role, timeout) if service_level else nullcontext() as sl, \
         new_dynamodb(dynamodb, role, secret) as ses:
        if sl:
            wait_for_service_level(ses, test_table_s, item, metrics, sl)
        with config_value_context(cql, 'alternator_timeout_in_ms', '0'), timed_out() if fails else nullcontext():
            ses.meta.client.put_item(TableName=test_table_s.name, Item=item)
