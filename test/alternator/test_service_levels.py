# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import pytest
import requests
import threading
from botocore.exceptions import ClientError
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from typing import Any, Callable, NamedTuple
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
    cmd = f'CREATE SERVICE LEVEL "{sl}"'
    if timeout:
        cmd += f' WITH timeout = {timeout}'
    cql.execute(cmd)
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

# The zero_timeout fixture gives this setup to the tests. The table fields
# hold table names, not table objects. A table object sends its requests with
# the session of the default role. These requests do not use the zero timeout.
@dataclass
class ZeroTimeout:
    client: Any                  # DynamoDB client of the role
    streams: Any                 # DynamoDB Streams client of the role
    table_name: str              # the table with write isolation only_rmw_uses_lwt
    table_lwt_name: str          # the table with write isolation always_use_lwt
    table_unsafe_rmw_name: str   # the table with write isolation unsafe_rmw
    table_with_stream_name: str  # the table with a stream
    arn: str                     # the ARN of the stream of table_with_stream_name
    item: dict                   # an item in table_name and table_unsafe_rmw_name
    key: dict                    # the key of item

# zero_timeout is a fixture for the service level timeout tests below
# (SCYLLADB-6038). It makes a role whose service level has a zero timeout, and
# DynamoDB and DynamoDB Streams sessions of that role. It also makes one table
# for each write isolation mode, so that the writes use each code path: a plain
# write, LWT, and a read before a write without LWT. A fourth table has a
# stream, for GetRecords.
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
        # PutItemLWT, ReadBeforeWriteRead and ReadBeforeWriteWrite (see
        # item_operations) write only if the item exists (ConditionExpression
        # 'attribute_exists(p)'). If the item does not exist, the condition
        # fails. Then the request ends before it writes.
        for t in [table, table_unsafe_rmw]:
            t.put_item(Item=item)
        arn, _ = wait_for_active_stream(dynamodbstreams, table_stream)
        with new_role(cql, superuser=True) as (role, secret):
            with new_service_level(cql, role, timeout='0ms') as sl, \
                 new_dynamodb(dynamodb, role, secret) as ses, \
                 new_dynamodb_streams(dynamodb, role, secret) as streams:
                key = {'p': item['p'], 'c': item['c']}
                wait_for_service_level(ses, table, key, metrics, sl)
                yield ZeroTimeout(client=ses.meta.client, streams=streams, table_name=table.name,
                                  table_lwt_name=table_lwt.name, table_unsafe_rmw_name=table_unsafe_rmw.name,
                                  table_with_stream_name=table_stream.name, arn=arn, item=item, key=key)

# delayed_reads() is a context manager. It makes each replica read of the given
# table wait in the replica (the replica_query_wait error injection). A timer
# ends the wait after "release" seconds. The exit from the context also ends
# it. delayed_reads() yields an event, which the timer sets when it ends the
# wait. Without a table, delayed_reads() changes nothing and yields None.
@contextmanager
def delayed_reads(rest_api, table, release):
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

def get_records(t: ZeroTimeout):
    shard_id = t.streams.describe_stream(StreamArn=t.arn)['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = t.streams.get_shard_iterator(StreamArn=t.arn, ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')['ShardIterator']
    return t.streams.get_records(ShardIterator=shard_iterator)

def create_table_delete_table(t: ZeroTimeout):
    name = unique_table_name()
    t.client.create_table(TableName=name, BillingMode='PAY_PER_REQUEST',
                          KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                          AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}])
    t.client.delete_table(TableName=name)

def tag_resource_untag_resource(t: ZeroTimeout):
    arn = t.client.describe_table(TableName=t.table_name)['Table']['TableArn']
    t.client.tag_resource(ResourceArn=arn, Tags=[{'Key': 'k', 'Value': 'v'}])
    t.client.untag_resource(ResourceArn=arn, TagKeys=['k'])

# A Case is one test case. "operation" sends one request with the role of
# zero_timeout. "delayed_table" returns the name of the table on which
# delayed_reads() makes the replica reads wait. If the case does not delay
# replica reads, "delayed_table" is None.
class Case(NamedTuple):
    operation: Callable[[ZeroTimeout], object]
    delayed_table: Callable[[ZeroTimeout], str] | None = None

# Each read and write of items, through each of its code paths.
item_operations = {
    'GetItem': Case(lambda t: t.client.get_item(TableName=t.table_name, Key=t.key), lambda t: t.table_name),
    'BatchGetItem': Case(lambda t: t.client.batch_get_item(RequestItems={t.table_name: {'Keys': [t.key]}}), lambda t: t.table_name),
    'Query': Case(lambda t: t.client.query(TableName=t.table_name, KeyConditionExpression='p = :p',
                                           ExpressionAttributeValues={':p': t.key['p']}),
                  lambda t: t.table_name),
    'QueryGSI': Case(lambda t: t.client.query(TableName=t.table_name, IndexName='gsi', KeyConditionExpression='x = :x',
                                              ExpressionAttributeValues={':x': t.item['x']}),
                     lambda t: f'{t.table_name}:gsi'),
    'Scan': Case(lambda t: t.client.scan(TableName=t.table_name), lambda t: t.table_name),
    'GetRecords': Case(get_records, lambda t: f'{t.table_with_stream_name}_scylla_cdc_log'),
    'PutItem': Case(lambda t: t.client.put_item(TableName=t.table_name, Item=t.item)),
    'UpdateItem': Case(lambda t: t.client.update_item(TableName=t.table_name, Key=t.key, UpdateExpression='SET a = :a',
                                                      ExpressionAttributeValues={':a': 1})),
    'DeleteItem': Case(lambda t: t.client.delete_item(TableName=t.table_name, Key={'p': random_string(), 'c': random_string()})),
    'BatchWriteItem': Case(lambda t: t.client.batch_write_item(RequestItems={t.table_name: [{'PutRequest': {'Item': t.item}}]})),
    'PutItemLWT': Case(lambda t: t.client.put_item(TableName=t.table_name, Item=t.item, ConditionExpression='attribute_exists(p)')),
    'UpdateItemLWT': Case(lambda t: t.client.update_item(TableName=t.table_name, Key=t.key, UpdateExpression='SET a = :a',
                                                         ExpressionAttributeValues={':a': 1}, ReturnValues='ALL_OLD')),
    # 8 partitions, so that some writes move to another shard for LWT, except
    # in 1 of 256 runs (the test server has 2 shards).
    'BatchWriteItemLWT': Case(lambda t: t.client.batch_write_item(RequestItems={t.table_lwt_name: [
                                  {'PutRequest': {'Item': {'p': random_string(), 'c': t.item['c']}}} for _ in range(8)]})),
    # A read before a write without LWT: one case delays the read, the other
    # case usually gets to the write, because the read usually ends first.
    'ReadBeforeWriteRead': Case(lambda t: t.client.put_item(TableName=t.table_unsafe_rmw_name, Item=t.item,
                                                            ConditionExpression='attribute_exists(p)'),
                                lambda t: t.table_unsafe_rmw_name),
    'ReadBeforeWriteWrite': Case(lambda t: t.client.put_item(TableName=t.table_unsafe_rmw_name, Item=t.item,
                                                             ConditionExpression='attribute_exists(p)')),
}

# With a zero service level timeout, each read and write of items by the role
# must fail with a timeout. The replica rejects a write with an expired timeout
# (database::apply()). Thus each write fails at once. The replica does not do
# this check at the start of a read. A read of data in memory can end before
# the timer of the coordinator fires. Thus each read case makes the replica read
# wait (delayed_reads()). Then the test checks that the request failed before
# the wait ended.
@pytest.mark.parametrize('op', item_operations.keys())
def test_service_level_timeout_items(zero_timeout, rest_api, op):
    case = item_operations[op]
    with delayed_reads(rest_api, case.delayed_table and case.delayed_table(zero_timeout), release=5) as released:
        with timed_out():
            case.operation(zero_timeout)
        assert not (released and released.is_set())

# Each schema and other control-plane operation.
control_plane_operations = {
    'CreateTableDeleteTable': Case(create_table_delete_table),
    'DescribeTable': Case(lambda t: t.client.describe_table(TableName=t.table_name), lambda t: 'view_build_status_v2'),
    'UpdateTable': Case(lambda t: t.client.update_table(TableName=t.table_name, BillingMode='PAY_PER_REQUEST')),
    'ListTables': Case(lambda t: t.client.list_tables()),
    'TagResourceUntagResource': Case(tag_resource_untag_resource),
    'ListTagsOfResource': Case(lambda t: t.client.list_tags_of_resource(
        ResourceArn=t.client.describe_table(TableName=t.table_name)['Table']['TableArn'])),
    'UpdateTimeToLive': Case(lambda t: t.client.update_time_to_live(TableName=t.table_with_stream_name,
        TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})),
    'DescribeTimeToLive': Case(lambda t: t.client.describe_time_to_live(TableName=t.table_with_stream_name)),
    'DescribeContinuousBackups': Case(lambda t: t.client.describe_continuous_backups(TableName=t.table_name)),
    'DescribeEndpoints': Case(lambda t: t.client.describe_endpoints()),
    'ListStreams': Case(lambda t: t.streams.list_streams(TableName=t.table_with_stream_name)),
    'DescribeStream': Case(lambda t: t.streams.describe_stream(StreamArn=t.arn)),
}

# With a zero service level timeout, each schema and other control-plane
# operation must succeed, because it does not use the service level timeout.
# Of these operations, only DescribeTable reads with a timeout: it reads the
# build status of the GSI with alternator_timeout_in_ms. Its case delays that
# read for 1 s, longer than the zero timeout, to show that the read does not
# use the service level timeout.
@pytest.mark.parametrize('op', control_plane_operations.keys())
def test_service_level_timeout_control_plane(zero_timeout, rest_api, op):
    case = control_plane_operations[op]
    with delayed_reads(rest_api, case.delayed_table and case.delayed_table(zero_timeout), release=1):
        case.operation(zero_timeout)

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
