# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the request timeout header (scylladb/scylladb#31652).
# These are Alternator extensions, so none of these tests apply to DynamoDB.

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import http.client
import json
import time
import urllib.parse

import boto3
import pytest
import requests
from botocore.exceptions import ClientError

from .util import (check_increases_metric, check_increases_metric_exact, client_headers, client_ssl_context, get_signed_request,
                   held_replica_reads, injection_enters, metrics, new_test_table, scylla_config_read, scylla_config_temporary,
                   scylla_config_write, scylla_inject_error)


@pytest.mark.parametrize('header,value', [('x-scylla-timeout', 'garbage')] + [('X-Scylla-Timeout', value) for value in
    ['', 'garbage', '500', '-1ms', '1us', '1ms1ns', '1d', '1mo', '999999999999999999999s', '4294967296ms']])
def test_invalid_timeout(scylla_only, test_table_s, header, value):
    with client_headers(test_table_s.meta.client, {header: value}):
        with pytest.raises(ClientError, match='ValidationException'):
            test_table_s.get_item(Key={'p': 'timeout'})


@pytest.mark.parametrize('value', ['1m', '1m1s', '60000ms', '60000000us', 'PT1M', '4294967295ms'])
def test_timeout_duration_syntax(scylla_only, test_table_s, value):
    # The largest millisecond duration must not overflow the request deadline.
    with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': value}):
        test_table_s.get_item(Key={'p': 'timeout'})


def test_timeout_large_duration_rmw(scylla_only, test_table_s):
    # The maximum supported timeout must also work in the CAS path (#31652).
    # ALL_OLD makes each write read the previous item and therefore use LWT.
    key = {'p': 'timeout_large_duration_rmw'}
    with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': '4294967295ms'}):
        test_table_s.put_item(Item=key, ReturnValues='ALL_OLD')
        response = test_table_s.update_item(Key=key, UpdateExpression='SET a = :v',
            ExpressionAttributeValues={':v': 'value'}, ReturnValues='ALL_OLD')
        assert response['Attributes'] == key
        response = test_table_s.delete_item(Key=key, ReturnValues='ALL_OLD')
        assert response['Attributes'] == {**key, 'a': 'value'}


@pytest.mark.parametrize('operation', ['GetItem', 'PutItem', 'UpdateItem', 'DeleteItem', 'Query', 'Scan',
                                       'BatchGetItem', 'BatchWriteItem', 'ConditionalPut'])
def test_timeout_operations(scylla_only, test_table_s, operation):
    client = test_table_s.meta.client
    name = test_table_s.name
    key = {'p': 'timeout'}
    operations = {
        'GetItem': lambda: client.get_item(TableName=name, Key=key),
        'PutItem': lambda: client.put_item(TableName=name, Item=key),
        'UpdateItem': lambda: client.update_item(TableName=name, Key=key, UpdateExpression='SET a = :v',
                                                ExpressionAttributeValues={':v': 'value'}),
        'DeleteItem': lambda: client.delete_item(TableName=name, Key=key),
        'Query': lambda: client.query(TableName=name, KeyConditionExpression='p = :p', ExpressionAttributeValues={':p': key['p']}),
        'Scan': lambda: client.scan(TableName=name),
        'BatchGetItem': lambda: client.batch_get_item(RequestItems={name: {'Keys': [key], 'ConsistentRead': True}}),
        'BatchWriteItem': lambda: client.batch_write_item(RequestItems={name: [{'PutRequest': {'Item': key}}]}),
        'ConditionalPut': lambda: client.put_item(TableName=name, Item=key, ConditionExpression='attribute_not_exists(p)'),
    }
    # 0ms stops each operation before execution. The tests with held reads
    # check the backend paths.
    with client_headers(client, {'X-Scylla-Timeout': '0ms'}):
        with pytest.raises(ClientError, match='InternalServerError.*timed out before execution'):
            operations[operation]()


def test_timeout_duplicate_header(scylla_only, dynamodb, test_table_s):
    # Repeated header fields are joined into one value, which is not a valid
    # duration. Boto3 cannot repeat a header, so send the request directly.
    payload = json.dumps({'TableName': test_table_s.name, 'Key': {'p': {'S': 'timeout'}}})
    req = get_signed_request(dynamodb, 'GetItem', payload)
    url = urllib.parse.urlparse(req.url)
    if url.scheme == 'https':
        connection = http.client.HTTPSConnection(url.hostname, url.port, timeout=10, context=client_ssl_context(dynamodb))
    else:
        connection = http.client.HTTPConnection(url.hostname, url.port, timeout=10)
    try:
        connection.putrequest('POST', url.path or '/', skip_accept_encoding=True)
        for name, value in req.headers.items():
            if name.lower() not in ['host', 'content-length']:
                connection.putheader(name, value)
        connection.putheader('X-Scylla-Timeout', '5s')
        connection.putheader('X-Scylla-Timeout', '10s')
        connection.putheader('Content-Length', str(len(req.body)))
        connection.endheaders(req.body)
        response = connection.getresponse()
        body = response.read().decode()
    finally:
        connection.close()
    assert response.status == 400
    assert 'ValidationException' in body


def test_timeout_batch_write_read_before_write(scylla_only, dynamodb, test_table_s, rest_api):
    # With alternator_force_read_before_write, BatchWriteItem reads each
    # previous item to size it. That read must use the request deadline.
    key = {'p': 'timeout_batch_read_before_write'}
    with scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'true'):
        with held_replica_reads(rest_api, test_table_s.name) as held_reads:
            start = time.monotonic()
            with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': '100ms'}):
                with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                    test_table_s.meta.client.batch_write_item(
                        RequestItems={test_table_s.name: [{'PutRequest': {'Item': key}}]})
            assert time.monotonic() - start < 5
            assert held_reads() > 0
    # The failed read prevents the write.
    assert 'Item' not in test_table_s.get_item(Key=key, ConsistentRead=True)


@pytest.mark.parametrize('batch', [False, True])
def test_timeout_does_not_start_write_after_read(scylla_only, dynamodb, rest_api, metrics, batch):
    # A read finishing after the deadline must not start a mutation, which
    # could otherwise store hints for later replay (scylladb/scylladb#31652).
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            Tags=[{'Key': 'system:write_isolation', 'Value': 'unsafe_rmw'}]) as table:
        key = {'p': 'timeout_after_read'}
        table.put_item(Item={**key, 'n': 0})

        def write(n):
            if batch:
                table.meta.client.batch_write_item(RequestItems={table.name: [
                    {'PutRequest': {'Item': {**key, 'n': n}}}]})
            else:
                table.update_item(Key=key, UpdateExpression='ADD n :one',
                    ExpressionAttributeValues={':one': 1})

        metric = 'scylla_storage_proxy_coordinator_total_write_attempts_local_node'
        # Exclude writes from background scheduling groups.
        labels = {'scheduling_group_name': 'sl:default'}
        config = scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'true') if batch else nullcontext()
        with config, scylla_inject_error(rest_api, 'alternator_timeout_after_read'):
            # Warm the read path and verify this metric observes our writes.
            with check_increases_metric(metrics, [metric], labels):
                write(1)
            with check_increases_metric_exact(metrics, metric, [(0, labels)]):
                with client_headers(table.meta.client, {'X-Scylla-Timeout': '1s'}):
                    with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                        write(2)
            assert injection_enters(rest_api, 'alternator_timeout_after_read') == 2
        assert table.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'n': 1}


@pytest.mark.parametrize('operation', ['PutItem', 'DeleteItem', 'UpdateItem', 'BatchWriteItem'])
def test_timeout_does_not_start_write_after_permissions(scylla_only, dynamodb, rest_api, metrics, operation):
    # Permission checks consume the deadline too; an expired plain write must
    # not reach mutation processing or hint creation (#31652).
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            Tags=[{'Key': 'system:write_isolation', 'Value': 'only_rmw_uses_lwt'}]) as table:
        key = {'p': 'timeout_after_permissions'}
        table.put_item(Item={**key, 'n': 0})

        def write():
            if operation == 'PutItem':
                table.put_item(Item={**key, 'n': 1})
            elif operation == 'BatchWriteItem':
                table.meta.client.batch_write_item(RequestItems={table.name: [
                    {'PutRequest': {'Item': {**key, 'n': 1}}}]})
            elif operation == 'DeleteItem':
                table.delete_item(Key=key)
            else:
                table.update_item(Key=key, UpdateExpression='SET n = :one',
                    ExpressionAttributeValues={':one': 1})

        metric = 'scylla_storage_proxy_coordinator_total_write_attempts_local_node'
        # Exclude writes from background scheduling groups.
        labels = {'scheduling_group_name': 'sl:default'}
        with scylla_config_temporary(dynamodb, 'alternator_enforce_authorization', 'true'), \
                scylla_config_temporary(dynamodb, 'alternator_force_read_before_write', 'false'), \
                scylla_inject_error(rest_api, 'alternator_timeout_after_permissions'):
            # The write that is not started is counted as a timeout.
            with check_increases_metric_exact(metrics, metric, [(0, labels)]), \
                    check_increases_metric(metrics, ['scylla_alternator_requests_timed_out_before_execution']):
                with client_headers(table.meta.client, {'X-Scylla-Timeout': '1s'}):
                    with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                        write()
            assert injection_enters(rest_api, 'alternator_timeout_after_permissions') == 1
            assert table.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'n': 0}
            # A request without a header retains the configured timeout.
            with check_increases_metric(metrics, [metric], labels):
                write()


def test_timeout_expired_before_execution(scylla_only, test_table_s, rest_api, metrics):
    # The server rejects an expired request before dispatching the operation,
    # so the error is distinguishable from a backend timeout.
    with check_increases_metric(metrics, ['scylla_alternator_requests_timed_out_before_execution']), \
            scylla_inject_error(rest_api, 'alternator_timeout_before_execution'):
        with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': '1s'}):
            with pytest.raises(ClientError, match='InternalServerError.*timed out before execution'):
                test_table_s.get_item(Key={'p': 'timeout_before_execution_read'}, ConsistentRead=True)
        assert injection_enters(rest_api, 'alternator_timeout_before_execution') == 1


@pytest.mark.parametrize('size', [1, 16 * 1024])
def test_timeout_expired_before_parsing(scylla_only, dynamodb, metrics, size):
    # An expired request must not parse even malformed JSON, including bodies
    # large enough to use the shard's parser thread (scylladb/scylladb#31652).
    req = get_signed_request(dynamodb, 'GetItem', '{' + ' ' * size, {'X-Scylla-Timeout': '0ms'})
    with check_increases_metric(metrics, ['scylla_alternator_requests_timed_out_before_execution']):
        response = requests.post(req.url, headers=req.headers, data=req.body, cert=req.cert, verify=False, timeout=5)
        assert response.status_code == 500
        assert 'timed out before execution' in response.text


def test_timeout_includes_request_processing(scylla_only, test_table_s, rest_api):
    # Expiration before execution must prevent a write from taking effect (#31652).
    key = {'p': 'timeout_before_execution'}
    test_table_s.put_item(Item={**key, 'n': 0})
    with scylla_inject_error(rest_api, 'alternator_timeout_before_execution'):
        with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': '1s'}):
            with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                test_table_s.update_item(Key=key, UpdateExpression='ADD n :one',
                    ExpressionAttributeValues={':one': 1})
        assert injection_enters(rest_api, 'alternator_timeout_before_execution') == 1
    assert test_table_s.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'n': 0}


def test_timeout_preserves_completed_write(scylla_only, test_table_s, rest_api):
    # A successful increment must not become a retriable error when sending
    # its result crosses the deadline (scylladb/scylladb#31652).
    key = {'p': 'completed_timeout_write'}
    # Initialize Paxos before the timed request, so lazy table creation cannot
    # consume its budget before the completed-response injection is reached.
    test_table_s.update_item(Key=key, UpdateExpression='SET n = :zero',
        ExpressionAttributeValues={':zero': 0}, ReturnValues='ALL_NEW')
    with scylla_inject_error(rest_api, 'alternator_delay_completed_request'):
        with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': '1s'}):
            result = test_table_s.update_item(Key=key, UpdateExpression='ADD n :one',
                ExpressionAttributeValues={':one': 1}, ReturnValues='ALL_NEW', ReturnConsumedCapacity='TOTAL')
    assert result['Attributes'] == {**key, 'n': 1}
    assert result['ConsumedCapacity']['CapacityUnits'] > 0
    assert test_table_s.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'n': 1}


def test_timeout_preserves_batch_get_partial_results(scylla_only, test_table_s, test_table_s_2, rest_api):
    # A timeout on one table must retain the other table's successful read and
    # return the remaining key in UnprocessedKeys, as without headers (#31652).
    key = {'p': 'timeout_partial'}
    test_table_s.put_item(Item=key)
    test_table_s_2.put_item(Item=key)
    injection = 'replica_query_wait'
    with scylla_inject_error(rest_api, injection, parameters={'table': test_table_s_2.name}):
        start = time.monotonic()
        with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': '100ms'}):
            response = test_table_s.meta.client.batch_get_item(RequestItems={
                table.name: {'Keys': [key], 'ConsistentRead': True}
                for table in [test_table_s, test_table_s_2]})
        # The global timeout is 30s, so this fails if the header is ignored.
        assert time.monotonic() - start < 5
        assert response['Responses'] == {test_table_s.name: [key]}
        assert response['UnprocessedKeys'] == {
            test_table_s_2.name: {'Keys': [key], 'ConsistentRead': True}}


def test_concurrent_request_timeouts(scylla_only, test_table_s, new_dynamodb_session):
    short = new_dynamodb_session().Table(test_table_s.name)
    long = new_dynamodb_session().Table(test_table_s.name)

    def read(table):
        try:
            table.get_item(Key={'p': 'timeout'})
            return True
        except ClientError as e:
            assert e.response['Error']['Code'] == 'InternalServerError'
            return False

    try:
        with client_headers(short.meta.client, {'X-Scylla-Timeout': '0ms'}), \
                client_headers(long.meta.client, {'X-Scylla-Timeout': '1m'}), ThreadPoolExecutor(max_workers=4) as pool:
            tables = [short, long] * 8
            assert list(pool.map(read, tables)) == [False, True] * 8
    finally:
        short.meta.client.close()
        long.meta.client.close()


@pytest.mark.parametrize('timeout', ['0ms', 'garbage'])
def test_timeout_control_plane_scope(scylla_only, dynamodb, timeout):
    # Control operations validate the extension but ignore valid deadlines.
    schema = {'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'}],
              'AttributeDefinitions': [{'AttributeName': 'p', 'AttributeType': 'S'}]}
    with new_test_table(dynamodb, **schema) as table:
        client = table.meta.client
        arn = client.describe_table(TableName=table.name)['Table']['TableArn']
        operations = [
            lambda: client.describe_table(TableName=table.name),
            lambda: client.list_tables(),
            lambda: client.tag_resource(ResourceArn=arn, Tags=[{'Key': 'timeout', 'Value': 'ignored'}]),
            lambda: client.list_tags_of_resource(ResourceArn=arn),
            lambda: client.untag_resource(ResourceArn=arn, TagKeys=['timeout']),
            lambda: client.describe_time_to_live(TableName=table.name),
            lambda: client.update_time_to_live(TableName=table.name,
                TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True}),
        ]
        with client_headers(client, {'X-Scylla-Timeout': timeout}):
            for operation in operations:
                if timeout == 'garbage':
                    with pytest.raises(ClientError, match='ValidationException'):
                        operation()
                else:
                    operation()
            if timeout != 'garbage':
                with new_test_table(dynamodb, **schema):
                    pass


@pytest.mark.parametrize('operation', ['GetItem', 'BatchGetItem', 'Query', 'Scan', 'QueryCount', 'ScanCount', 'FilteredQuery', 'FilteredScan'])
def test_timeout_blocked_backend_read(scylla_only, test_table_s, rest_api, operation):
    # Hold the real replica read until after the response. A frontend sleep or
    # a final response override cannot satisfy this test (#31652).
    table = test_table_s
    key = {'p': 'blocked_timeout_read'}
    table.put_item(Item={**key, 'v': 'value'})
    client = table.meta.client
    query = {'TableName': table.name, 'KeyConditionExpression': 'p = :p',
             'ExpressionAttributeValues': {':p': key['p']}, 'ConsistentRead': True}
    scan = {'TableName': table.name, 'ConsistentRead': True}
    operations = {
        'GetItem': lambda: table.get_item(Key=key, ConsistentRead=True),
        'BatchGetItem': lambda: client.batch_get_item(RequestItems={table.name: {'Keys': [key], 'ConsistentRead': True}}),
        'Query': lambda: client.query(**query),
        'Scan': lambda: client.scan(**scan),
        'QueryCount': lambda: client.query(**query, Select='COUNT'),
        'ScanCount': lambda: client.scan(**scan, Select='COUNT'),
        'FilteredQuery': lambda: client.query(**query, FilterExpression='attribute_exists(v)'),
        'FilteredScan': lambda: client.scan(**scan, FilterExpression='attribute_exists(v)'),
    }
    with held_replica_reads(rest_api, table.name) as held_reads:
        start = time.monotonic()
        with client_headers(client, {'X-Scylla-Timeout': '100ms'}):
            with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                operations[operation]()
        assert time.monotonic() - start < 5
        assert held_reads() > 0
    assert table.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'v': 'value'}


@pytest.mark.parametrize('header', [None, '100ms', '1m'])
def test_timeout_backend_global_override(scylla_only, dynamodb, test_table_s, rest_api, header):
    # Exercise real work beyond the global timeout when the header raises it,
    # plus a smaller header and the no-header baseline (#31652).
    client = test_table_s.meta.client
    global_ms = '100' if header in [None, '1m'] else '30000'
    original = scylla_config_read(dynamodb, 'alternator_timeout_in_ms')
    scylla_config_write(dynamodb, 'alternator_timeout_in_ms', global_ms)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            with held_replica_reads(rest_api, test_table_s.name) as held_reads:
                context = client_headers(client, {'X-Scylla-Timeout': header}) if header else nullcontext()
                with context:
                    start = time.monotonic()
                    pending = pool.submit(test_table_s.get_item, Key={'p': 'global_timeout'}, ConsistentRead=True)
                    if header == '1m':
                        try:
                            deadline = time.monotonic() + 5
                            while held_reads() == 0:
                                assert time.monotonic() < deadline
                                time.sleep(0.001)
                            time.sleep(0.2)
                        finally:
                            requests.post(f'{rest_api}/v2/error_injection/injection/replica_query_wait/message', timeout=5).raise_for_status()
                        assert 'Item' not in pending.result(timeout=5)
                    else:
                        with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                            pending.result(timeout=5)
                    assert time.monotonic() - start < 5
    finally:
        # Restoration must not inherit the short timeout being tested.
        with client_headers(dynamodb.meta.client, {'X-Scylla-Timeout': '1m'}):
            scylla_config_write(dynamodb, 'alternator_timeout_in_ms', original)


@pytest.fixture(scope='module')
def timeout_index_table(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}, {'AttributeName': 'c', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[{'AttributeName': name, 'AttributeType': 'S'} for name in ['p', 'c', 'x']],
            GlobalSecondaryIndexes=[{'IndexName': 'gsi', 'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                                     'Projection': {'ProjectionType': 'ALL'}}],
            LocalSecondaryIndexes=[{'IndexName': 'lsi', 'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                                                                      {'AttributeName': 'x', 'KeyType': 'RANGE'}],
                                    'Projection': {'ProjectionType': 'ALL'}}]) as table:
        with table.batch_writer() as batch:
            for i in range(12):
                batch.put_item(Item={'p': 'partition', 'c': str(i), 'x': 'indexed', 'v': 'present'})
        deadline = time.monotonic() + 30
        while table.query(IndexName='gsi', KeyConditionExpression='x = :x',
                ExpressionAttributeValues={':x': 'indexed'}, Select='COUNT')['Count'] != 12:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        yield table


@pytest.mark.parametrize('index', ['gsi', 'lsi'])
@pytest.mark.parametrize('operation', ['query', 'scan'])
@pytest.mark.parametrize('mode', ['count', 'filtered'])
def test_timeout_index_reads(scylla_only, timeout_index_table, rest_api, index, operation, mode):
    # Index reads, COUNT and filtered pages must all receive the HTTP budget.
    table = timeout_index_table
    arguments = {'IndexName': index, 'Limit': 3}
    if operation == 'query':
        key, value = ('x', 'indexed') if index == 'gsi' else ('p', 'partition')
        arguments.update(KeyConditionExpression=f'{key} = :key', ExpressionAttributeValues={':key': value})
    if mode == 'count':
        arguments['Select'] = 'COUNT'
    else:
        arguments['FilterExpression'] = 'attribute_not_exists(v)'
    request = getattr(table, operation)
    with client_headers(table.meta.client, {'X-Scylla-Timeout': '1m'}):
        pages = scanned = count = 0
        deadline = time.monotonic() + 30
        while True:
            assert time.monotonic() < deadline, 'Index pagination did not finish'
            response = request(**arguments)
            pages += 1
            scanned += response['ScannedCount']
            count += response['Count']
            if 'LastEvaluatedKey' not in response:
                break
            arguments['ExclusiveStartKey'] = response['LastEvaluatedKey']
        assert pages >= 4
        assert scanned == 12
        assert count == (12 if mode == 'count' else 0)
    arguments.pop('ExclusiveStartKey', None)
    backing_table = table.name + (':gsi' if index == 'gsi' else '!:lsi')
    with held_replica_reads(rest_api, backing_table) as held_reads:
        start = time.monotonic()
        with client_headers(table.meta.client, {'X-Scylla-Timeout': '100ms'}):
            with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                request(**arguments)
        assert time.monotonic() - start < 5
        assert held_reads() > 0


def test_timeout_without_header_is_per_step(scylla_only, dynamodb, rest_api):
    # Without the header, the read and the write of an unsafe RMW each get a
    # new alternator_timeout_in_ms. With the header, one deadline covers both.
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            Tags=[{'Key': 'system:write_isolation', 'Value': 'unsafe_rmw'}]) as table:
        key = {'p': 'per_step'}
        table.put_item(Item={**key, 'n': 0})

        def increment():
            table.update_item(Key=key, UpdateExpression='ADD n :one', ExpressionAttributeValues={':one': 1})

        original = scylla_config_read(dynamodb, 'alternator_timeout_in_ms')
        # The hook between the read and the write sleeps 200 ms without a
        # header, which is longer than this global timeout.
        scylla_config_write(dynamodb, 'alternator_timeout_in_ms', '150')
        try:
            with scylla_inject_error(rest_api, 'alternator_timeout_after_read'):
                increment()
                with client_headers(table.meta.client, {'X-Scylla-Timeout': '150ms'}):
                    with pytest.raises(ClientError, match='InternalServerError.*timed out'):
                        increment()
        finally:
            # Restoration must not inherit the short timeout being tested.
            with client_headers(dynamodb.meta.client, {'X-Scylla-Timeout': '1m'}):
                scylla_config_write(dynamodb, 'alternator_timeout_in_ms', original)
        assert table.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'n': 1}


def test_timeout_first_lwt_on_new_tablet_table(scylla_only, dynamodb):
    # The first LWT write to a table with tablets creates its Paxos state table
    # within the request deadline. Retries with a short header must still make
    # progress, and fail only with timeouts until then.
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            Tags=[{'Key': 'system:initial_tablets', 'Value': '1'},
                  {'Key': 'system:write_isolation', 'Value': 'always_use_lwt'}]) as table:
        deadline = time.monotonic() + 30
        with client_headers(table.meta.client, {'X-Scylla-Timeout': '100ms'}):
            while True:
                try:
                    table.put_item(Item={'p': 'first_lwt'})
                    break
                except ClientError as e:
                    assert e.response['Error']['Code'] == 'InternalServerError'
                    assert time.monotonic() < deadline, 'The first LWT write made no progress'
        assert table.get_item(Key={'p': 'first_lwt'}, ConsistentRead=True)['Item'] == {'p': 'first_lwt'}


def test_timeout_invalid_header_after_authentication(scylla_only, request, dynamodb, test_table_s):
    # An invalid header is reported only after authentication, so an
    # unauthenticated client gets no information from it.
    if request.config.getoption('mtls'):
        pytest.skip('mTLS does not check SigV4 signatures')
    url = dynamodb.meta.client._endpoint.host
    client = boto3.client('dynamodb', endpoint_url=url, region_name='us-east-1', aws_access_key_id='alternator',
                          aws_secret_access_key='wrong_key', verify=not url.startswith('https'))
    try:
        with client_headers(client, {'X-Scylla-Timeout': 'garbage'}):
            with pytest.raises(ClientError, match='UnrecognizedClientException'):
                client.get_item(TableName=test_table_s.name, Key={'p': {'S': 'timeout'}})
    finally:
        client.close()


def test_timeout_invalid_header_after_unknown_operation(scylla_only, dynamodb):
    # An unknown operation is reported before an invalid header.
    req = get_signed_request(dynamodb, 'NoSuchOperation', '{}', {'X-Scylla-Timeout': 'garbage'})
    response = requests.post(req.url, headers=req.headers, data=req.body, cert=req.cert, verify=False, timeout=5)
    assert response.status_code == 400
    assert 'UnknownOperationException' in response.text
