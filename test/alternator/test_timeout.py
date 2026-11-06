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

import pytest
import requests
from botocore.exceptions import ClientError

from .util import client_headers, client_ssl_context, get_signed_request, new_test_table, scylla_config_temporary, scylla_inject_error


@pytest.mark.parametrize('header', ['X-Scylla-Timeout', 'x-scylla-timeout'])
@pytest.mark.parametrize('value', ['', 'garbage', '-1ms', '1us', '1ms1ns', '1d', '1mo', '999999999999999999999s', '4294967296ms'])
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


def test_timeout_header_overrides_global(scylla_only, dynamodb, test_table_s):
    client = test_table_s.meta.client
    with client_headers(client, {'X-Scylla-Timeout': '1m'}):
        test_table_s.get_item(Key={'p': 'timeout'})
    for header in ['X-Scylla-Timeout', 'x-scylla-timeout']:
        with client_headers(client, {header: '0ms'}):
            with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                test_table_s.get_item(Key={'p': 'timeout'})
    # Header state must not leak into another request using the same client.
    test_table_s.get_item(Key={'p': 'timeout'})
    # The header can raise the configured timeout, as well as lower it.
    for header in ['X-Scylla-Timeout', 'x-scylla-timeout']:
        # Keep the header during restoration of the global setting too.
        with client_headers(client, {header: '1m'}):
            with scylla_config_temporary(dynamodb, 'alternator_timeout_in_ms', '0'):
                test_table_s.get_item(Key={'p': 'timeout'})


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
    with client_headers(client, {'X-Scylla-Timeout': '0ms'}):
        with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
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
        with scylla_inject_error(rest_api, 'replica_query_wait_table_match'), \
                scylla_inject_error(rest_api, 'replica_query_wait', parameters={'table': test_table_s.name}):
            start = time.monotonic()
            with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': '100ms'}):
                with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                    test_table_s.meta.client.batch_write_item(
                        RequestItems={test_table_s.name: [{'PutRequest': {'Item': key}}]})
            assert time.monotonic() - start < 5
            assert requests.get(f'{rest_api}/v2/error_injection/injection/replica_query_wait_table_match/enters', timeout=5).json() > 0
    # The failed read prevents the write.
    assert 'Item' not in test_table_s.get_item(Key=key, ConsistentRead=True)


def test_timeout_expired_before_execution(scylla_only, test_table_s, rest_api):
    # The server rejects an expired request before dispatching the operation,
    # so the error is distinguishable from a backend timeout.
    with scylla_inject_error(rest_api, 'alternator_timeout_before_execution'):
        with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': '1ms'}):
            with pytest.raises(ClientError, match='InternalServerError.*timed out before execution'):
                test_table_s.get_item(Key={'p': 'timeout_before_execution_read'}, ConsistentRead=True)


def test_timeout_includes_request_processing(scylla_only, test_table_s, rest_api):
    # Expiration before execution must prevent a write from taking effect (#31652).
    key = {'p': 'timeout_before_execution'}
    test_table_s.put_item(Item={**key, 'n': 0})
    with scylla_inject_error(rest_api, 'alternator_timeout_before_execution'):
        with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': '1ms'}):
            with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                test_table_s.update_item(Key=key, UpdateExpression='ADD n :one',
                    ExpressionAttributeValues={':one': 1})
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
        with client_headers(test_table_s.meta.client, {'X-Scylla-Timeout': '100ms'}):
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
    with scylla_inject_error(rest_api, 'replica_query_wait_table_match'), \
            scylla_inject_error(rest_api, 'replica_query_wait', parameters={'table': table.name}):
        start = time.monotonic()
        with client_headers(client, {'X-Scylla-Timeout': '100ms'}):
            with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                operations[operation]()
        assert time.monotonic() - start < 5
        assert requests.get(f'{rest_api}/v2/error_injection/injection/replica_query_wait_table_match/enters', timeout=5).json() > 0
    assert table.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'v': 'value'}


@pytest.mark.parametrize('header', [None, '100ms', '1s'])
def test_timeout_backend_global_override(scylla_only, dynamodb, test_table_s, rest_api, header):
    # Exercise real work beyond the global timeout when the header raises it,
    # plus a smaller header and the no-header baseline (#31652).
    client = test_table_s.meta.client
    global_ms = '100' if header in [None, '1s'] else '30000'
    with scylla_config_temporary(dynamodb, 'alternator_timeout_in_ms', global_ms):
        with ThreadPoolExecutor(max_workers=1) as pool:
            with scylla_inject_error(rest_api, 'replica_query_wait_table_match'), \
                    scylla_inject_error(rest_api, 'replica_query_wait', parameters={'table': test_table_s.name}):
                context = client_headers(client, {'X-Scylla-Timeout': header}) if header else nullcontext()
                with context:
                    start = time.monotonic()
                    pending = pool.submit(test_table_s.get_item, Key={'p': 'global_timeout'}, ConsistentRead=True)
                    if header == '1s':
                        try:
                            deadline = time.monotonic() + 5
                            while requests.get(f'{rest_api}/v2/error_injection/injection/replica_query_wait_table_match/enters', timeout=5).json() == 0:
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
    with scylla_inject_error(rest_api, 'replica_query_wait_table_match'), \
            scylla_inject_error(rest_api, 'replica_query_wait', parameters={'table': backing_table}):
        start = time.monotonic()
        with client_headers(table.meta.client, {'X-Scylla-Timeout': '100ms'}):
            with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                request(**arguments)
        assert time.monotonic() - start < 5
        assert requests.get(f'{rest_api}/v2/error_injection/injection/replica_query_wait_table_match/enters', timeout=5).json() > 0


@pytest.fixture(scope='module')
def timeout_wide_partition(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}, {'AttributeName': 'c', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[{'AttributeName': name, 'AttributeType': 'S'} for name in ['p', 'c']]) as table:
        with table.batch_writer() as batch:
            for i in range(2000):
                batch.put_item(Item={'p': 'wide', 'c': f'{i:04}', 'v': 'x' * 1024})
        yield table


def test_timeout_filtered_wide_query(scylla_only, timeout_wide_partition, rest_api):
    # Query fetches one backend page and filters it before returning
    # LastEvaluatedKey. Continuing requires a new HTTP request with a new
    # deadline. Check a large filtered page and a held initial read (#31652).
    table = timeout_wide_partition
    arguments = {'KeyConditionExpression': 'p = :p', 'ExpressionAttributeValues': {':p': 'wide'},
                 'FilterExpression': 'attribute_not_exists(v)', 'ConsistentRead': True}
    with client_headers(table.meta.client, {'X-Scylla-Timeout': '1m'}):
        response = table.query(**arguments)
    assert response['Items'] == []
    assert response['Count'] == 0
    assert response['ScannedCount'] > 100
    assert 'LastEvaluatedKey' in response
    with scylla_inject_error(rest_api, 'replica_query_wait_table_match'), \
            scylla_inject_error(rest_api, 'replica_query_wait', parameters={'table': table.name}):
        start = time.monotonic()
        with client_headers(table.meta.client, {'X-Scylla-Timeout': '100ms'}):
            with pytest.raises(ClientError, match='InternalServerError.*([Tt]imeout|timed out)'):
                table.query(**arguments)
        assert time.monotonic() - start < 5
        assert requests.get(f'{rest_api}/v2/error_injection/injection/replica_query_wait_table_match/enters', timeout=5).json() > 0
