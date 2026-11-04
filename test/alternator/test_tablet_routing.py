# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Opt-in tablet hints share routing V2 metadata with CQL, including on replicas."""

from contextlib import contextmanager
from decimal import Decimal
import re
import uuid

from botocore.exceptions import ClientError
import pytest

from test.pylib.skip_types import skip_env

from .util import full_scan, new_dynamodb, new_role, new_test_table
from .test_compressed_response import client_response_decompression, server_response_compression

pytestmark = pytest.mark.usefixtures('scylla_only')

VERSION_HEADER = 'X-Scylla-Tablet-Version'
SHORT_VERSION_HEADER = 'X-Scylla-T-V'
INFO_HEADER = 'x-scylla-tablet-info'


@contextmanager
def routing_headers(dynamodb, value='2:-', **headers):
    """Sign the extra headers normally, and remove them after this request scope."""
    def before_sign(request, **kwargs):
        if value is not None:
            request.headers[VERSION_HEADER] = value
        for name, text in headers.items():
            request.headers[name] = text
    events = dynamodb.meta.client.meta.events
    events.register('before-sign.dynamodb', before_sign)
    try:
        yield
    finally:
        events.unregister('before-sign.dynamodb', before_sign)


def routing_info(response):
    return response['ResponseMetadata']['HTTPHeaders'].get(INFO_HEADER)


def parse_info(value, mode='regular'):
    match = re.fullmatch(
        r'2;start=(-?\d+);end=(-?\d+);version=([0-9a-f]{16});mode=(regular|strong);replicas="([^"]*)"', value)
    assert match, value
    start, end, version, actual_mode, entries = match.groups()
    assert actual_mode == mode, value
    replicas = []
    for entry in entries.split():
        host, shard = entry.split('/')
        assert str(uuid.UUID(host)) == host
        assert re.fullmatch(r'0|[1-9][0-9]*', shard)
        replicas.append((host, int(shard)))
    assert replicas
    return int(start), int(end), int(version, 16), replicas


def version_block(version, index, matching=True):
    nibble = (version >> (4 * index)) & 15
    return f'2:{(index << 4) | (nibble if matching else nibble ^ 15):02x}'


@pytest.fixture
def tablet_table(test_table_sn, has_tablets):
    if not has_tablets:
        skip_env('Requires a tablet-backed Alternator table')
    return test_table_sn


@pytest.mark.parametrize('operation', ['get_item', 'put_item', 'update_item', 'delete_item', 'query'])
@pytest.mark.parametrize('consistent', [False, True])
def test_tablet_routing_operations(dynamodb, tablet_table, operation, consistent):
    """All success paths, including missing reads/deletes and empty queries, can refresh."""
    key = {'p': f'routing-{operation}-{consistent}', 'c': Decimal(0)}
    if operation == 'put_item':
        args = {'Item': key}
    elif operation == 'query':
        args = {'KeyConditionExpression': 'p = :p', 'ExpressionAttributeValues': {':p': key['p']}, 'ConsistentRead': consistent}
    else:
        args = {'Key': key}
        if operation == 'get_item':
            args['ConsistentRead'] = consistent
        elif operation == 'update_item':
            args.update(UpdateExpression='SET v = :v', ExpressionAttributeValues={':v': 1})
    call = getattr(tablet_table, operation)
    if operation in ['get_item', 'query']:
        tablet_table.delete_item(Key=key)
    initial = call(**args)
    assert routing_info(initial) is None
    if operation == 'get_item':
        assert 'Item' not in initial
    elif operation == 'query':
        assert initial['Items'] == []
    with routing_headers(dynamodb):
        info = routing_info(call(**args))
    _, _, version, _ = parse_info(info)
    if operation in ['get_item', 'query']:
        tablet_table.put_item(Item=key)
        with routing_headers(dynamodb):
            present = call(**args)
            assert routing_info(present) == info
            if operation == 'get_item':
                assert present['Item'] == key
            else:
                assert present['Items'] == [key]
    for index in range(16):
        with routing_headers(dynamodb, version_block(version, index)):
            assert routing_info(call(**args)) is None
        with routing_headers(dynamodb, version_block(version, index, matching=False).upper()):
            assert routing_info(call(**args)) == info
    # The same SDK connection pool carries a subsequent request without the extension.
    assert routing_info(call(**args)) is None


@pytest.mark.parametrize('value', ['', ' ', '2', ':-', '-2:-', '+2:-', 'v2:-', '2 :dc', '2:', '2:0', '2:000', '2:gg', '2:d c', '2:dc;', '2:--'])
def test_tablet_routing_invalid_prevents_write(dynamodb, tablet_table, value):
    key = {'p': 'routing-invalid', 'c': Decimal(0)}
    tablet_table.delete_item(Key=key)
    with routing_headers(dynamodb, value):
        with pytest.raises(ClientError, match='ValidationException') as error:
            tablet_table.put_item(Item=key)
        assert routing_info(error.value.response) is None
    assert 'Item' not in tablet_table.get_item(Key=key, ConsistentRead=True)


@pytest.mark.parametrize('value', ['1:', '3:uninterpreted suffix', '999999999999999999999999:!'])
def test_tablet_routing_unsupported(dynamodb, tablet_table, value):
    with routing_headers(dynamodb, value):
        assert routing_info(tablet_table.get_item(Key={'p': 'unsupported', 'c': 0})) is None


def test_tablet_routing_errors(dynamodb, tablet_table):
    key = {'p': 'routing-condition', 'c': Decimal(0)}
    tablet_table.put_item(Item={**key, 'v': 1})
    with routing_headers(dynamodb):
        for operation, args in [
            ('get_item', {'Key': {'p': key['p']}}),
            ('put_item', {'Item': key, 'ConditionExpression': 'attribute_not_exists(p)'}),
            ('update_item', {'Key': key, 'UpdateExpression': 'SET v = :v', 'ExpressionAttributeValues': {':v': 2},
                             'ConditionExpression': 'attribute_not_exists(p)'}),
            ('delete_item', {'Key': key, 'ConditionExpression': 'attribute_not_exists(p)'}),
        ]:
            with pytest.raises(ClientError) as error:
                getattr(tablet_table, operation)(**args)
            assert routing_info(error.value.response) is None


@pytest.mark.parametrize('isolation', ['always_use_lwt', 'only_rmw_uses_lwt', 'unsafe_rmw'])
def test_tablet_routing_write_isolation(dynamodb, tablet_table, isolation):
    """LWT and ordinary writes have identical regular-table hint semantics."""
    tablet_table.meta.client.tag_resource(ResourceArn=tablet_table.table_arn,
        Tags=[{'Key': 'system:write_isolation', 'Value': isolation}])
    key = {'p': 'routing-isolation', 'c': Decimal(0)}
    try:
        with routing_headers(dynamodb):
            info = routing_info(tablet_table.put_item(Item={**key, 'v': 1}))
            parse_info(info)
            assert routing_info(tablet_table.update_item(Key=key, UpdateExpression='SET v = :v',
                ExpressionAttributeValues={':v': 2}, ConditionExpression='attribute_exists(p)')) == info
            assert routing_info(tablet_table.delete_item(Key=key, ConditionExpression='attribute_exists(p)')) == info
    finally:
        tablet_table.meta.client.untag_resource(ResourceArn=tablet_table.table_arn, TagKeys=['system:write_isolation'])


@pytest.mark.parametrize('fixture,key', [('test_table_s', 'routing-string'), ('test_table_n', Decimal('123.50')), ('test_table_b', b'\x00routing\xff')])
def test_tablet_routing_key_encoding(dynamodb, request, fixture, key, has_tablets, cql):
    if not has_tablets:
        skip_env('Requires tablets')
    table = request.getfixturevalue(fixture)
    table.put_item(Item={'p': key})
    statement = cql.prepare(f'SELECT token(p) FROM "alternator_{table.name}"."{table.name}" WHERE p = ?')
    token = cql.execute(statement, [key]).one()[0]
    with routing_headers(dynamodb):
        start, end, _, _ = parse_info(routing_info(table.get_item(Key={'p': key})))
        query_info = parse_info(routing_info(table.query(KeyConditionExpression='p = :p', ExpressionAttributeValues={':p': key})))
    assert start < token <= end
    assert query_info[:2] == (start, end)


@pytest.mark.parametrize('count', [1, 300])
@pytest.mark.parametrize('encoding', ['identity', 'gzip', 'deflate'])
def test_tablet_routing_compression_and_cors(dynamodb, tablet_table, count, encoding):
    """Both buffered and streamed replies preserve the hint through compression."""
    p = f'routing-output-{count}-{encoding}'
    with tablet_table.batch_writer() as batch:
        for c in range(count):
            batch.put_item(Item={'p': p, 'c': c, 'v': 'x' * 128})
    args = {'KeyConditionExpression': 'p = :p', 'ExpressionAttributeValues': {':p': p}, 'ConsistentRead': True}
    with routing_headers(dynamodb):
        expected = routing_info(tablet_table.get_item(Key={'p': p, 'c': 0}))
    with server_response_compression(dynamodb, compression_threshold=1), client_response_decompression(dynamodb):
        with routing_headers(dynamodb, **{'Accept-Encoding': encoding, 'Origin': 'https://example.com'}):
            response = tablet_table.query(**args)
    assert len(response['Items']) == count
    assert routing_info(response) == expected
    exposed = response['ResponseMetadata']['HTTPHeaders']['access-control-expose-headers'].lower().split(',')
    assert INFO_HEADER in exposed
    if encoding != 'identity':
        assert response['WasCompressed'] == encoding


def test_tablet_routing_discovery(dynamodb, tablet_table):
    with routing_headers(dynamodb):
        _, _, _, replicas = parse_info(routing_info(tablet_table.get_item(Key={'p': 'discovery', 'c': 0})))
    identities = {}
    for name in ['local', 'peers']:
        # Limit forces pagination even on the smallest multi-node deployments.
        for row in full_scan(dynamodb.Table(f'.scylla.alternator.system.{name}'), Limit=1):
            identities[row['host_id']] = row
    for host_id, _ in replicas:
        assert {'host_id', 'rpc_address', 'data_center', 'rack'} <= identities[host_id].keys()


def test_tablet_routing_excluded_operations(dynamodb, tablet_table):
    """Operations outside the extension ignore even a malformed header."""
    with routing_headers(dynamodb, 'invalid'):
        assert routing_info(tablet_table.scan(Limit=1)) is None
        assert routing_info(dynamodb.batch_get_item(RequestItems={tablet_table.name: {'Keys': [{'p': 'excluded', 'c': 0}]}})) is None
        assert routing_info(dynamodb.batch_write_item(RequestItems={tablet_table.name: [{'PutRequest': {'Item': {'p': 'excluded', 'c': 0}}}]})) is None


def test_tablet_routing_index_query(dynamodb, has_tablets):
    if not has_tablets:
        skip_env('Requires tablets')
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}, {'AttributeName': 'c', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[{'AttributeName': k, 'AttributeType': 'S'} for k in ['p', 'c', 'g']],
            GlobalSecondaryIndexes=[{'IndexName': 'by_g', 'KeySchema': [{'AttributeName': 'g', 'KeyType': 'HASH'}],
                                     'Projection': {'ProjectionType': 'ALL'}}],
            LocalSecondaryIndexes=[{'IndexName': 'local_g', 'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                                                                        {'AttributeName': 'g', 'KeyType': 'RANGE'}],
                                    'Projection': {'ProjectionType': 'ALL'}}]) as table:
        with routing_headers(dynamodb):
            for index, key in [('by_g', 'g'), ('local_g', 'p')]:
                response = table.query(IndexName=index, KeyConditionExpression=f'{key} = :v', ExpressionAttributeValues={':v': 'absent'})
                assert routing_info(response) is None


def test_tablet_routing_vnodes(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            Tags=[{'Key': 'system:initial_tablets', 'Value': 'none'}]) as table:
        for value in ['2:-', '2:00', '2:01']:
            with routing_headers(dynamodb, value):
                assert routing_info(table.put_item(Item={'p': 'vnode'})) is None
                assert routing_info(table.get_item(Key={'p': 'vnode'})) is None
                assert routing_info(table.query(KeyConditionExpression='p = :p', ExpressionAttributeValues={':p': 'vnode'})) is None
                assert routing_info(table.update_item(Key={'p': 'vnode'}, UpdateExpression='SET v = :v',
                    ExpressionAttributeValues={':v': 1})) is None
                assert routing_info(table.delete_item(Key={'p': 'vnode'})) is None


@pytest.mark.parametrize('header,value', [(VERSION_HEADER.lower(), '2:-'), (VERSION_HEADER.upper(), '\t2:- '),
    (VERSION_HEADER, ' 02:-\t'), (SHORT_VERSION_HEADER.lower(), '2:-'), (SHORT_VERSION_HEADER.upper(), '\t2:- '),
    ('X-sCyLlA-t-V', ' 02:-\t')])
def test_tablet_routing_http_header_syntax(dynamodb, tablet_table, header, value):
    with routing_headers(dynamodb, None, **{header: value}):
        parse_info(routing_info(tablet_table.get_item(Key={'p': 'header-syntax', 'c': 0})))


@pytest.mark.parametrize('valid_secret', [False, True])
def test_tablet_routing_auth_errors(dynamodb, tablet_table, cql, valid_secret):
    with new_role(cql) as (role, key):
        with new_dynamodb(dynamodb, role, key if valid_secret else 'wrong-secret') as client:
            with routing_headers(client):
                error_type = 'AccessDeniedException' if valid_secret else 'UnrecognizedClientException'
                with pytest.raises(ClientError, match=error_type) as error:
                    client.Table(tablet_table.name).get_item(Key={'p': 'auth-errors', 'c': 0})
                assert routing_info(error.value.response) is None


@pytest.mark.parametrize('header', [VERSION_HEADER, SHORT_VERSION_HEADER])
def test_tablet_routing_header_alias(dynamodb, tablet_table, header):
    key = {'p': 'header-alias', 'c': 0}
    with routing_headers(dynamodb, None, **{header: '2:-'}):
        info = routing_info(tablet_table.get_item(Key=key))
    _, _, version, _ = parse_info(info)
    for index in range(16):
        with routing_headers(dynamodb, None, **{header: version_block(version, index)}):
            assert routing_info(tablet_table.get_item(Key=key)) is None
        with routing_headers(dynamodb, None, **{header: version_block(version, index, matching=False)}):
            assert routing_info(tablet_table.get_item(Key=key)) == info


@pytest.mark.parametrize('value,alias', [('2:-', ' 02:- '), ('2:dc', '2:DC')])
def test_tablet_routing_agreeing_header_aliases(dynamodb, tablet_table, value, alias):
    key = {'p': 'agreeing-aliases', 'c': 0}
    with routing_headers(dynamodb, value):
        expected = routing_info(tablet_table.get_item(Key=key))
    with routing_headers(dynamodb, value, **{SHORT_VERSION_HEADER.lower(): alias}):
        assert routing_info(tablet_table.get_item(Key=key)) == expected


@pytest.mark.parametrize('value,alias', [('2:-', '2:00'), ('2:00', '2:01'), ('', '2:-'), ('2:-', ''), ('3:any', '2:-')])
def test_tablet_routing_invalid_header_aliases_prevent_write(dynamodb, tablet_table, value, alias):
    key = {'p': 'invalid-aliases', 'c': 0}
    tablet_table.delete_item(Key=key)
    with routing_headers(dynamodb, value, **{SHORT_VERSION_HEADER: alias}):
        with pytest.raises(ClientError, match='ValidationException') as error:
            tablet_table.put_item(Item=key)
        assert routing_info(error.value.response) is None
    assert 'Item' not in tablet_table.get_item(Key=key, ConsistentRead=True)
