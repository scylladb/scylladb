# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tablet hints across coordinators, CAS shards and tablet lifecycle changes."""

import asyncio
import http.client
import json
import time

import aiohttp
import pytest

from test.alternator.test_tablet_routing import (
    INFO_HEADER, VERSION_HEADER, parse_info, routing_headers, routing_info, version_block,
)
from test.alternator.util import full_scan
from test.cluster.test_alternator import alternator_config, get_alternator, unique_table_name
from test.cluster.util import reconnect_driver
from test.cqlpy import cql_frames
from test.cqlpy.strong_consistency.test_tablets_routing_v2 import EXTENSION, RoutingInformation
from test.pylib.rest_client import inject_error, read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for


def create_table(client, tablets='2'):
    return client.create_table(TableName=unique_table_name(), BillingMode='PAY_PER_REQUEST',
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
        Tags=[{'Key': 'system:initial_tablets', 'Value': tablets}])


def read_hint(client, table, key='routing-key', block='2:-'):
    with routing_headers(client, block):
        return routing_info(client.Table(table).get_item(Key={'p': key}, ConsistentRead=True))


def cql_hint(address, statement_id, key, version):
    """Use raw EXECUTE to control the block on a tablet-backed table."""
    with cql_frames.connection(address, 9042, {EXTENSION: ''}) as sock:
        sock.settimeout(30)
        body = cql_frames.encode_short_bytes(statement_id)
        body += cql_frames.encode_short(0x0004)  # QUORUM
        body += cql_frames.encode_byte(0x01)   # VALUES
        body += cql_frames.encode_short(1)
        body += cql_frames.encode_bytes(key.encode())
        body += cql_frames.encode_byte(int(version_block(version, 0, matching=False)[2:], 16))
        response = cql_frames.request(sock, cql_frames.EXECUTE, body)
        assert response.opcode == cql_frames.RESULT
        info = RoutingInformation(response.custom_payload['tablets-routing-v2'])
        return (info.first_token, info.last_token, info.version & ((1 << 64) - 1),
                [(str(host), shard) for host, shard in info.replicas])


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_alternator_routing_allocation_failure(manager: ScyllaClusterManager):
    """Advisory allocation failures preserve successful data results."""
    from test.cluster.test_alternator_tablet_routing_sc import request
    server = await manager.server_add(config={**alternator_config, 'alternator_enforce_authorization': False},
                                      cmdline=['--smp', '1'])
    client = get_alternator(server.ip_addr)
    table = await asyncio.to_thread(create_table, client, '1')
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            for operation, arguments in [
                ('PutItem', {'Item': {'p': {'S': 'present'}}}),
                ('UpdateItem', {'Key': {'p': {'S': 'present'}}, 'UpdateExpression': 'SET a = :a',
                                'ExpressionAttributeValues': {':a': {'S': 'value'}}}),
                ('GetItem', None), ('Query', None), ('DeleteItem', {'Key': {'p': {'S': 'present'}}}),
            ]:
                expected, header = await request(session, server, table.name, operation, arguments=arguments)
                assert header is not None
                async with inject_error(manager.api, server.ip_addr, 'alternator_tablet_routing_info_bad_alloc'):
                    assert await request(session, server, table.name, operation, arguments=arguments) == (expected, None)
                    assert await manager.api.get_injection_enter_count(server.ip_addr, 'alternator_tablet_routing_info_bad_alloc') == 1
    finally:
        await asyncio.to_thread(table.delete)
        client.meta.client.close()


async def test_alternator_routing_coordinators_and_migration(manager: ScyllaClusterManager):
    """RF=1: both the replica and non-replica return shared V2 info, before/after a move."""
    config = {**alternator_config, 'experimental_features': ['strongly-consistent-tables']}
    servers = await manager.servers_add(2, config=config, cmdline=['--smp', '1'])
    await manager.disable_tablet_balancing()
    clients = [get_alternator(s.ip_addr) for s in servers]
    table = await asyncio.to_thread(create_table, clients[0], '1')
    ks = f'alternator_{table.name}'
    try:
        info = await asyncio.to_thread(read_hint, clients[0], table.name)
        old = parse_info(info)
        assert len(old[3]) == 1
        assert await asyncio.to_thread(read_hint, clients[1], table.name) == info
        cql = manager.get_cql()
        select = cql.prepare(f'SELECT p FROM "{ks}"."{table.name}" WHERE p = ?')
        assert await asyncio.to_thread(cql_hint, servers[0].ip_addr, select.query_id, 'routing-key', old[2]) == old

        # Every hinted UUID is discoverable through authenticated Alternator APIs,
        # including the UUID of the non-contacted node in a two-node cluster.
        identities = {}
        for name in ['local', 'peers']:
            rows = await asyncio.to_thread(full_scan, clients[0].Table(f'.scylla.alternator.system.{name}'), Limit=1)
            identities.update({row['host_id']: row for row in rows})
        hosts = [str(await manager.get_host_id(s.server_id)) for s in servers]
        assert set(identities) == set(hosts)
        for host_id in hosts:
            assert {'rpc_address', 'data_center', 'rack'} <= identities[host_id].keys()

        source_host, source_shard = old[3][0]
        destination_host = next(host for host in hosts if host != source_host)
        await asyncio.wait_for(manager.api.move_tablet(servers[0].ip_addr, ks, table.name,
            source_host, source_shard, destination_host, 0, old[1]), 60)
        for server in servers:
            await read_barrier(manager.api, server.ip_addr)
        new_info = await asyncio.to_thread(read_hint, clients[0], table.name)
        new = parse_info(new_info)
        assert new[3] == [(destination_host, 0)]
        assert new[3][0][0] in identities
        differing = next(i for i in range(16) if ((old[2] ^ new[2]) >> (4 * i)) & 15)
        for client in clients:
            assert await asyncio.to_thread(read_hint, client, table.name, 'routing-key', version_block(old[2], differing)) == new_info
            assert await asyncio.to_thread(read_hint, client, table.name, 'routing-key', version_block(new[2], differing)) is None
        assert await asyncio.to_thread(cql_hint, servers[1].ip_addr, select.query_id, 'routing-key', new[2]) == new
    finally:
        await asyncio.to_thread(table.delete)
        for client in clients:
            client.meta.client.close()


async def test_alternator_routing_cas_bounce(manager: ScyllaClusterManager):
    """One persistent HTTP connection writes keys whose CAS owners are on both shards."""
    server = await manager.server_add(config=alternator_config, cmdline=['--smp', '2'])
    await manager.disable_tablet_balancing()
    client = get_alternator(server.ip_addr)
    table = await asyncio.to_thread(create_table, client)
    ks = f'alternator_{table.name}'
    try:
        tablets = sorted(await get_all_tablet_replicas(manager, server, ks, table.name))
        assert len(tablets) == 2
        for target_shard, tablet in enumerate(tablets):
            host, shard = tablet.replicas[0]
            if shard != target_shard:
                await asyncio.wait_for(
                    manager.api.move_tablet(server.ip_addr, ks, table.name, host, shard, host, target_shard, tablet.last_token), 60)
        await read_barrier(manager.api, server.ip_addr)
        # Find deterministic keys for each tablet using the server's actual key encoding.
        keys = {}
        for i in range(128):
            key = f'bounce-{i}'
            hint = await asyncio.to_thread(read_hint, client, table.name, key)
            shard = parse_info(hint)[3][0][1]
            keys.setdefault(shard, (key, hint))
            if len(keys) == 2:
                break
        assert set(keys) == {0, 1}
        before = (await manager.metrics.query(server.ip_addr)).get('scylla_alternator_shard_bounce_for_lwt') or 0

        def write_on_one_connection():
            conn = http.client.HTTPConnection(server.ip_addr, 8000, timeout=60)
            try:
                socket = None
                for key, expected in keys.values():
                    item = {'p': {'S': key}}
                    version = parse_info(expected)[2]
                    for operation, args in [
                        ('PutItem', {'Item': item, 'ConditionExpression': 'attribute_not_exists(v)'}),
                        ('UpdateItem', {'Key': item, 'UpdateExpression': 'SET v = :v', 'ExpressionAttributeValues': {':v': {'N': '1'}},
                                        'ConditionExpression': 'attribute_exists(p)'}),
                        ('DeleteItem', {'Key': item, 'ConditionExpression': 'attribute_not_exists(missing)'}),
                    ]:
                        for value in ['2:-', version_block(version, 13), version_block(version, 13, matching=False)]:
                            conn.request('POST', '/', json.dumps({'TableName': table.name, **args}),
                                {'X-Amz-Target': f'DynamoDB_20120810.{operation}', VERSION_HEADER: value,
                                 'Content-Type': 'application/x-amz-json-1.0'})
                            if socket is None:
                                socket = conn.sock
                            assert conn.sock is socket
                            response = conn.getresponse()
                            body = response.read()
                            assert response.status == 200, body
                            assert response.getheader(INFO_HEADER) == (None if value == version_block(version, 13) else expected)
                            # A separate request on the very same socket must not inherit intent.
                            conn.request('POST', '/', json.dumps({'TableName': table.name, 'Key': item}),
                                {'X-Amz-Target': 'DynamoDB_20120810.GetItem', 'Content-Type': 'application/x-amz-json-1.0'})
                            assert conn.sock is socket
                            response = conn.getresponse()
                            body = response.read()
                            assert response.status == 200, body
                            assert response.getheader(INFO_HEADER) is None
            finally:
                conn.close()
        await asyncio.to_thread(write_on_one_connection)
        after = (await manager.metrics.query(server.ip_addr)).get('scylla_alternator_shard_bounce_for_lwt') or 0
        assert after > before
    finally:
        await asyncio.to_thread(table.delete)
        client.meta.client.close()


async def test_alternator_routing_resize(manager: ScyllaClusterManager):
    """Forced refresh reports new bounds even when replica identity and hash are unchanged."""
    server = await manager.server_add(config={**alternator_config, 'tablet_load_stats_refresh_interval_in_seconds': 1}, cmdline=['--smp', '1'])
    client = get_alternator(server.ip_addr)
    table = await asyncio.to_thread(create_table, client, '1')
    ks = f'alternator_{table.name}'
    try:
        original = parse_info(await asyncio.to_thread(read_hint, client, table.name))
        for count in [2, 1]:
            await manager.get_cql().run_async(f'ALTER TABLE "{ks}"."{table.name}" WITH tablets = '
                f"{{'min_tablet_count': {count}, 'max_tablet_count': {count}}}")
            async def resized():
                tablets = await get_all_tablet_replicas(manager, server, ks, table.name)
                return tablets if len(tablets) == count else None
            tablets = sorted(await wait_for(resized, time.time() + 60, period=0.1))
            current = parse_info(await asyncio.to_thread(read_hint, client, table.name))
            assert current[2:] == original[2:]
            if count == 2:
                assert current[:2] != original[:2]
            else:
                assert current[:2] == original[:2]
            assert current[1] in [tablet.last_token for tablet in tablets]
    finally:
        await asyncio.to_thread(table.delete)
        client.meta.client.close()


async def test_alternator_routing_multiple_dcs(manager: ScyllaClusterManager):
    servers = [await manager.server_add(config={**alternator_config, 'rf_rack_valid_keyspaces': False},
        cmdline=['--smp', '1'], property_file={'dc': dc, 'rack': 'r1'}) for dc in ['dc1', 'dc2']]
    await manager.disable_tablet_balancing()
    client = get_alternator(servers[0].ip_addr)
    table = await asyncio.to_thread(create_table, client, '1')
    ks = f'alternator_{table.name}'
    try:
        await manager.get_cql().run_async(f'ALTER KEYSPACE "{ks}" WITH replication = '
            "{'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}")
        for server in servers:
            await read_barrier(manager.api, server.ip_addr)
        info = parse_info(await asyncio.to_thread(read_hint, client, table.name))
        tablets = await get_all_tablet_replicas(manager, servers[0], ks, table.name)
        assert len(info[3]) == 2
        assert info[3] == sorted((str(host), shard) for host, shard in tablets[0].replicas)
        assert {host for host, _ in info[3]} == {str(await manager.get_host_id(server.server_id)) for server in servers}
    finally:
        await asyncio.to_thread(table.delete)
        client.meta.client.close()


async def test_alternator_routing_migration_publication(manager: ScyllaClusterManager):
    """The table-level publication guard applies to forced and conditional HTTP refreshes."""
    server = await manager.server_add(config=alternator_config, cmdline=['--smp', '1'])
    # Keep finalized bounds stable while comparing forced and conditional hints.
    await manager.disable_tablet_balancing()
    client = get_alternator(server.ip_addr)
    table = await asyncio.to_thread(create_table, client, 'none')
    name = table.name
    ks = f'alternator_{name}'
    try:
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)
        await manager.api.upgrade_node_to_tablets(server.ip_addr)
        await manager.server_restart(server.server_id)
        await reconnect_driver(manager)
        await manager.get_ready_cql([server])
        client.meta.client.close()
        client = get_alternator(server.ip_addr)
        for value in ['2:-', '2:00', '2:01']:
            assert await asyncio.to_thread(read_hint, client, name, 'routing-key', value) is None
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)
        await read_barrier(manager.api, server.ip_addr)
        info = await asyncio.to_thread(read_hint, client, name)
        _, _, version, _ = parse_info(info)
        assert await asyncio.to_thread(read_hint, client, name, 'routing-key', version_block(version, 0, matching=False)) == info
    finally:
        await asyncio.to_thread(client.Table(name).delete)
        client.meta.client.close()


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_alternator_routing_after_table_drop(manager: ScyllaClusterManager):
    """A materialized Query remains successful when its schema's table link expires."""
    server = await manager.server_add(config=alternator_config, cmdline=['--smp', '1'])
    client = get_alternator(server.ip_addr)
    table = await asyncio.to_thread(create_table, client, '1')
    dropped = False
    pending = None
    try:
        await asyncio.to_thread(table.put_item, Item={'p': 'drop-after-read'})
        assert await asyncio.to_thread(read_hint, client, table.name, 'drop-after-read')
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async def query():
                async with session.post(f'http://{server.ip_addr}:8000/', json={
                        'TableName': table.name,
                        'KeyConditionExpression': 'p = :p',
                        'ExpressionAttributeValues': {':p': {'S': 'drop-after-read'}},
                        'ConsistentRead': True,
                    }, headers={
                        'X-Amz-Target': 'DynamoDB_20120810.Query',
                        'Content-Type': 'application/x-amz-json-1.0',
                        VERSION_HEADER: '2:-',
                    }) as response:
                    body = await response.json(content_type=None)
                    assert response.status == 200, body
                    assert INFO_HEADER not in response.headers
                    return body

            async with inject_error(manager.api, server.ip_addr, 'alternator_query_after_read') as injection:
                pending = asyncio.create_task(query())
                try:
                    await log.wait_for('alternator_query_after_read: waiting for message', from_mark=mark, timeout=30)
                    async with session.post(f'http://{server.ip_addr}:8000/', json={'TableName': table.name}, headers={
                            'X-Amz-Target': 'DynamoDB_20120810.DeleteTable',
                            'Content-Type': 'application/x-amz-json-1.0',
                        }, timeout=aiohttp.ClientTimeout(total=20)) as response:
                        body = await response.json(content_type=None)
                        assert response.status == 200, body
                        dropped = True
                finally:
                    await injection.message()
            result = await asyncio.wait_for(pending, timeout=30)
            assert result['Items'] == [{'p': {'S': 'drop-after-read'}}]
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        if not dropped:
            await asyncio.to_thread(table.delete)
        client.meta.client.close()
