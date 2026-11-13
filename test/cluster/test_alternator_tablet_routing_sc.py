# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Global SC routing metadata over HTTP, without changing data execution."""

import asyncio
from contextlib import asynccontextmanager
import json
import time

import aiohttp
import pytest

from test.alternator.test_tablet_routing import INFO_HEADER, VERSION_HEADER, SHORT_VERSION_HEADER, parse_info, version_block
from test.cluster.test_alternator import alternator_config, unique_table_name
from test.cluster.test_alternator_tablet_routing import cql_hint
from test.cluster.test_strong_consistency import get_table_raft_group_id
from test.cluster.test_strong_consistency_replicas_unavailable import down
from test.cluster.util import ensure_raft_group_leader_on
from test.pylib.rest_client import HTTPError, inject_error, read_barrier
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts


CONFIG = {**alternator_config, 'experimental_features': ['strongly-consistent-tables'],
          'alternator_enforce_authorization': False}


async def request(session, server, name, operation='GetItem', key='present', consistent=True, block='2:-', arguments=None,
                  version_header=VERSION_HEADER):
    headers = {'Content-Type': 'application/x-amz-json-1.0', 'X-Amz-Target': f'DynamoDB_20120810.{operation}'}
    if block is not None:
        headers[version_header] = block
    body = {'TableName': name}
    if operation in ['GetItem', 'Query']:
        body['ConsistentRead'] = consistent
    if operation == 'GetItem':
        body['Key'] = {'p': {'S': key}}
    elif operation == 'Query':
        body.update(KeyConditionExpression='p = :p', ExpressionAttributeValues={':p': {'S': key}})
    else:
        assert operation in ['PutItem', 'UpdateItem', 'DeleteItem'] and arguments is not None
        body.update(arguments)
    async with session.post(f'http://{server.ip_addr}:8000/', data=json.dumps(body), headers=headers) as response:
        data = await response.json(content_type=None)
        assert response.status == 200, data
        return data, response.headers.get(INFO_HEADER)


async def current_hint(session, server, name, key='present', expected_replicas=None):
    async def check():
        body, header = await request(session, server, name, key=key)
        if header is None:
            return None
        info = parse_info(header, mode='strong')
        if expected_replicas is not None and info[3] != expected_replicas:
            return None
        return body, header
    return await wait_for(check, time.time() + 60, period=0.05)


async def known_leader(manager, server, group, expected=None):
    async def check():
        try:
            leader = await asyncio.wait_for(manager.api.get_raft_leader(server.ip_addr, group), 5)
        except HTTPError as error:
            if error.code == 400 and 'not found' in error.message:
                return None
            raise
        return leader if expected is None or str(leader) == str(expected) else None
    return await wait_for(check, time.time() + 60, period=0.05)


async def table_groups_ready(manager, servers, ks, name):
    table_id = await manager.get_table_id(ks, name)
    rows = await manager.get_cql().run_async(f'SELECT last_token, raft_group_id FROM system.tablets WHERE table_id = {table_id}')
    groups = {row.last_token: str(row.raft_group_id) for row in rows}
    tablets = await get_all_tablet_replicas(manager, servers[0], ks, name)
    by_host = {str(await manager.get_host_id(server.server_id)): server for server in servers}
    for tablet in tablets:
        group = groups[tablet.last_token]
        replicas = [by_host[str(host)] for host, _ in tablet.replicas]
        leader = await known_leader(manager, replicas[0], group)
        for server in replicas:
            await known_leader(manager, server, group, leader)
    return groups, tablets


@asynccontextmanager
async def sc_table(manager, servers, replication, tablets=1, populate=True):
    # Native CQL creates and populates the SC table through its supported Raft
    # path. HTTP assertions below concern routing hints, not SC data execution.
    name = unique_table_name()
    ks = f'alternator_{name}'
    cql = manager.get_cql()
    options = ', '.join(f"'{dc}': {rf}" for dc, rf in replication.items())
    await asyncio.wait_for(cql.run_async(f'CREATE KEYSPACE "{ks}" WITH replication = '
        f"{{'class': 'NetworkTopologyStrategy', {options}}} AND tablets = {{'initial': {tablets}}} AND consistency = 'global'"), 30)
    try:
        await asyncio.wait_for(cql.run_async(f'CREATE TABLE "{ks}"."{name}" (p text PRIMARY KEY, ":attrs" map<text, blob>)'), 30)
        await table_groups_ready(manager, servers, ks, name)
        if populate:
            insert = cql.prepare(f'INSERT INTO "{ks}"."{name}" (p) VALUES (?)')
            await asyncio.wait_for(cql.run_async(insert, ['present']), 30)
        groups, replicas = await table_groups_ready(manager, servers, ks, name)
        by_host = {str(await manager.get_host_id(server.server_id)): server for server in servers}
        for tablet in replicas:
            for host, _ in tablet.replicas:
                await read_barrier(manager.api, by_host[str(host)].ip_addr, groups[tablet.last_token], timeout=30)
        yield name, ks
    finally:
        await asyncio.wait_for(cql.run_async(f'DROP KEYSPACE IF EXISTS "{ks}"'), 30)


async def keys_for_replicas(manager, ks, name, tablets, wanted):
    """Find fixed candidate keys through the server's partition-key encoding."""
    cql = manager.get_cql()
    insert = cql.prepare(f'INSERT INTO "{ks}"."{name}" (p) VALUES (?)')
    token_query = cql.prepare(f'SELECT token(p) FROM "{ks}"."{name}" WHERE p = ?')
    async def find():
        keys = {}
        for i in range(128):
            key = f'shard-key-{i}'
            await cql.run_async(insert, [key])
            token = (await cql.run_async(token_query, [key]))[0][0]
            index = next(i for i, tablet in enumerate(tablets) if token <= tablet.last_token)
            tablet = tablets[index]
            host, shard = tablet.replicas[0]
            replica = (str(host), shard)
            if replica in wanted:
                start = tablets[index-1].last_token if index else -(1 << 63)
                keys.setdefault(replica, (key, token, (start, tablet.last_token)))
            if keys.keys() == wanted:
                return keys
        raise AssertionError(f'Candidate keys did not cover replicas {wanted}; found {keys.keys()}')
    return await asyncio.wait_for(find(), 60)


@pytest.mark.parametrize('consistent', [False, True])
async def test_sc_routing_reads_and_version_blocks(manager, consistent):
    server = await manager.server_add(config=CONFIG, cmdline=['--smp', '1'], property_file={'dc': 'dc1', 'rack': 'r1'})
    await manager.disable_tablet_balancing()
    async with sc_table(manager, [server], {'dc1': 1}) as (name, ks):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            for operation in ['GetItem', 'Query']:
                present, disabled = await request(session, server, name, operation, consistent=consistent, block=None)
                assert disabled is None
                expected = {'p': {'S': 'present'}}
                if operation == 'GetItem':
                    assert present['Item'] == expected
                else:
                    assert present['Items'] == [expected]
                _, header = await request(session, server, name, operation, consistent=consistent)
                info = parse_info(header, mode='strong')
                assert ';leader=' not in header
                for index in range(16):
                    _, matching = await request(session, server, name, operation, consistent=consistent, block=version_block(info[2], index))
                    assert matching is None
                    _, mismatching = await request(session, server, name, operation, consistent=consistent,
                        block=version_block(info[2], index, matching=False))
                    assert mismatching == header
                missing, missing_header = await request(session, server, name, operation, key='missing', consistent=consistent)
                if operation == 'GetItem':
                    assert 'Item' not in missing
                else:
                    assert missing['Items'] == []
                assert missing_header == header
                assert (await request(session, server, name, operation, consistent=consistent, block=None))[1] is None
            select = manager.get_cql().prepare(f'SELECT p FROM "{ks}"."{name}" WHERE p = ?')
            assert await asyncio.to_thread(cql_hint, server.ip_addr, select.query_id, 'present', info[2]) == info


async def test_sc_routing_existing_unconditional_write_paths(manager):
    """Hints enrich successful existing writes; this does not test Raft write semantics."""
    server = await manager.server_add(config=CONFIG, cmdline=['--smp', '1'], property_file={'dc': 'dc1', 'rack': 'r1'})
    await manager.disable_tablet_balancing()
    async with sc_table(manager, [server], {'dc1': 1}) as (name, ks):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            _, header = await request(session, server, name)
            version = parse_info(header, mode='strong')[2]
            key = {'p': {'S': 'write-metadata'}}
            for operation, arguments in [
                ('PutItem', {'Item': key}),
                ('UpdateItem', {'Key': key, 'UpdateExpression': 'SET v = :v', 'ExpressionAttributeValues': {':v': {'N': '1'}}}),
                ('DeleteItem', {'Key': key}),
            ]:
                for block in ['2:-', version_block(version, 11), version_block(version, 11, matching=False), None]:
                    _, actual = await request(session, server, name, operation, block=block, arguments=arguments)
                    assert actual == (header if block in ['2:-', version_block(version, 11, matching=False)] else None)


async def test_sc_routing_remote_shard(manager):
    servers = await manager.servers_add(2, config=CONFIG, cmdline=['--smp', '2'],
        property_file=[{'dc': 'dc1', 'rack': f'r{i}'} for i in range(2)])
    await manager.disable_tablet_balancing()
    async with sc_table(manager, servers, {'dc1': 1}, tablets=4) as (name, ks):
        tablets = sorted(await get_all_tablet_replicas(manager, servers[0], ks, name))
        shard_one = [tablet for tablet in tablets if tablet.replicas[0][1] == 1]
        assert shard_one, f'Initial placement has no shard1 replica: {tablets}'
        owner_id, shard = shard_one[0].replicas[0]
        replica = (str(owner_id), shard)
        ids = {str(await manager.get_host_id(server.server_id)): server for server in servers}
        owner = ids[replica[0]]
        keys = await keys_for_replicas(manager, ks, name, tablets, {replica})
        key, token, bounds = keys[replica]
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            expected = None
            for operation in ['GetItem', 'Query']:
                if operation == 'GetItem':
                    _, header = await current_hint(session, owner, name, key=key, expected_replicas=[replica])
                else:
                    _, header = await request(session, owner, name, operation, key=key)
                info = parse_info(header, mode='strong')
                assert info[:2] == bounds
                assert info[0] < token <= info[1]
                assert info[3] == [replica]
                if expected is None:
                    expected = header
                assert header == expected
                assert (await request(session, owner, name, operation, key=key, block=version_block(info[2], 7)))[1] is None
                assert (await request(session, owner, name, operation, key=key,
                    block=version_block(info[2], 7, matching=False)))[1] == header


async def test_sc_routing_nonreplica_uses_cached_leader(manager, build_mode):
    """Cached and replica-local hints omit a leader already known to be down."""
    config = CONFIG
    if build_mode != 'release':
        # Keep the old leader in the followers' Raft state after gossip marks
        # it down. Bootstrap picks the first leader without waiting for a tick.
        config = config | {'error_injections_at_startup': [
            {'name': 'strongly-consistent-raft-group-tick-interval-in-ms', 'value': '20000'}]}
    servers = await manager.servers_add(4, config=config, cmdline=['--smp', '1'],
        property_file=[{'dc': 'dc1', 'rack': f'r{i % 3}'} for i in range(4)])
    await manager.disable_tablet_balancing()
    async with sc_table(manager, servers, {'dc1': 3}, populate=False) as (name, ks):
        tablet, = await get_all_tablet_replicas(manager, servers[0], ks, name)
        ids = {str(await manager.get_host_id(server.server_id)): server for server in servers}
        owner = ids[str(tablet.replicas[0][0])]
        replicas = {ids[str(host)] for host, _ in tablet.replicas}
        contact = next(server for server in servers if server not in replicas)
        # A setup write through contact would cache the leader before the
        # first HTTP request. Populate only through the replica instead.
        cql = manager.get_cql()
        owner_host, = await wait_for_cql_and_get_hosts(cql, [owner], time.time() + 30)
        insert = cql.prepare(f'INSERT INTO "{ks}"."{name}" (p) VALUES (?)')
        await asyncio.wait_for(cql.run_async(insert, ['present'], host=owner_host), 30)
        group = await get_table_raft_group_id(manager, ks, name)
        await read_barrier(manager.api, owner.ip_addr, group, timeout=30)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            expected, header = await request(session, owner, name)
            info = parse_info(header, mode='strong')
            assert await request(session, contact, name) == (expected, None)
            # A CQL request through the non-replica is redirected to the leader,
            # which caches the leader on the shard that handled it.
            select = cql.prepare(f'SELECT p FROM "{ks}"."{name}" WHERE p = ?')
            assert await asyncio.to_thread(cql_hint, contact.ip_addr, select.query_id, 'present', info[2]) == info
            assert await request(session, contact, name) == (expected, header)
            assert (await request(session, contact, name, block=version_block(info[2], 7)))[1] is None
            assert (await request(session, contact, name,
                block=version_block(info[2], 7, matching=False)))[1] == header
            # Keep a read quorum after stopping the cached leader. An advisory
            # hint must not direct clients to a host already known to be down.
            if build_mode != 'release':
                # Let the restarted node rediscover its leader at the normal pace.
                await manager.server_update_config(ids[info[3][0][0]].server_id,
                    config_options={'error_injections_at_startup': []})
            async with down(manager, servers, [ids[info[3][0][0]]]):
                assert await request(session, contact, name) == (expected, None)
                if build_mode != 'release':
                    follower = next(server for server in replicas if server != ids[info[3][0][0]])
                    assert str(await manager.api.get_raft_leader(follower.ip_addr, group)) == info[3][0][0]
                    assert await request(session, follower, name, consistent=False) == (expected, None)
                    assert str(await manager.api.get_raft_leader(follower.ip_addr, group)) == info[3][0][0]


async def test_sc_routing_local_shard_forwarding(manager):
    server = await manager.server_add(config=CONFIG, cmdline=['--smp', '2'], property_file={'dc': 'dc1', 'rack': 'r1'})
    await manager.disable_tablet_balancing()
    async with sc_table(manager, [server], {'dc1': 1}, tablets=2) as (name, ks):
        tablets = sorted(await get_all_tablet_replicas(manager, server, ks, name))
        host = str(await manager.get_host_id(server.server_id))
        wanted = {(host, 0), (host, 1)}
        assert wanted <= {(str(replica_host), shard) for tablet in tablets for replica_host, shard in tablet.replicas}, tablets
        keys = await keys_for_replicas(manager, ks, name, tablets, wanted)
        # One TCP connection is pinned to one HTTP shard. Addressing data owned
        # on both shards makes at least one metadata lookup cross an SMP boundary.
        connections = 0
        trace = aiohttp.TraceConfig()
        async def created(session, context, params):
            nonlocal connections
            connections += 1
        trace.on_connection_create_end.append(created)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), connector=aiohttp.TCPConnector(limit=1),
                trace_configs=[trace]) as session:
            for replica, (key, token, bounds) in keys.items():
                _, header = await current_hint(session, server, name, key=key, expected_replicas=[replica])
                info = parse_info(header, mode='strong')
                assert info[:2] == bounds
                assert info[0] < token <= info[1]
                assert info[3] == [replica]
        assert connections == 1


async def test_sc_routing_leader_change_and_cql_parity(manager):
    servers = await manager.servers_add(3, config=CONFIG, cmdline=['--smp', '1'],
        property_file=[{'dc': 'dc1', 'rack': f'r{i}'} for i in range(3)])
    await manager.disable_tablet_balancing()
    async with sc_table(manager, servers, {'dc1': 3}) as (name, ks):
        group = await get_table_raft_group_id(manager, ks, name)
        by_host = {str(await manager.get_host_id(server.server_id)): server for server in servers}
        host_ids = sorted(by_host)
        # A middle-sorted leader distinguishes a rotation from merely moving
        # one entry to the front while leaving the remainder sorted.
        old_leader, new_leader, observer = [by_host[host] for host in [host_ids[1], host_ids[0], host_ids[2]]]
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            await ensure_raft_group_leader_on(manager, old_leader, group)
            for server in servers:
                await known_leader(manager, server, group, host_ids[1])
            _, old_header = await request(session, observer, name)
            old = parse_info(old_header, mode='strong')
            assert old[3] == [(host, 0) for host in host_ids[1:] + host_ids[:1]]
            await ensure_raft_group_leader_on(manager, new_leader, group)
            for server in servers:
                await known_leader(manager, server, group, host_ids[0])
            _, new_header = await request(session, observer, name)
            new = parse_info(new_header, mode='strong')
            assert new[:2] == old[:2]
            assert new[3] == [(host, 0) for host in host_ids]
            assert new[2] != old[2]
            index = next(i for i in range(16) if ((old[2] ^ new[2]) >> (4*i)) & 15)
            assert (await request(session, old_leader, name, block=version_block(old[2], index)))[1] == new_header
            assert (await request(session, old_leader, name, block=version_block(new[2], index)))[1] is None
            select = manager.get_cql().prepare(f'SELECT p FROM "{ks}"."{name}" WHERE p = ?')
            assert await asyncio.to_thread(cql_hint, observer.ip_addr, select.query_id, 'present', new[2]) == new


async def test_sc_routing_global_multidc_serving_set(manager):
    servers = [await manager.server_add(config=CONFIG, cmdline=['--smp', '1'], property_file={'dc': dc, 'rack': 'r1'})
               for dc in ['dc1', 'dc2']]
    await manager.disable_tablet_balancing()
    async with sc_table(manager, servers, {'dc1': 1, 'dc2': 1}) as (name, ks):
        group = await get_table_raft_group_id(manager, ks, name)
        host_ids = [str(await manager.get_host_id(server.server_id)) for server in servers]
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            leader = await known_leader(manager, servers[0], group)
            for server in servers:
                await known_leader(manager, server, group, leader)
            headers = [(await request(session, server, name))[1] for server in servers]
            assert headers[0] == headers[1]
            _, _, _, replicas = parse_info(headers[0], mode='strong')
            assert len(replicas) == 2
            assert {host for host, _ in replicas} == set(host_ids)
            assert replicas[0][0] == str(leader)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_routing_advisory_omission_and_deadline(manager, record_property):
    server = await manager.server_add(config=CONFIG, cmdline=['--smp', '1'], property_file={'dc': 'dc1', 'rack': 'r1'})
    await manager.disable_tablet_balancing()
    async with sc_table(manager, [server], {'dc1': 1}) as (name, ks):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            expected_body, header = await request(session, server, name)
            parse_info(header, mode='strong')
            healthy_latencies = []
            for _ in range(5):
                begin = time.monotonic()
                body, hint = await request(session, server, name)
                healthy_latencies.append(time.monotonic() - begin)
                assert body == expected_body and hint == header
            record_property('healthy_http_seconds', json.dumps(healthy_latencies))
            async with inject_error(manager.api, server.ip_addr, 'sc_tablet_routing_info_unavailable'):
                body, omitted = await request(session, server, name)
                assert body == expected_body
                assert omitted is None
            stalled_latencies = []
            for _ in range(5):
                async with inject_error(manager.api, server.ip_addr, 'sc_tablet_routing_info_admission_pause') as pause, \
                        inject_error(manager.api, server.ip_addr, 'sc_tablet_routing_info_completed'):
                    try:
                        begin = time.monotonic()
                        body, omitted = await request(session, server, name)
                        elapsed = time.monotonic() - begin
                        stalled_latencies.append(elapsed)
                        assert body == expected_body
                        assert omitted is None
                        # Advisory work must not consume the backend's full
                        # one-second deadline after the data operation succeeds.
                        assert elapsed < max(healthy_latencies) + 0.5, elapsed
                    finally:
                        await asyncio.wait_for(pause.message(), 5)
                        await manager.api.wait_for_injection_enter(server.ip_addr, 'sc_tablet_routing_info_completed')
            record_property('stalled_metadata_http_seconds', json.dumps(stalled_latencies))
            assert (await request(session, server, name))[1] == header


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_routing_overlaps_data_execution(manager):
    """Routing collection starts without waiting for the data operation to finish."""
    server = await manager.server_add(config=CONFIG, cmdline=['--smp', '1'], property_file={'dc': 'dc1', 'rack': 'r1'})
    await manager.disable_tablet_balancing()
    async with sc_table(manager, [server], {'dc1': 1}) as (name, ks):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            for operation in ['GetItem', 'Query', 'PutItem', 'UpdateItem', 'DeleteItem']:
                arguments = None
                if operation == 'PutItem':
                    arguments = {'Item': {'p': {'S': 'overlap'}}}
                elif operation in ['UpdateItem', 'DeleteItem']:
                    arguments = {'Key': {'p': {'S': 'overlap'}}}
                    if operation == 'UpdateItem':
                        arguments['UpdateExpression'] = 'SET a = :a'
                        arguments['ExpressionAttributeValues'] = {':a': {'S': 'value'}}
                is_write = operation not in ['GetItem', 'Query']
                # Local writes bypass the mutation RPC handler. Pause their
                # table application instead, without blocking unrelated tables.
                data_pause = ('table_apply_wait_for_memory' if is_write else
                              {'GetItem': 'alternator_get_item_before_read', 'Query': 'alternator_query_before_read'}[operation])
                data_parameters = {'table': name} if is_write else {}
                metadata_pause = 'sc_tablet_routing_info_admission_pause'
                # Hold the data operation, so it cannot complete while we check
                # that metadata collection has started.
                async with inject_error(manager.api, server.ip_addr, data_pause, parameters=data_parameters) as data:
                    async with inject_error(manager.api, server.ip_addr, metadata_pause) as metadata:
                        pending = asyncio.create_task(request(session, server, name, operation, arguments=arguments))
                        try:
                            # Both pauses must be released before the metadata
                            # injection's 30-second message timeout can abort the node.
                            deadline = time.time() + 5
                            await manager.api.wait_for_injection_enter(server.ip_addr, data_pause, deadline=deadline)
                            await manager.api.wait_for_injection_enter(server.ip_addr, metadata_pause,
                                                                      deadline=deadline)
                            assert not pending.done()
                            await metadata.message()
                            await data.message()
                            _, header = await pending
                            parse_info(header, mode='strong')
                        finally:
                            await metadata.message()
                            await data.message()
                            if not pending.done():
                                pending.cancel()
                            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_routing_wait_allocation_failure(manager):
    """Failure to allocate the advisory wait preserves a completed read."""
    servers = await manager.servers_add(2, config=CONFIG, cmdline=['--smp', '1'],
        property_file=[{'dc': 'dc1', 'rack': f'r{i}'} for i in range(2)])
    await manager.disable_tablet_balancing()
    async with sc_table(manager, servers, {'dc1': 1}) as (name, ks):
        tablet, = await get_all_tablet_replicas(manager, servers[0], ks, name)
        ids = {str(await manager.get_host_id(server.server_id)): server for server in servers}
        owner = ids[str(tablet.replicas[0][0])]
        # Only a replica collects metadata itself; stall that local lookup.
        contact = owner
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            expected, header = await request(session, contact, name)
            assert header is not None
            async with inject_error(manager.api, owner.ip_addr, 'sc_tablet_routing_info_admission_pause') as pause, \
                    inject_error(manager.api, contact.ip_addr, 'alternator_tablet_routing_wait_bad_alloc'), \
                    inject_error(manager.api, contact.ip_addr, 'sc_tablet_routing_info_completed'):
                try:
                    assert await request(session, contact, name) == (expected, None)
                    assert await manager.api.get_injection_enter_count(contact.ip_addr, 'alternator_tablet_routing_wait_bad_alloc') == 1
                finally:
                    await manager.api.wait_for_injection_enter(owner.ip_addr, 'sc_tablet_routing_info_admission_pause')
                    await pause.message()
                    await manager.api.wait_for_injection_enter(contact.ip_addr, 'sc_tablet_routing_info_completed')


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_routing_wait_budget(manager, record_property):
    """A live wait budget controls stalled hints while ready hints remain available."""
    servers = await manager.servers_add(2, config=CONFIG, cmdline=['--smp', '1'],
        property_file=[{'dc': 'dc1', 'rack': f'r{i}'} for i in range(2)])
    await manager.disable_tablet_balancing()
    async with sc_table(manager, servers, {'dc1': 1}) as (name, ks):
        tablet, = await get_all_tablet_replicas(manager, servers[0], ks, name)
        ids = {str(await manager.get_host_id(server.server_id)): server for server in servers}
        owner = ids[str(tablet.replicas[0][0])]
        # Only a replica collects metadata itself; stall that local lookup.
        contact = owner
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            begin = time.monotonic()
            expected, header = await request(session, contact, name)
            baseline = time.monotonic() - begin
            # The long setting also rejects an implementation hard-coded to
            # the 50 ms default, without tight upper bounds on short requests.
            for budget in [0, 5, 50, 250]:
                setting = 'alternator_tablet_routing_wait_timeout_in_ms'
                await manager.server_update_config(contact.server_id, setting, budget)
                async def configured():
                    return True if await manager.api.get_config(contact.ip_addr, setting) == budget else None
                await wait_for(configured, time.time() + 10, period=0.01)
                async with inject_error(manager.api, owner.ip_addr, 'sc_tablet_routing_info_admission_pause') as pause, \
                        inject_error(manager.api, contact.ip_addr, 'sc_tablet_routing_info_completed'):
                    try:
                        begin = time.monotonic()
                        body, hint = await request(session, contact, name)
                        elapsed = time.monotonic() - begin
                        assert body == expected and hint is None
                        if budget == 250:
                            # Allow cached timeout-clock granularity while
                            # rejecting a wait hard-coded to the 50 ms default.
                            assert elapsed >= 0.2, elapsed
                        # Stay below the backend's one-second deadline.
                        assert elapsed < baseline + budget / 1000 + 0.5, elapsed
                        record_property(f'wait_budget_{budget}_http_seconds', elapsed)
                    finally:
                        await manager.api.wait_for_injection_enter(owner.ip_addr, 'sc_tablet_routing_info_admission_pause')
                        await pause.message()
                        await manager.api.wait_for_injection_enter(contact.ip_addr, 'sc_tablet_routing_info_completed')
                # Holding the data operation lets metadata complete first, even
                # when the response is configured to spend no time waiting for it.
                async with inject_error(manager.api, contact.ip_addr, 'alternator_get_item_before_read') as data, \
                        inject_error(manager.api, contact.ip_addr, 'sc_tablet_routing_info_completed'):
                    pending = asyncio.create_task(request(session, contact, name))
                    try:
                        await manager.api.wait_for_injection_enter(contact.ip_addr, 'alternator_get_item_before_read')
                        await manager.api.wait_for_injection_enter(contact.ip_addr, 'sc_tablet_routing_info_completed')
                        await data.message()
                        assert await pending == (expected, header)
                    finally:
                        await data.message()
                        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_routing_admission_is_bounded(manager):
    """Busy metadata collection skips hints instead of retaining more completed Query results."""
    # Wait for the full backend deadline, so a probe queued for admission
    # would take about one second instead of returning immediately.
    config = {**CONFIG, 'alternator_tablet_routing_wait_timeout_in_ms': 1000}
    server = await manager.server_add(config=config, cmdline=['--smp', '1'], property_file={'dc': 'dc1', 'rack': 'r1'})
    await manager.disable_tablet_balancing()
    async with sc_table(manager, [server], {'dc1': 1}) as (name, ks):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), connector=aiohttp.TCPConnector(limit=128)) as session:
            begin = time.monotonic()
            expected, initial_header = await request(session, server, name, 'Query')
            baseline = time.monotonic() - begin
            parse_info(initial_header, mode='strong')
            injection = 'sc_tablet_routing_info_admission_pause'
            async with inject_error(manager.api, server.ip_addr, injection) as pause:
                requests = [asyncio.create_task(request(session, server, name, 'Query')) for _ in range(128)]
                try:
                    responses = await asyncio.wait_for(asyncio.gather(*requests), 10)
                    assert all(body == expected and header is None for body, header in responses)
                    # Timed-out HTTP callers must not release admission for work
                    # still parked in the backend. Only the bounded set entered.
                    admitted = await asyncio.wait_for(manager.api.get_injection_enter_count(server.ip_addr, injection), 5)
                    assert 0 < admitted <= 32, admitted
                    begin = time.monotonic()
                    body, header = await request(session, server, name, 'Query')
                    elapsed = time.monotonic() - begin
                    assert body == expected and header is None
                    # A queued probe would wait for the one-second backend deadline.
                    assert elapsed < baseline + 0.5, elapsed
                    assert await manager.api.get_injection_enter_count(server.ip_addr, injection) == admitted
                finally:
                    try:
                        await asyncio.wait_for(pause.message(), 5)
                    finally:
                        for pending in requests:
                            if not pending.done():
                                pending.cancel()
                        await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), 5)
            async def recovered():
                body, header = await request(session, server, name, 'Query')
                return (body, header) if header is not None else None
            body, header = await wait_for(recovered, time.time() + 10, period=0.05)
            assert body == expected
            assert header == initial_header


async def test_sc_routing_header_alias(manager):
    server = await manager.server_add(config=CONFIG, cmdline=['--smp', '1'], property_file={'dc': 'dc1', 'rack': 'r1'})
    await manager.disable_tablet_balancing()
    async with sc_table(manager, [server], {'dc1': 1}) as (name, ks):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            _, expected = await request(session, server, name)
            _, _, version, _ = parse_info(expected, mode='strong')
            for header in [VERSION_HEADER, SHORT_VERSION_HEADER.lower()]:
                for block in ['2:-', version_block(version, 0), version_block(version, 0, matching=False)]:
                    _, actual = await request(session, server, name, block=block, version_header=header)
                    assert actual == (None if block == version_block(version, 0) else expected)
