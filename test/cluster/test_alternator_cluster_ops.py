#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Alternator tables on a multi-node cluster: through node restarts, node
# failures, topology changes and upgrades, in several DCs, and with nodetool.

import asyncio
import glob
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from test.cluster.test_alternator import alternator_config, get_alternator, unique_table_name
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

RESOURCES = Path(__file__).parent.parent / 'resource'


def create_table(ip, **schema):
    if 'KeySchema' not in schema:
        schema |= dict(KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                       AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}])
    table = get_alternator(ip).create_table(TableName=unique_table_name(), BillingMode='PAY_PER_REQUEST', **schema)
    table.wait_until_exists()
    return table


def scan(table, **kwargs):
    response = table.scan(ConsistentRead=True, **kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ConsistentRead=True, ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items += response['Items']
    return items


def put_items(table, items):
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


# sstables of an Alternator table written by Scylla 4.0.rc1, loaded with a
# refresh, are read with all their items: the old on-disk encoding of items
# is still understood.
async def test_load_old_alternator_sstables(manager: ScyllaClusterManager):
    server = await manager.server_add(config=alternator_config)
    table = create_table(server.ip_addr, KeySchema=[{'AttributeName': 'hash_key', 'KeyType': 'HASH'}],
                         AttributeDefinitions=[{'AttributeName': 'hash_key', 'AttributeType': 'S'}])
    workdir = await manager.server_get_workdir(server.server_id)
    ks = f'alternator_{table.name}'
    [table_dir] = glob.glob(os.path.join(workdir, 'data', ks, f'{table.name}-*'))
    src = RESOURCES / 'sstables' / 'alternator_4.0.rc1'
    for f in os.listdir(src):
        shutil.copy2(src / f, os.path.join(table_dir, 'upload', f))
    await manager.api.load_new_sstables(server.ip_addr, ks, table.name)
    expected = [{'hash_key': f'test{i}', 'x': {'hello': f'world{i}'}} for i in range(100)]
    assert sorted(scan(table), key=lambda i: i['hash_key']) == sorted(expected, key=lambda i: i['hash_key'])
    table.delete()


# With two of the three replicas down, a ConsistentRead fails with
# Alternator's error for an unavailable LOCAL_QUORUM, while an eventually
# consistent read still works.
async def test_consistent_read_with_replicas_down(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=alternator_config, auto_rack_dc='dc1')
    table = create_table(servers[0].ip_addr)
    table.put_item(Item={'p': 'key', 'v': 'value'})
    for s in servers[1:]:
        await manager.server_stop_gracefully(s.server_id)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[1].ip_addr)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[2].ip_addr)
    with pytest.raises(ClientError, match='Cannot achieve consistency level for cl LOCAL_QUORUM'):
        table.get_item(Key={'p': 'key'}, ConsistentRead=True)
    assert table.get_item(Key={'p': 'key'}, ConsistentRead=False)['Item'] == {'p': 'key', 'v': 'value'}
    for s in servers[1:]:
        await manager.server_start(s.server_id)
    await wait_for_cql_and_get_hosts(manager.get_cql(), servers, time.time() + 60)
    table.delete()


# Items written through a node of one DC can be read through a node of the
# other DC: the table's keyspace is replicated to every DC.
async def test_alternator_multi_dc(manager: ScyllaClusterManager):
    dc1 = await manager.server_add(config=alternator_config, property_file={'dc': 'dc1', 'rack': 'r1'})
    dc2 = await manager.server_add(config=alternator_config, property_file={'dc': 'dc2', 'rack': 'r1'})
    table = create_table(dc1.ip_addr)
    put_items(table, [{'p': f'k{i}', 'v': i} for i in range(10)])
    [row] = await manager.get_cql().run_async(f"SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'alternator_{table.name}'")
    assert {'dc1', 'dc2'} <= set(row.replication)
    table2 = get_alternator(dc2.ip_addr).Table(table.name)

    def all_items_in_dc2():
        items = table2.scan(ConsistentRead=False)['Items']
        return len(items) == 10 or None
    await wait_for(lambda: asyncio.to_thread(all_items_in_dc2), time.time() + 60)
    table.delete()


# A tag set on a table through one node is seen through another node.
async def test_table_tag_seen_through_other_node(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config=alternator_config, auto_rack_dc='dc1')
    table = create_table(servers[0].ip_addr)
    client1 = get_alternator(servers[0].ip_addr).meta.client
    client2 = get_alternator(servers[1].ip_addr).meta.client
    arn = client1.describe_table(TableName=table.name)['Table']['TableArn']
    client1.tag_resource(ResourceArn=arn, Tags=[{'Key': 'system:write_isolation', 'Value': 'always_use_lwt'}])

    async def tag_seen():
        tags = client2.list_tags_of_resource(ResourceArn=arn)['Tags']
        return {'Key': 'system:write_isolation', 'Value': 'always_use_lwt'} in tags or None
    await wait_for(tag_seen, time.time() + 60)
    table.delete()


# nodetool tablestats (of everything, of the keyspace, of the table) and
# info work and show the table when the table has a GSI, which is
# implemented by a materialized view.
async def test_nodetool_with_alternator_gsi(manager: ScyllaClusterManager):
    server = await manager.server_add(config=alternator_config)
    table = create_table(server.ip_addr,
                         KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                         AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}, {'AttributeName': 'g', 'AttributeType': 'S'}],
                         GlobalSecondaryIndexes=[{'IndexName': 'gsi', 'KeySchema': [{'AttributeName': 'g', 'KeyType': 'HASH'}],
                                                  'Projection': {'ProjectionType': 'ALL'}}])
    table.put_item(Item={'p': 'key', 'g': 'gsi_key'})
    exe = await manager.server_get_exe(server.server_id)
    ks = f'alternator_{table.name}'
    for args in [["tablestats"], ["tablestats", ks], ["tablestats", f"{ks}.{table.name}"]]:
        result = subprocess.run([exe, "nodetool", *args, "-h", server.ip_addr], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert table.name in result.stdout
    result = subprocess.run([exe, "nodetool", "info", "-h", server.ip_addr], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    table.delete()


# PutItems with LWT write isolation into the same partition through two
# coordinators concurrently keep all the attributes of each item.
# Reproduces https://github.com/scylladb/scylladb/issues/7218
async def test_putitem_contention(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config=alternator_config, auto_rack_dc='dc1')
    table = create_table(servers[0].ip_addr,
                         KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}, {'AttributeName': 'c', 'KeyType': 'RANGE'}],
                         AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}, {'AttributeName': 'c', 'AttributeType': 'N'}],
                         Tags=[{'Key': 'system:write_isolation', 'Value': 'always_use_lwt'}])
    n = 200

    def writer(server, start):
        t = get_alternator(server.ip_addr).Table(table.name)
        for i in range(start, start + n):
            t.put_item(Item={'p': 'p', 'c': i, 'v1': 'a', 'v2': 'b'})
    threads = [threading.Thread(target=writer, args=(s, i * n)) for i, s in enumerate(servers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    items = scan(table)
    assert len(items) == 2 * n
    assert all(item['v1'] == 'a' and item['v2'] == 'b' for item in items)
    table.delete()


# Items with an expired TTL are all deleted by the TTL scanners while a node
# is decommissioned.
async def test_ttl_expiration_during_decommission(manager: ScyllaClusterManager):
    servers = await manager.servers_add(4, config=alternator_config, property_file={'dc': 'dc1', 'rack': 'r1'})
    table = create_table(servers[0].ip_addr, Tags=[{'Key': 'system:initial_tablets', 'Value': 'none'}])
    get_alternator(servers[0].ip_addr).meta.client.update_time_to_live(
        TableName=table.name, TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})
    put_items(table, [{'p': f'k{i}', 'expiration': int(time.time()) + 2} for i in range(100)])
    await manager.decommission_node(servers[3].server_id)

    async def all_expired():
        return not scan(table) or None
    await wait_for(all_expired, time.time() + 120)
    table.delete()
