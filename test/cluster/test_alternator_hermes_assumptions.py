#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Tests of the Alternator behaviour that Hermes assumes, for the cases that
# need a cluster: here, an upgrade. The header of
# test/alternator/test_hermes_assumptions.py explains what Hermes is, what
# these tests pin and what to do when one of them fails.

import time

import pytest

from test.cluster.test_alternator import alternator_config, get_alternator, full_query, unique_table_name
from test.cluster.util import reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts


@pytest.mark.skip_mode(mode='release', reason='dev mode is enough for this test')
@pytest.mark.skip_mode(mode='debug', reason='dev mode is enough for this test')
async def test_alternator_upgrade_index_key_column(manager: ScyllaClusterManager, scylla_2025_1: ScyllaVersionDescription, scylla_binary: str):
    """Before 2025.4, the key attribute of an LSI that is not a key of the
       base table had its own regular column in the base table, not a member
       in ":attrs". Before 2025.1, the same was true for a GSI. On 2025.1, a
       GSI also reads its key from such a column when the attribute is the
       key of an LSI too. The table keeps this column after an upgrade, and
       PutItem continues to write the attribute into it, because the views use
       this column as their key. Hermes assumes, in A14: "On tables created
       before 2025.1 (GSIs) or 2025.4 (LSIs), an index key attribute has its
       own regular column, written as a plain CQL value and not into
       `:attrs`, and keeps it after an upgrade; `PutItem` still writes it
       there." and "Hermes binds such a column, and binds null when the item
       lacks the attribute". This test creates such a table, with an LSI and
       a GSI on the same attribute, on 2025.1, upgrades the node, and checks
       that:
        - the table keeps the column,
        - PutItem writes the attribute into the column, not into ":attrs",
        - a CQL write of the column, as Hermes does it, reads back as the
          attribute, and a CQL write of null removes the attribute,
        - the LSI and the GSI find the items that have the attribute.
    """
    server = (await manager.servers_add(1, config=alternator_config, version=scylla_2025_1))[0]
    table = get_alternator(server.ip_addr).create_table(TableName=unique_table_name(),
        BillingMode='PAY_PER_REQUEST',
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'},
                   {'AttributeName': 'c', 'KeyType': 'RANGE'}],
        AttributeDefinitions=[{'AttributeName': a, 'AttributeType': 'S'} for a in 'pcl'],
        LocalSecondaryIndexes=[
            {'IndexName': 'lsi',
             'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                           {'AttributeName': 'l', 'KeyType': 'RANGE'}],
             'Projection': {'ProjectionType': 'ALL'}}],
        GlobalSecondaryIndexes=[
            {'IndexName': 'gsi',
             'KeySchema': [{'AttributeName': 'l', 'KeyType': 'HASH'}],
             'Projection': {'ProjectionType': 'ALL'}}])
    table_name = table.name
    cql_table = f'"alternator_{table_name}"."{table_name}"'
    def columns(cql):
        rows = cql.execute("SELECT column_name, kind, type FROM system_schema.columns "
                           "WHERE keyspace_name = %s AND table_name = %s",
                           [f'alternator_{table_name}', table_name])
        return {(r.column_name, r.kind, r.type) for r in rows}
    old_columns = {('p', 'partition_key', 'text'), ('c', 'clustering', 'text'),
                   ('l', 'regular', 'text'), (':attrs', 'regular', 'map<text, blob>')}
    assert columns(manager.get_cql()) == old_columns

    await manager.server_change_version(server.server_id, scylla_binary)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
    assert columns(cql) == old_columns

    table = get_alternator(server.ip_addr).Table(table_name)
    item1 = {'p': 'p', 'c': 'c1', 'l': 'l1', 'v': 'v1'}
    table.put_item(Item=item1)
    row = cql.execute(f'SELECT l, ":attrs" FROM {cql_table} WHERE p = %s AND c = %s', ['p', 'c1']).one()
    assert row.l == 'l1'
    assert set(row[1]) == {'v'}

    insert = f'INSERT INTO {cql_table} (p, c, ":attrs", l) VALUES (%s, %s, %s, %s) USING TIMESTAMP %s'
    ts = int(time.time() * 1_000_000) - 60_000_000
    item2 = {'p': 'p', 'c': 'c2', 'l': 'l2', 'v': 'v2'}
    cql.execute(insert, ['p', 'c2', {'v': b'\x00' + b'v2'}, 'l2', ts])
    assert table.get_item(Key={'p': 'p', 'c': 'c2'}, ConsistentRead=True)['Item'] == item2
    item3 = {'p': 'p', 'c': 'c3', 'v': 'v3'}
    cql.execute(insert, ['p', 'c3', {'v': b'\x00' + b'v3'}, 'l3', ts])
    cql.execute(insert, ['p', 'c3', {'v': b'\x00' + b'v3'}, None, ts + 1])
    assert table.get_item(Key={'p': 'p', 'c': 'c3'}, ConsistentRead=True)['Item'] == item3

    # The GSI is eventually consistent: wait until it has the expected items.
    async def gsi_has(l, expected):
        async def check():
            return full_query(table, IndexName='gsi', ConsistentRead=False, KeyConditionExpression='l = :l',
                              ExpressionAttributeValues={':l': l}) == expected or None
        assert await wait_for(check, time.time() + 60)
    for item in (item1, item2):
        assert full_query(table, IndexName='lsi', KeyConditionExpression='p = :p AND l = :l',
                          ExpressionAttributeValues={':p': 'p', ':l': item['l']}) == [item]
        await gsi_has(item['l'], [item])
    assert full_query(table, IndexName='lsi', KeyConditionExpression='p = :p AND l = :l',
                      ExpressionAttributeValues={':p': 'p', ':l': 'l3'}) == []
    await gsi_has('l3', [])
    table.delete()
