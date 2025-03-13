#!/usr/bin/env python3

import asyncio
import os
import pytest
import shutil
import logging
import json

from test.pylib.minio_server import MinioServer
from cassandra.protocol import ConfigurationException
from test.pylib.manager_client import ManagerClient
from test.cluster.util import reconnect_driver
from test.cluster.object_store.conftest import get_s3_resource, format_tuples
from test.cqlpy.rest_api import scylla_inject_error
from test.cluster.test_config import wait_for_config

logger = logging.getLogger(__name__)


def create_ks_and_cf(cql, s3_server):
    ks = 'test_ks'
    cf = 'test_cf'

    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                      'replication_factor': '1'})
    storage_opts = format_tuples(type='S3',
                                 endpoint=s3_server.address,
                                 bucket=s3_server.bucket_name)

    cql.execute((f"CREATE KEYSPACE {ks} WITH"
                  f" REPLICATION = {replication_opts} AND STORAGE = {storage_opts};"))
    cql.execute(f"CREATE TABLE {ks}.{cf} ( name text primary key, value text );")

    rows = [('0', 'zero'),
            ('1', 'one'),
            ('2', 'two')]
    for row in rows:
        cql_fmt = "INSERT INTO {}.{} ( name, value ) VALUES ('{}', '{}');"
        cql.execute(cql_fmt.format(ks, cf, *row))

    return ks, cf


@pytest.mark.asyncio
async def test_basic(manager: ManagerClient, s3_server):
    '''verify ownership table is updated, and tables written to S3 can be read after scylla restarts'''

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    workdir = await manager.server_get_workdir(server.server_id)
    print(f'Create keyspace (minio listening at {s3_server.address})')
    ks, cf = create_ks_and_cf(cql, s3_server)

    assert not os.path.exists(os.path.join(workdir, f'data/{ks}')), "S3-backed keyspace has local directory created"
    # Sanity check that the path is constructed correctly
    assert os.path.exists(os.path.join(workdir, 'data/system')), "Datadir is elsewhere"

    desc = cql.execute(f"DESCRIBE KEYSPACE {ks}").one().create_statement
    # The storage_opts wraps options with '{ <options> }' while the DESCRIBE
    # does it like '{<options>}' so strip the corner branches and spaces for check
    assert f"{{'type': 'S3', 'bucket': '{s3_server.bucket_name}', 'endpoint': '{s3_server.address}'}}" in desc, "DESCRIBE generates unexpected storage options"

    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    rows = {x.name: x.value for x in res}
    assert len(rows) > 0, 'Test table is empty'

    await manager.api.flush_keyspace(server.ip_addr, ks)

    # Check that the ownership table is populated properly
    res = cql.execute("SELECT * FROM system.sstables;")
    tid = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{cf}'").one()
    for row in res:
        assert row.owner == tid.id, \
            f'Unexpected entry owner in registry: {row.owner}'
        assert row.status == 'sealed', f'Unexpected entry status in registry: {row.status}'

    print('Restart scylla')
    await manager.server_restart(server.server_id)
    cql = await reconnect_driver(manager)

    # Shouldn't be recreated by populator code
    assert not os.path.exists(os.path.join(workdir, f'data/{ks}')), "S3-backed keyspace has local directory resurrected"

    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    have_res = {x.name: x.value for x in res}
    assert have_res == rows, f'Unexpected table content: {have_res}'

    print('Drop table')
    cql.execute(f"DROP TABLE {ks}.{cf};")
    # Check that the ownership table is de-populated
    res = cql.execute("SELECT * FROM system.sstables;")
    rows = "\n".join(f"{row.owner} {row.status}" for row in res)
    assert not rows, 'Unexpected entries in registry'


@pytest.mark.asyncio
async def test_garbage_collect(manager: ManagerClient, s3_server):
    '''verify ownership table is garbage-collected on boot'''

    sstable_entries = []

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()

    print(f'Create keyspace (minio listening at {s3_server.address})')
    ks, cf = create_ks_and_cf(cql, s3_server)

    await manager.api.flush_keyspace(server.ip_addr, ks)
    # Mark the sstables as "removing" to simulate the problem
    res = cql.execute("SELECT * FROM system.sstables;")
    for row in res:
        sstable_entries.append((row.owner, row.generation))
    print(f'Found entries: {[ str(ent[1]) for ent in sstable_entries ]}')
    for owner, gen in sstable_entries:
        cql.execute("UPDATE system.sstables SET status = 'removing'"
                     f" WHERE owner = {owner} AND generation = {gen};")

    print('Restart scylla')
    await manager.server_restart(server.server_id)
    cql = await reconnect_driver(manager)

    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    have_res = {x.name: x.value for x in res}
    # Must be empty as no sstables should have been picked up
    assert not have_res, f'Sstables not cleaned, got {have_res}'
    # Make sure objects also disappeared
    objects = get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.all()
    print(f'Found objects: {[ objects ]}')
    for o in objects:
        for ent in sstable_entries:
            assert not o.key.startswith(str(ent[1])), f'Sstable object not cleaned, found {o.key}'


@pytest.mark.asyncio
async def test_populate_from_quarantine(manager: ManagerClient, s3_server):
    '''verify sstables are populated from quarantine state'''

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()

    print(f'Create keyspace (minio listening at {s3_server.address})')
    ks, cf = create_ks_and_cf(cql, s3_server)

    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    rows = {x.name: x.value for x in res}
    assert len(rows) > 0, 'Test table is empty'

    await manager.api.flush_keyspace(server.ip_addr, ks)
    # Move the sstables into "quarantine"
    res = cql.execute("SELECT * FROM system.sstables;")
    assert len(list(res)) > 0, 'No entries in registry'
    for row in res:
        cql.execute("UPDATE system.sstables SET state = 'quarantine'"
                     f" WHERE owner = {row.owner} AND generation = {row.generation};")

    print('Restart scylla')
    await manager.server_restart(server.server_id)
    cql = await reconnect_driver(manager)

    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    have_res = {x.name: x.value for x in res}
    # Quarantine entries must have been processed normally
    assert have_res == rows, f'Unexpected table content: {have_res}'


@pytest.mark.asyncio
async def test_misconfigured_storage(manager: ManagerClient, s3_server):
    '''creating keyspace with unknown endpoint is not allowed'''
    # scylladb/scylladb#15074
    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    print(f'Create keyspace (minio listening at {s3_server.address})')
    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                      'replication_factor': '1'})
    storage_opts = format_tuples(type='S3',
                                 endpoint='unknown_endpoint',
                                 bucket=s3_server.bucket_name)

    with pytest.raises(ConfigurationException):
        cql.execute((f"CREATE KEYSPACE test_ks WITH"
                      f" REPLICATION = {replication_opts} AND STORAGE = {storage_opts};"))


@pytest.mark.asyncio
async def test_memtable_flush_retries(manager: ManagerClient, tmpdir, s3_server):
    '''verify that memtable flush doesn't crash in case storage access keys are incorrect'''

    print('Spoof the object-store config')
    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)

    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    print(f'Create keyspace (minio listening at {s3_server.address})')

    ks, cf = create_ks_and_cf(cql, s3_server)
    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    rows = {x.name: x.value for x in res}

    with scylla_inject_error(cql, "s3_client_fail_authorization"):
        print(f'Flush keyspace')
        flush = asyncio.create_task(manager.api.flush_keyspace(server.ip_addr, ks))
        print(f'Wait few seconds')
        await asyncio.sleep(8)

    print(f'Wait for flush to finish')
    await flush

    print(f'Check the sstables table')
    res = cql.execute("SELECT * FROM system.sstables;")
    ssts = "\n".join(f"{row.owner} {row.generation} {row.status}" for row in res)
    print(f'sstables:\n{ssts}')

    print('Restart scylla')
    await manager.server_restart(server.server_id)
    cql = await reconnect_driver(manager)

    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    have_res = { x.name: x.value for x in res }
    assert have_res == dict(rows), f'Unexpected table content: {have_res}'

@pytest.mark.asyncio
async def test_get_object_store_endpoints(manager: ManagerClient, s3_server):
    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    badconf = MinioServer.create_conf('a', 123, 'bad_region')
    cfg = {'object_storage_endpoints': objconf + badconf}

    print('Scylla returns the object storage endpoints')
    server = await manager.server_add(config=cfg)
    endpoints = await manager.api.get_config(server.ip_addr, 'object_storage_endpoints')

    print('Also check the returned string is valid JSON')
    del objconf[0]['name']
    del badconf[0]['name']
    assert json.loads(endpoints[s3_server.address]) == objconf[0]
    assert json.loads(endpoints['a']) == badconf[0]

    print('Check that system.config contains the object storage endpoints')
    cql = manager.get_cql()
    res = json.loads(cql.execute("SELECT value FROM system.config WHERE name = 'object_storage_endpoints';").one().value)
    assert s3_server.address in res and 'a' in res
    assert json.loads(res[s3_server.address]) == objconf[0]
    assert json.loads(res['a']) == badconf[0]

    print('Update config with a new endpoint and SIGHUP Scylla to reload configuration')
    new_endpoint = MinioServer.create_conf('b', 456, 'good_region')
    await manager.server_update_config(server.server_id, 'object_storage_endpoints', new_endpoint)
    await wait_for_config(manager, server, 'object_storage_endpoints', {'b': '{ "port": 456, "use_https": false, "aws_region": "good_region", "iam_role_arn": "" }'})

    print('Trying to create a keyspace with an endpoint not configured in object_storage_endpoints should trip storage_manager::is_known_endpoint()')
    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                      'replication_factor': '1'})
    storage_opts = format_tuples(type='S3',
                                 endpoint='a',
                                 bucket=s3_server.bucket_name)
    with pytest.raises(ConfigurationException):
        cql.execute((f'CREATE KEYSPACE random_ks WITH'
                      f' REPLICATION = {replication_opts} AND STORAGE = {storage_opts};'))

    print('Passing a known endpoint will make the CREATE KEYSPACE stmt to succeed')
    storage_opts = format_tuples(type='S3',
                                 endpoint='b',
                                 bucket=s3_server.bucket_name)
    cql.execute((f'CREATE KEYSPACE random_ks WITH'
                    f' REPLICATION = {replication_opts} AND STORAGE = {storage_opts};'))
    

