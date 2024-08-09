#!/usr/bin/env python3

import asyncio
import os
import requests
import pytest
import shutil
import logging

from test.pylib.minio_server import MinioServer
from cassandra.protocol import ConfigurationException
from test.pylib.manager_client import ManagerClient
from test.topology.util import reconnect_driver
from test.object_store.conftest import get_s3_resource
from test.object_store.conftest import format_tuples

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

    cfg = {'enable_user_defined_functions': False,
           'object_storage_config_file': str(s3_server.config_file),
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
    for row in res:
        assert row.location.startswith(workdir), \
            f'Unexpected entry location in registry: {row.location}'
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
    rows = "\n".join(f"{row.location} {row.status}" for row in res)
    assert not rows, 'Unexpected entries in registry'


@pytest.mark.asyncio
async def test_garbage_collect(manager: ManagerClient, s3_server):
    '''verify ownership table is garbage-collected on boot'''

    sstable_entries = []

    cfg = {'enable_user_defined_functions': False,
           'object_storage_config_file': str(s3_server.config_file),
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()

    print(f'Create keyspace (minio listening at {s3_server.address})')
    ks, cf = create_ks_and_cf(cql, s3_server)

    await manager.api.flush_keyspace(server.ip_addr, ks)
    # Mark the sstables as "removing" to simulate the problem
    res = cql.execute("SELECT * FROM system.sstables;")
    for row in res:
        sstable_entries.append((row.location, row.generation))
    print(f'Found entries: {[ str(ent[1]) for ent in sstable_entries ]}')
    for loc, gen in sstable_entries:
        cql.execute("UPDATE system.sstables SET status = 'removing'"
                     f" WHERE location = '{loc}' AND generation = {gen};")

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

    cfg = {'enable_user_defined_functions': False,
           'object_storage_config_file': str(s3_server.config_file),
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
                     f" WHERE location = '{row.location}' AND generation = {row.generation};")

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
    cfg = {'enable_user_defined_functions': False,
           'object_storage_config_file': str(s3_server.config_file),
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
    local_config = tmpdir / 'object_storage.yaml'
    MinioServer.create_conf_file(s3_server.address, s3_server.port, 'bad_key', 'bad_secret', 'bad_region', local_config)

    orig_config = s3_server.config_file
    s3_server.config_file = local_config

    cfg = {'enable_user_defined_functions': False,
           'object_storage_config_file': str(s3_server.config_file),
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    print(f'Create keyspace (minio listening at {s3_server.address})')

    ks, cf = create_ks_and_cf(cql, s3_server)
    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    rows = {x.name: x.value for x in res}

    print(f'Flush keyspace')
    flush = asyncio.create_task(manager.api.flush_keyspace(server.ip_addr, ks))
    print(f'Wait few seconds')
    await asyncio.sleep(8)
    print(f'Restore and reload config')
    shutil.copyfile(orig_config, s3_server.config_file)
    # this option is not live-updateable, and is here just dur to manager client limitations
    # the actual config is updated with the copyfile above
    await manager.server_update_config(server.server_id, 'object_storage_config_file', str(s3_server.config_file))
    print(f'Wait for flush to finish')
    await flush
    print(f'Check the sstables table')
    res = cql.execute("SELECT * FROM system.sstables;")
    ssts = "\n".join(f"{row.location} {row.generation} {row.status}" for row in res)
    print(f'sstables:\n{ssts}')

    print('Restart scylla')
    await manager.server_restart(server.server_id)
    cql = await reconnect_driver(manager)

    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    have_res = { x.name: x.value for x in res }
    assert have_res == dict(rows), f'Unexpected table content: {have_res}'
