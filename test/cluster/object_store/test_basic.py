#!/usr/bin/env python3

import asyncio
import os
import time
import pytest
import shutil
import logging
import json

from test.pylib.minio_server import MinioServer
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import reconnect_driver
from test.pylib.object_storage import format_tuples, keyspace_options
from test.cqlpy.rest_api import scylla_inject_error
from test.cluster.test_config import wait_for_config
from test.cluster.util import new_test_keyspace
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.skip_types import skip_bug

logger = logging.getLogger(__name__)


@pytest.mark.parametrize('replication_factor', [1, 3])
@pytest.mark.parametrize('mode', ['normal', 'encrypted'])
@pytest.mark.asyncio
async def test_basic(manager: ManagerClient, object_storage, tmp_path, mode, replication_factor):
    '''verify ownership table is updated, and tables written to object storage can be read after scylla restarts.
    Parametrized over replication_factor to also verify RF=3 with multiple servers.'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    if mode == 'encrypted':
        d = tmp_path / "system_keys"
        d.mkdir()
        cfg = cfg | {
            'system_key_directory': str(d),
            'user_info_encryption': { 'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory' }
        }

    servers = []
    for rack_num in range(replication_factor):
        property_file = {"dc": "dc1", "rack": f"r{rack_num}"}
        server = await manager.server_add(config=cfg, property_file=property_file)
        servers.append(server)

    cql = manager.get_cql()
    workdir = await manager.server_get_workdir(servers[0].server_id)
    print(f'Create keyspace (storage server listening at {object_storage.address})')
    async with new_test_keyspace(manager, keyspace_options(object_storage, rf=replication_factor)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (name text PRIMARY KEY, value int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (name, value) VALUES ('{k}', {k});") for k in range(4)])

        assert not os.path.exists(os.path.join(workdir, f'data/{ks}')), "object storage backed keyspace has local directory created"
        # Sanity check that the path is constructed correctly
        assert os.path.exists(os.path.join(workdir, 'data/system')), "Datadir is elsewhere"

        desc = cql.execute(f"DESCRIBE KEYSPACE {ks}").one().create_statement
        # The storage_opts wraps options with '{ <options> }' while the DESCRIBE
        # does it like '{<options>}' so strip the corner branches and spaces for check
        assert f"{{'type': '{object_storage.type}', 'bucket': '{object_storage.bucket_name}', 'endpoint': '{object_storage.address}'}}" in desc, "DESCRIBE generates unexpected storage options"

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        rows = {x.name: x.value for x in res}
        assert len(rows) > 0, 'Test table is empty'

        print('Flush keyspace on all servers')
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)

        # Check that the ownership table is populated properly
        tid = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'").one()
        res = cql.execute("SELECT * FROM system.sstables;")
        for row in res:
            assert row.table_id == tid.id, \
                f'Unexpected entry table_id in registry: {row.table_id}'
            assert row.status == 'sealed', f'Unexpected entry status in registry: {row.status}'

        if replication_factor > 1:
            tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
            print(f'Found {len(tablet_replicas)} tablets')
            hosts = {await manager.get_host_id(s.server_id): s.ip_addr for s in servers}
            for idx, tablet in enumerate(tablet_replicas):
                replica_ips = [hosts[r[0]] for r in tablet.replicas]
                print(f'Tablet {idx}: last_token={tablet.last_token}, replicas={replica_ips}')
                assert len(replica_ips) == replication_factor, f'Expected RF={replication_factor} replicas, got {len(replica_ips)}'

        print('Restart scylla')
        for server in servers:
            await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)

        # Shouldn't be recreated by populator code
        assert not os.path.exists(os.path.join(workdir, f'data/{ks}')), "object storage backed keyspace has local directory resurrected"

        stmt = SimpleStatement(f"SELECT * FROM {ks}.test;", consistency_level=ConsistencyLevel.ALL)
        res = cql.execute(stmt)
        have_res = {x.name: x.value for x in res}
        assert have_res == rows, f'Unexpected table content: {have_res}'

        print('Drop table')
        cql.execute(f"DROP TABLE {ks}.test;")
        # Check that the ownership table is de-populated
        res = cql.execute("SELECT * FROM system.sstables;")
        rows = "\n".join(f"{row.table_id} {row.status}" for row in res)
        assert not rows, 'Unexpected entries in registry'

@pytest.mark.asyncio
async def test_garbage_collect(manager: ManagerClient, object_storage):
    '''verify ownership table is garbage-collected on boot'''

    sstable_entries = []

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    cmd = ['--logger-log-level', 's3=trace:http=debug:gcp_storage=trace']
    server = await manager.server_add(config=cfg, cmdline=cmd)

    cql = manager.get_cql()

    print(f'Create keyspace (storage server listening at {object_storage.address})')
    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (name text PRIMARY KEY, value int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (name, value) VALUES ('{k}', {k});") for k in range(4)])

        await manager.api.flush_keyspace(server.ip_addr, ks)
        # Mark the sstables as "removing" to simulate the problem
        res = cql.execute("SELECT * FROM system.sstables;")
        for row in res:
            sstable_entries.append((row.table_id, row.node_owner, row.generation))
        print(f'Found entries: {[ str(ent[2]) for ent in sstable_entries ]}')
        for table_id, node_owner, gen in sstable_entries:
            cql.execute("UPDATE system.sstables SET status = 'removing'"
                         f" WHERE table_id = {table_id} AND node_owner = {node_owner} AND generation = {gen};")

        print('Restart scylla')
        await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        have_res = {x.name: x.value for x in res}
        # Must be empty as no sstables should have been picked up
        assert not have_res, f'Sstables not cleaned, got {have_res}'
        # Make sure objects also disappeared
        objects = object_storage.get_resource().Bucket(object_storage.bucket_name).objects.all()
        print(f'Found objects: {[ objects ]}')
        for o in objects:
            for ent in sstable_entries:
                assert not o.key.startswith(str(ent[2])), f'Sstable object not cleaned, found {o.key}'


@pytest.mark.asyncio
async def test_populate_from_quarantine(manager: ManagerClient, object_storage):
    '''verify sstables are populated from quarantine state'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()

    print(f'Create keyspace (storage server listening at {object_storage.address})')
    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (name text PRIMARY KEY, value int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (name, value) VALUES ('{k}', {k});") for k in range(4)])

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        rows = {x.name: x.value for x in res}
        assert len(rows) > 0, 'Test table is empty'

        await manager.api.flush_keyspace(server.ip_addr, ks)
        # Move the sstables into "quarantine"
        res = cql.execute("SELECT * FROM system.sstables;")
        assert len(list(res)) > 0, 'No entries in registry'
        for row in res:
            cql.execute("UPDATE system.sstables SET state = 'quarantine'"
                         f" WHERE table_id = {row.table_id} AND node_owner = {row.node_owner} AND generation = {row.generation};")

        print('Restart scylla')
        await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        have_res = {x.name: x.value for x in res}
        # Quarantine entries must have been processed normally
        assert have_res == rows, f'Unexpected table content: {have_res}'


@pytest.mark.asyncio
async def test_misconfigured_storage(manager: ManagerClient, object_storage):
    '''creating keyspace with unknown endpoint is not allowed'''
    # scylladb/scylladb#15074
    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    print(f'Create keyspace (storage server listening at {object_storage.address})')
    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                      'replication_factor': '1'})
    storage_opts = format_tuples(type=f'{object_storage.type}',
                                 endpoint='unknown_endpoint',
                                 bucket=object_storage.bucket_name)

    with pytest.raises(ConfigurationException):
        cql.execute((f"CREATE KEYSPACE test_ks WITH"
                      f" REPLICATION = {replication_opts} AND STORAGE = {storage_opts};"))


@pytest.mark.asyncio
async def test_memtable_flush_retries(manager: ManagerClient, tmpdir, object_storage):
    '''verify that memtable flush doesn't crash in case storage access keys are incorrect'''

    print('Spoof the object-store config')
    objconf = object_storage.create_endpoint_conf()

    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    print(f'Create keyspace (storage server listening at {object_storage.address})')

    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (name text PRIMARY KEY, value int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (name, value) VALUES ('{k}', {k});") for k in range(4)])

        res = cql.execute(f"SELECT * FROM {ks}.test;")
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
        ssts = "\n".join(f"{row.table_id} {row.generation} {row.status}" for row in res)
        print(f'sstables:\n{ssts}')

        print('Restart scylla')
        await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        have_res = { x.name: x.value for x in res }
        assert have_res == dict(rows), f'Unexpected table content: {have_res}'

@pytest.mark.asyncio
@pytest.mark.parametrize('config_with_full_url', [True, False])
async def test_get_object_store_endpoints(manager: ManagerClient, config_with_full_url):
    if config_with_full_url:
        objconf = MinioServer.create_conf('http://a:123', 'region')
    else:
        objconf = MinioServer.create_conf('a', 'region')
        objconf[0]["port"] = 123
        objconf[0]["use_https"] = False
        del objconf[0]["type"]

    cfg = {'object_storage_endpoints': objconf}

    print('Scylla returns the object storage endpoints')
    server = await manager.server_add(config=cfg)
    endpoints = await manager.api.get_config(server.ip_addr, 'object_storage_endpoints')

    name = objconf[0]['name']
    del objconf[0]['name']

    print('Also check the returned string is valid JSON')
    assert name in endpoints
    assert json.loads(endpoints[name]) == objconf[0]

    print('Check that system.config contains the object storage endpoints')
    cql = manager.get_cql()
    res = json.loads(cql.execute("SELECT value FROM system.config WHERE name = 'object_storage_endpoints';").one().value)
    assert name in res
    assert json.loads(res[name]) == objconf[0]


@pytest.mark.asyncio
async def test_create_keyspace_after_config_update(manager: ManagerClient, object_storage):
    print('Trying to create a keyspace with an endpoint not configured in object_storage_endpoints should trip storage_manager::is_known_endpoint()')
    server = await manager.server_add()
    cql = manager.get_cql()
    endpoint = object_storage.address  
    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                      'replication_factor': '1'})
    storage_opts = format_tuples(type=f'{object_storage.type}',
                                 endpoint=endpoint,
                                 bucket=object_storage.bucket_name)

    with pytest.raises(ConfigurationException):
        cql.execute((f'CREATE KEYSPACE random_ks WITH'
                      f' REPLICATION = {replication_opts} AND STORAGE = {storage_opts};'))

    print('Update config with a new endpoint and SIGHUP Scylla to reload configuration')
    objconf = object_storage.create_endpoint_conf()
    await manager.server_update_config(server.server_id, 'object_storage_endpoints', objconf)
    ep = objconf[0]
    if ep['type'] == 's3':
        expected_conf = f'{{ "type": "s3", "aws_region": "{ep["aws_region"]}", "iam_role_arn": "{ep["iam_role_arn"]}" }}'
    else:
        expected_conf = f'{{ "type": "gs", "credentials_file": "{ep["credentials_file"]}" }}'
    await wait_for_config(manager, server, 'object_storage_endpoints', {ep['name']: expected_conf})

    print('Passing a known endpoint will make the CREATE KEYSPACE stmt to succeed')
    cql.execute((f'CREATE KEYSPACE random_ks WITH'
                    f' REPLICATION = {replication_opts} AND STORAGE = {storage_opts};'))

    print('Create a table, insert data and flush the keyspace to force object_storage_client creation')
    cql.execute(f'CREATE TABLE random_ks.test (name text PRIMARY KEY, value int);')
    await cql.run_async(f"INSERT INTO random_ks.test (name, value) VALUES ('test_key', 123);")
    await manager.api.flush_keyspace(server.ip_addr, 'random_ks')
    res = cql.execute(f"SELECT value FROM random_ks.test WHERE name = 'test_key';")
    assert res.one().value == 123, f'Unexpected value after flush: {res.one().value}'

    # Now that a live object_storage_client exists for this endpoint, push a
    # config update that modifies the endpoint parameters.  This exercises the
    # update_config_sync path on an already-instantiated client
    print('Push a config update to reconfigure the live object_storage_client')
    updated_objconf = object_storage.create_endpoint_conf()
    updated_ep = updated_objconf[0]
    if updated_ep['type'] == 's3':
        updated_ep['aws_region'] = 'updated-region'
        updated_expected_conf = f'{{ "type": "s3", "aws_region": "{updated_ep["aws_region"]}", "iam_role_arn": "{updated_ep["iam_role_arn"]}" }}'
    else:
        updated_ep['credentials_file'] = ''
        updated_expected_conf = f'{{ "type": "gs", "credentials_file": "{updated_ep["credentials_file"]}" }}'
        skip_bug("https://scylladb.atlassian.net/browse/SCYLLADB-1559")

    await manager.server_update_config(server.server_id, 'object_storage_endpoints', updated_objconf)
    await wait_for_config(manager, server, 'object_storage_endpoints', {updated_ep['name']: updated_expected_conf})

    print('Verify the reconfigured client still works: insert more data and flush')
    await cql.run_async(f"INSERT INTO random_ks.test (name, value) VALUES ('after_reconfig', 456);")
    await manager.api.flush_keyspace(server.ip_addr, 'random_ks')
    res = cql.execute(f"SELECT value FROM random_ks.test WHERE name = 'after_reconfig';")
    assert res.one().value == 456, f'Unexpected value after reconfiguration flush: {res.one().value}'

    print('Verify all data is intact')
    rows = {r.name: r.value for r in cql.execute(f'SELECT * FROM random_ks.test;')}
    assert rows == {'test_key': 123, 'after_reconfig': 456}, f'Unexpected table content: {rows}'


@pytest.mark.asyncio
async def test_tablet_migration_with_encryption(manager: ManagerClient, s3_server, tmp_path):
    """Verify that tablet migration works correctly with encrypted SSTables on S3 storage.

    Reproduces https://scylladb.atlassian.net/browse/SCYLLADB-1704

    sstable_stream_sink_impl::load_metadata() used file_exists() on a local path to check
    for the Scylla metadata component. For S3-backed storage this always returned false,
    causing each streamed SSTable component to get a different encryption key. When the
    SSTable was later read back, decryption with the wrong key produced garbage, leading
    to OOM crashes.

    The fix uses _sst->_storage->exists() which works for all storage backends.
    """
    d = tmp_path / "system_keys"
    d.mkdir()

    objconf = s3_server.create_endpoint_conf()
    cfg = {
        'enable_user_defined_functions': False,
        'object_storage_endpoints': objconf,
        'experimental_features': ['keyspace-storage-options'],
        'system_key_directory': str(d),
        'user_info_encryption': {'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory'},
    }

    cmdline = ['--smp=1']
    servers = [await manager.server_add(config=cfg, cmdline=cmdline)]
    await manager.disable_tablet_balancing()

    servers.append(await manager.server_add(config=cfg, cmdline=cmdline))

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    storage_opts = format_tuples(type='S3', endpoint=s3_server.address, bucket=s3_server.bucket_name)
    ks = 'test_enc_migration_ks'
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', "
                        f"'replication_factor': 1}} AND tablets = {{'initial': 1}} AND STORAGE = {storage_opts}")

    try:
        await cql.run_async(f"""CREATE TABLE {ks}.t (pk int PRIMARY KEY, v text)
            WITH scylla_encryption_options = {{
                'cipher_algorithm': 'AES/ECB/PKCS5Padding',
                'secret_key_strength': 128,
                'key_provider': 'LocalFileSystemKeyProviderFactory',
                'secret_key_file': '{d}/data_encryption_key'
            }}""")

        # Insert enough data to produce a non-trivial SSTable
        for i in range(100):
            await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({i}, 'value_{i}')")

        # Flush to ensure data is written as an SSTable to S3
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        # Verify data before migration
        rows_before = {r.pk: r.v for r in cql.execute(f"SELECT pk, v FROM {ks}.t")}
        assert len(rows_before) == 100, f"Expected 100 rows before migration, got {len(rows_before)}"

        # Move the tablet to the other server
        tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 't')
        host_ids = await asyncio.gather(*[manager.get_host_id(s.server_id) for s in servers])
        src_host, src_shard = tablet_replicas[0].replicas[0]
        dst_host = host_ids[1] if src_host == host_ids[0] else host_ids[0]
        await manager.api.move_tablet(servers[0].ip_addr, ks, "t",
                                      src_host, src_shard, dst_host, 0,
                                      tablet_replicas[0].last_token)

        # Verify data after migration - this would fail with the bug because
        # SSTable components on S3 would be encrypted with different keys
        rows_after = {r.pk: r.v for r in cql.execute(f"SELECT pk, v FROM {ks}.t")}
        assert rows_after == rows_before, (
            f"Data mismatch after tablet migration. "
            f"Before: {len(rows_before)} rows, After: {len(rows_after)} rows"
        )
    finally:
        await cql.run_async(f"DROP KEYSPACE IF EXISTS {ks}")
