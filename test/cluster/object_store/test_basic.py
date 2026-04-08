#!/usr/bin/env python3

import asyncio
import os
import pytest
import shutil
import logging
import json

from test.pylib.minio_server import MinioServer
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.cluster.util import reconnect_driver
from test.cluster.object_store.conftest import format_tuples, keyspace_options
from test.cqlpy.rest_api import scylla_inject_error
from test.cluster.test_config import wait_for_config
from test.cluster.util import new_test_keyspace
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import unique_name

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
            assert row.owner == tid.id, \
                f'Unexpected entry owner in registry: {row.owner}'
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
        rows = "\n".join(f"{row.owner} {row.status}" for row in res)
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
            sstable_entries.append((row.owner, row.generation))
        print(f'Found entries: {[ str(ent[1]) for ent in sstable_entries ]}')
        for owner, gen in sstable_entries:
            cql.execute("UPDATE system.sstables SET status = 'removing'"
                         f" WHERE owner = {owner} AND generation = {gen};")

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
                assert not o.key.startswith(str(ent[1])), f'Sstable object not cleaned, found {o.key}'


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
                         f" WHERE owner = {row.owner} AND generation = {row.generation};")

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
        ssts = "\n".join(f"{row.owner} {row.generation} {row.status}" for row in res)
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
    server = await manager.server_add()
    cql = manager.get_cql()

    print('Trying to create a keyspace with an endpoint not configured in object_storage_endpoints should trip storage_manager::is_known_endpoint()')
    endpoint = 'http://a:456'
    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                      'replication_factor': '1'})
    storage_opts = format_tuples(type=f'{object_storage.type}',
                                 endpoint=endpoint,
                                 bucket=object_storage.bucket_name)

    with pytest.raises(ConfigurationException):
        cql.execute((f'CREATE KEYSPACE random_ks WITH'
                      f' REPLICATION = {replication_opts} AND STORAGE = {storage_opts};'))

    print('Update config with a new endpoint and SIGHUP Scylla to reload configuration')
    new_endpoint = MinioServer.create_conf(endpoint, 'region')
    await manager.server_update_config(server.server_id, 'object_storage_endpoints', new_endpoint)
    await wait_for_config(manager, server, 'object_storage_endpoints', {endpoint: '{ "type": "s3", "aws_region": "region", "iam_role_arn": "" }'})

    print('Passing a known endpoint will make the CREATE KEYSPACE stmt to succeed')
    cql.execute((f'CREATE KEYSPACE random_ks WITH'
                    f' REPLICATION = {replication_opts} AND STORAGE = {storage_opts};'))


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_drop_table_object_storage_no_deadlock(manager: ManagerClient, object_storage):
    """
    Verify DROP TABLE on an object-storage-backed keyspace does not deadlock
    and that the on_before_drop_column_family listener atomically removes
    sstables registry entries via a partition tombstone in the group0 command.

    Without the listener, wipe() during schema merge would either deadlock
    (trying to acquire the group0 operation mutex already held by the DROP
    TABLE caller) or, with the in_group0_drop_schema_trx safety flag, silently
    skip per-SSTable registry cleanup — leaking registry entries.

    Part 1 enables an error injection that skips the tombstone, proving
    that registry entries leak when the listener is bypassed.

    Part 2 runs without the injection, proving that the listener correctly
    cleans up registry entries and that DROP TABLE completes without deadlock.

    Reproduces SCYLLADB-1004.
    """
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options'],
    }
    server = await manager.server_add(config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        # injection skips the tombstone — registry entries leak
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.t1 (pk, v) VALUES ({i}, {i})")
        await manager.api.flush_keyspace(server.ip_addr, ks)

        tid = await cql.run_async(
            f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 't1'")
        table_id = tid[0].id

        # Verify registry entries exist before DROP
        rows = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id])
        assert len(rows) > 0, "Expected registry entries before DROP TABLE"

        await manager.api.enable_injection(
            server.ip_addr, "skip_sstables_registry_drop_tombstone", one_shot=False)

        await asyncio.wait_for(
            cql.run_async(f"DROP TABLE {ks}.t1"),
            timeout=10)

        # Without the tombstone, registry entries should still exist (leaked)
        rows = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id])
        assert len(rows) > 0, "Registry entries should leak when tombstone is skipped"

        await manager.api.disable_injection(
            server.ip_addr, "skip_sstables_registry_drop_tombstone")

        # no injection — tombstone cleans up registry entries
        await cql.run_async(f"CREATE TABLE {ks}.t2 (pk int PRIMARY KEY, v int)")
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.t2 (pk, v) VALUES ({i}, {i})")
        await manager.api.flush_keyspace(server.ip_addr, ks)

        tid = await cql.run_async(
            f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 't2'")
        table_id = tid[0].id

        rows = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id])
        assert len(rows) > 0, "Expected registry entries before DROP TABLE"

        await asyncio.wait_for(
            cql.run_async(f"DROP TABLE {ks}.t2"),
            timeout=10)

        # With the tombstone, registry entries should be cleaned up
        rows = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id])
        assert len(rows) == 0, f"Registry entries should be cleaned up after DROP TABLE, found {len(rows)}"


@pytest.mark.asyncio
async def test_drop_keyspace_object_storage_cleans_registry(manager: ManagerClient, object_storage):
    """
    Verify DROP KEYSPACE on an object-storage-backed keyspace cleans up
    sstables registry entries for ALL tables in the keyspace.

    The on_before_drop_keyspace listener iterates all tables in the
    keyspace and adds partition tombstones for each table's registry
    entries into the group0 command.  This test creates two tables,
    verifies their registry entries exist, drops the keyspace, and
    confirms that registry entries for both tables are gone.

    Uses asyncio.wait_for with a timeout to detect a potential deadlock.
    """
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options'],
    }
    server = await manager.server_add(config=cfg)
    cql = manager.get_cql()

    ks = unique_name()
    ks_opts = keyspace_options(object_storage)
    await cql.run_async(f"CREATE KEYSPACE IF NOT EXISTS {ks} {ks_opts}")

    await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")
    await cql.run_async(f"CREATE TABLE {ks}.t2 (pk int PRIMARY KEY, v int)")

    for i in range(10):
        await cql.run_async(f"INSERT INTO {ks}.t1 (pk, v) VALUES ({i}, {i})")
        await cql.run_async(f"INSERT INTO {ks}.t2 (pk, v) VALUES ({i}, {i})")
    await manager.api.flush_keyspace(server.ip_addr, ks)

    tid1 = await cql.run_async(
        f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 't1'")
    table_id1 = tid1[0].id
    tid2 = await cql.run_async(
        f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 't2'")
    table_id2 = tid2[0].id

    rows1 = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id1])
    assert len(rows1) > 0, "Expected registry entries for t1 before DROP KEYSPACE"
    rows2 = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id2])
    assert len(rows2) > 0, "Expected registry entries for t2 before DROP KEYSPACE"

    await asyncio.wait_for(
        cql.run_async(f"DROP KEYSPACE {ks}"),
        timeout=10)

    # After DROP KEYSPACE, registry entries for both tables should be gone
    rows1 = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id1])
    assert len(rows1) == 0, f"Registry entries for t1 should be cleaned up after DROP KEYSPACE, found {len(rows1)}"
    rows2 = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id2])
    assert len(rows2) == 0, f"Registry entries for t2 should be cleaned up after DROP KEYSPACE, found {len(rows2)}"
  
@pytest.mark.asyncio
async def test_truncate_object_storage_cleans_registry(manager: ManagerClient, object_storage):
    """
    Verify TRUNCATE on an object-storage-backed table cleans up sstables
    registry entries while keeping the table itself intact.
    """
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options'],
    }
    server = await manager.server_add(config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.t1 (pk, v) VALUES ({i}, {i})")
        await manager.api.flush_keyspace(server.ip_addr, ks)

        tid = await cql.run_async(
            f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 't1'")
        table_id = tid[0].id

        rows = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id])
        assert len(rows) > 0, "Expected registry entries before TRUNCATE"

        await cql.run_async(f"TRUNCATE {ks}.t1")

        # After truncate, registry entries should be cleaned up
        rows = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id])
        assert len(rows) == 0, f"Registry entries should be cleaned up after TRUNCATE, found {len(rows)}"

        # Table should still exist but be empty
        res = await cql.run_async(f"SELECT * FROM {ks}.t1")
        assert len(res) == 0, f"Table should be empty after TRUNCATE, found {len(res)} rows"

        # Verify the table is still functional: insert new data, flush, check new registry entries
        for i in range(5):
            await cql.run_async(f"INSERT INTO {ks}.t1 (pk, v) VALUES ({i}, {i})")
        await manager.api.flush_keyspace(server.ip_addr, ks)

        rows = await cql.run_async("SELECT * FROM system.sstables WHERE owner = %s", parameters=[table_id])
        assert len(rows) > 0, "Expected new registry entries after inserting into truncated table"

        res = await cql.run_async(f"SELECT * FROM {ks}.t1")
        assert len(res) == 5, f"Expected 5 rows after re-inserting into truncated table, found {len(res)}"
