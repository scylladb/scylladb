#!/usr/bin/env python3
import json
import os
import logging
import asyncio
import subprocess
import tempfile

import pytest
import time
import random

from test.cqlpy.util import local_process_id
from test.pylib.manager_client import ManagerClient
from test.cluster.object_store.conftest import format_tuples
from test.cluster.conftest import skip_mode
from test.cluster.util import wait_for_cql_and_get_hosts, get_replication, new_test_keyspace
from test.pylib.rest_client import read_barrier
from test.pylib.util import unique_name, wait_all
from cassandra.cluster import ConsistencyLevel
from collections import defaultdict
from test.pylib.util import wait_for
import statistics

logger = logging.getLogger(__name__)


def create_ks_and_cf(cql):
    ks = 'test_ks'
    cf = 'test_cf'

    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy', 'replication_factor': '1'})
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(f"CREATE TABLE {ks}.{cf} ( name text primary key, value text );")

    rows = [('0', 'zero'),
            ('1', 'one'),
            ('2', 'two')]
    for row in rows:
        cql_fmt = "INSERT INTO {}.{} ( name, value ) VALUES ('{}', '{}');"
        cql.execute(cql_fmt.format(ks, cf, *row))

    return ks, cf


async def prepare_snapshot_for_backup(manager: ManagerClient, server, snap_name='backup'):
    cql = manager.get_cql()
    print('Create keyspace')
    ks, cf = create_ks_and_cf(cql)
    print('Flush keyspace')
    await manager.api.flush_keyspace(server.ip_addr, ks)
    print('Take keyspace snapshot')
    await manager.api.take_snapshot(server.ip_addr, ks, snap_name)

    return ks, cf


@pytest.mark.asyncio

async def test_simple_backup(manager: ManagerClient, object_storage):
    '''check that backing up a snapshot for a keyspace works'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    print('Backup snapshot')
    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', object_storage.address, object_storage.bucket_name, prefix)
    print(f'Started task {tid}')
    status = await manager.api.get_task_status(server.ip_addr, tid)
    print(f'Status: {status}, waiting to finish')
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')
    assert (status['progress_total'] > 0) and (status['progress_completed'] == status['progress_total'])

    objects = set(o.key for o in object_storage.get_resource().Bucket(object_storage.bucket_name).objects.all())
    for f in files:
        print(f'Check {f} is in backup')
        assert f'{prefix}/{f}' in objects

    # Check that task runs in the streaming sched group
    log = await manager.server_open_log(server.server_id)
    res = await log.grep(r'INFO.*\[shard [0-9]:([a-z]+)\] .* Backup sstables from .* to')
    assert len(res) == 1 and res[0][1].group(1) == 'strm'


@pytest.mark.asyncio
@pytest.mark.parametrize("move_files", [False, True])
async def test_backup_move(manager: ManagerClient, object_storage, move_files):
    '''check that backing up a snapshot by _moving_ sstable to object storage'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    print('Backup snapshot')
    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', object_storage.address, object_storage.bucket_name, prefix,
                                   move_files=move_files)
    print(f'Started task {tid}')
    status = await manager.api.get_task_status(server.ip_addr, tid)
    print(f'Status: {status}, waiting to finish')
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')
    assert (status['progress_total'] > 0) and (status['progress_completed'] == status['progress_total'])

    # all components in the "backup" snapshot should have been moved into bucket if move_files
    assert len(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup')) == 0 if move_files else len(files)


@pytest.mark.asyncio
@pytest.mark.parametrize("ne_parameter", [ "endpoint", "bucket", "snapshot" ])
async def test_backup_with_non_existing_parameters(manager: ManagerClient, object_storage, ne_parameter):
    '''backup should fail if either of the parameters does not exist'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    backup_snap_name = 'backup'
    ks, cf = await prepare_snapshot_for_backup(manager, server, snap_name = backup_snap_name)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf,
            backup_snap_name if ne_parameter != 'snapshot' else 'no-such-snapshot',
            object_storage.address if ne_parameter != 'endpoint' else 'no-such-endpoint',
            object_storage.bucket_name if ne_parameter != 'bucket' else 'no-such-bucket',
            prefix)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None
    assert status['state'] == 'failed'
    if ne_parameter == 'endpoint':
        assert status['error'] == 'std::invalid_argument (endpoint no-such-endpoint not found)'


@pytest.mark.asyncio
async def test_backup_endpoint_config_is_live_updateable(manager: ManagerClient, object_storage):
    '''backup should fail if the endpoint is invalid/inaccessible
       after updating the config, it should succeed'''

    cfg = {'enable_user_defined_functions': False,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'sstables_manager=debug']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    prefix = f'{cf}/backup'

    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', object_storage.address, object_storage.bucket_name, prefix)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None
    assert status['state'] == 'failed'
    assert status['error'] == f'std::invalid_argument (endpoint {object_storage.address} not found)'

    objconf = object_storage.create_endpoint_conf()
    await manager.server_update_config(server.server_id, 'object_storage_endpoints', objconf)

    async def endpoint_appeared_in_config():
        await read_barrier(manager.api, server.ip_addr)
        resp = await manager.api.get_config(server.ip_addr, 'object_storage_endpoints')
        for ep in objconf:
            if ep['name'] not in resp:
                return None
        return True
    await wait_for(endpoint_appeared_in_config, deadline=time.time() + 60)

    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', object_storage.address, object_storage.bucket_name, prefix)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None
    assert status['state'] == 'done'


async def do_test_backup_abort(manager: ManagerClient, object_storage,
                               breakpoint_name, min_files, max_files = None):
    '''helper for backup abort testing'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 1

    await manager.api.enable_injection(server.ip_addr, breakpoint_name, one_shot=True)
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    print('Backup snapshot')
    # use a unique(ish) path, because we're running more than one test using the same minio and ks/cf name.
    # If we just use {cf}/backup, files like "schema.cql" and "manifest.json" will remain after previous test
    # case, and we will count these erroneously.
    prefix = f'{cf_dir}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', object_storage.address, object_storage.bucket_name, prefix)

    print(f'Started task {tid}, aborting it early')
    await log.wait_for(breakpoint_name + ': waiting', from_mark=mark)
    await manager.api.abort_task(server.ip_addr, tid)
    await manager.api.message_injection(server.ip_addr, breakpoint_name)
    status = await manager.api.wait_task(server.ip_addr, tid)
    print(f'Status: {status}')
    assert (status is not None) and (status['state'] == 'failed')
    assert "seastar::abort_requested_exception (abort requested)" in status['error']

    objects = set(o.key for o in object_storage.get_resource().Bucket(object_storage.bucket_name).objects.all())
    uploaded_count = 0
    for f in files:
        in_backup = f'{prefix}/{f}' in objects
        print(f'Check {f} is in backup: {in_backup}')
        if in_backup:
            uploaded_count += 1
    # Note: since s3 client is abortable and run async, we might fail even the first file
    # regardless of if we set the abort status before or after the upload is initiated.
    # Parallelism is a pain.
    assert min_files <= uploaded_count < len(files)
    assert max_files is None or uploaded_count < max_files


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_backup_is_abortable(manager: ManagerClient, object_storage):
    '''check that backing up a snapshot for a keyspace works'''
    await do_test_backup_abort(manager, object_storage, breakpoint_name="backup_task_pause", min_files=0)


@pytest.mark.asyncio

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_backup_is_abortable_in_s3_client(manager: ManagerClient, object_storage):
    '''check that backing up a snapshot for a keyspace works'''
    await do_test_backup_abort(manager, object_storage, breakpoint_name="backup_task_pre_upload", min_files=0, max_files=1)


async def do_test_simple_backup_and_restore(manager: ManagerClient, object_storage, tmpdir, do_encrypt = False, do_abort = False):
    '''check that restoring from backed up snapshot for a keyspace:table works'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    if do_encrypt:
        d = tmpdir / "system_keys"
        d.mkdir()
        cfg = cfg | {
            'system_key_directory': str(d),
            'user_info_encryption': { 'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory' }
        }
    cmd = ['--logger-log-level', 'sstables_loader=debug:sstable_directory=trace:snapshots=trace:s3=trace:sstable=debug:http=debug:encryption=debug:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)

    cql = manager.get_cql()
    workdir = await manager.server_get_workdir(server.server_id)

    # This test is sensitive not to share the bucket with any other test
    # that can run in parallel, so generate some unique name for the snapshot
    snap_name = unique_name('backup_')
    print(f'Create and backup keyspace (snapshot name is {snap_name})')
    ks, cf = await prepare_snapshot_for_backup(manager, server, snap_name)

    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]

    def list_sstables():
        return [f for f in os.scandir(f'{workdir}/data/{ks}/{cf_dir}') if f.is_file()]

    orig_res = cql.execute(f"SELECT * FROM {ks}.{cf}")
    orig_rows = {x.name: x.value for x in orig_res}

    # include a "suffix" in the key to mimic the use case where scylla-manager
    # 1. backups sstables of multiple snapshots, and deduplicate the backup'ed
    #    sstables by only upload the new sstables
    # 2. restore a given snapshot by collecting all sstables of this snapshot from
    #    multiple places
    #
    # in this test, we:
    # 1. upload:
    #    prefix: {prefix}/{suffix}
    #    sstables:
    #    - 1-TOC.txt
    #    - 2-TOC.txt
    #    - ...
    # 2. download:
    #    prefix = {prefix}
    #    sstables:
    #    - {suffix}/1-TOC.txt
    #    - {suffix}/2-TOC.txt
    #    - ...
    suffix = 'suffix'
    old_files = list_sstables();
    toc_names = [f'{suffix}/{entry.name}' for entry in old_files if entry.name.endswith('TOC.txt')]

    prefix = f'{cf}/{snap_name}'
    tid = await manager.api.backup(server.ip_addr, ks, cf, snap_name, object_storage.address, object_storage.bucket_name, f'{prefix}/{suffix}')
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')

    print('Drop the table data and validate it\'s gone')
    cql.execute(f"TRUNCATE TABLE {ks}.{cf};")
    files = list_sstables()
    assert len(files) == 0
    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    assert not res
    objects = set(o.key for o in object_storage.get_resource().Bucket(object_storage.bucket_name).objects.filter(Prefix=prefix))
    assert len(objects) > 0

    print('Try to restore')
    tid = await manager.api.restore(server.ip_addr, ks, cf, object_storage.address, object_storage.bucket_name, prefix, toc_names)

    if do_abort:
        await manager.api.abort_task(server.ip_addr, tid)

    status = await manager.api.wait_task(server.ip_addr, tid)
    if not do_abort:
        assert status is not None
        assert status['state'] == 'done'
        assert status['progress_units'] == 'batches'
        assert status['progress_completed'] == status['progress_total']
        assert status['progress_completed'] > 0

    print('Check that sstables came back')
    files = list_sstables()

    sstable_names = [f'{entry.name}' for entry in files if entry.name.endswith('.db')]
    db_objects = [object for object in objects if object.endswith('.db')]

    if do_abort:
        assert len(files) >= 0
        # These checks can be viewed as dubious. We restore (atm) on a mutation basis mostly.
        # There is no guarantee we'll generate the same amount of sstables as was in the original
        # backup (?). But, since we are not stressing the server here (not provoking memtable flushes),
        # we should in principle never generate _more_ sstables than originated the backup.
        assert len(old_files) >= len(files)
        assert len(sstable_names) <= len(db_objects)
    else:
        assert len(files) > 0
        assert (status is not None) and (status['state'] == 'done')
        print(f'Check that data came back too')
        res = cql.execute(f"SELECT * FROM {ks}.{cf};")
        rows = { x.name: x.value for x in res }
        assert rows == orig_rows, "Unexpected table contents after restore"

    print('Check that backup files are still there')  # regression test for #20938
    post_objects = set(o.key for o in object_storage.get_resource().Bucket(object_storage.bucket_name).objects.filter(Prefix=prefix))
    assert objects == post_objects

@pytest.mark.asyncio
async def test_simple_backup_and_restore(manager: ManagerClient, object_storage, tmp_path):
    '''check that restoring from backed up snapshot for a keyspace:table works'''
    await do_test_simple_backup_and_restore(manager, object_storage, tmp_path, False, False)

@pytest.mark.asyncio
async def test_abort_simple_backup_and_restore(manager: ManagerClient, object_storage, tmp_path):
    '''check that restoring from backed up snapshot for a keyspace:table works'''
    await do_test_simple_backup_and_restore(manager, object_storage, tmp_path, False, True)



async def do_abort_restore(manager: ManagerClient, object_storage):
    # Define configuration for the servers.
    objconf = object_storage.create_endpoint_conf()
    config = {'enable_user_defined_functions': False,
              'object_storage_endpoints': objconf,
              'experimental_features': ['keyspace-storage-options'],
              'task_ttl_in_seconds': 300,
              }

    servers = await manager.servers_add(servers_num=3, config=config, auto_rack_dc='dc1')

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    # Create keyspace, table, and fill data
    logger.info("Creating keyspace and table, then inserting data...")

    table = 'test_cf'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as keyspace:
        create_table_query = f"CREATE TABLE {keyspace}.{table} (name text PRIMARY KEY, value text);"
        await cql.run_async(create_table_query)

        insert_stmt = cql.prepare(f"INSERT INTO {keyspace}.{table} (name, value) VALUES (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL

        num_keys = 10000
        await asyncio.gather(*(cql.run_async(insert_stmt, (str(i), str(i))) for i in range(num_keys)))

        # Flush keyspace on all servers
        logger.info("Flushing keyspace on all servers...")
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, keyspace)

        # Take snapshot for keyspace
        snapshot_name = unique_name('backup_')
        logger.info(f"Taking snapshot '{snapshot_name}' for keyspace '{keyspace}'...")
        for server in servers:
            await manager.api.take_snapshot(server.ip_addr, keyspace, snapshot_name)

        # Collect snapshot files from each server
        async def get_snapshot_files(server, snapshot_name):
            workdir = await manager.server_get_workdir(server.server_id)
            data_path = os.path.join(workdir, 'data', keyspace)
            cf_dirs = os.listdir(data_path)
            if not cf_dirs:
                raise RuntimeError(f"No column family directories found in {data_path}")
            # Assumes that there is only one column family directory under the keyspace.
            cf_dir = cf_dirs[0]
            snapshot_path = os.path.join(data_path, cf_dir, 'snapshots', snapshot_name)
            return [
                f.name for f in os.scandir(snapshot_path)
                if f.is_file() and f.name.endswith('TOC.txt')
            ]

        sstables = {}
        for server in servers:
            snapshot_files = await get_snapshot_files(server, snapshot_name)
            sstables[server.server_id] = snapshot_files

        # Backup the keyspace on each server to S3
        prefix = f"{table}/{snapshot_name}"
        logger.info(f"Backing up keyspace using prefix '{prefix}' on all servers...")
        for server in servers:
            backup_tid = await manager.api.backup(
                server.ip_addr,
                keyspace,
                table,
                snapshot_name,
                object_storage.address,
                object_storage.bucket_name,
                prefix
            )
            backup_status = await manager.api.wait_task(server.ip_addr, backup_tid)
            assert backup_status is not None and backup_status.get('state') == 'done', \
                f"Backup task failed on server {server.server_id}"

        # Truncate data and start restore
        logger.info("Dropping table data...")
        await cql.run_async(f"TRUNCATE TABLE {keyspace}.{table};")
        logger.info("Initiating restore operations...")

        logs = [await manager.server_open_log(server.server_id) for server in servers]

        injection = "stream_mutation_fragments" # "block_load_and_stream"
        await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, injection, True) for s in servers))

        restore_task_ids = {}
        for server in servers:
            restore_tid = await manager.api.restore(
                server.ip_addr,
                keyspace,
                table,
                object_storage.address,
                object_storage.bucket_name,
                prefix,
                sstables[server.server_id]
            )
            restore_task_ids[server.server_id] = restore_tid

        await wait_all([l.wait_for(f"{injection}: waiting", timeout=10) for l in logs])

        logger.info("Aborting restore tasks...")
        await asyncio.gather(*(manager.api.abort_task(server.ip_addr, restore_task_ids[server.server_id]) for server in servers))

        await asyncio.gather(*(manager.api.message_injection(s.ip_addr, injection) for s in servers))

        # Check final status of restore tasks
        failed = False
        for server in servers:
            final_status = await manager.api.wait_task(server.ip_addr, restore_task_ids[server.server_id])
            logger.info(f"Restore task status on server {server.server_id}: {final_status}")
            assert (final_status is not None)
            failed |= final_status['state'] == 'failed'
        assert failed, "Expected at least one restore task to fail after aborting"

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_abort_restore_with_rpc_error(manager: ManagerClient, object_storage):
    await do_abort_restore(manager, object_storage)


@pytest.mark.asyncio

async def test_simple_backup_and_restore_with_encryption(manager: ManagerClient, object_storage, tmp_path):
    '''check that restoring from backed up snapshot for a keyspace:table works'''
    await do_test_simple_backup_and_restore(manager, object_storage, tmp_path, True, False)

# Helper class to parametrize the test below
class topo:
    def __init__(self, rf, nodes, racks, dcs):
        self.rf = rf
        self.nodes = nodes
        self.racks = racks
        self.dcs = dcs

async def create_cluster(topology, rf_rack_valid_keyspaces, manager, logger, object_storage=None):
    logger.info(f'Start cluster with {topology.nodes} nodes in {topology.dcs} DCs, {topology.racks} racks, rf_rack_valid_keyspaces: {rf_rack_valid_keyspaces}')

    cfg = {'task_ttl_in_seconds': 300, 'rf_rack_valid_keyspaces': rf_rack_valid_keyspaces}
    if object_storage:
        objconf = object_storage.create_endpoint_conf()
        cfg['object_storage_endpoints'] = objconf

    cmd = [ '--logger-log-level', 'sstables_loader=debug:sstable_directory=trace:snapshots=trace:s3=trace:sstable=debug:http=debug:api=info' ]
    servers = []
    host_ids = {}

    cur_dc = 0
    cur_rack = 0
    for s in range(topology.nodes):
        dc = f"dc{cur_dc}"
        rack = f"rack{cur_rack}"
        cur_dc += 1
        if cur_dc >= topology.dcs:
            cur_dc = 0
            cur_rack += 1
            if cur_rack >= topology.racks:
                cur_rack = 0
        s = await manager.server_add(config=cfg, cmdline=cmd, property_file={'dc': dc, 'rack': rack})
        logger.info(f'Created node {s.ip_addr} in {dc}.{rack}')
        servers.append(s)
        host_ids[s.server_id] = await manager.get_host_id(s.server_id)

    return servers,host_ids

async def create_dataset(manager, ks, cf, topology, logger, num_keys=256, min_tablet_count=None, schema=None, consistency_level=ConsistencyLevel.ALL):
    cql = manager.get_cql()
    logger.info(f'Create keyspace, {topology=}')
    keys = range(num_keys)
    replication_opts = {'class': 'NetworkTopologyStrategy'}
    replication_opts['replication_factor'] = f'{topology.rf}'
    replication_opts = format_tuples(replication_opts)

    print(replication_opts)

    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))

    if schema is None:
        if min_tablet_count is not None:
            logger.info(f'Creating schema with min_tablet_count={min_tablet_count}')
        schema = create_schema(ks, cf, min_tablet_count)
    cql.execute(schema)

    stmt = cql.prepare(f"INSERT INTO {ks}.{cf} ( pk, value ) VALUES (?, ?)")
    if consistency_level is not None:
        stmt.consistency_level = consistency_level
    await asyncio.gather(*(cql.run_async(stmt, (str(k), k)) for k in keys))

    return schema, keys, replication_opts

async def do_restore_server(manager, logger, ks, cf, s, toc_names, scope, primary_replica_only, prefix, object_storage):
    logger.info(f'Restore {s.ip_addr} with {toc_names}, scope={scope}')
    tid = await manager.api.restore(s.ip_addr, ks, cf, object_storage.address, object_storage.bucket_name, prefix, toc_names, scope, primary_replica_only=primary_replica_only)
    status = await manager.api.wait_task(s.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')

async def take_snapshot(ks, servers, manager, logger):
    logger.info(f'Take snapshot and collect sstables lists')
    snap_name = unique_name('backup_')
    sstables = dict()
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)
        await manager.api.take_snapshot(s.ip_addr, ks, snap_name)
        workdir = await manager.server_get_workdir(s.server_id)
        cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
        tocs = [ f.name for f in os.scandir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/{snap_name}') if f.is_file() and f.name.endswith('TOC.txt') ]
        logger.info(f'Collected sstables from {s.ip_addr}:{cf_dir}/snapshots/{snap_name}: {tocs}')
        sstables[s] = tocs

    return snap_name,sstables

async def check_data_is_back(manager, logger, cql, ks, cf, keys, servers, topology, host_ids, scope, primary_replica_only, log_marks, different_min_tablet_count=False):
    logger.info(f'Check the data is back')

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, scope, primary_replica_only)

    if different_min_tablet_count:
        logger.info(f'Skipping streaming directions checks, we restored with a different min_tablet_count, so streaming is not predictable')
        return

    host_ids_per_dc = defaultdict(list)
    host_ids_per_dc_rack = dict()
    servers_by_host_id = dict()
    for s in servers:
        host = host_ids[s.server_id]
        host_ids_per_dc[s.datacenter].append(host)
        host_ids_per_dc_rack.setdefault(s.datacenter, defaultdict(list))[s.rack].append(host)
        servers_by_host_id[host] = s

    logger.info(f'Validate streaming directions')
    for s in servers:
        streamed_to = defaultdict(int)
        log, mark = log_marks[s.server_id]
        res = await log.grep(r'sstables_loader - load_and_stream:.*target_node=(?P<target_host_id>[0-9a-f-]+)', from_mark=mark)
        for r in res:
            target_host_id = r[1].group('target_host_id')
            if scope == 'all':
                assert target_host_id in servers_by_host_id.keys()
            elif scope == 'dc':
                assert servers_by_host_id[target_host_id].datacenter == s.datacenter
            elif scope == 'rack':
                assert servers_by_host_id[target_host_id].datacenter == s.datacenter
                assert servers_by_host_id[target_host_id].rack == s.rack
            elif scope == 'node':
                assert target_host_id == str(host_ids[s.server_id])
            streamed_to[target_host_id] += 1

        # validate balance only when rf == #racks
        if topology.rf != topology.racks:
            logger.info(f'Skipping balance checks since rf != racks ({topology.rf} != {topology.racks})')
            continue

        if scope == 'all':
            assert set(streamed_to.keys()).issubset(set(host_ids.values()))
            assert len(streamed_to) == topology.rf * topology.dcs
        elif scope == 'dc':
            # it's guaranteed the node replicated only within the datacenter by asserts above
            assert set(streamed_to.keys()).issubset(set(host_ids_per_dc[s.datacenter]))
            assert len(streamed_to) == topology.rf
        elif scope == 'rack' and topology.rf == topology.racks:
            assert set(streamed_to.keys()).issubset(set(host_ids_per_dc_rack[s.datacenter][s.rack]))
            assert len(streamed_to) == 1

        # asses balance
        streamed_to_counts = streamed_to.values()
        assert len(streamed_to_counts) > 0
        mean_count = statistics.mean(streamed_to_counts)
        max_deviation = max(abs(count - mean_count) for count in streamed_to_counts)
        if not primary_replica_only:
            assert max_deviation == 0, f'if primary_replica_only is False, streaming should be perfectly balanced: {streamed_to}'
            continue

        assert max_deviation < 0.1 * mean_count, f'node {s.ip_addr} streaming to primary replicas was unbalanced: {streamed_to}'

async def do_load_sstables(ks, cf, servers, topology, sstables, scope, manager, logger, prefix = None, object_storage = None, primary_replica_only = False, load_fn=do_restore_server):
    logger.info(f'Loading {servers=} with {sstables=} scope={scope}')
    sstables_per_server = defaultdict(list)
    # rf_rack_valid can be True also with rack lists
    rf_rack_valid = topology.rf == topology.racks
    if scope == 'all' or scope == 'dc' or not rf_rack_valid:
        sstables_per_dc = defaultdict(list)
        for s, sstables_list in sstables.items():
            sstables_per_dc[s.datacenter].extend(sstables_list)
        servers_per_dc = defaultdict(list)
        for s in servers:
            servers_per_dc[s.datacenter].append(s)
        
        for dc, sstables_in_dc in sstables_per_dc.items():
            for s in servers_per_dc[dc]:
                if scope == 'node':
                    # If not rf_rack_valid, each node should load data from all sstables in the DC
                    # Otherwise, as done in the case below, each node load data from all sstables in its rack
                    # (since it is ensured that every rack has a replica of each mutation)
                    sstables_per_server[s] = sstables_in_dc
                else:
                    sstables_per_server[s] = sstables[s]
    elif scope == 'rack' or scope == 'node':
        servers_per_dc_rack = dict()
        sstables_per_dc_rack = dict()
        for s, sstables_list in sstables.items():
            servers_per_dc_rack.setdefault(s.datacenter, defaultdict(list))[s.rack].append(s)
            sstables_per_dc_rack.setdefault(s.datacenter, defaultdict(list))[s.rack].extend(sstables_list)
        for dc, racks in sstables_per_dc_rack.items(): 
            for rack, sstables_in_rack in racks.items():
                if scope == 'rack':
                    assert topology.rf == topology.racks
                    for s in servers_per_dc_rack[dc][rack]:
                        sstables_per_server[s] = sstables[s]
                else:
                    assert scope == 'node'
                    for s in servers_per_dc_rack[dc][rack]:
                        sstables_per_server[s] = sstables_in_rack
    else:
        raise f"do_load_sstables: {scope=} not supported"

    await asyncio.gather(*(load_fn(manager, logger, ks, cf, s, sstables, scope, primary_replica_only, prefix, object_storage) for s, sstables in sstables_per_server.items()))
    if primary_replica_only:
        await manager.api.tablet_repair(servers[0].ip_addr, ks, cf, 'all', timeout=600)

async def do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger):
    logger.info(f'Backup to {snap_name}')
    tid = await manager.api.backup(s.ip_addr, ks, cf, snap_name, object_storage.address, object_storage.bucket_name, prefix)
    status = await manager.api.wait_task(s.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')

async def collect_mutations(cql, server, manager, ks, cf):
    host = await wait_for_cql_and_get_hosts(cql, [server], time.time() + 30)
    await read_barrier(manager.api, server.ip_addr)  # scylladb/scylladb#18199
    ret = defaultdict(list)
    for frag in await cql.run_async(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.{cf})", host=host[0]):
        ret[frag.pk].append({'mutation_source': frag.mutation_source, 'partition_region': frag.partition_region, 'node': server.ip_addr})
    return ret

async def check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, scope=None, primary_replica_only=None, expected_replicas = None):
    '''Check that each mutation is replicated to the expected number of replicas'''
    if expected_replicas is None:
        expected_replicas = topology.rf * topology.dcs

    mutations = defaultdict(list)
    by_node = await asyncio.gather(*(collect_mutations(cql, s, manager, ks, cf) for s in servers))
    for node_frags in by_node:
        for pk, frags in node_frags.items():
            mutations[pk].append(frags)

    for k in random.sample(keys, 10):
        if not str(k) in mutations:
            logger.info(f'Mutations: {mutations}')
            assert False, f"Key '{k}' not found in mutations. {topology=} {scope=} {primary_replica_only=}"

        if len(mutations[str(k)]) != expected_replicas:
            logger.info(f'Mutations: {mutations}')
            assert False, f"'{k}' is replicated {len(mutations[str(k)])} times, expected {expected_replicas}"

async def mark_all_logs(manager, servers):
    log_marks = dict()
    for s in servers:
        log = await manager.server_open_log(s.server_id)
        log_marks[s.server_id] = (log, await log.mark())
    return log_marks

def create_schema(ks, cf, min_tablet_count=None):
    schema = f"CREATE TABLE {ks}.{cf} ( pk text primary key, value int )"
    if min_tablet_count is not None:
        schema += f" WITH tablets = {{'min_tablet_count': {min_tablet_count}}}"
    schema += ';'
    return schema

@pytest.mark.asyncio
@pytest.mark.parametrize("topology_rf_validity", [
        (topo(rf = 1, nodes = 3, racks = 1, dcs = 1), True),
        (topo(rf = 3, nodes = 5, racks = 1, dcs = 1), False),
        (topo(rf = 1, nodes = 4, racks = 2, dcs = 1), True),
        (topo(rf = 3, nodes = 6, racks = 2, dcs = 1), False),
        (topo(rf = 2, nodes = 8, racks = 4, dcs = 2), True)
    ])

async def test_restore_with_streaming_scopes(build_mode: str, manager: ManagerClient, object_storage, topology_rf_validity):
    '''Check that restoring of a cluster with stream scopes works'''

    topology, rf_rack_valid_keyspaces = topology_rf_validity

    servers, host_ids = await create_cluster(topology, rf_rack_valid_keyspaces, manager, logger, object_storage)

    await manager.disable_tablet_balancing()
    cql = manager.get_cql()

    ks = 'ks'
    cf = 'cf'

    num_keys = 10

    scopes = ['rack', 'dc'] if build_mode == 'debug' else ['all', 'dc', 'rack', 'node']
    restored_min_tablet_counts = [5] if build_mode == 'debug' else [2, 5, 10]
    
    schema, keys, replication_opts = await create_dataset(manager, ks, cf, topology, logger, num_keys=num_keys, min_tablet_count=5)

    # validate replicas assertions hold on fresh dataset
    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, scope=None, primary_replica_only=False, expected_replicas = None)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)
    prefix = f'{cf}/{snap_name}'

    await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger) for s in servers))

    for scope in scopes:
        # We can support rack-aware restore with rack lists, if we restore the rack-list per dc as it was at backup time.
        # Otherwise, with numeric replication_factor we'd pick arbitrary subset of the racks when the keyspace
        # is initially created and an arbitrary subset or the rack at restore time.
        if scope == 'rack' and topology.rf != topology.racks:
            logger.info(f'Skipping scope={scope} test since rf={topology.rf} != racks={topology.racks} and it cannot be supported with numeric replication_factor')
            continue
        pros = [False] if scope == 'node' else [True, False]
        for pro in pros:
            for restored_min_tablet_count in restored_min_tablet_counts:
                logger.info(f'Re-initialize keyspace with min_tablet_count={restored_min_tablet_count} from min_tablet_count=5')
                cql.execute(f'DROP KEYSPACE {ks}')
                cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
                schema = create_schema(ks, cf, restored_min_tablet_count)
                cql.execute(schema)

                log_marks = await mark_all_logs(manager, servers)

                await do_load_sstables(ks, cf, servers, topology, sstables, scope, manager, logger, prefix=prefix, object_storage=object_storage, primary_replica_only=pro)

                await check_data_is_back(manager, logger, cql, ks, cf, keys, servers, topology, host_ids, scope, primary_replica_only=pro, log_marks=log_marks, different_min_tablet_count=(restored_min_tablet_count != 512))

@pytest.mark.asyncio
async def test_restore_with_non_existing_sstable(manager: ManagerClient, object_storage):
    '''Check that restore task fails well when given a non-existing sstable'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    cql = manager.get_cql()
    print('Create keyspace')
    ks, cf = create_ks_and_cf(cql)

    # The name must be parseable by sstable layer, yet such file shouldn't exist
    sstable_name = 'me-3gou_0fvw_4r94g2h8nw60b8ly4c-big-TOC.txt'
    tid = await manager.api.restore(server.ip_addr, ks, cf, object_storage.address, object_storage.bucket_name, 'no_such_prefix', [sstable_name])
    status = await manager.api.wait_task(server.ip_addr, tid)
    print(f'Status: {status}')
    assert 'state' in status and status['state'] == 'failed'
    assert 'error' in status and 'Not Found' in status['error']


@pytest.mark.asyncio
async def test_backup_broken_streaming(manager: ManagerClient, s3_storage):
    # Define configuration for the servers.
    objconf = s3_storage.create_endpoint_conf()
    config = {
        'enable_user_defined_functions': False,
        'object_storage_endpoints': objconf,
        'experimental_features': ['keyspace-storage-options'],
        'task_ttl_in_seconds': 300,
    }
    cmd = ['--smp', '1', '--logger-log-level', 'sstables_loader=debug:sstable=debug']
    server = await manager.server_add(config=config, cmdline=cmd)

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    pid = local_process_id(cql)
    if not pid:
        pytest.skip("Can't find local Scylla process")
    # Now that we know the process id, use /proc to find the executable.
    try:
        scylla_path = os.readlink(f'/proc/{pid}/exe')
    except:
        pytest.skip("Can't find local Scylla executable")
    # Confirm that this executable is a real tool-providing Scylla by trying
    # to run it with the "--list-tools" option
    try:
        subprocess.check_output([scylla_path, '--list-tools'])
    except:
        pytest.skip("Local server isn't Scylla")

    async with new_test_keyspace(manager,
                                 "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as keyspace:
        table = 'test_cf'
        create_table_query = (
            f"CREATE TABLE {keyspace}.{table} (name text PRIMARY KEY, value text) "
            f"WITH tablets = {{'min_tablet_count': '16'}};"
        )
        cql.execute(create_table_query)

        expected_rows = 0
        with tempfile.TemporaryDirectory() as tmp_dir:
            resource_dir = "test/resource/sstables/fully_partially_contained_ssts"
            schema_file = os.path.join(tmp_dir, "schema.cql")
            with open(schema_file, "w") as f:
                f.write(f"CREATE TABLE {keyspace}.{table} (name text PRIMARY KEY, value text)")
                f.flush()
            for root, _, files in os.walk(resource_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    print("Processing file:", local_path)
                    sst = subprocess.check_output(
                        [scylla_path, "sstable", "write", "--schema-file", schema_file, "--input-format", "json",
                         "--output-dir", tmp_dir, "--input-file", local_path])
                    expected_rows += json.loads(subprocess.check_output(
                        [scylla_path, "sstable", "query", "-q", f"SELECT COUNT(*) FROM scylla_sstable.{table}",
                         "--output-format", "json", "--sstables",
                         os.path.join(tmp_dir, f"me-{sst.decode().strip()}-big-TOC.txt")]).decode())[0]['count']

            prefix = unique_name('/test/streaming_')
            s3_resource = s3_storage.get_resource()
            bucket = s3_resource.Bucket(s3_storage.bucket_name)
            sstables = []

            print(f"Uploading files from '{tmp_dir}' to prefix '{prefix}':")

            for root, _, files in os.walk(tmp_dir):
                for file in files:
                    if file.endswith("-TOC.txt"):
                        sstables.append(file)
                    local_path = os.path.join(root, file)
                    s3_key = f"{prefix}/{file}"

                    print(f" - Uploading {local_path} to {s3_key}")
                    bucket.upload_file(local_path, s3_key)

        restore_task_id = await manager.api.restore(
            server.ip_addr, keyspace, table,
            s3_storage.address, s3_storage.bucket_name,
            prefix, sstables, "node"
        )

        status = await manager.api.wait_task(server.ip_addr, restore_task_id)
        assert status and status.get(
            'state') == 'done', f"Restore task failed on server {server.server_id}. Reason {status}"

        res = cql.execute(f"SELECT COUNT(*) FROM {keyspace}.{table} BYPASS CACHE USING TIMEOUT 600s;")

        assert res[0].count == expected_rows, f"number of rows after restore is incorrect: {res[0].count}"

@pytest.mark.asyncio
async def test_restore_primary_replica_same_rack_scope_rack(manager: ManagerClient, object_storage):
    '''Check that restoring with primary_replica_only and scope rack streams only to primary replica in the same rack.
    The test checks that each mutation exists exactly 2 times within the cluster, once in each rack
    (each restoring node streams to one primary replica in its rack. Without primary_replica_only we'd see 4 replicas, 2 in each rack).
    The test also checks that the logs of each restoring node shows streaming to a single node, which is the primary replica within the same rack.'''

    topology = topo(rf = 4, nodes = 8, racks = 2, dcs = 1)
    scope = "rack"
    ks = 'ks'
    cf = 'cf'

    servers, host_ids = await create_cluster(topology, False, manager, logger, object_storage)

    await manager.disable_tablet_balancing()
    cql = manager.get_cql()

    schema, keys, replication_opts = await create_dataset(manager, ks, cf, topology, logger)

    # validate replicas assertions hold on fresh dataset
    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)
    prefix = f'{cf}/{snap_name}'

    await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    await asyncio.gather(*(do_restore_server(manager, logger, ks, cf, s, sstables[s], scope, True, prefix, object_storage) for s in servers))

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, scope, primary_replica_only=True, expected_replicas=2)

    logger.info(f'Validate streaming directions')
    for i, s in enumerate(servers):
        log = await manager.server_open_log(s.server_id)
        res = await log.grep(r'INFO.*sstables_loader - load_and_stream: ops_uuid=([0-9a-z-]+).*target_node=([0-9a-z-]+),.*num_bytes_sent=([0-9]+)')
        nodes_by_operation = defaultdict(list)
        for r in res:
            nodes_by_operation[r[1].group(1)].append(r[1].group(2))

        scope_nodes = set([ str(host_ids[s.server_id]) for s in servers if s.rack == servers[i].rack ])
        for op, nodes in nodes_by_operation.items():
            logger.info(f'Operation {op} streamed to nodes {nodes}')
            assert len(nodes) == 1, "Each streaming operation should stream to exactly one primary replica"
            assert nodes[0] in scope_nodes, f"Primary replica should be within the scope {scope}"

@pytest.mark.asyncio
async def test_restore_primary_replica_different_rack_scope_dc(manager: ManagerClient, object_storage):
    '''Check that restoring with primary_replica_only and scope dc permits cross-rack streaming.
    The test checks that each mutation exists exactly 1 time within the cluster, in one of the racks.
    (each restoring node would pick the same primary replica, one would pick it within its own rack(itself), one would pick it from the other rack.
     Without primary_replica_only we'd see 2 replicas, 1 in each rack).
    The test also checks that the logs of each restoring node shows streaming to two nodes because cross-rack streaming is allowed
    and eventually one node, depending on tablet_id of mutations, will end up choosing either of the two nodes as primary replica.'''

    topology = topo(rf = 2, nodes = 2, racks = 2, dcs = 1)
    scope = "dc"
    ks = 'ks'
    cf = 'cf'

    servers, host_ids = await create_cluster(topology, True, manager, logger, object_storage)

    await manager.disable_tablet_balancing()
    cql = manager.get_cql()

    schema, keys, replication_opts = await create_dataset(manager, ks, cf, topology, logger)

    # validate replicas assertions hold on fresh dataset
    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)
    prefix = f'{cf}/{snap_name}'

    await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    await asyncio.gather(*(do_restore_server(manager, logger, ks, cf, s, sstables[s], scope, True, prefix, object_storage) for s in servers))

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, scope, primary_replica_only=True, expected_replicas=1)

    logger.info(f'Validate streaming directions')
    for i, s in enumerate(servers):
        log = await manager.server_open_log(s.server_id)
        res = await log.grep(r'INFO.*sstables_loader - load_and_stream:.*target_node=([0-9a-z-]+),.*num_bytes_sent=([0-9]+)')
        streamed_to = set([ r[1].group(1) for r in res ])
        logger.info(f'{s.ip_addr} {host_ids[s.server_id]} streamed to {streamed_to}')
        assert len(streamed_to) == 2

@pytest.mark.asyncio
async def test_restore_primary_replica_same_dc_scope_dc(manager: ManagerClient, object_storage):
    '''Check that restoring with primary_replica_only and scope dc streams only to primary replica in the local dc.
    The test checks that each mutation exists exactly 2 times within the cluster, once in each dc
    (each restoring node streams to one primary replica in its dc. Without primary_replica_only we'd see 4 replicas, 2 in each dc).
    The test also checks that the logs of each restoring node shows streaming to a single node, which is the primary replica within the same dc.'''

    topology = topo(rf = 4, nodes = 8, racks = 2, dcs = 2)
    scope = "dc"
    ks = 'ks'
    cf = 'cf'

    servers, host_ids = await create_cluster(topology, False, manager, logger, object_storage)

    await manager.disable_tablet_balancing()
    cql = manager.get_cql()

    schema, keys, replication_opts = await create_dataset(manager, ks, cf, topology, logger)

    # validate replicas assertions hold on fresh dataset
    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)
    prefix = f'{cf}/{snap_name}'

    await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    await asyncio.gather(*(do_restore_server(manager, logger, ks, cf, s, sstables[s], scope, True, prefix, object_storage) for s in servers))

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, scope, primary_replica_only=True, expected_replicas=2)

    logger.info(f'Validate streaming directions')
    for i, s in enumerate(servers):
        log = await manager.server_open_log(s.server_id)
        res = await log.grep(r'INFO.*sstables_loader - load_and_stream: ops_uuid=([0-9a-z-]+).*target_node=([0-9a-z-]+),.*num_bytes_sent=([0-9]+)')
        nodes_by_operation = defaultdict(list)
        for r in res:
            nodes_by_operation[r[1].group(1)].append(r[1].group(2))

        scope_nodes = set([ str(host_ids[s.server_id]) for s in servers if s.datacenter == servers[i].datacenter ])
        for op, nodes in nodes_by_operation.items():
            logger.info(f'Operation {op} streamed to nodes {nodes}')
            assert len(nodes) == 1, "Each streaming operation should stream to exactly one primary replica"
            assert nodes[0] in scope_nodes, f"Primary replica should be within the scope {scope}"

@pytest.mark.asyncio
async def test_restore_primary_replica_different_dc_scope_all(manager: ManagerClient, object_storage):
    '''Check that restoring with primary_replica_only and scope all permits cross-dc streaming.
    The test checks that each mutation exists exactly 1 time within the cluster, in only one of the dcs.
    (each restoring node would pick the same primary replica, one would pick it within its own dc(itself), one would pick it from the other dc.
     Without primary_replica_only, we'd see 2 replicas, 1 in each dc).
    The test also checks that the logs of each restoring node shows streaming to two nodes because cross-dc streaming is allowed
    and eventually one node, depending on tablet_id of mutations, will end up choosing either of the two nodes as primary replica.'''

    topology = topo(rf = 1, nodes = 2, racks = 1, dcs = 2)
    scope = "all"
    ks = 'ks'
    cf = 'cf'

    servers, host_ids = await create_cluster(topology, False, manager, logger, object_storage)

    await manager.disable_tablet_balancing()
    cql = manager.get_cql()

    schema, keys, replication_opts = await create_dataset(manager, ks, cf, topology, logger)

    # validate replicas assertions hold on fresh dataset
    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, expected_replicas=2)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)
    prefix = f'{cf}/{snap_name}'

    await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    r_servers = servers

    await asyncio.gather(*(do_restore_server(manager, logger, ks, cf, s, sstables[s], scope, True, prefix, object_storage) for s in r_servers))

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, scope, primary_replica_only=True, expected_replicas=1)

    logger.info(f'Validate streaming directions')
    for i, s in enumerate(r_servers):
        log = await manager.server_open_log(s.server_id)
        res = await log.grep(r'INFO.*sstables_loader - load_and_stream:.*target_node=([0-9a-z-]+),.*num_bytes_sent=([0-9]+)')
        streamed_to = set([ r[1].group(1) for r in res ])
        logger.info(f'{s.ip_addr} {host_ids[s.server_id]} streamed to {streamed_to}, expected {r_servers}')
        assert len(streamed_to) == 2
