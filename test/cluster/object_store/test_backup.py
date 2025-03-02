#!/usr/bin/env python3

import os
import logging
import asyncio
import pytest
import time
import random

from test.pylib.minio_server import MinioServer
from test.pylib.manager_client import ManagerClient
from test.cluster.object_store.conftest import get_s3_resource, format_tuples
from test.cluster.conftest import skip_mode
from test.cluster.util import wait_for_cql_and_get_hosts
from concurrent.futures import ThreadPoolExecutor
from test.pylib.rest_client import read_barrier
from test.pylib.util import unique_name, wait_for_first_completed
from cassandra.query import SimpleStatement              # type: ignore # pylint: disable=no-name-in-module

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
async def test_simple_backup(manager: ManagerClient, s3_server):
    '''check that backing up a snapshot for a keyspace works'''

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    print('Backup snapshot')
    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', s3_server.address, s3_server.bucket_name, prefix)
    print(f'Started task {tid}')
    status = await manager.api.get_task_status(server.ip_addr, tid)
    print(f'Status: {status}, waiting to finish')
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')
    assert (status['progress_total'] > 0) and (status['progress_completed'] == status['progress_total'])

    objects = set(o.key for o in get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.all())
    for f in files:
        print(f'Check {f} is in backup')
        assert f'{prefix}/{f}' in objects

    # Check that task runs in the streaming sched group
    log = await manager.server_open_log(server.server_id)
    res = await log.grep(r'INFO.*\[shard [0-9]:([a-z]+)\] .* Backup sstables from .* to')
    assert len(res) == 1 and res[0][1].group(1) == 'strm'


@pytest.mark.asyncio
@pytest.mark.parametrize("move_files", [False, True])
async def test_backup_move(manager: ManagerClient, s3_server, move_files):
    '''check that backing up a snapshot by _moving_ sstable to object storage'''

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    print('Backup snapshot')
    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', s3_server.address, s3_server.bucket_name, prefix,
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
async def test_backup_to_non_existent_bucket(manager: ManagerClient, s3_server):
    '''backup should fail if the destination bucket does not exist'''

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', s3_server.address, "non-existant-bucket", prefix)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None
    assert status['state'] == 'failed'
    assert 'S3 request failed. Code: 15. Reason: Access Denied.' in status['error']


async def test_backup_to_non_existent_endpoint(manager: ManagerClient, s3_server):
    '''backup should fail if the endpoint is invalid/inaccessible'''

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', "does_not_exist", s3_server.bucket_name, prefix)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None
    assert status['state'] == 'failed'
    assert status['error'] == 'std::invalid_argument (endpoint does_not_exist not found)'

async def do_test_backup_abort(manager: ManagerClient, s3_server,
                               breakpoint_name, min_files, max_files = None):
    '''helper for backup abort testing'''

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace']
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
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', s3_server.address, s3_server.bucket_name, prefix)

    print(f'Started task {tid}, aborting it early')
    await log.wait_for(breakpoint_name + ': waiting', from_mark=mark)
    await manager.api.abort_task(server.ip_addr, tid)
    await manager.api.message_injection(server.ip_addr, breakpoint_name)
    status = await manager.api.wait_task(server.ip_addr, tid)
    print(f'Status: {status}')
    assert (status is not None) and (status['state'] == 'failed')
    assert "seastar::abort_requested_exception (abort requested)" in status['error']

    objects = set(o.key for o in get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.all())
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
async def test_backup_to_non_existent_snapshot(manager: ManagerClient, s3_server):
    '''backup should fail if the snapshot does not exist'''

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'nonexistent-snapshot',
                                   s3_server.address, s3_server.bucket_name, prefix)
    # The task is expected to fail immediately due to invalid snapshot name.
    # However, since internal implementation details may change, we'll wait for
    # task completion if immediate failure doesn't occur.
    actual_state = None
    for status_api in [manager.api.get_task_status,
                       manager.api.wait_task]:
        status = await status_api(server.ip_addr, tid)
        assert status is not None
        actual_state = status['state']
        if actual_state == 'failed':
            break
    else:
        assert actual_state == 'failed'


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_backup_is_abortable(manager: ManagerClient, s3_server):
    '''check that backing up a snapshot for a keyspace works'''
    await do_test_backup_abort(manager, s3_server, breakpoint_name="backup_task_pause", min_files=0)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_backup_is_abortable_in_s3_client(manager: ManagerClient, s3_server):
    '''check that backing up a snapshot for a keyspace works'''
    await do_test_backup_abort(manager, s3_server, breakpoint_name="backup_task_pre_upload", min_files=0, max_files=1)


async def do_test_simple_backup_and_restore(manager: ManagerClient, s3_server, do_abort = False):
    '''check that restoring from backed up snapshot for a keyspace:table works'''

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'sstables_loader=debug:sstable_directory=trace:snapshots=trace:s3=trace:sstable=debug:http=debug']
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
    tid = await manager.api.backup(server.ip_addr, ks, cf, snap_name, s3_server.address, s3_server.bucket_name, f'{prefix}/{suffix}')
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')

    print('Drop the table data and validate it\'s gone')
    cql.execute(f"TRUNCATE TABLE {ks}.{cf};")
    files = list_sstables()
    assert len(files) == 0
    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    assert not res
    objects = set(o.key for o in get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.filter(Prefix=prefix))
    assert len(objects) > 0

    print('Try to restore')
    tid = await manager.api.restore(server.ip_addr, ks, cf, s3_server.address, s3_server.bucket_name, prefix, toc_names)

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
    post_objects = set(o.key for o in get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.filter(Prefix=prefix))
    assert objects == post_objects

@pytest.mark.asyncio
async def test_simple_backup_and_restore(manager: ManagerClient, s3_server):
    '''check that restoring from backed up snapshot for a keyspace:table works'''
    await do_test_simple_backup_and_restore(manager, s3_server, False)


async def do_abort_restore(manager: ManagerClient, s3_server):
    # Define configuration for the servers.
    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    config = {'enable_user_defined_functions': False,
              'object_storage_endpoints': objconf,
              'experimental_features': ['keyspace-storage-options'],
              'task_ttl_in_seconds': 300,
              }

    servers = await manager.servers_add(servers_num=3, config=config)

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    # Create keyspace, table, and fill data
    print("Creating keyspace and table, then inserting data...")

    def create_keyspace_and_table(cql):
        keyspace = 'test_ks'
        table = 'test_cf'
        replication_opts = format_tuples({
            'class': 'NetworkTopologyStrategy',
            'replication_factor': '3'
        })
        create_ks_query = f"CREATE KEYSPACE {keyspace} WITH REPLICATION = {replication_opts};"
        create_table_query = f"CREATE TABLE {keyspace}.{table} (name text PRIMARY KEY, value text);"
        cql.execute(create_ks_query)
        cql.execute(create_table_query)

        def insert_rows(cql, keyspace, table, inserts):
            for _ in range(inserts):
                key = os.urandom(64).hex()
                value = os.urandom(1024).hex()
                insert_query = f"INSERT INTO {keyspace}.{table} (name, value) VALUES ('{key}', '{value}');"
                cql.execute(insert_query)

        thread_count = 128
        rows_per_thread = 100000 // thread_count
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            # Submit tasks for each thread
            futures = [
                executor.submit(
                    insert_rows,
                    cql, keyspace, table,
                    rows_per_thread
                )
                for _ in range(thread_count)
            ]

        # Ensure all tasks are completed
        for future in futures:
            future.result()
        return keyspace, table

    keyspace, table = create_keyspace_and_table(cql)

    # Flush keyspace on all servers
    print("Flushing keyspace on all servers...")
    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, keyspace)

    # Take snapshot for keyspace
    snapshot_name = unique_name('backup_')
    print(f"Taking snapshot '{snapshot_name}' for keyspace '{keyspace}'...")
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
    print(f"Backing up keyspace using prefix '{prefix}' on all servers...")
    for server in servers:
        backup_tid = await manager.api.backup(
            server.ip_addr,
            keyspace,
            table,
            snapshot_name,
            s3_server.address,
            s3_server.bucket_name,
            prefix
        )
        backup_status = await manager.api.wait_task(server.ip_addr, backup_tid)
        assert backup_status is not None and backup_status.get('state') == 'done', \
            f"Backup task failed on server {server.server_id}"

    # Truncate data and start restore
    print("Dropping table data...")
    cql.execute(f"TRUNCATE TABLE {keyspace}.{table};")
    print("Initiating restore operations...")

    restore_task_ids = {}
    for server in servers:
        restore_tid = await manager.api.restore(
            server.ip_addr,
            keyspace,
            table,
            s3_server.address,
            s3_server.bucket_name,
            prefix,
            sstables[server.server_id]
        )
        restore_task_ids[server.server_id] = restore_tid

    await asyncio.sleep(0.1)

    print("Aborting restore tasks...")
    for server in servers:
        await manager.api.abort_task(server.ip_addr, restore_task_ids[server.server_id])

    # Check final status of restore tasks
    for server in servers:
        final_status = await manager.api.wait_task(server.ip_addr, restore_task_ids[server.server_id])
        print(f"Restore task status on server {server.server_id}: {final_status}")
        assert (final_status is not None) and (final_status['state'] == 'failed')
    logs = [await manager.server_open_log(server.server_id) for server in servers]
    await wait_for_first_completed([l.wait_for("Failed to handle STREAM_MUTATION_FRAGMENTS \(receive and distribute phase\) for .+: Streaming aborted", timeout=10) for l in logs])

@pytest.mark.asyncio
@pytest.mark.skip(reason="a very slow test (20+ seconds), skipping it")
async def test_abort_restore_with_rpc_error(manager: ManagerClient, s3_server):
    await do_abort_restore(manager, s3_server)


@pytest.mark.asyncio
async def test_abort_simple_backup_and_restore(manager: ManagerClient, s3_server):
    '''check that restoring from backed up snapshot for a keyspace:table works'''
    await do_test_simple_backup_and_restore(manager, s3_server, True)


# Helper class to parametrize the test below
class topo:
    def __init__(self, rf, nodes, racks, dcs):
        self.rf = rf
        self.nodes = nodes
        self.racks = racks
        self.dcs = dcs

async def create_cluster(topology, rf_rack_valid_keyspaces, manager, logger, s3_server=None):
    logger.info(f'Start cluster with {topology.nodes} nodes in {topology.dcs} DCs, {topology.racks} racks')

    cfg = {'task_ttl_in_seconds': 300, 'rf_rack_valid_keyspaces': rf_rack_valid_keyspaces}
    if s3_server:
        objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
        cfg['object_storage_endpoints'] = objconf

    cmd = [ '--logger-log-level', 'sstables_loader=debug:sstable_directory=trace:snapshots=trace:s3=trace:sstable=debug:http=debug' ]
    servers = []
    host_ids = {}

    for s in range(topology.nodes):
        dc = f'dc{s % topology.dcs}'
        rack = f'rack{s % topology.racks}'
        s = await manager.server_add(config=cfg, cmdline=cmd, property_file={'dc': dc, 'rack': rack})
        logger.info(f'Created node {s.ip_addr} in {dc}.{rack}')
        servers.append(s)
        host_ids[s.server_id] = await manager.get_host_id(s.server_id)

    return servers,host_ids

def create_dataset(manager, ks, cf, topology, logger):
    cql = manager.get_cql()
    logger.info(f'Create keyspace, rf={topology.rf}')
    keys = range(256)
    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy', 'replication_factor': f'{topology.rf}'})
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))

    schema = f"CREATE TABLE {ks}.{cf} ( pk int primary key, value text );"
    cql.execute(schema)
    for k in keys:
        cql.execute(f"INSERT INTO {ks}.{cf} ( pk, value ) VALUES ({k}, '{k}');")

    return schema, keys, replication_opts

async def take_snapshot(ks, servers, manager, logger):
    logger.info(f'Take snapshot and collect sstables lists')
    snap_name = unique_name('backup_')
    sstables = []
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)
        await manager.api.take_snapshot(s.ip_addr, ks, snap_name)
        workdir = await manager.server_get_workdir(s.server_id)
        cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
        tocs = [ f.name for f in os.scandir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/{snap_name}') if f.is_file() and f.name.endswith('TOC.txt') ]
        logger.info(f'Collected sstables from {s.ip_addr}:{cf_dir}/snapshots/{snap_name}: {tocs}')
        sstables += tocs

    return snap_name,sstables

def compute_scope(topology, servers):
    if topology.dcs > 1:
        scope = 'dc'
        r_servers = servers[:topology.dcs]
    elif topology.racks > 1:
        scope = 'rack'
        r_servers = servers[:topology.racks]
    else:
        scope = 'node'
        r_servers = servers

    return scope,r_servers

async def check_data_is_back(manager, logger, cql, ks, cf, keys, servers, topology, r_servers, host_ids, scope):
    logger.info(f'Check the data is back')
    async def collect_mutations(server):
        host = await wait_for_cql_and_get_hosts(cql, [server], time.time() + 30)
        await read_barrier(manager.api, server.ip_addr)  # scylladb/scylladb#18199
        ret = {}
        for frag in await cql.run_async(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.{cf})", host=host[0]):
            if not frag.pk in ret:
                ret[frag.pk] = []
            ret[frag.pk].append({'mutation_source': frag.mutation_source, 'partition_region': frag.partition_region, 'node': server.ip_addr})
        return ret

    by_node = await asyncio.gather(*(collect_mutations(s) for s in servers))
    mutations = {}
    for node_frags in by_node:
        for pk in node_frags:
            if not pk in mutations:
                mutations[pk] = []
            mutations[pk].append(node_frags[pk])

    for k in random.sample(keys, 17):
        if not k in mutations:
            logger.info(f'{k} not found in mutations')
            logger.info(f'Mutations: {mutations}')
            assert False, "Key not found in mutations"
        if len(mutations[k]) != topology.rf * topology.dcs:
            logger.info(f'{k} is replicated {len(mutations[k])} times only, expect {topology.rf * topology.dcs}')
            logger.info(f'Mutations: {mutations}')
            assert False, "Key not replicated enough"

    logger.info(f'Validate streaming directions')
    for i, s in enumerate(r_servers):
        log = await manager.server_open_log(s.server_id)
        res = await log.grep(r'INFO.*sstables_loader - load_and_stream:.*target_node=([0-9a-z-]+),.*num_bytes_sent=([0-9]+)')
        streamed_to = set([ str(host_ids[s.server_id]) ] + [ r[1].group(1) for r in res ])
        scope_nodes = set([ str(host_ids[s.server_id]) ])
        # See comment near merge_tocs() above for explanation of servers list filtering below
        if scope == 'rack':
            scope_nodes.update([ str(host_ids[s.server_id]) for s in servers[i::topology.racks] ])
        elif scope == 'dc':
            scope_nodes.update([ str(host_ids[s.server_id]) for s in servers[i::topology.dcs] ])
        logger.info(f'{s.ip_addr} streamed to {streamed_to}, expected {scope_nodes}')
        assert streamed_to == scope_nodes

@pytest.mark.asyncio
@pytest.mark.parametrize("topology_rf_validity", [
        (topo(rf = 1, nodes = 3, racks = 1, dcs = 1), True),
        (topo(rf = 3, nodes = 5, racks = 1, dcs = 1), False),
        (topo(rf = 1, nodes = 4, racks = 2, dcs = 1), True),
        (topo(rf = 3, nodes = 6, racks = 2, dcs = 1), False),
        (topo(rf = 3, nodes = 6, racks = 3, dcs = 1), True),
        (topo(rf = 2, nodes = 8, racks = 4, dcs = 2), True)
    ])
async def test_restore_with_streaming_scopes(manager: ManagerClient, s3_server, topology_rf_validity):
    '''Check that restoring of a cluster with stream scopes works'''

    topology, rf_rack_valid_keyspaces = topology_rf_validity
    logger.info(f'Start cluster with {topology.nodes} nodes in {topology.dcs} DCs, {topology.racks} racks, rf_rack_valid_keyspaces: {rf_rack_valid_keyspaces}')

    servers, host_ids = await create_cluster(topology, rf_rack_valid_keyspaces, manager, logger, s3_server)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)
    cql = manager.get_cql()

    ks = 'ks'
    cf = 'cf'

    schema, keys, replication_opts = create_dataset(manager, ks, cf, topology, logger)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)

    logger.info(f'Backup to {snap_name}')
    prefix = f'{cf}/{snap_name}'
    async def do_backup(s):
        tid = await manager.api.backup(s.ip_addr, ks, cf, snap_name, s3_server.address, s3_server.bucket_name, prefix)
        status = await manager.api.wait_task(s.ip_addr, tid)
        assert (status is not None) and (status['state'] == 'done')

    await asyncio.gather(*(do_backup(s) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    logger.info(f'Restore')
    async def do_restore(s, toc_names, scope):
        logger.info(f'Restore {s.ip_addr} with {toc_names}, scope={scope}')
        tid = await manager.api.restore(s.ip_addr, ks, cf, s3_server.address, s3_server.bucket_name, prefix, toc_names, scope)
        status = await manager.api.wait_task(s.ip_addr, tid)
        assert (status is not None) and (status['state'] == 'done')

    scope,r_servers = compute_scope(topology, servers)

    await asyncio.gather(*(do_restore(s, sstables, scope) for s in r_servers))

    await check_data_is_back(manager, logger, cql, ks, cf, keys, servers, topology, r_servers, host_ids, scope)

@pytest.mark.asyncio
async def test_restore_with_non_existing_sstable(manager: ManagerClient, s3_server):
    '''Check that restore task fails well when given a non-existing sstable'''

    objconf = MinioServer.create_conf(s3_server.address, s3_server.port, s3_server.region)
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    cql = manager.get_cql()
    print('Create keyspace')
    ks, cf = create_ks_and_cf(cql)

    # The name must be parseable by sstable layer, yet such file shouldn't exist
    sstable_name = 'me-3gou_0fvw_4r94g2h8nw60b8ly4c-big-TOC.txt'
    tid = await manager.api.restore(server.ip_addr, ks, cf, s3_server.address, s3_server.bucket_name, 'no_such_prefix', [sstable_name])
    status = await manager.api.wait_task(server.ip_addr, tid)
    print(f'Status: {status}')
    assert 'state' in status and status['state'] == 'failed'
    assert 'error' in status and 'Not Found' in status['error']
