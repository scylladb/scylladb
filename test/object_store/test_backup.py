#!/usr/bin/env python3

import asyncio
import os
import requests
import pytest
import logging
import time
import random

from test.pylib.manager_client import ManagerClient
from test.object_store.conftest import format_tuples
from test.object_store.conftest import get_s3_resource
from test.topology.conftest import skip_mode
from test.topology.util import wait_for_cql_and_get_hosts
from test.pylib.rest_client import read_barrier
from test.pylib.util import unique_name
from cassandra.cluster import ConsistencyLevel, Session
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

async def prepare_snapshot_for_backup(manager: ManagerClient, server, snap_name = 'backup'):
    cql = manager.get_cql()
    workdir = await manager.server_get_workdir(server.server_id)
    print(f'Create keyspace')
    ks, cf = create_ks_and_cf(cql)
    print('Flush keyspace')
    await manager.api.flush_keyspace(server.ip_addr, ks)
    print('Take keyspace snapshot')
    await manager.api.take_snapshot(server.ip_addr, ks, snap_name)

    return ks, cf

@pytest.mark.asyncio
async def test_simple_backup(manager: ManagerClient, s3_server):
    '''check that backing up a snapshot for a keyspace works'''

    cfg = {'enable_user_defined_functions': False,
           'object_storage_config_file': str(s3_server.config_file),
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = [ '--logger-log-level', 'snapshots=trace:task_manager=trace' ]
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

    objects = set([ o.key for o in get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.all() ])
    for f in files:
        print(f'Check {f} is in backup')
        assert f'{prefix}/{f}' in objects

    # Check that task runs in the streaming sched group
    log = await manager.server_open_log(server.server_id)
    res = await log.grep(r'INFO.*\[shard [0-9]:([a-z]+)\] .* Backup sstables from .* to')
    assert len(res) == 1 and res[0][1].group(1) == 'strm'


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_backup_is_abortable(manager: ManagerClient, s3_server):
    '''check that backing up a snapshot for a keyspace works'''

    cfg = {'enable_user_defined_functions': False,
           'object_storage_config_file': str(s3_server.config_file),
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = [ '--logger-log-level', 'snapshots=trace:task_manager=trace' ]
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 1

    await manager.api.enable_injection(server.ip_addr, "backup_task_pause", one_shot=True)
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    print('Backup snapshot')
    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', s3_server.address, s3_server.bucket_name, prefix)

    print(f'Started task {tid}, aborting it early')
    await log.wait_for('backup task: waiting', from_mark=mark)
    await manager.api.abort_task(server.ip_addr, tid)
    await manager.api.message_injection(server.ip_addr, "backup_task_pause")
    status = await manager.api.wait_task(server.ip_addr, tid)
    print(f'Status: {status}')
    assert (status is not None) and (status['state'] == 'failed')

    objects = set([ o.key for o in get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.all() ])
    uploaded_count = 0
    for f in files:
        print(f'Check {f} is in backup')
        if f'{prefix}/{f}' in objects:
            uploaded_count += 1
    assert uploaded_count > 0 and uploaded_count < len(files)


@pytest.mark.asyncio
async def test_simple_backup_and_restore(manager: ManagerClient, s3_server):
    '''check that restoring from backed up snapshot for a keyspace:table works'''

    cfg = {'enable_user_defined_functions': False,
           'object_storage_config_file': str(s3_server.config_file),
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = [ '--logger-log-level', 'sstables_loader=debug:sstable_directory=trace:snapshots=trace:s3=trace:sstable=debug:http=debug' ]
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
        return [ f for f in os.scandir(f'{workdir}/data/{ks}/{cf_dir}') if f.is_file() ]

    orig_res = cql.execute(f"SELECT * FROM {ks}.{cf}")
    orig_rows = { x.name: x.value for x in orig_res }

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
    toc_names = [f'{suffix}/{entry.name}' for entry in list_sstables() if entry.name.endswith('TOC.txt')]

    prefix = f'{cf}/{snap_name}'
    tid = await manager.api.backup(server.ip_addr, ks, cf, snap_name, s3_server.address, s3_server.bucket_name, f'{prefix}/{suffix}')
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')

    print(f'Drop the table data and validate it\'s gone')
    cql.execute(f"TRUNCATE TABLE {ks}.{cf};")
    files = list_sstables()
    assert len(files) == 0
    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    assert not res
    objects = set([ o.key for o in get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.filter(Prefix=prefix) ])
    assert len(objects) > 0

    print(f'Try to restore')
    tid = await manager.api.restore(server.ip_addr, ks, cf, s3_server.address, s3_server.bucket_name, prefix, toc_names)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')
    print(f'Check that sstables came back')
    files = list_sstables()
    assert len(files) > 0
    print(f'Check that data came back too')
    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    rows = { x.name: x.value for x in res }
    assert rows == orig_rows, "Unexpected table contents after restore"

    print(f'Check that backup files are still there') # regression test for #20938
    post_objects = set([ o.key for o in get_s3_resource(s3_server).Bucket(s3_server.bucket_name).objects.filter(Prefix=prefix) ])
    assert objects == post_objects


# Helper class to parametrize the test below
class topo:
    def __init__(self, rf, nodes, racks, dcs):
        self.rf = rf
        self.nodes = nodes
        self.racks = racks
        self.dcs = dcs

@pytest.mark.asyncio
@pytest.mark.parametrize("topology", [
        topo(rf = 1, nodes = 3, racks = 1, dcs = 1),
        topo(rf = 3, nodes = 5, racks = 1, dcs = 1),
        topo(rf = 1, nodes = 4, racks = 2, dcs = 1),
        topo(rf = 3, nodes = 6, racks = 2, dcs = 1),
        topo(rf = 3, nodes = 6, racks = 3, dcs = 1),
        topo(rf = 2, nodes = 8, racks = 4, dcs = 2)
    ])
async def test_restore_with_streaming_scopes(manager: ManagerClient, s3_server, topology):
    '''Check that restoring of a cluster with stream scopes works'''

    logger.info(f'Start cluster with {topology.nodes} nodes in {topology.dcs} DCs, {topology.racks} racks')
    cfg = { 'object_storage_config_file': str(s3_server.config_file), 'task_ttl_in_seconds': 300 }
    cmd = [ '--logger-log-level', 'sstables_loader=debug:sstable_directory=trace:snapshots=trace:s3=trace:sstable=debug:http=debug' ]
    servers = []
    for s in range(topology.nodes):
        dc = f'dc{s % topology.dcs}'
        rack = f'rack{s % topology.racks}'
        s = await manager.server_add(config=cfg, cmdline=cmd, property_file={'dc': dc, 'rack': rack})
        logger.info(f'Created node {s.ip_addr} in {dc}.{rack}')
        servers.append(s)

    cql = manager.get_cql()

    logger.info(f'Create keyspace, rf={topology.rf}')
    keys = range(256)
    ks = 'ks'
    cf = 'cf'
    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy', 'replication_factor': f'{topology.rf}'})
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))

    schema = f"CREATE TABLE {ks}.{cf} ( pk int primary key, value text );"
    cql.execute(schema)
    for k in keys:
        cql.execute(f"INSERT INTO {ks}.{cf} ( pk, value ) VALUES ({k}, '{k}');")

    logger.info(f'Collect sstables lists')
    sstables = []
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)
        workdir = await manager.server_get_workdir(s.server_id)
        cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
        tocs = [ f.name for f in os.scandir(f'{workdir}/data/{ks}/{cf_dir}') if f.is_file() and f.name.endswith('TOC.txt') ]
        logger.info(f'Collected sstables from {s.ip_addr}: {tocs}')
        sstables.append(tocs)

    snap_name = unique_name('backup_')
    logger.info(f'Backup to {snap_name}')
    prefix = f'{cf}/{snap_name}'
    async def do_backup(s):
        await manager.api.take_snapshot(s.ip_addr, ks, snap_name)
        tid = await manager.api.backup(s.ip_addr, ks, cf, snap_name, s3_server.address, s3_server.bucket_name, prefix)
        await manager.api.wait_task(s.ip_addr, tid)

    await asyncio.gather(*(do_backup(s) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    logger.info(f'Restore')
    async def do_restore(s, toc_names, scope):
        logger.info(f'Restore {s.ip_addr} with {toc_names}, scope={scope}')
        tid = await manager.api.restore(s.ip_addr, ks, cf, s3_server.address, s3_server.bucket_name, prefix, toc_names, scope)
        await manager.api.wait_task(s.ip_addr, tid)

    def merge_tocs(sstables, step):
        merged = []
        # Servers and corresponding sstable lists are collected like this
        #   server0, dc0, rack0
        #   server1, dc1, rack1
        #   server2, dc0, rack2
        #   server3, dc1, rack3
        #   server4, dc0, rack0
        #   server5, dc1, rack1
        #   server6, dc0, rack2
        #   server7, dc1, rack3
        # So to collect tocs from e.g. each DC we need to get all even ones
        # in [0] and all odd in [1]
        # Similarly, collecting tocs from each rack means putting 0th, 4th, ...
        # in [0], 1st, 5th, ... in [1] and so on
        for i in range(step):
            l = [ toc for l in sstables[i::step] for toc in l ]
            merged.append(l)
        return merged

    if topology.dcs > 1:
        scope = 'dc'
        r_servers = servers[:topology.dcs]
        r_tocs = merge_tocs(sstables, topology.dcs)
    elif topology.racks > 1:
        scope = 'rack'
        r_servers = servers[:topology.racks]
        r_tocs = merge_tocs(sstables, topology.racks)
    else:
        scope = 'node'
        r_servers = servers
        r_tocs = sstables

    await asyncio.gather(*(do_restore(r[0], r[1], scope) for r in zip(r_servers, r_tocs)))

    logger.info(f'Check the data is back')
    async def check_mutations(server, key):
        host = await wait_for_cql_and_get_hosts(cql, [server], time.time() + 30)
        await read_barrier(manager.api, server.ip_addr)  # scylladb/scylladb#18199
        res = await cql.run_async(f"SELECT partition_region FROM MUTATION_FRAGMENTS({ks}.{cf}) WHERE pk={key}", host=host[0])
        for fragment in res:
            if fragment.partition_region == 0: # partition start
                return True
        return False

    for k in random.sample(keys, 17):
        res = await asyncio.gather(*(check_mutations(s, k) for s in servers))
        assert res.count(True) == topology.rf * topology.dcs
