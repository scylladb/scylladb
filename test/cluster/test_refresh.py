#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

#!/usr/bin/env python3

import contextlib
import os
import logging
import asyncio
import pytest
import time
import random
import shutil
import uuid
from collections import defaultdict

from cassandra.cluster import ConsistencyLevel
from test.pylib.minio_server import MinioServer
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.object_storage import format_tuples
from test.cluster.object_store.test_backup import topo, take_snapshot, do_test_streaming_scopes
from test.cluster.util import new_test_keyspace
from test.pylib.rest_client import read_barrier
from test.pylib.util import unique_name, wait_for

logger = logging.getLogger(__name__)


async def wait_for_upload_dir_empty(upload_dir, timeout=30):
    '''
    Wait until the upload directory is empty with a timeout.
    SSTable unlinking is asynchronous and in rare situations, it can happen
    that not all sstables are deleted from the upload dir immediately after refresh is done.
    '''
    deadline = time.time() + timeout
    async def check_empty():
        files = os.listdir(upload_dir)
        if not files:
            return True
        return None
    await wait_for(check_empty, deadline, period=0.5)

class SSTablesOnLocalStorage:
    def __init__(self):
        self.tmpdir = f'tmpbackup-{str(uuid.uuid4())}'
        self.object_storage = None

    async def save_one(self, manager, s, ks, cf):
        workdir = await manager.server_get_workdir(s.server_id)
        cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
        tmpbackup = os.path.join(workdir, f'../{self.tmpdir}')
        os.makedirs(tmpbackup, exist_ok=True)

        snapshots_dir = os.path.join(f'{workdir}/data/{ks}', cf_dir, 'snapshots')
        snapshots_dir = os.path.join(snapshots_dir, os.listdir(snapshots_dir)[0])
        exclude_list = ['manifest.json', 'schema.cql']

        for item in os.listdir(snapshots_dir):
            src_path = os.path.join(snapshots_dir, item)
            dst_path = os.path.join(tmpbackup, item)
            if item not in exclude_list:
                shutil.copy2(src_path, dst_path)

    async def refresh_one(self, manager, s, ks, cf, toc_names, scope, primary_replica_only):
        # Get the list of toc_names that this node needs to load and find all sstables
        # that correspond to these toc_names, copy them to the upload directory and then
        # call refresh
        workdir = await manager.server_get_workdir(s.server_id)
        cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
        upload_dir = os.path.join(f'{workdir}/data/{ks}', cf_dir, 'upload')
        os.makedirs(upload_dir, exist_ok=True)
        tmpbackup = os.path.join(workdir, f'../{self.tmpdir}')
        for toc in toc_names:
            basename = toc.removesuffix('-TOC.txt')
            for item in os.listdir(tmpbackup):
                if item.startswith(basename):
                    src_path = os.path.join(tmpbackup, item)
                    dst_path = os.path.join(upload_dir, item)
                    shutil.copy2(src_path, dst_path)

        logger.info(f'Refresh {s.ip_addr} with {toc_names}, scope={scope}')
        await manager.api.load_new_sstables(s.ip_addr, ks, cf, scope=scope, primary_replica=primary_replica_only, load_and_stream=True)

    async def save(self, manager, servers, snap_name, prefix, ks, cf, logger):
        for s in servers:
            await self.save_one(manager, s, ks, cf)

    async def restore(self, manager, sstables_per_server, prefix, ks, cf, scope, primary_replica_only, logger):
        await asyncio.gather(*(self.refresh_one(manager, s, ks, cf, sstables, scope, primary_replica_only) for s, sstables in sstables_per_server.items()))

@pytest.mark.parametrize("topology", [
        topo(rf = 1, nodes = 3, racks = 1, dcs = 1),
        topo(rf = 3, nodes = 5, racks = 1, dcs = 1),
        topo(rf = 1, nodes = 4, racks = 2, dcs = 1),
        topo(rf = 3, nodes = 6, racks = 2, dcs = 1),
        topo(rf = 2, nodes = 8, racks = 4, dcs = 2)
    ])
async def test_refresh_with_streaming_scopes(build_mode: str, manager: ScyllaClusterManager, topology):
    '''Check that refreshing of a cluster with stream scopes works'''
    await do_test_streaming_scopes(build_mode, manager, topology, SSTablesOnLocalStorage())


async def test_refresh_deletes_uploaded_sstables(manager: ScyllaClusterManager):
    '''
    Check that refreshing a cluster deletes the sstable files from the upload directory after loading
    '''

    servers = await manager.servers_add(2)

    cql = manager.get_cql()

    await manager.disable_tablet_balancing()

    cf = 'cf'

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int)")
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, value) VALUES (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL
        keys = range(256)
        await asyncio.gather(*(cql.run_async(insert_stmt, (str(k), k)) for k in keys))

        await take_snapshot(ks, servers, manager, logger)

        dirs = defaultdict(dict)

        logger.info(f'Move sstables to tmp dir')
        tmpdir = f'tmpbackup-{str(uuid.uuid4())}'
        for s in servers:
            workdir = await manager.server_get_workdir(s.server_id)
            cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
            cf_dir = os.path.join(f'{workdir}/data/{ks}', cf_dir)
            tmpbackup = os.path.join(workdir, f'../{tmpdir}')
            dirs[s.server_id]["workdir"] = workdir
            dirs[s.server_id]["cf_dir"] = cf_dir
            dirs[s.server_id]["tmpbackup"] = tmpbackup
            os.makedirs(tmpbackup, exist_ok=True)

            snapshots_dir = os.path.join(cf_dir, 'snapshots')
            snapshots_dir = os.path.join(snapshots_dir, os.listdir(snapshots_dir)[0])
            exclude_list = ['manifest.json', 'schema.cql']

            for item in os.listdir(snapshots_dir):
                src_path = os.path.join(snapshots_dir, item)
                dst_path = os.path.join(tmpbackup, item)
                if item not in exclude_list:
                    shutil.copy2(src_path, dst_path)

        logger.info(f'Clear data by truncating')
        cql.execute(f'TRUNCATE TABLE {ks}.{cf};')

        logger.info(f'Copy sstables to upload dir (with shuffling)')
        shuffled = list(range(len(servers)))
        random.shuffle(shuffled)
        for i, s in enumerate(servers):
            other = servers[shuffled[i]]
            cf_dir = dirs[other.server_id]["cf_dir"]
            tmpbackup = dirs[s.server_id]["tmpbackup"]
            shutil.copytree(tmpbackup, os.path.join(cf_dir, 'upload'), dirs_exist_ok=True)

        logger.info(f'Refresh')
        await asyncio.gather(*(manager.api.load_new_sstables(s.ip_addr, ks, cf, scope='rack', load_and_stream=True) for s in servers))

        assert {row.pk for row in cql.execute(f"SELECT pk FROM {ks}.{cf}")} == {str(k) for k in keys}

        for s in servers:
            cf_dir = dirs[s.server_id]["cf_dir"]
            upload_dir = os.path.join(cf_dir, 'upload')
            assert os.path.exists(upload_dir)
            await wait_for_upload_dir_empty(upload_dir)

        shutil.rmtree(tmpbackup)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_refresh_does_not_claim_sstables_created_by_another_shard(manager: ScyllaClusterManager):
    '''
    A regular refresh mutates the level of every uploaded sstable, which hard-links
    it into a new generation in the very upload directory that all shards are
    listing.  Generations carry no shard affinity, so a shard whose listing is
    still running can claim another shard's in-flight sstable and schedule all of
    its components for removal.

    Park shard 1 at the beginning of its listing and shard 0 in the middle of its
    rewrites, so that shard 1 gets to list a directory full of unsealed sstables
    belonging to shard 0, and check that the refresh still succeeds.

    Tablets are disabled on purpose: with tablets, refresh is auto-promoted to
    load-and-stream, which does not mutate the sstable level and so never writes
    into the directory being listed.
    '''
    pause_scan = 'sstable_directory_pause_scan'
    pause_rewrite = 'pause_sstable_component_rewrite'
    cf = 'cf'
    # A high loading concurrency keeps many rewrites in flight, and thus many
    # unsealed sstables in the upload directory, when shard 0 parks.
    server = await manager.server_add(cmdline=['--smp=2'],
                                      config={'initial_sstable_loading_concurrency': 16})
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        # Leveled compaction with a tiny sstable size, so that the data ends up in
        # many sstables above level 0.  Level 0 sstables are not rewritten, and the
        # more of them are rewritten the likelier shard 1 is to claim one.
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int PRIMARY KEY, value blob) WITH compaction = "
                            "{'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 1}")
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, value) VALUES (?, ?)")
        keys = range(16384)
        for begin in range(0, len(keys), 1024):
            await asyncio.gather(*(cql.run_async(insert_stmt, (k, random.randbytes(1024)))
                                   for k in keys[begin:begin + 1024]))
            await manager.api.keyspace_flush(server.ip_addr, ks, cf)
        await manager.api.keyspace_compaction(server.ip_addr, ks, cf)

        async def sstables_above_level_zero():
            info = await manager.api.get_sstable_info(server.ip_addr, ks, cf)
            levels = [sst['level'] for entry in info for sst in entry['sstables']]
            logger.info(f'SSTable levels: {levels}')
            return True if len([l for l in levels if l > 0]) >= 8 else None
        await wait_for(sstables_above_level_zero, time.time() + 60)

        workdir = await manager.server_get_workdir(server.server_id)
        cf_dir = os.path.join(f'{workdir}/data/{ks}', os.listdir(f'{workdir}/data/{ks}')[0])
        upload_dir = os.path.join(cf_dir, 'upload')

        snap_name, _ = await take_snapshot(ks, [server], manager, logger)
        snapshot_dir = os.path.join(cf_dir, 'snapshots', snap_name)

        logger.info('Clear data by truncating')
        cql.execute(f'TRUNCATE TABLE {ks}.{cf};')

        logger.info(f'Copy sstables from {snapshot_dir} to {upload_dir}')
        os.makedirs(upload_dir, exist_ok=True)
        for item in os.listdir(snapshot_dir):
            if item not in ['manifest.json', 'schema.cql']:
                shutil.copy2(os.path.join(snapshot_dir, item), os.path.join(upload_dir, item))

        await manager.api.enable_injection(server.ip_addr, pause_scan, one_shot=False, parameters={'shard': '1'})
        await manager.api.enable_injection(server.ip_addr, pause_rewrite, one_shot=False)
        try:
            refresh = asyncio.create_task(
                manager.api.load_new_sstables(server.ip_addr, ks, cf, load_and_stream=False))
            # Both shards enter the scan injection; only shard 1 parks in it.
            await manager.api.wait_for_injection_enter(server.ip_addr, pause_scan, threshold=2)
            # Shard 0 is free to run ahead and park inside its rewrites, leaving
            # unsealed sstables in the upload directory.  With the listing and the
            # processing properly separated it cannot get that far: it waits for
            # shard 1 to finish listing first, so this wait times out.
            with contextlib.suppress(AssertionError):  # times out on a fixed build
                await manager.api.wait_for_injection_enter(server.ip_addr, pause_rewrite,
                                                           deadline=time.time() + 10)
            logger.info('Releasing the listing on shard 1')
            await manager.api.message_injection(server.ip_addr, pause_scan)
            # Give shard 1 the time to list the directory before the rewrites are
            # allowed to complete and seal.
            await asyncio.sleep(5)
            await manager.api.disable_injection(server.ip_addr, pause_rewrite)
            await refresh
        finally:
            await manager.api.disable_injection(server.ip_addr, pause_scan)
            await manager.api.disable_injection(server.ip_addr, pause_rewrite)

        assert {row.pk for row in cql.execute(f"SELECT pk FROM {ks}.{cf}")} == set(keys)
        await wait_for_upload_dir_empty(upload_dir)
