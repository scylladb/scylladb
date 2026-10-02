#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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

from test.pylib.minio_server import MinioServer
from test.pylib.manager_client import ManagerClient
from test.cluster.object_store.conftest import format_tuples
from test.cluster.object_store.test_backup import topo, create_cluster, take_snapshot, create_dataset, check_data_is_back, do_load_sstables, mark_all_logs, check_mutation_replicas
from test.cluster.util import wait_for_cql_and_get_hosts
from test.pylib.rest_client import read_barrier
from test.pylib.util import unique_name

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.parametrize("topology_rf_validity", [
        (topo(rf = 1, nodes = 3, racks = 1, dcs = 1), True),
        (topo(rf = 3, nodes = 5, racks = 1, dcs = 1), False),
        (topo(rf = 1, nodes = 4, racks = 2, dcs = 1), True),
        (topo(rf = 3, nodes = 6, racks = 2, dcs = 1), False),
        (topo(rf = 2, nodes = 8, racks = 4, dcs = 2), True)
    ])
async def test_refresh_with_streaming_scopes(build_mode: str, manager: ManagerClient, topology_rf_validity):
    '''
    Check that refreshing a cluster with stream scopes works

    This test creates a cluster specified by the topology parameter above,
    configurable number of nodes, tacks, datacenters, and replication factor.

    It creates a dataset, takes a snapshot and copies the sstables of all nodes to a temporary
    location. It then truncates the table so all sstables are gone, copies all the sstables into
    each node's upload directory, and refreshes the nodes given the scope passed as the test parameter.

    The test then performs two types of checks:
    1) Check that the data is back in the table by getting all mutations from the nodes and checking
    that a random sample of them contains the expected key and that they are replicated according to RF * DCS factor.
    2) Check that the streaming communication between nodes is as expected according to the scope parameter of the test.
    This stage parses the logs and checks that the data was streamed to nodes within the configured scope.
    '''
 
    topology, rf_rack_valid_keyspaces = topology_rf_validity

    servers, host_ids = await create_cluster(topology, rf_rack_valid_keyspaces, manager, logger)

    cql = manager.get_cql()

    await manager.disable_tablet_balancing()

    ks = 'ks'
    cf = 'cf'
    _, keys, _ = await create_dataset(manager, ks, cf, topology, logger, num_keys=10, min_tablet_count=5)

    # validate replicas assertions hold on fresh dataset
    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, scope=None, primary_replica_only=False, expected_replicas = None)

    _, sstables = await take_snapshot(ks, servers, manager, logger)

    logger.info(f'Move sstables to tmp dir')
    tmpdir = f'tmpbackup-{str(uuid.uuid4())}'
    for s in servers:
        workdir = await manager.server_get_workdir(s.server_id)
        cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
        tmpbackup = os.path.join(workdir, f'../{tmpdir}')
        os.makedirs(tmpbackup, exist_ok=True)

        snapshots_dir = os.path.join(f'{workdir}/data/{ks}', cf_dir, 'snapshots')
        snapshots_dir = os.path.join(snapshots_dir, os.listdir(snapshots_dir)[0])
        exclude_list = ['manifest.json', 'schema.cql']

        for item in os.listdir(snapshots_dir):
            src_path = os.path.join(snapshots_dir, item)
            dst_path = os.path.join(tmpbackup, item)
            if item not in exclude_list:
                shutil.copy2(src_path, dst_path)

    logger.info(f'Refresh')
    async def do_refresh(manager, logger, ks, cf, s, toc_names, scope, primary_replica_only, _prefix=None, _object_storage=None):
        # Get the list of toc_names that this node needs to load and find all sstables
        # that correspond to these toc_names, copy them to the upload directory and then
        # call refresh
        workdir = await manager.server_get_workdir(s.server_id)
        cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
        upload_dir = os.path.join(f'{workdir}/data/{ks}', cf_dir, 'upload')
        os.makedirs(upload_dir, exist_ok=True)
        tmpbackup = os.path.join(workdir, f'../{tmpdir}')
        for toc in toc_names:
            basename = toc.removesuffix('-TOC.txt')
            for item in os.listdir(tmpbackup):
                if item.startswith(basename):
                    src_path = os.path.join(tmpbackup, item)
                    dst_path = os.path.join(upload_dir, item)
                    shutil.copy2(src_path, dst_path)

        logger.info(f'Refresh {s.ip_addr} with {toc_names}, scope={scope}')
        await manager.api.load_new_sstables(s.ip_addr, ks, cf, scope=scope, primary_replica=primary_replica_only, load_and_stream=True)

    scopes = ['rack', 'dc'] if build_mode == 'debug' else ['all', 'dc', 'rack', 'node']
    for scope in scopes:
        # We can support rack-aware restore with rack lists, if we restore the rack-list per dc as it was at backup time.
        # Otherwise, with numeric replication_factor we'd pick arbitrary subset of the racks when the keyspace
        # is initially created and an arbitrary subset or the rack at restore time.
        if scope == 'rack' and topology.rf != topology.racks:
            logger.info(f'Skipping scope={scope} test since rf={topology.rf} != racks={topology.racks} and it cannot be supported with numeric replication_factor')
            continue
        pros = [False] if scope == 'node' else [False, True]
        for pro in pros:
            logger.info(f'Clear data by truncating, make sure the tablets map stays intact')
            cql.execute(f'TRUNCATE TABLE {ks}.{cf};')

            log_marks = await mark_all_logs(manager, servers)

            await do_load_sstables(ks, cf, servers, topology, sstables, scope, manager, logger, primary_replica_only=pro, load_fn=do_refresh)

            await check_data_is_back(manager, logger, cql, ks, cf, keys, servers, topology, host_ids, scope, primary_replica_only=pro, log_marks=log_marks)

    shutil.rmtree(tmpbackup)

async def test_refresh_deletes_uploaded_sstables(manager: ManagerClient):
    '''
    Check that refreshing a cluster deletes the sstable files from the upload directory after loading
    '''

    topology = topo(rf = 1, nodes = 2, racks = 1, dcs = 1)

    servers, host_ids = await create_cluster(topology, True, manager, logger)

    cql = manager.get_cql()

    await manager.disable_tablet_balancing()

    ks = 'ks'
    cf = 'cf'
    _, keys, _ = await create_dataset(manager, ks, cf, topology, logger)

    _, sstables = await take_snapshot(ks, servers, manager, logger)

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
    async def do_refresh(s, toc_names, scope):
        logger.info(f'Refresh {s.ip_addr} with {toc_names}, scope={scope}')
        await manager.api.load_new_sstables(s.ip_addr, ks, cf, scope=scope, load_and_stream=True)

    scope = 'rack'
    r_servers = servers

    await asyncio.gather(*(do_refresh(s, sstables, scope) for s in r_servers))

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf)

<<<<<<< HEAD
    for s in r_servers:
        cf_dir = dirs[s.server_id]["cf_dir"]
        files = os.listdir(os.path.join(cf_dir, 'upload'))
        assert files == [], f'Upload dir not empty on server {s.server_id}: {files}'

    shutil.rmtree(tmpbackup)
||||||| parent of 5cbd11fd43 (test/cluster: cover refresh racing with its own sstable rewrites)
        shutil.rmtree(tmpbackup)
=======
        shutil.rmtree(tmpbackup)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_refresh_does_not_claim_sstables_created_by_another_shard(manager: ManagerClient):
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
    scan_done = 'sstable_directory_scan_done'
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

        async def sstable_levels():
            info = await manager.api.get_sstable_info(server.ip_addr, ks, cf)
            return [sst['level'] for entry in info for sst in entry['sstables']]

        async def enough_sstables_above_level_zero():
            return True if len([l for l in await sstable_levels() if l > 0]) >= 8 else None
        await wait_for(enough_sstables_above_level_zero, time.time() + 60)

        # Freeze the sstable set: leveled compaction keeps running in the
        # background, and what is checked here has to be what gets snapshotted.
        await manager.api.disable_autocompaction(server.ip_addr, ks, cf)
        levels = await sstable_levels()
        logger.info(f'SSTable levels: {levels}')
        # Only sstables above level 0 are rewritten by refresh, so without them
        # the test would not exercise anything.
        assert len([l for l in levels if l > 0]) >= 8, f'expected sstables above level 0, got {levels}'

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
        await manager.api.enable_injection(server.ip_addr, scan_done, one_shot=False)
        await manager.api.enable_injection(server.ip_addr, pause_rewrite, one_shot=False)
        try:
            refresh = asyncio.create_task(
                manager.api.load_new_sstables(server.ip_addr, ks, cf, load_and_stream=False))
            # Both shards enter the scan injection; only shard 1 parks in it.
            await manager.api.wait_for_injection_enter(server.ip_addr, pause_scan, threshold=2)
            # Shard 0 is free to run ahead and park inside its rewrites, leaving
            # unsealed sstables -- a TemporaryTOC and no TOC -- in the upload
            # directory.  That is the state shard 1 has to list, and every one of
            # them is a chance for it to claim a generation that hashes to it, so
            # wait for a good number rather than for the first.  With the listing
            # and the processing properly separated shard 0 cannot get that far:
            # it waits for shard 1 to finish listing first, so this times out.
            def unsealed_sstables():
                return [f for f in os.listdir(upload_dir) if f.endswith('-TOC.txt.tmp')]

            async def enough_unsealed_sstables():
                return True if len(unsealed_sstables()) >= 6 else None
            with contextlib.suppress(AssertionError):  # times out on a fixed build
                await wait_for(enough_unsealed_sstables, time.time() + 10)
            logger.info(f'{len(unsealed_sstables())} unsealed sstables in the upload dir')
            logger.info('Releasing the listing on shard 1')
            await manager.api.message_injection(server.ip_addr, pause_scan)
            # Shard 0 is done listing long before shard 1 is released, so both
            # shards having reached the end of the listing means shard 1 has
            # walked the directory the rewrites are parked in.  Only then let
            # them complete and seal.
            await manager.api.wait_for_injection_enter(server.ip_addr, scan_done, threshold=2)
            await manager.api.disable_injection(server.ip_addr, pause_rewrite)
            await refresh
        finally:
            await manager.api.disable_injection(server.ip_addr, pause_scan)
            await manager.api.disable_injection(server.ip_addr, scan_done)
            await manager.api.disable_injection(server.ip_addr, pause_rewrite)

        assert {row.pk for row in cql.execute(f"SELECT pk FROM {ks}.{cf}")} == set(keys)
        await wait_for_upload_dir_empty(upload_dir)
>>>>>>> 5cbd11fd43 (test/cluster: cover refresh racing with its own sstable rewrites)
