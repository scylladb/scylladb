#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

#!/usr/bin/env python3

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
from test.pylib.manager_client import ManagerClient
from test.pylib.object_storage import format_tuples
from test.cluster.object_store.test_backup import topo, take_snapshot, do_test_streaming_scopes
from test.cluster.util import new_test_keyspace, get_topology_version
from test.pylib.rest_client import read_barrier
from test.pylib.util import unique_name, wait_for, wait_for_cql_and_get_hosts

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
async def test_refresh_with_streaming_scopes(build_mode: str, manager: ManagerClient, topology):
    '''Check that refreshing of a cluster with stream scopes works'''
    await do_test_streaming_scopes(build_mode, manager, topology, SSTablesOnLocalStorage())


async def test_refresh_deletes_uploaded_sstables(manager: ManagerClient):
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
async def test_refresh_load_and_stream_releases_stale_topology_version(manager: ManagerClient):
    """
    Regression test for SCYLLADB-3336: load_and_stream() held a single erm
    (pinning its topology version) for the whole streaming operation,
    blocking concurrent topology changes' barrier_and_drain until streaming
    finished -- hours, for large data sets. Fixed by
    sstable_streamer::refresh_erm(), which re-fetches the erm between
    batches.

    Creates enough sstables for >1 batch, pauses load_and_stream right after
    it acquires its erm, bootstraps a node concurrently (its
    barrier_and_drain reports our erm's version as stale), then resumes
    load_and_stream and checks the erm is refreshed and the barrier
    completes promptly -- not only after every batch finishes streaming.
    """
    cmdline = ['--logger-log-level', 'sstables_loader=debug']
    server = await manager.server_add(cmdline=cmdline)

    cql = manager.get_cql()
    ks = unique_name("ks_")
    cf = "test"

    await cql.run_async(
        f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
        f"AND tablets = {{'enabled': false}}")
    try:
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int PRIMARY KEY, c int)")

        # One sstable per row: with num_sstables=20 > batch size (16), we get
        # more than one streaming batch.
        num_sstables = 20
        for pk in range(num_sstables):
            await cql.run_async(f"INSERT INTO {ks}.{cf} (pk, c) VALUES ({pk}, {pk})")
            await manager.api.flush_keyspace(server.ip_addr, ks)

        snap_name = unique_name("snap_")
        await manager.api.take_snapshot(server.ip_addr, ks, snap_name)

        workdir = await manager.server_get_workdir(server.server_id)
        cf_dir = os.listdir(f"{workdir}/data/{ks}")[0]
        cf_path = os.path.join(f"{workdir}/data/{ks}", cf_dir)
        upload_dir = os.path.join(cf_path, "upload")
        os.makedirs(upload_dir, exist_ok=True)
        snapshots_dir = os.path.join(cf_path, "snapshots", snap_name)
        exclude_list = ["manifest.json", "schema.cql"]
        for item in os.listdir(snapshots_dir):
            if item not in exclude_list:
                shutil.copy2(os.path.join(snapshots_dir, item), os.path.join(upload_dir, item))

        hosts = await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
        version_before = await get_topology_version(cql, hosts[0])

        await manager.api.enable_injection(server.ip_addr, "load_and_stream_before_streaming_batch", one_shot=True)
        server_log = await manager.server_open_log(server.server_id)
        log_mark = await server_log.mark()

        refresh_task = asyncio.create_task(
            manager.api.load_new_sstables(server.ip_addr, ks, cf, load_and_stream=True))
        await manager.api.wait_for_injection_enter(server.ip_addr, "load_and_stream_before_streaming_batch")
        logger.info("load_and_stream paused before its first batch, holding erm at topology version %d", version_before)

        # Bootstrapping bumps the topology version and triggers a
        # barrier_and_drain that must wait for our paused load_and_stream's
        # stale erm.
        bootstrap_task = asyncio.create_task(manager.server_add())

        await server_log.wait_for(
            r"Got raft_topology_cmd::barrier_and_drain,.*stale versions \(version: use_count\):.*"
            rf"\b{version_before}\b",
            from_mark=log_mark, timeout=60)
        logger.info("bootstrap's barrier_and_drain reported our erm's topology version %d as stale", version_before)

        # Resume load_and_stream; refresh_erm() runs first and releases the
        # stale erm.
        await manager.api.message_injection(server.ip_addr, "load_and_stream_before_streaming_batch")

        _, matches = await server_log.wait_for(
            r"refreshed effective_replication_map for table .* to topology version (\d+)",
            from_mark=log_mark, timeout=30)
        refreshed_version = int(matches[0][1].group(1))
        assert refreshed_version > version_before, \
            f"expected refresh_erm() to move past the stale version {version_before}, got {refreshed_version}"

        # Must complete promptly, not after load_and_stream streams every
        # remaining batch.
        await server_log.wait_for(r"raft_topology_cmd::barrier_and_drain version \d+: done",
                                   from_mark=log_mark, timeout=30)
        await asyncio.wait_for(bootstrap_task, timeout=60)
        logger.info("bootstrap completed")

        await asyncio.wait_for(refresh_task, timeout=60)
        logger.info("load_and_stream completed")

        assert {row.pk for row in cql.execute(f"SELECT pk FROM {ks}.{cf}")} == set(range(num_sstables))
    finally:
        await cql.run_async(f"DROP KEYSPACE IF EXISTS {ks}")
