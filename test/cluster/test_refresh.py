#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
from test.cluster.object_store.conftest import format_tuples
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

@pytest.mark.asyncio
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
