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

from test.pylib.minio_server import MinioServer
from test.pylib.manager_client import ManagerClient
from test.cluster.object_store.conftest import format_tuples
from test.cluster.object_store.test_backup import topo, create_cluster, take_snapshot, create_dataset, check_data_is_back, do_load_sstables, mark_all_logs, create_schema, check_mutation_replicas
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
        (topo(rf = 3, nodes = 6, racks = 3, dcs = 1), True),
        (topo(rf = 2, nodes = 8, racks = 4, dcs = 2), True)
    ])
async def test_refresh_with_streaming_scopes(manager: ManagerClient, topology_rf_validity):
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

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    ks = 'ks'
    cf = 'cf'
    _, keys, _ = await create_dataset(manager, ks, cf, topology, logger, num_keys=10000, min_tablet_count=512)

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

    for scope in ['all', 'dc', 'rack', 'node']:
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
