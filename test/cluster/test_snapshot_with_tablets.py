#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import itertools
import logging
import os

from test.cqlpy import nodetool
from test.pylib.manager_client import ManagerClient
from test.cluster.object_store.test_backup import create_cluster, topo
from test.cluster.util import new_test_keyspace, new_test_table, unique_name
from cassandra import ConsistencyLevel
import pytest

logger = logging.getLogger(__name__)

async def get_snapshot_files(manager:ManagerClient, server, keyspace:str, table:str, snapshot_name:str):
    """Gets TOC files from server"""
    workdir = await manager.server_get_workdir(server.server_id)
    data_path = os.path.join(workdir, 'data', keyspace)
    cf_dirs = os.listdir(data_path)
    # Assumes that there is only one column family directory for table under the keyspace.
    for cf_dir in cf_dirs:
        if cf_dir.startswith(table):
            snapshot_path = os.path.join(data_path, cf_dir, 'snapshots', snapshot_name)
            return [
                f.name for f in os.scandir(snapshot_path)
                if f.is_file() and f.name.endswith('TOC.txt')
            ]
    raise RuntimeError(f"No column family directories found in {data_path} for {table}")

async def prepare_write_workload(cql, table_name, flush=True, n: int = None):
    """write some data"""
    keys = list(range(n if n else 100))
    c1_values = ['value1']
    c2_values = ['value2']

    statement = cql.prepare(f"INSERT INTO {table_name} (key, c1, c2) VALUES (?, ?, ?)")
    statement.consistency_level = ConsistencyLevel.ALL

    await asyncio.gather(*[cql.run_async(statement, params) for params in
                           list(map(lambda x, y, z: [x, y, z], keys,
                                    itertools.cycle(c1_values),
                                    itertools.cycle(c2_values)))]
                                    )

    if flush:
        nodetool.flush(cql, table_name)

@pytest.mark.asyncio
async def test_snapshot_on_all_nodes(manager: ManagerClient):
    """
    Tests that a topology operation snapshot is done on all nodes,
    not just the initiator.
    """
    topology = topo(rf = 3, nodes = 3, racks = 3, dcs = 1)

    servers, host_ids = await create_cluster(topology, True, manager, logger)

    snapshot_name = unique_name('snap_')

    async with new_test_keyspace(manager, f"WITH REPLICATION = {{ 'replication_factor' : {topology.rf} }} AND tablets = {{'initial': 20 }}") as ks:
        async with new_test_table(manager, ks, "key int, c1 text, c2 text, PRIMARY KEY (key)", "") as tbl:
            cf = tbl.split('.')[1]
            await prepare_write_workload(manager.get_cql(), tbl, flush=False)
            await manager.api.take_cluster_snapshot(servers[0].ip_addr, ks, tag=snapshot_name, tables=[cf])
            try:
                # Collect snapshot files from each server
                for s in servers:
                    files = await get_snapshot_files(manager, s, ks, cf, snapshot_name)
                    assert len(files) > 0
            finally:
                #todo: clear snapshot
                pass
