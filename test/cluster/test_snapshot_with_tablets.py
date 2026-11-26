#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import os
import json

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo
from test.cluster.object_store.test_backup import do_test_snapshot_on_all_nodes, create_cluster, topo
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.util import unique_name

logger = logging.getLogger(__name__)


async def get_snapshot_path(manager:ScyllaClusterManager, server, keyspace:str, table:str, snapshot_name:str):
    """Gets snapshot path files for server and snapshot"""
    workdir = await manager.server_get_workdir(server.server_id)
    data_path = os.path.join(workdir, 'data', keyspace)
    cf_dirs = os.listdir(data_path)
    # Assumes that there is only one column family directory for table under the keyspace.
    for cf_dir in cf_dirs:
        if cf_dir.startswith(table):
            snapshot_path = os.path.join(data_path, cf_dir, 'snapshots', snapshot_name)
            return snapshot_path
    raise RuntimeError(f"No column family directories found in {data_path} for {table}")

async def get_snapshot_files(manager:ScyllaClusterManager, server, keyspace:str, table:str, snapshot_name:str):
    """Gets TOC files from server"""
    snapshot_path = await get_snapshot_path(manager, server, keyspace, table, snapshot_name)
    return [
        f.name for f in os.scandir(snapshot_path)
        if f.is_file() and f.name.endswith('TOC.txt')
    ]

async def get_snapshot_manifest(manager:ScyllaClusterManager, server, keyspace:str, table:str, snapshot_name:str):
    """Gets TOC files from server"""
    snapshot_path = await get_snapshot_path(manager, server, keyspace, table, snapshot_name)
    with open(os.path.join(snapshot_path, 'manifest.json'), encoding='utf-8') as f:
        return json.load(f)


async def test_snapshot_on_all_nodes(manager: ScyllaClusterManager):
    """
    Tests that a topology operation snapshot is done on all nodes,
    not just the initiator.
    """
    async def check(manager: ScyllaClusterManager, snapshot_name: str, ks: str, cf:str, servers: list[ServerInfo]):
        cql = manager.get_cql()
        # A cluster snapshot writes its catalog rows under the committed
        # attempt's name, "{tag}/{attempt}"; the marker keeps the tag.
        committed = [r.attempt for r in cql.execute(
            f"SELECT attempt, committed FROM system_distributed.snapshot_attempts WHERE tag = '{snapshot_name}'") if r.committed]
        assert len(committed) == 1, f'expected one committed attempt for {snapshot_name}, got {committed}'
        rows_name = f'{snapshot_name}/{committed[0]}'
        # Collect snapshot files from each server
        for s in servers:
            files = await get_snapshot_files(manager, s, ks, cf, snapshot_name)
            assert len(files) > 0
            manifest = await get_snapshot_manifest(manager, s, ks, cf, snapshot_name)

            assert len(manifest['tablets'])
            tablets = { t['id']: t for t in manifest['tablets'] }
            for sst in manifest['sstables']:
                assert sst['tablet_id'] is not None
                assert tablets[sst['tablet_id']]

            # check sstables were added to sys_dist
            sstables = list(cql.execute(f"""
                        SELECT * FROM system_distributed.snapshot_sstables WHERE 
                        snapshot_name = '{rows_name}' AND \"keyspace\" = '{ks}' AND
                        \"table\" = '{cf}' AND datacenter = '{s.datacenter}' AND
                        rack = '{s.rack}'
                        """))
            assert len(sstables) >= len(files)
            tocs = set([t.toc_name for t in sstables])
            for f in files:
                assert f in tocs
            # todo: filter by actual host

        snap = list(cql.execute(f"""
                                SELECT * FROM system_distributed.snapshots WHERE 
                                name = '{snapshot_name}'
                                """))
        assert len(snap) == 1
        snap_ks = list(cql.execute(f"""
                                SELECT * FROM system_distributed.snapshot_keyspaces WHERE 
                                snapshot_name = '{rows_name}' AND keyspace_name = '{ks}'
                                """))
        assert len(snap_ks) == 1

        snap_cf = list(cql.execute(f"""
                                SELECT * FROM system_distributed.snapshot_tables WHERE 
                                snapshot_name = '{rows_name}' AND keyspace_name = '{ks}' AND
                                table_name = '{cf}'
                                """))
        assert len(snap_cf) == 1

    await do_test_snapshot_on_all_nodes(manager, check)


async def test_list_cluster_snapshots(manager: ScyllaClusterManager):
    """
    Tests that every node lists the cluster snapshots with the tables each of
    them contains, and that a snapshot taken on one node is not listed.
    """
    servers, _ = await create_cluster(topo(rf=3, nodes=3, racks=3, dcs=1), manager, logger)

    def listed(snapshots):
        return {s['tag']: {(t['keyspace'], t['table']) for t in s['tables']} for s in snapshots}

    assert await manager.api.list_cluster_snapshots(servers[0].ip_addr) == []

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : 3 } AND tablets = { 'initial': 2 }") as ks:
        async with new_test_table(manager, ks, "pk int primary key") as t1, new_test_table(manager, ks, "pk int primary key") as t2:
            cf1, cf2 = t1.split('.')[1], t2.split('.')[1]
            whole_keyspace, one_table, node_local = unique_name('snap_'), unique_name('snap_'), unique_name('snap_')
            await manager.api.take_cluster_snapshot(servers[0].ip_addr, ks, tag=whole_keyspace)
            await manager.api.take_cluster_snapshot(servers[1].ip_addr, ks, tag=one_table, tables=[cf2])
            await manager.api.take_snapshot(servers[0].ip_addr, ks, node_local)

            for s in servers:
                snapshots = await manager.api.list_cluster_snapshots(s.ip_addr)
                assert listed(snapshots) == {
                    whole_keyspace: {(ks, cf1), (ks, cf2)},
                    one_table: {(ks, cf2)},
                }
                assert all(snapshot['created_at'] for snapshot in snapshots)

