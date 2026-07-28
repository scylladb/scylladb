#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import os
import json

from test.pylib.manager_client import ManagerClient, ServerInfo
from test.cluster.object_store.test_backup import do_test_snapshot_on_all_nodes

logger = logging.getLogger(__name__)


async def get_snapshot_path(manager:ManagerClient, server, keyspace:str, table:str, snapshot_name:str):
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

async def get_snapshot_files(manager:ManagerClient, server, keyspace:str, table:str, snapshot_name:str):
    """Gets TOC files from server"""
    snapshot_path = await get_snapshot_path(manager, server, keyspace, table, snapshot_name)
    return [
        f.name for f in os.scandir(snapshot_path)
        if f.is_file() and f.name.endswith('TOC.txt')
    ]

async def get_snapshot_manifest(manager:ManagerClient, server, keyspace:str, table:str, snapshot_name:str):
    """Gets TOC files from server"""
    snapshot_path = await get_snapshot_path(manager, server, keyspace, table, snapshot_name)
    with open(os.path.join(snapshot_path, 'manifest.json'), encoding='utf-8') as f:
        return json.load(f)


async def test_snapshot_on_all_nodes(manager: ManagerClient):
    """
    Tests that a topology operation snapshot is done on all nodes,
    not just the initiator.
    """
    async def check(manager: ManagerClient, snapshot_name: str, ks: str, cf:str, servers: list[ServerInfo]):
        cql = manager.get_cql()
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
                        snapshot_name = '{snapshot_name}' AND \"keyspace\" = '{ks}' AND
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
                                snapshot_name = '{snapshot_name}' AND keyspace_name = '{ks}'
                                """))
        assert len(snap_ks) == 1

        snap_cf = list(cql.execute(f"""
                                SELECT * FROM system_distributed.snapshot_tables WHERE 
                                snapshot_name = '{snapshot_name}' AND keyspace_name = '{ks}' AND
                                table_name = '{cf}'
                                """))
        assert len(snap_cf) == 1

    await do_test_snapshot_on_all_nodes(manager, check)

