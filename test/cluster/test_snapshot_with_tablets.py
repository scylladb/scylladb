#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import os
import json
import uuid

import pytest
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import HTTPError
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

async def snapshot_dirs_of(manager: ScyllaClusterManager, servers, keyspace: str, table: str, snapshot_name: str):
    """The snapshot directories of the table on the given servers, those that exist"""
    paths = [await get_snapshot_path(manager, s, keyspace, table, snapshot_name) for s in servers]
    return [p for p in paths if os.path.isdir(p)]

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


async def test_clear_cluster_snapshot_table(manager: ScyllaClusterManager):
    """
    Tests that clearing one table of a cluster snapshot removes that table's
    snapshot directories and catalog rows on every node and keeps the other
    table, and that clearing the last table clears the snapshot.
    """
    servers, _ = await create_cluster(topo(rf=3, nodes=3, racks=3, dcs=1), manager, logger)
    cql = manager.get_cql()

    def listed(snapshots):
        return {s['tag']: {(t['keyspace'], t['table']) for t in s['tables']} for s in snapshots}

    async def clear(tag, ks, cf):
        tid = await manager.api.clear_cluster_snapshot(servers[0].ip_addr, tag, keyspace=ks, table=cf)
        status = await manager.api.wait_task(servers[0].ip_addr, tid)
        assert status is not None and status.get('state') == 'done', f'Clear of {ks}.{cf} in {tag}: {status}'

    def catalog_rows(name, ks, cf):
        rows = list(cql.execute(f"SELECT * FROM system_distributed.snapshot_tables WHERE snapshot_name = '{name}'"
                                f" AND keyspace_name = '{ks}' AND table_name = '{cf}'"))
        for s in servers:
            rows += list(cql.execute(f"SELECT * FROM system_distributed.snapshot_sstables WHERE snapshot_name = '{name}'"
                                     f" AND \"keyspace\" = '{ks}' AND \"table\" = '{cf}'"
                                     f" AND datacenter = '{s.datacenter}' AND rack = '{s.rack}'"))
            rows += list(cql.execute(f"SELECT * FROM system_distributed.snapshot_tablets WHERE snapshot_name = '{name}'"
                                     f" AND keyspace_name = '{ks}' AND table_name = '{cf}' AND datacenter = '{s.datacenter}'"))
        return rows

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : 3 } AND tablets = { 'initial': 2 }") as ks:
        async with new_test_table(manager, ks, "pk int primary key") as t1, new_test_table(manager, ks, "pk int primary key") as t2:
            cf1, cf2 = t1.split('.')[1], t2.split('.')[1]
            for t in (t1, t2):
                await asyncio.gather(*(cql.run_async(f"INSERT INTO {t} (pk) VALUES ({i})") for i in range(20)))
            await asyncio.gather(*(manager.api.flush_keyspace(s.ip_addr, ks) for s in servers))

            tag = unique_name('snap_')
            await manager.api.take_cluster_snapshot(servers[0].ip_addr, ks, tag=tag)
            attempts = list(cql.execute(f"SELECT attempt, committed FROM system_distributed.snapshot_attempts WHERE tag = '{tag}'"))
            assert [r.committed for r in attempts] == [True]
            name = f'{tag}/{attempts[0].attempt}'
            for cf in (cf1, cf2):
                assert len(await snapshot_dirs_of(manager, servers, ks, cf, tag)) == len(servers)
                assert catalog_rows(name, ks, cf)

            with pytest.raises(HTTPError, match='has no table') as e:
                await manager.api.clear_cluster_snapshot(servers[0].ip_addr, tag, keyspace=ks, table='no_such_table')
            assert e.value.code == 400
            with pytest.raises(HTTPError, match='must be given together') as e:
                await manager.api.clear_cluster_snapshot(servers[0].ip_addr, tag, table=cf1)
            assert e.value.code == 400
            # Blank values must not fall back to a clear of the whole snapshot.
            with pytest.raises(HTTPError, match='must be given together') as e:
                await manager.api.client.delete("/storage_service/tablets/snapshots", host=servers[0].ip_addr,
                                                params={"tag": tag, "keyspace": "", "table": ""})
            assert e.value.code == 400

            await clear(tag, ks, cf1)

            assert not await snapshot_dirs_of(manager, servers, ks, cf1, tag), f'A snapshot directory of {cf1} survived the clear'
            assert not catalog_rows(name, ks, cf1), f'Catalog rows of {cf1} survived the clear'
            assert len(await snapshot_dirs_of(manager, servers, ks, cf2, tag)) == len(servers)
            assert catalog_rows(name, ks, cf2)
            assert [r.committed for r in cql.execute(
                f"SELECT committed FROM system_distributed.snapshot_attempts WHERE tag = '{tag}'")] == [True]
            for s in servers:
                assert listed(await manager.api.list_cluster_snapshots(s.ip_addr)) == {tag: {(ks, cf2)}}

            await clear(tag, ks, cf2)

            assert not await snapshot_dirs_of(manager, servers, ks, cf2, tag), f'A snapshot directory of {cf2} survived the clear'
            assert not catalog_rows(name, ks, cf2), f'Catalog rows of {cf2} survived the clear'
            assert not list(cql.execute(f"SELECT * FROM system_distributed.snapshots WHERE name = '{tag}'"))
            assert not list(cql.execute(f"SELECT * FROM system_distributed.snapshot_attempts WHERE tag = '{tag}'"))
            assert not list(cql.execute(f"SELECT * FROM system_distributed.snapshot_nodes WHERE snapshot_name = '{name}'"))
            for s in servers:
                assert await manager.api.list_cluster_snapshots(s.ip_addr) == []


async def test_clear_cluster_snapshot_last_table_with_failed_attempt(manager: ScyllaClusterManager):
    """
    Tests that clearing the only table of the committed attempt clears the
    whole snapshot, even when a failed attempt of the same tag left rows for
    another table.
    """
    servers, _ = await create_cluster(topo(rf=3, nodes=3, racks=3, dcs=1), manager, logger)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : 3 } AND tablets = { 'initial': 2 }") as ks:
        async with new_test_table(manager, ks, "pk int primary key") as t1, new_test_table(manager, ks, "pk int primary key") as t2:
            cf1, cf2 = t1.split('.')[1], t2.split('.')[1]
            await asyncio.gather(*(cql.run_async(f"INSERT INTO {t1} (pk) VALUES ({i})") for i in range(20)))
            await asyncio.gather(*(manager.api.flush_keyspace(s.ip_addr, ks) for s in servers))
            tag = unique_name('snap_')
            await manager.api.take_cluster_snapshot(servers[0].ip_addr, ks, tag=tag, tables=[cf1])
            assert len(await snapshot_dirs_of(manager, servers, ks, cf1, tag)) == len(servers)

            # The residue of a failed attempt of the same tag, for the other table.
            failed = uuid.uuid4()
            cql.execute(SimpleStatement(f"INSERT INTO system_distributed.snapshot_attempts (tag, attempt, committed)"
                                        f" VALUES ('{tag}', {failed}, false)", consistency_level=ConsistencyLevel.QUORUM))
            cql.execute(SimpleStatement(f"INSERT INTO system_distributed.snapshot_tables (snapshot_name, keyspace_name, table_name)"
                                        f" VALUES ('{tag}/{failed}', '{ks}', '{cf2}')", consistency_level=ConsistencyLevel.QUORUM))

            tid = await manager.api.clear_cluster_snapshot(servers[0].ip_addr, tag, keyspace=ks, table=cf1)
            status = await manager.api.wait_task(servers[0].ip_addr, tid)
            assert status is not None and status.get('state') == 'done', f'Clear of {cf1} in {tag}: {status}'

            assert not list(cql.execute(f"SELECT * FROM system_distributed.snapshots WHERE name = '{tag}'"))
            assert not list(cql.execute(f"SELECT * FROM system_distributed.snapshot_attempts WHERE tag = '{tag}'"))
            assert not list(cql.execute(f"SELECT * FROM system_distributed.snapshot_tables WHERE snapshot_name = '{tag}/{failed}'"))
            assert not await snapshot_dirs_of(manager, servers, ks, cf1, tag)
            for s in servers:
                assert await manager.api.list_cluster_snapshots(s.ip_addr) == []


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_clear_cluster_snapshot_table_failure_resume(manager: ScyllaClusterManager):
    """
    Tests that a clear of one table keeps the snapshot uncommitted while it
    runs, so a clear that fails midway leaves the snapshot unlisted, and that
    running it again finishes the table and commits the snapshot again.
    """
    servers, _ = await create_cluster(topo(rf=3, nodes=3, racks=3, dcs=1), manager, logger)
    cql = manager.get_cql()

    def listed(snapshots):
        return {s['tag']: {(t['keyspace'], t['table']) for t in s['tables']} for s in snapshots}

    def committed(tag):
        return [r.committed for r in cql.execute(f"SELECT committed FROM system_distributed.snapshot_attempts WHERE tag = '{tag}'")]

    async def clear(tag, ks, cf, expect):
        tid = await manager.api.clear_cluster_snapshot(servers[0].ip_addr, tag, keyspace=ks, table=cf)
        status = await manager.api.wait_task(servers[0].ip_addr, tid)
        assert status is not None and status.get('state') == expect, f'Clear of {ks}.{cf} in {tag}: expected {expect}, got {status}'

    async with new_test_keyspace(manager, "WITH REPLICATION = { 'replication_factor' : 3 } AND tablets = { 'initial': 2 }") as ks:
        async with new_test_table(manager, ks, "pk int primary key") as t1, new_test_table(manager, ks, "pk int primary key") as t2:
            cf1, cf2 = t1.split('.')[1], t2.split('.')[1]
            for t in (t1, t2):
                await asyncio.gather(*(cql.run_async(f"INSERT INTO {t} (pk) VALUES ({i})") for i in range(20)))
            await asyncio.gather(*(manager.api.flush_keyspace(s.ip_addr, ks) for s in servers))
            tag = unique_name('snap_')
            await manager.api.take_cluster_snapshot(servers[0].ip_addr, ks, tag=tag)
            assert committed(tag) == [True]

            await manager.api.enable_injection(servers[0].ip_addr, 'cluster_clear_snapshot_collect', one_shot=True)
            await clear(tag, ks, cf1, 'failed')

            assert list(cql.execute(f"SELECT * FROM system_distributed.snapshots WHERE name = '{tag}'"))
            assert committed(tag) == [False], 'A failed clear of one table should leave the snapshot uncommitted'
            for s in servers:
                assert await manager.api.list_cluster_snapshots(s.ip_addr) == []
            assert len(await snapshot_dirs_of(manager, servers, ks, cf2, tag)) == len(servers)

            await clear(tag, ks, cf1, 'done')

            assert committed(tag) == [True]
            assert not await snapshot_dirs_of(manager, servers, ks, cf1, tag)
            assert len(await snapshot_dirs_of(manager, servers, ks, cf2, tag)) == len(servers)
            for s in servers:
                assert listed(await manager.api.list_cluster_snapshots(s.ip_addr)) == {tag: {(ks, cf2)}}


async def test_clear_cluster_snapshot_last_table_of_keyspace(manager: ScyllaClusterManager):
    """
    Tests that clearing the only table of one keyspace in a snapshot of two
    keyspaces deletes the catalog row of that keyspace and keeps the other.
    """
    servers, _ = await create_cluster(topo(rf=3, nodes=3, racks=3, dcs=1), manager, logger)
    cql = manager.get_cql()
    opts = "WITH REPLICATION = { 'replication_factor' : 3 } AND tablets = { 'initial': 2 }"

    async with new_test_keyspace(manager, opts) as ks1, new_test_keyspace(manager, opts) as ks2:
        async with new_test_table(manager, ks1, "pk int primary key") as t1, new_test_table(manager, ks2, "pk int primary key") as t2:
            cf1, cf2 = t1.split('.')[1], t2.split('.')[1]
            for t in (t1, t2):
                await asyncio.gather(*(cql.run_async(f"INSERT INTO {t} (pk) VALUES ({i})") for i in range(20)))
            await asyncio.gather(*(manager.api.flush_keyspace(s.ip_addr, ks) for s in servers for ks in (ks1, ks2)))
            tag = unique_name('snap_')
            await manager.api.take_cluster_snapshot(servers[0].ip_addr, f'{ks1},{ks2}', tag=tag)
            attempts = list(cql.execute(f"SELECT attempt FROM system_distributed.snapshot_attempts WHERE tag = '{tag}'"))
            name = f'{tag}/{attempts[0].attempt}'

            def keyspace_rows():
                return {r.keyspace_name for r in cql.execute(
                    f"SELECT keyspace_name FROM system_distributed.snapshot_keyspaces WHERE snapshot_name = '{name}'")}
            assert keyspace_rows() == {ks1, ks2}

            tid = await manager.api.clear_cluster_snapshot(servers[0].ip_addr, tag, keyspace=ks1, table=cf1)
            status = await manager.api.wait_task(servers[0].ip_addr, tid)
            assert status is not None and status.get('state') == 'done', f'Clear of {ks1}.{cf1} in {tag}: {status}'

            assert keyspace_rows() == {ks2}
            assert [r.committed for r in cql.execute(
                f"SELECT committed FROM system_distributed.snapshot_attempts WHERE tag = '{tag}'")] == [True]
            for s in servers:
                assert {(t['keyspace'], t['table']) for snapshot in await manager.api.list_cluster_snapshots(s.ip_addr)
                        for t in snapshot['tables']} == {(ks2, cf2)}

