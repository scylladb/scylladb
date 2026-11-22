#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Tests of DELETE ALL DATA, which deletes the data of a tablet table written at or before
its timestamp, like a DELETE of every partition, with a table-wide truncation tombstone.
See docs/dev/delete-all-data.md.
"""
import asyncio
import logging
import pytest

from cassandra.query import SimpleStatement, ConsistencyLevel

import time

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_view, wait_for
from test.cluster.util import new_test_keyspace, reconnect_driver, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


async def select_pks(cql, table: str, cl: ConsistencyLevel = ConsistencyLevel.ALL, where: str = "") -> set[int]:
    rows = await cql.run_async(SimpleStatement(f"SELECT pk FROM {table} {where}", consistency_level=cl))
    return {row.pk for row in rows}


async def test_delete_all_data_using_timestamp_with_views(manager: ScyllaClusterManager):
    """
    DELETE ALL DATA ... USING TIMESTAMP deletes the data written at or before the timestamp,
    in the base table and in its materialized views and secondary indexes.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        table = f"{ks}.cf"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {table} WHERE pk IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, pk)")
        await cql.run_async(f"CREATE INDEX ON {table} (v)")
        await wait_for_view(cql, "mv", len(servers))

        truncated_at = 1000
        for pk in range(10):
            await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk}) USING TIMESTAMP {truncated_at - 1}",
                                                consistency_level=ConsistencyLevel.ALL))
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (100, 100) USING TIMESTAMP {truncated_at + 1}",
                                            consistency_level=ConsistencyLevel.ALL))

        await cql.run_async(f"DELETE ALL DATA FROM {table} USING TIMESTAMP {truncated_at}")

        assert await select_pks(cql, table) == {100}
        # View and index updates are asynchronous, so the surviving row may not be there yet.
        async def view_has_surviving_row():
            return True if await select_pks(cql, f"{ks}.mv") == {100} else None
        await wait_for(view_has_surviving_row, time.time() + 60)
        async def index_has_surviving_row():
            return True if await select_pks(cql, table, where="WHERE v = 100") == {100} else None
        await wait_for(index_has_surviving_row, time.time() + 60)
        assert await select_pks(cql, table, where="WHERE v = 1") == set()


async def test_delete_all_data_with_replica_down(manager: ScyllaClusterManager):
    """
    DELETE ALL DATA does not require all the table's replicas to be alive: a replica which is
    down when it runs observes the deletion when it comes back, before it serves.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        table = f"{ks}.cf"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        for pk in range(10):
            await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk})", consistency_level=ConsistencyLevel.ALL))

        await manager.server_stop_gracefully(servers[2].server_id)
        await manager.server_not_sees_other_server(servers[0].ip_addr, servers[2].ip_addr)

        await cql.run_async(f"DELETE ALL DATA FROM {table}")
        assert await select_pks(cql, table, cl=ConsistencyLevel.QUORUM) == set()

        await manager.server_start(servers[2].server_id)
        await manager.server_sees_other_server(servers[0].ip_addr, servers[2].ip_addr)

        # Read the data of the replica which was down, through it.
        cql2 = await manager.get_cql_exclusive(servers[2])
        assert await select_pks(cql2, table, cl=ConsistencyLevel.ONE) == set()
        assert await select_pks(cql, table) == set()


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_truncation_tombstone_gc(manager: ScyllaClusterManager):
    """
    The truncation tombstone is garbage-collected once its tombstone GC grace period
    passed (immediately with tombstone_gc mode 'immediate') and no replica holds data
    covered by it, which major compaction makes so. Data written after the deletion
    survives, and the truncated data does not reappear.
    """
    cfg = {'error_injections_at_startup': ['short_truncation_tombstone_gc_interval']}
    servers = await manager.servers_add(3, auto_rack_dc="dc1", config=cfg)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        table = f"{ks}.cf"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int) WITH tombstone_gc = {{'mode': 'immediate'}}")
        table_id = (await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'cf'"))[0].id

        truncated_at = 1000
        for pk in range(10):
            await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk}) USING TIMESTAMP {truncated_at - 1}",
                                                consistency_level=ConsistencyLevel.ALL))
        # The covered data is in sstables, so the replicas hold covered data until they are compacted.
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)
        await cql.run_async(f"DELETE ALL DATA FROM {table} USING TIMESTAMP {truncated_at}")
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (100, 100) USING TIMESTAMP {truncated_at + 1}",
                                            consistency_level=ConsistencyLevel.ALL))

        async def data_deleted_at_timestamp():
            rows = await cql.run_async(f"SELECT data_deleted_at_timestamp FROM system.tablets WHERE table_id = {table_id} LIMIT 1")
            return rows[0].data_deleted_at_timestamp if rows else None
        assert await data_deleted_at_timestamp() == truncated_at

        for server in servers:
            await manager.api.keyspace_compaction(server.ip_addr, ks, "cf")

        async def gc_done():
            return True if await data_deleted_at_timestamp() is None else None
        await wait_for(gc_done, time.time() + 60)

        assert await select_pks(cql, table) == {100}


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_truncation_tombstone_gc_single_replica(manager: ScyllaClusterManager):
    """
    Tablets with a single replica are not repaired, so in tombstone_gc mode 'repair' nothing
    flushes the hints and batchlog, which may hold writes covered by the truncation tombstone,
    on their behalf. The truncation tombstone is therefore garbage-collected only once the
    topology coordinator flushed the hints and batchlog on all nodes, after the propagation delay.
    """
    cfg = {'error_injections_at_startup': ['short_truncation_tombstone_gc_interval']}
    servers = await manager.servers_add(2, auto_rack_dc="dc1", config=cfg)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.cf"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)"
                            f" WITH tombstone_gc = {{'mode': 'repair', 'propagation_delay_in_seconds': '1'}}")
        table_id = (await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'cf'"))[0].id

        truncated_at = 1000
        for pk in range(10):
            await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk}) USING TIMESTAMP {truncated_at - 1}")
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)

        logs = [await manager.server_open_log(server.server_id) for server in servers]
        marks = [await log.mark() for log in logs]

        await cql.run_async(f"DELETE ALL DATA FROM {table} USING TIMESTAMP {truncated_at}")
        await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (100, 100) USING TIMESTAMP {truncated_at + 1}")

        async def data_deleted_at_timestamp():
            rows = await cql.run_async(f"SELECT data_deleted_at_timestamp FROM system.tablets WHERE table_id = {table_id} LIMIT 1")
            return rows[0].data_deleted_at_timestamp if rows else None
        assert await data_deleted_at_timestamp() == truncated_at

        for server in servers:
            await manager.api.keyspace_compaction(server.ip_addr, ks, "cf")

        async def gc_done():
            return True if await data_deleted_at_timestamp() is None else None
        await wait_for(gc_done, time.time() + 60)

        # The topology coordinator flushed the hints and batchlog before clearing the truncation tombstone.
        flushes = [m for log, mark in zip(logs, marks) for m in await log.grep("Flushed hints and batchlog, flush_time=", from_mark=mark)]
        assert flushes, "the truncation tombstone was garbage-collected without flushing hints and batchlog"

        assert await select_pks(cql, table) == {100}


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_deleted_data_is_not_replayed_from_commitlog(manager: ScyllaClusterManager):
    """
    Data which was only in memtables when DELETE ALL DATA deleted it is never written to an
    sstable: the memtables are flushed stripped of it. Its commitlog entries must not be
    replayed once the truncation tombstone is garbage-collected, or a node which crashes
    brings the deleted data back.
    """
    # A short commitlog sync period, so that the writes are in the commitlog files when the nodes crash.
    cfg = {'error_injections_at_startup': ['short_truncation_tombstone_gc_interval'], 'commitlog_sync_period_in_ms': 100}
    servers = await manager.servers_add(3, auto_rack_dc="dc1", config=cfg)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        table = f"{ks}.cf"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int) WITH tombstone_gc = {{'mode': 'immediate'}}")
        table_id = (await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'cf'"))[0].id

        deleted_at = 1000
        # Not flushed: the data is in the memtables and in the commitlog only.
        for pk in range(10):
            await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk}) USING TIMESTAMP {deleted_at - 1}",
                                                consistency_level=ConsistencyLevel.ALL))
        await asyncio.sleep(1)
        await cql.run_async(f"DELETE ALL DATA FROM {table} USING TIMESTAMP {deleted_at}")
        assert await select_pks(cql, table) == set()

        async def gc_done():
            rows = await cql.run_async(f"SELECT data_deleted_at_timestamp FROM system.tablets WHERE table_id = {table_id} LIMIT 1")
            return True if not rows or rows[0].data_deleted_at_timestamp is None else None
        await wait_for(gc_done, time.time() + 60)

        # Crash all the nodes, so that they replay their commitlogs.
        for server in servers:
            await manager.server_stop(server.server_id, convict=False)
        for server in servers:
            await manager.server_start(server.server_id)
        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        assert await select_pks(cql, table) == set()


async def test_truncate_clears_truncation_tombstone(manager: ScyllaClusterManager):
    """
    TRUNCATE removes the table's data physically from all the replicas, so none of them
    holds data covered by a truncation tombstone set by DELETE ALL DATA, and TRUNCATE clears
    it, for the table and its views. Data older than the cleared tombstone can then be
    written, or restored, into the table.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        table = f"{ks}.cf"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {table} WHERE v IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (v, pk)")
        await wait_for_view(cql, "mv", len(servers))

        async def data_deleted_at_timestamp(name: str):
            if name == "mv":
                table_id = (await cql.run_async(f"SELECT id FROM system_schema.views WHERE keyspace_name = '{ks}' AND view_name = '{name}'"))[0].id
            else:
                table_id = (await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{name}'"))[0].id
            rows = await cql.run_async(f"SELECT data_deleted_at_timestamp FROM system.tablets WHERE table_id = {table_id} LIMIT 1")
            return rows[0].data_deleted_at_timestamp if rows else None

        deleted_at = 1000
        write = SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (1, 1) USING TIMESTAMP {deleted_at - 1}", consistency_level=ConsistencyLevel.ALL)
        await cql.run_async(write)
        await cql.run_async(f"DELETE ALL DATA FROM {table} USING TIMESTAMP {deleted_at}")
        assert await data_deleted_at_timestamp("cf") == deleted_at
        assert await data_deleted_at_timestamp("mv") == deleted_at
        # While the truncation tombstone is set, a write it covers is dropped.
        await cql.run_async(write)
        assert await select_pks(cql, table) == set()

        await cql.run_async(f"TRUNCATE {table}")
        assert await data_deleted_at_timestamp("cf") is None
        assert await data_deleted_at_timestamp("mv") is None
        assert await select_pks(cql, table) == set()

        await cql.run_async(write)
        assert await select_pks(cql, table) == {1}

        async def view_has_row():
            rows = await cql.run_async(SimpleStatement(f"SELECT pk FROM {ks}.mv", consistency_level=ConsistencyLevel.ALL))
            return True if {row.pk for row in rows} == {1} else None
        await wait_for(view_has_row, time.time() + 60)
