#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace, reconnect_driver

import pytest


@pytest.mark.asyncio
async def test_data_survives_crash(manager: ManagerClient):
    """Verify that SC table data survives a non-graceful crash and is recovered
    from commitlog replay. After a crash, committed raft entries in the commitlog
    must be re-applied to memtables even if they were already snapshotted, because
    the snapshot data may not have been flushed to sstables."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        # Prevent automatic memtable flushes so data stays in the commitlog
        # and is not persisted to sstables before the crash.
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        for pk in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # Crash the node (non-graceful stop — no flush)
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        for pk in range(5):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"

        # Verify that the snapshot index was advanced during replay.
        # After commitlog replay, store_snapshot_index should have bumped
        # snapshot.idx to commit_idx for the tablet's raft group.
        table_id = await manager.get_table_id(ks.replace('"', ''), "test")
        tablet_rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        assert len(tablet_rows) == 1
        group_id = tablet_rows[0].raft_group_id
        snp_rows = await cql.run_async(f"SELECT idx FROM system.raft_groups_snapshots WHERE shard = 0 AND group_id = {group_id}")
        assert len(snp_rows) == 1, f"Expected snapshot row for group {group_id}"
        assert snp_rows[0].idx > 0, f"Expected snapshot idx > 0 after replay, got {snp_rows[0].idx}"

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_schema_upgrade_during_replay(manager: ManagerClient):
    """Verify that SC table data survives a crash even when the schema was altered
    between writes. During commitlog replay, mutations written under the old schema
    must be upgraded to the current schema before being applied to memtables."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        # Prevent automatic memtable flushes so data stays in the commitlog
        # and is not persisted to sstables before the crash.
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Write under the original schema
        for pk in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # ALTER TABLE — adds a new column, changing the schema version
        await cql.run_async(f"ALTER TABLE {ks}.test ADD v text;")

        # Write under the new schema
        for pk in range(5, 10):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c, v) VALUES ({pk}, {pk * 10}, 'hello')")

        # Crash the node (non-graceful stop — no flush)
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # Verify rows written under the old schema
        for pk in range(5):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"
            assert rows[0].v is None, f"pk={pk}: expected v=None, got v={rows[0].v}"

        # Verify rows written under the new schema
        for pk in range(5, 10):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"
            assert rows[0].v == 'hello', f"pk={pk}: expected v='hello', got v={rows[0].v}"

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_double_crash_recovery(manager: ManagerClient):
    """Verify that SC table data survives two consecutive crashes.
    Write data, crash, restart (commitlog replay restores data), write more data,
    crash again, restart, and verify all data (from both write phases) is present."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Phase 1: Write initial data
        for pk in range(10):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # First crash
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # Verify phase 1 data survived
        for pk in range(10):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"After 1st crash: expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10

        # Phase 2: Write more data
        for pk in range(10, 20):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # Second crash
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # Verify all data (both phases) survived
        for pk in range(20):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"After 2nd crash: expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_crash_with_multiple_commitlog_segments(manager: ManagerClient):
    """Verify crash recovery when data spans multiple commitlog segments.
    Uses a small commitlog segment size to force segment rotation, writes
    enough rows to span multiple segments, crashes, and verifies all data
    is recovered from replaying multiple segment files."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        # Small segment size to force multiple segments. The minimum effective
        # segment size is clamped internally, but a low value triggers rotation.
        'commitlog_segment_size_in_mb': 1,
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int, padding text);")

        # Write enough rows with padding to span multiple segments.
        num_rows = 200
        padding = 'x' * 4096  # 4KB padding per row to fill segments faster
        for pk in range(num_rows):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c, padding) VALUES ({pk}, {pk * 10}, '{padding}')")

        # Crash
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # Verify all rows survived
        rows_by_pk = {}
        for pk in range(num_rows):
            rows = await cql.run_async(f"SELECT pk, c FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Missing row for pk={pk}"
            rows_by_pk[pk] = rows[0].c
        assert len(rows_by_pk) == num_rows, f"Expected {num_rows} rows, got {len(rows_by_pk)}"
        for pk in range(num_rows):
            assert rows_by_pk[pk] == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows_by_pk[pk]}"

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_crash_recovery_multi_tablet(manager: ManagerClient):
    """Verify crash recovery with multiple tablets (independent raft groups).
    Creates a table with 4 tablets, writes data distributed across all tablets,
    crashes, and verifies all data is recovered — testing that commitlog replay
    correctly routes entries to multiple independent raft groups."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Write enough rows to ensure data lands on different tablets.
        num_rows = 40
        for pk in range(num_rows):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # Crash
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # Verify all rows survived
        rows_by_pk = {}
        for pk in range(num_rows):
            rows = await cql.run_async(f"SELECT pk, c FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Missing row for pk={pk}"
            rows_by_pk[pk] = rows[0].c
        assert len(rows_by_pk) == num_rows, f"Expected {num_rows} rows, got {len(rows_by_pk)}"
        for pk in range(num_rows):
            assert rows_by_pk[pk] == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows_by_pk[pk]}"

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_crash_recovery_after_flush(manager: ManagerClient):
    """Verify crash recovery when some data was flushed to sstables before the crash.
    Write data, flush it to sstables (so it is persisted on disk), then write
    more data (which exists only in the commitlog), crash, and verify both the
    flushed and unflushed data are present after recovery."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        ks_name = ks.replace('"', '')  # strip quotes for API call
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Phase 1: Write data and flush to sstables
        for pk in range(10):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")
        await manager.api.keyspace_flush(server.ip_addr, ks_name)

        # Phase 2: Write more data (only in commitlog, not flushed)
        for pk in range(10, 20):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # Crash (non-graceful — phase 2 data is only in commitlog)
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # Verify all data: flushed (phase 1) + replayed from commitlog (phase 2)
        for pk in range(20):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"

    await manager.server_stop_gracefully(server.server_id)
