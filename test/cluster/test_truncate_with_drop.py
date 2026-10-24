#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import asyncio
from datetime import datetime, timezone

from test.cluster.util import new_test_keyspace, create_new_test_keyspace, with_physical_truncate
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from cassandra.query import SimpleStatement, ConsistencyLevel

import pytest

logger = logging.getLogger(__name__)

# These tests check the truncation records written by the physical TRUNCATE, which is used for
# tablet tables until the EVENTUALLY_CONSISTENT_TRUNCATE cluster feature is enabled, so they suppress the feature.

async def test_truncation_on_drop(manager: ScyllaClusterManager):
    await manager.server_add(config=with_physical_truncate({}))
    cql = manager.get_cql()

    # Create a keyspace
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')
        table_id = await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'")
        table_id = table_id[0].id

        keys = range(1024)
        await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});') for k in keys])
        await cql.run_async(f'TRUNCATE TABLE {ks}.test')

        # should have some truncation records now
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM system.truncated where table_uuid={table_id}'))
        assert row[0].count > 0

        await cql.run_async(f"DROP TABLE {ks}.test")

        # should have no truncation records now
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM system.truncated where table_uuid={table_id}'))
        assert row[0].count == 0

async def test_truncation_records_pruned_on_dirty_restart(manager: ScyllaClusterManager):
    server = await manager.server_add(config=with_physical_truncate({}))
    cql = manager.get_cql()

    async def restart():
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        cql, _ = await manager.get_ready_cql([server])
        return cql
    
    # Create a keyspace
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')
        table_id = await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'")
        table_id = table_id[0].id

        keys = range(1024)
        await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});') for k in keys])
        await cql.run_async(f'TRUNCATE TABLE {ks}.test')

        # should have some truncation records now
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM system.truncated where table_uuid={table_id}'))
        assert row[0].count > 0

        logger.debug("Kill + restart the node")
        cql = await restart()

        # should still have same truncation records
        row2 = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM system.truncated where table_uuid={table_id}'))
        assert row2[0].count == row[0].count

        # should _not_ have any data.
        row2 = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM {ks}.test'))
        assert row2[0].count == 0

        logger.debug("Fake 'old' dropped table")
        # don't do this at home kids.

        await cql.run_async(f"DELETE FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'")
        await cql.run_async(f"DELETE FROM system.tablets WHERE table_id = {table_id}")

        logger.debug("Kill + restart the node again")
        cql = await restart()

        # should have no truncation records now
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM system.truncated where table_uuid={table_id}', consistency_level=ConsistencyLevel.ONE))
        assert row[0].count == 0


@pytest.mark.parametrize("truncate_first", [False, True])
async def test_recreate_dropped_table_with_same_id(manager: ScyllaClusterManager, truncate_first: bool):
    """Drop a table, recreate it WITH ID = <the same id> and kill the node.

    DESCRIBE ... WITH INTERNALS emits the table id, and schema restore tools
    recreate tables from it right after dropping them, so a recreated table
    may share its id with a dropped one. Truncation records are keyed by the
    table id, so a record left behind by the drop would apply to the
    recreated table after a restart: a truncation time in the future makes
    paxos reject all decisions for the table, and batchlog replay skip all
    its batches.

    Verify that, after the node is killed and restarted, the recreated table
    has no truncation record in the future and accepts LWT writes.

    With truncate_first, the table is truncated before it is dropped, so it
    has a persisted truncation record with a real truncation time when the
    drop starts.

    Regression test for SCYLLADB-5240.
    """
    server = await manager.server_add()
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")
        rows = await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'")
        table_id = rows[0].id

        keys = range(100)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k})") for k in keys])
        if truncate_first:
            await cql.run_async(f"TRUNCATE TABLE {ks}.test")
            await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k})") for k in keys])

        await cql.run_async(f"DROP TABLE {ks}.test")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH ID = {table_id}")

        logger.debug("Kill + restart the node")
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        cql, _ = await manager.get_ready_cql([server])

        # A record left by an older truncate may survive the kill, but none may be in the future.
        rows = await cql.run_async(SimpleStatement(f"SELECT truncated_at FROM system.truncated WHERE table_uuid = {table_id}"))
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        assert all(row.truncated_at <= now for row in rows), f"Unexpected truncation records: {rows}"
        if not truncate_first:
            assert not rows, f"Unexpected truncation records: {rows}"

        # FIXME: enable once SCYLLADB-5245 is fixed: commitlog replay resurrects the
        # data of the dropped table into the recreated one, if the writes above
        # were written to the commitlog before the node was killed. With the
        # default periodic commitlog sync this happens if a periodic sync (every
        # 10s) runs between the writes and the kill, which makes the check flaky
        # on slow machines and builds. See
        # test_dropped_table_data_not_resurrected_in_recreated_table.
        # rows = await cql.run_async(f"SELECT * FROM {ks}.test")
        # assert not rows, f"Data of the dropped table was resurrected: {rows}"

        # Use a key that was not written before the drop, so that the check does
        # not depend on whether the dropped data was resurrected (SCYLLADB-5245).
        new_key = len(keys)
        rows = await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({new_key}, {new_key}) IF NOT EXISTS")
        assert rows[0].applied
        rows = await cql.run_async(f"SELECT c FROM {ks}.test WHERE pk = {new_key}")
        assert len(rows) == 1 and rows[0].c == new_key


@pytest.mark.xfail(reason="SCYLLADB-5245", strict=True)
async def test_dropped_table_data_not_resurrected_in_recreated_table(manager: ScyllaClusterManager):
    """Drop a table with data, recreate it WITH ID = <the same id>, and kill the node.

    The commitlog replayer skips entries only by the replay positions stored in
    system.truncated for existing tables. The drop deletes the truncation
    records of the table, so after a restart, commitlog entries written to the
    dropped table, if their segments were not recycled yet, are replayed into
    the recreated table, which has the same id, resurrecting the dropped data.

    The commitlog is in batch mode, so that the writes are in the commitlog
    when the node is killed (with the default periodic mode they are typically
    still in memory, and lost).

    Reproducer for SCYLLADB-5245.
    """
    server = await manager.server_add(config={'commitlog_sync': 'batch'})
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")
        rows = await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'")
        table_id = rows[0].id

        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k})") for k in range(100)])
        await cql.run_async(f"DROP TABLE {ks}.test")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH ID = {table_id}")

        logger.debug("Kill + restart the node")
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        cql, _ = await manager.get_ready_cql([server])

        rows = await cql.run_async(f"SELECT * FROM {ks}.test")
        assert not rows, f"Data of the dropped table was resurrected: {len(rows)} rows"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_crash_during_drop_keyspace(manager: ScyllaClusterManager):
    """Kill the node while DROP KEYSPACE is cleaning up its tables.

    The drop is committed to the schema tables before the tables are
    truncated and their storage is destroyed, one after the other. Kill the
    node while the first table is being truncated, after its memtables were
    cleared, and verify that after a restart the keyspace and its tables are
    gone, commitlog replay reports no errors, and the keyspace can be
    recreated.

    Directories of the tables whose cleanup did not complete are left behind
    on disk. That is a known limitation, not checked here.

    Regression test for SCYLLADB-5240.
    """
    server = await manager.server_add()
    cql = manager.get_cql()

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    ks = await create_new_test_keyspace(cql, ks_opts)
    tables = ("t1", "t2", "t3")
    for table in tables:
        await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, c int)")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{table} (pk, c) VALUES ({k}, {k})") for k in range(100)])

    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    await manager.api.enable_injection(server.ip_addr, "database_truncate_wait", one_shot=True)
    drop = asyncio.ensure_future(cql.run_async(f"DROP KEYSPACE {ks}"))
    await manager.api.wait_for_injection_enter(server.ip_addr, "database_truncate_wait")

    logger.debug("Kill + restart the node")
    await manager.server_stop(server.server_id, convict=False)
    await asyncio.gather(drop, return_exceptions=True)
    await manager.server_start(server.server_id)
    cql, _ = await manager.get_ready_cql([server])

    rows = await cql.run_async(f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'")
    assert not rows, f"Keyspace {ks} exists after restart"
    rows = await cql.run_async(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{ks}'")
    assert not rows, f"Tables of keyspace {ks} exist after restart: {rows}"

    assert await log.grep("commitlog_replayer - Log replay complete", from_mark=mark)
    assert not await log.grep("error replaying", from_mark=mark)

    await cql.run_async(f"CREATE KEYSPACE {ks} {ks_opts}")
    for table in tables:
        await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, c int)")
        rows = await cql.run_async(f"SELECT * FROM {ks}.{table}")
        assert not rows, f"Data of the dropped table {ks}.{table} appeared in the recreated table: {rows}"
    await cql.run_async(f"DROP KEYSPACE {ks}")


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_drop_keyspace_during_shutdown(manager: ScyllaClusterManager):
    """Drop a keyspace while the node is shutting down.

    The compaction manager closes the gates of its compaction states as soon
    as the node is asked to stop, while the group0 state machine still applies
    the drop, truncating the keyspace's tables one after the other. Disabling
    the compaction of a table whose gate is closed must succeed: the raft
    applier stops the raft instance on an error, and the drop leaves the tables
    after the failed one unstopped, which aborts the node when they are destroyed.

    Reproducer for SCYLLADB-4647 and SCYLLADB-4993.
    """
    # A single shard, so that the compaction manager closing the gates, which the
    # test waits for, happens on the shard which truncates the tables.
    server = await manager.server_add(cmdline=['--smp', '1', '--logger-log-level', 'compaction_manager=debug'],
                                      config=with_physical_truncate({}))
    cql = manager.get_cql()

    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    tables = ("t1", "t2", "t3")
    for table in tables:
        await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY)")

    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    # Hold the truncate of the first table, with its compaction disabled, until
    # the compaction manager closed the gates of all tables. The truncates of the
    # other tables then find their gates closed. The tables are truncated in table
    # id order, which needn't be the order they were created in, so wait for the
    # gates of all tables.
    await manager.api.enable_injection(server.ip_addr, "truncate_compaction_disabled_wait", one_shot=True)
    drop = asyncio.ensure_future(cql.run_async(f"DROP KEYSPACE {ks}"))
    await log.wait_for("truncate_compaction_disabled_wait: waiting for message", from_mark=mark)

    stop = asyncio.create_task(manager.server_stop_gracefully(server.server_id))
    for table in tables:
        await log.wait_for(f"compaction_manager - Closing compaction state gate of {ks}.{table} ", from_mark=mark)
    await manager.api.message_injection(server.ip_addr, "truncate_compaction_disabled_wait")
    # Fails if the node aborts while stopping.
    await stop
    # The node stops serving CQL early in the shutdown, so the driver may not see the drop completing.
    await asyncio.gather(drop, return_exceptions=True)

    for table in tables:
        assert await log.grep(f"database - Truncated {ks}.{table}", from_mark=mark)
    assert not await log.grep("applier fiber stopped because of the error", from_mark=mark)
