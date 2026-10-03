#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import asyncio

from test.cluster.util import new_test_keyspace, create_new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from cassandra.query import SimpleStatement, ConsistencyLevel

import pytest

logger = logging.getLogger(__name__)

async def test_truncation_on_drop(manager: ScyllaClusterManager):
    await manager.server_add()
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
    server = await manager.server_add()
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


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_drop_keyspace_during_shutdown(manager: ScyllaClusterManager):
    """Drop a keyspace while the node is shutting down.

    The compaction manager closes the gates of its compaction states as soon
    as the node is asked to stop, while the group0 state machine still applies
    the drop, truncating the keyspace's tables one after the other. Disabling
    the compaction of a table whose gate is closed must be reported as an
    abort: the raft applier stops the raft instance on any other error.

    Reproducer for SCYLLADB-4647.
    """
    server = await manager.server_add(cmdline=['--logger-log-level', 'compaction_manager=debug'])
    cql = manager.get_cql()

    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    for table in ("t1", "t2"):
        await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY)")

    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    # Hold the truncate of the first table, with its compaction disabled, until
    # the compaction manager closed the gate of the other table. The truncate of
    # the other table then finds its gate closed. The tables are truncated in
    # table id order, which needn't be the order they were created in, so wait
    # for the gates of both tables.
    await manager.api.enable_injection(server.ip_addr, "truncate_compaction_disabled_wait", one_shot=True)
    drop = cql.run_async(f"DROP KEYSPACE {ks}")
    await log.wait_for("truncate_compaction_disabled_wait: waiting for message", from_mark=mark)

    stop = asyncio.create_task(manager.server_stop_gracefully(server.server_id))
    await log.wait_for(f"compaction_manager - Closing compaction state gate of {ks}.t1 ",
                       f"compaction_manager - Closing compaction state gate of {ks}.t2 ", from_mark=mark)
    await manager.api.message_injection(server.ip_addr, "truncate_compaction_disabled_wait")
    await stop
    # The drop is not completed by the stopped node, the driver reports an error.
    with pytest.raises(Exception):
        await drop

    assert await log.grep("applier fiber stopped because state machine was aborted", from_mark=mark)
    assert not await log.grep("applier fiber stopped because of the error", from_mark=mark)
