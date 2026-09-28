#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import asyncio

from test.topology.util import new_test_keyspace, create_new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode
from cassandra.query import SimpleStatement, ConsistencyLevel

import pytest

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_truncation_on_drop(manager: ManagerClient):
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

@pytest.mark.asyncio
async def test_truncation_records_pruned_on_dirty_restart(manager: ManagerClient):
    server = await manager.server_add()
    cql = manager.get_cql()

    async def restart():
        await manager.server_stop(server.server_id)
        await manager.server_start(server.server_id)
        manager.driver_close()
        await manager.driver_connect()
        return manager.cql
    
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


@skip_mode('release', 'error injections are not supported in release mode')
async def test_drop_keyspace_during_shutdown(manager: ManagerClient):
    """Drop a keyspace while the node is shutting down.

    The compaction manager closes the gates of its compaction states as soon
    as the node is asked to stop, while the group0 state machine still applies
    the drop, truncating the keyspace's tables, at most 8 at a time. Disabling
    the compaction of a table whose gate is closed must succeed: the raft
    applier stops the raft instance on an error.

    Reproducer for SCYLLADB-4647.
    """
    # A single shard, so that the compaction manager closing the gates, which the
    # test waits for, happens on the shard which truncates the tables.
    server = await manager.server_add(cmdline=['--smp', '1', '--logger-log-level', 'compaction_manager=debug'])
    cql = manager.get_cql()

    # One more table than the drop truncates concurrently (max_concurrent in db/schema_applier.cc).
    tables = [f"t{i}" for i in range(9)]
    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    for table in tables:
        await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY)")

    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    # Hold the truncates of the first 8 tables, with their compaction disabled, until
    # the compaction manager closed the gates of all tables. The truncate of the last
    # table then only starts after its gate is closed. Which table is the last one
    # isn't known, so wait for the gates of all tables.
    await manager.api.enable_injection(server.ip_addr, "truncate_compaction_disabled_wait", one_shot=False)
    drop = asyncio.ensure_future(cql.run_async(f"DROP KEYSPACE {ks}"))
    async def held_truncates():
        return len(await log.grep("truncate_compaction_disabled_wait: wait", from_mark=mark))
    while await held_truncates() < len(tables) - 1:
        await asyncio.sleep(0.1)

    stop = asyncio.create_task(manager.server_stop_gracefully(server.server_id))
    for table in tables:
        await log.wait_for(f"compaction_manager - Closing compaction state gate of {ks}.{table} ", from_mark=mark)
    # Each message releases a single waiter, a message sent ahead releases the next one to wait.
    for _ in tables:
        await manager.api.message_injection(server.ip_addr, "truncate_compaction_disabled_wait")
    await stop
    # The node stops serving CQL early in the shutdown, so the driver may not see the drop completing.
    await asyncio.gather(drop, return_exceptions=True)

    for table in tables:
        assert await log.grep(f"database - Truncated {ks}.{table}", from_mark=mark)
    assert not await log.grep("applier fiber stopped because of the error", from_mark=mark)
