#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# A BATCH message carries its statements one by one, and Scylla prepares each
# textual one separately, a large one in a thread that yields (SCYLLADB-219).
# Other requests can run in between. An error injection makes every token of
# the large statement slow to parse, or pauses the parse, so they run during
# it.
import pytest
from cassandra.query import BatchStatement, BatchType  # type: ignore

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import inject_error
from test.cluster.util import new_test_keyspace

# Scylla parses a statement this large in a thread that yields.
LARGE = 16 * 1024
TOKEN = "delay_thread_parse_token"
PAUSE_AFTER_COMMIT = "schema_applier_pause_before_commit_on_other_shards"
KS_OPTS = "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_use_during_batch_message(manager: ScyllaClusterManager):
    """A USE on the connection must not change the keyspace of statements of a
    BATCH message that already started."""
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, KS_OPTS) as ks1, new_test_keyspace(manager, KS_OPTS) as ks2:
        for ks in (ks1, ks2):
            await cql.run_async(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v text)")

        # A session that uses ks1, and a batch that names no keyspace.
        session = cql.cluster.connect()
        try:
            await session.run_async(f"USE {ks1}")
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            batch.add(f"UPDATE t SET v = '{'x' * LARGE}' WHERE k = 1")
            batch.add("INSERT INTO t (k, v) VALUES (2, 'y')")

            # The parse pauses at its first token, and the session switches to
            # ks2 meanwhile.
            async with inject_error(manager.api, server.ip_addr, TOKEN, parameters={"pause": "1"}) as pause:
                result = session.run_async(batch)
                await manager.api.wait_for_injection_enter(server.ip_addr, TOKEN)
                await session.run_async(f"USE {ks2}")
                await pause.message()
                await result
        finally:
            session.shutdown()

        # Both statements went to ks1.
        assert sorted(row.k for row in await cql.run_async(f"SELECT k FROM {ks1}.t")) == [1, 2]
        assert not await cql.run_async(f"SELECT k FROM {ks2}.t")


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_schema_change_during_batch_message(manager: ScyllaClusterManager):
    """A schema change between the prepares of two statements of a BATCH message
    would leave them on different versions of the table, and a conditional batch
    would crash the node. A schema change during the parse of a statement must
    not wait for the parse, and the batch must run on the new version."""
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, b int, d int)")
        await cql.run_async(f"INSERT INTO {ks}.t (p, b, d) VALUES (0, 10, 20)")

        # A conditional batch with a large second statement.
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(f"UPDATE {ks}.t SET b = 11 WHERE p = 0 IF b = 10")
        batch.add(f"UPDATE {ks}.t SET d = 21 WHERE p = 0 IF d = 20 /* {'x' * LARGE} */")

        # The parse of the second statement pauses at its first token, after
        # the first statement is parsed. The ALTER runs to the end meanwhile.
        async with inject_error(manager.api, server.ip_addr, TOKEN, parameters={"pause": "1"}) as pause:
            result = cql.run_async(batch)
            await manager.api.wait_for_injection_enter(server.ip_addr, TOKEN)
            await cql.run_async(f"ALTER TABLE {ks}.t ADD a int")
            assert not result.done(), "the BATCH message ended before the ALTER"

            await pause.message()
            rows = await result

        # The batch applied both updates, and the ALTER added the column.
        assert rows[0][0]
        row = (await cql.run_async(f"SELECT a, b, d FROM {ks}.t WHERE p = 0"))[0]
        assert (row.a, row.b, row.d) == (None, 11, 21)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_stale_prepared_statement_in_batch_message(manager: ScyllaClusterManager):
    """A schema change is applied to a table first, and the table's prepared
    statements leave the cache a little later. A BATCH message in between must
    not run a statement prepared before the change next to one prepared after
    it: a conditional batch would crash the node. Scylla answers UNPREPARED for
    the old one, the driver prepares it again, and the batch runs."""
    # With one shard, the injection pauses the schema change right after it is
    # applied to the table.
    server = await manager.server_add(cmdline=['--smp', '1'])
    cql = manager.get_cql()
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, b int, d int)")
        await cql.run_async(f"INSERT INTO {ks}.t (p, b, d) VALUES (0, 10, 20)")
        # Whatever the first conditional write of a table sets up is set up
        # before the schema change is paused.
        await cql.run_async(f"INSERT INTO {ks}.t (p, b, d) VALUES (1, 1, 1) IF NOT EXISTS")

        # One statement is prepared before the ALTER, which pauses after it is
        # applied to the table.
        old = cql.prepare(f"UPDATE {ks}.t SET b = 11 WHERE p = 0 IF b = 10")
        await manager.api.enable_injection(server.ip_addr, PAUSE_AFTER_COMMIT, one_shot=True)
        alter = cql.run_async(f"ALTER TABLE {ks}.t ADD a int")
        await manager.api.wait_for_injection_enter(server.ip_addr, PAUSE_AFTER_COMMIT)

        # The other statement is prepared during the pause, and the batch runs
        # both.
        new = cql.prepare(f"UPDATE {ks}.t SET d = 21 WHERE p = 0 IF d = 20")
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(old)
        batch.add(new)
        rows = await cql.run_async(batch)

        await manager.api.message_injection(server.ip_addr, PAUSE_AFTER_COMMIT)
        await alter

        # The batch applied both updates, and the ALTER added the column.
        assert rows[0][0]
        row = (await cql.run_async(f"SELECT a, b, d FROM {ks}.t WHERE p = 0"))[0]
        assert (row.a, row.b, row.d) == (None, 11, 21)
