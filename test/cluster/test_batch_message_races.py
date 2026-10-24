#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# A BATCH message carries its statements one by one, and Scylla prepares each
# textual one separately, a large one in a thread that yields (SCYLLADB-219).
# Other requests can run in between. An error injection makes every token of
# the large statement slow to parse, so they run during its parse.
import pytest
from cassandra.query import BatchStatement, BatchType  # type: ignore

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import inject_error
from test.cluster.util import new_test_keyspace

# Scylla parses a statement this large in a thread that yields.
LARGE = 16 * 1024
SPIN = "parse_statement_in_thread_spin"
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
        session = cql.cluster.connect()
        try:
            await session.run_async(f"USE {ks1}")
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            batch.add(f"INSERT INTO t (k, v) VALUES (1, '{'x' * LARGE}')")
            batch.add("INSERT INTO t (k, v) VALUES (2, 'y')")
            async with inject_error(manager.api, server.ip_addr, SPIN):
                result = session.run_async(batch)
                await session.run_async(f"USE {ks2}")
                enters = await manager.api.get_injection_enter_count(server.ip_addr, SPIN)
                await result
                assert await manager.api.get_injection_enter_count(server.ip_addr, SPIN) > enters
        finally:
            session.shutdown()
        assert sorted(row.k for row in await cql.run_async(f"SELECT k FROM {ks1}.t")) == [1, 2]
        assert not await cql.run_async(f"SELECT k FROM {ks2}.t")


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_schema_change_during_batch_message(manager: ScyllaClusterManager):
    """A schema change between the prepares of two statements of a BATCH message
    leaves them on different versions of the table. The batch must be prepared
    again and run, and a conditional one must not crash the node."""
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, b int, d int)")
        await cql.run_async(f"INSERT INTO {ks}.t (p, b, d) VALUES (0, 10, 20)")
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(f"UPDATE {ks}.t SET b = 11 WHERE p = 0 IF b = 10")
        # The IN list gives the parse enough tokens to outlast the ALTER.
        batch.add(f"UPDATE {ks}.t SET d = 21 WHERE p = 0 IF d IN ({', '.join(['20'] * 30)}) /* {'x' * LARGE} */")
        async with inject_error(manager.api, server.ip_addr, SPIN):
            result = cql.run_async(batch)
            await manager.api.wait_for_injection_enter(server.ip_addr, SPIN)
            await cql.run_async(f"ALTER TABLE {ks}.t ADD a int")
            enters = await manager.api.get_injection_enter_count(server.ip_addr, SPIN)
            rows = await result
            assert await manager.api.get_injection_enter_count(server.ip_addr, SPIN) > enters
        assert rows[0][0]
        row = (await cql.run_async(f"SELECT b, d FROM {ks}.t WHERE p = 0"))[0]
        assert (row.b, row.d) == (11, 21)
