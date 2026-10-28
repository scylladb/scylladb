#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Test consistency of schema changes with server hard stop.
"""
import asyncio
import time
from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore
from test.cluster.util import new_test_keyspace, wait_for_token_ring_and_group0_consistency
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts
import pytest

pytestmark = pytest.mark.prepare_3_racks_cluster


async def test_topology_schema_changes(manager, random_tables):
    """Test schema consistency with restart, add, and sudden stop of servers"""
    table = await random_tables.add_table(ncolumns=5)
    servers = await manager.running_servers()

    # Test add column after server restart
    await manager.server_restart(servers[1].server_id)
    await manager.servers_see_each_other(servers)
    await table.add_column()
    await random_tables.verify_schema()

    # Test add column after adding a server
    await manager.server_add(property_file=servers[1].property_file())
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    await table.add_column()
    await random_tables.verify_schema()

    # Test add column after hard stop of a server (1/3)
    await manager.server_stop(servers[1].server_id, convict=True)
    await table.add_column()
    await random_tables.verify_schema()


KS_OPTS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"


def all_cl(query):
    return SimpleStatement(query, consistency_level=ConsistencyLevel.ALL)


async def test_prepared_statement_after_metadata_alter_and_restarts(manager: ScyllaClusterManager):
    """A prepared CL=ALL insert keeps working after an ALTER that changes
    only table metadata (the comment) and restarts of the other replicas."""
    servers = await manager.running_servers()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)")
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        insert = cql.prepare(f"INSERT INTO {ks}.t (k, v) VALUES (?, ?)")
        insert.consistency_level = ConsistencyLevel.ALL
        await cql.run_async(f"ALTER TABLE {ks}.t WITH comment = 'altered'")
        for s in servers[1:]:
            await manager.server_restart(s.server_id)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        for k in range(20):
            await cql.run_async(insert, [k, k], host=hosts[0])
        assert sorted(await cql.run_async(all_cl(f"SELECT k, v FROM {ks}.t"))) == [(k, k) for k in range(20)]


@pytest.mark.parametrize("ddl", ["create", "alter", "drop"])
async def test_schema_change_while_node_is_killed(manager: ScyllaClusterManager, ddl: str):
    """A node killed while a table is created, altered or dropped has the
    new schema, and the data, once it is back."""
    servers = await manager.running_servers()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        if ddl != "create":
            await cql.run_async(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)")
        statement = {"create": f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)",
                     "alter": f"ALTER TABLE {ks}.t ADD c int",
                     "drop": f"DROP TABLE {ks}.t"}[ddl]
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        await asyncio.gather(cql.run_async(statement, host=hosts[0]), manager.server_stop(servers[1].server_id, convict=False))
        await manager.server_start(servers[1].server_id)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        await read_barrier(manager.api, servers[1].ip_addr)
        tables = await cql.run_async(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{ks}'", host=hosts[1])
        if ddl == "drop":
            assert list(tables) == []
            return
        assert [r.table_name for r in tables] == ["t"]
        columns = {r.column_name for r in await cql.run_async(
            f"SELECT column_name FROM system_schema.columns WHERE keyspace_name = '{ks}' AND table_name = 't'", host=hosts[1])}
        assert columns == ({"k", "v", "c"} if ddl == "alter" else {"k", "v"})
        await cql.run_async(all_cl(f"INSERT INTO {ks}.t (k, v) VALUES (1, 1)"), host=hosts[0])
        assert list(await cql.run_async(all_cl(f"SELECT k, v FROM {ks}.t"), host=hosts[1])) == [(1, 1)]


@pytest.mark.parametrize("graceful", [True, False], ids=["graceful", "kill"])
async def test_rejoined_node_reads_added_column(manager: ScyllaClusterManager, graceful: bool):
    """A node down while a column was added and written serves that column
    at CL=ALL once it is back."""
    servers = await manager.running_servers()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)")
        await cql.run_async(all_cl(f"INSERT INTO {ks}.t (k, v) VALUES (1, 1)"))
        if graceful:
            await manager.server_stop_gracefully(servers[1].server_id)
        else:
            await manager.server_stop(servers[1].server_id, convict=False)
        await cql.run_async(f"ALTER TABLE {ks}.t ADD c int")
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.t (k, v, c) VALUES (2, 2, 2)", consistency_level=ConsistencyLevel.QUORUM))
        await manager.server_start(servers[1].server_id)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        rows = await cql.run_async(all_cl(f"SELECT k, v, c FROM {ks}.t"), host=hosts[1])
        assert sorted(rows) == [(1, 1, None), (2, 2, 2)]


@pytest.mark.parametrize("same_columns", [False, True], ids=["other_columns", "same_columns"])
async def test_table_recreated_while_node_down(manager: ScyllaClusterManager, same_columns: bool):
    """A table dropped and created again with the same name while a replica
    was down does not get the old table's data back once the replica
    returns: reads at CL=ALL see only what was written to the new table."""
    servers = await manager.running_servers()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, KS_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)")
        await cql.run_async(all_cl(f"INSERT INTO {ks}.t (k, v) VALUES (1, 1)"))
        await manager.server_stop_gracefully(servers[1].server_id)
        await cql.run_async(f"DROP TABLE {ks}.t")
        await cql.run_async(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, {'v int' if same_columns else 'w text'})")
        await manager.server_start(servers[1].server_id)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        await read_barrier(manager.api, servers[1].ip_addr)
        if same_columns:
            await cql.run_async(all_cl(f"INSERT INTO {ks}.t (k, v) VALUES (2, 2)"), host=hosts[0])
            assert list(await cql.run_async(all_cl(f"SELECT k, v FROM {ks}.t"), host=hosts[1])) == [(2, 2)]
        else:
            assert list(await cql.run_async(all_cl(f"SELECT * FROM {ks}.t"), host=hosts[1])) == []
