#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
CQL queries whose data is spread over several nodes: scans and range
tombstones with RF=1, and USING TIMEOUT with RF=3, also with a node down.

Ported from scylla-dtest's cql_additional_tests.py. Like the dtests, the
keyspaces use vnodes, by the nodes' default.
"""

import logging

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, ReadTimeout, WriteTimeout
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


# Like the dtests' nodes, new keyspaces use vnodes by default.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled'}


def keyspace_opts(rf: int) -> str:
    return f"WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"


# A scan of a table with RF=1 on two nodes returns the rows of both nodes
# (CASSANDRA-1337).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_scan_across_two_nodes(manager: ScyllaClusterManager):
    await manager.servers_add(2, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (k text PRIMARY KEY, v int);")
        await cql.run_async(f"INSERT INTO {ks}.test (k, v) VALUES ('foo', 0)")
        await cql.run_async(f"INSERT INTO {ks}.test (k, v) VALUES ('bar', 1)")
        assert sorted(await cql.run_async(f"SELECT * FROM {ks}.test")) == [("bar", 1), ("foo", 0)]


# Deletions by a clustering prefix (range tombstones) of partitions spread
# over three nodes with RF=1, so the mutations are sent between nodes: the
# rows with the prefix are gone, before and after the nodes flush.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_range_tombstones_across_nodes(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.test1"
        await cql.run_async(f"CREATE TABLE {table} (k int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY (k, c1, c2));")
        rows, col1, col2 = 5, 2, 2
        cpr = col1 * col2
        for i in range(rows):
            for j in range(col1):
                for k in range(col2):
                    n = (i * cpr) + (j * col2) + k
                    await cql.run_async(f"INSERT INTO {table} (k, c1, c2, v1, v2) VALUES ({i}, {j}, {k}, {n}, {n})")

        async def check(first):
            for i in range(rows):
                res = await cql.run_async(f"SELECT v1, v2 FROM {table} WHERE k = {i}")
                assert [tuple(r) for r in res] == [(x, x) for x in range(i * cpr + first, (i + 1) * cpr)]
        await check(0)
        for i in range(rows):
            await cql.run_async(f"DELETE FROM {table} WHERE k = {i} AND c1 = 0")
        await check(col1)
        for s in servers:
            await manager.api.flush_all_keyspaces(s.ip_addr)
        await check(col1)


# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_cql_timeout_parameter.
# USING TIMEOUT: writes and reads with a 60 minute timeout succeed, with a
# 0 ms one they time out at the coordinator, also with one of the three
# replicas down (#7777). As in the dtest's session, statements use
# consistency level ONE; its prepare(nodes=3, rf=3) puts the nodes in dc1,
# one per rack.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_using_timeout_with_node_down(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, auto_rack_dc="dc1")
    cql = manager.get_cql()

    def one(query):
        return SimpleStatement(query, consistency_level=ConsistencyLevel.ONE)

    async with new_test_keyspace(manager, keyspace_opts(3)) as ks:
        table = f"{ks}.test"
        await cql.run_async(one(f"CREATE TABLE {table} (k int, p int, s int, v int, PRIMARY KEY (k, p))"))
        for value in range(10):
            await cql.run_async(one(f"INSERT INTO {table}(k, p) VALUES ({value}, {value}) USING TIMEOUT 60m"))
            await cql.run_async(one(f"UPDATE {table} USING TIMEOUT 60m SET v = {value} WHERE p = {value} AND k = {value}"))
        timeout_msg = "Coordinator node timed out waiting for replica nodes"
        with pytest.raises(WriteTimeout, match=timeout_msg):
            await cql.run_async(one(f"INSERT INTO {table}(k, p) VALUES (3, 4) USING TIMEOUT 0ms"))
        with pytest.raises(ReadTimeout, match=timeout_msg):
            await cql.run_async(one(f"SELECT * FROM {table} USING TIMEOUT 0ms"))
        await manager.server_stop_gracefully(servers[2].server_id)
        await manager.others_not_see_server(servers[2].ip_addr)
        with pytest.raises(WriteTimeout, match=timeout_msg):
            await cql.run_async(one(f"UPDATE {table} USING TIMEOUT 0ms SET v = 1 WHERE p = 1 AND k = 1"))
        with pytest.raises(ReadTimeout, match=timeout_msg):
            await cql.run_async(one(f"SELECT * FROM {table} USING TIMEOUT 0ms"))
        assert len(list(await cql.run_async(one(f"SELECT * FROM {table} USING TIMEOUT 60m")))) == 10
        await manager.server_start(servers[2].server_id)


# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_nameless_index.
# Four nodes in one rack and RF=1: an index created without a name gets the
# default name, finds the rows (index writes are asynchronous, so the read is
# retried), and can be dropped by that name, after which the query is refused.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nameless_index(manager: ScyllaClusterManager):
    # The dtests' node defaults: DROP INDEX snapshots the index view, and schema changes flush.
    await manager.servers_add(4, config={**CONFIG, 'auto_snapshot': True, 'flush_schema_tables_after_modification': True},
                              property_file=[{'dc': 'dc1', 'rack': 'rack1'}] * 4)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        cql.execute(f"USE {ks}")
        cql.execute("CREATE TABLE users (id text PRIMARY KEY, birth_year int)")
        cql.execute("CREATE INDEX on users(birth_year)")
        for name, year in [('Tom', 42), ('Paul', 24), ('Bob', 42)]:
            cql.execute(f"INSERT INTO users (id, birth_year) VALUES ('{name}', {year})")
        for attempt in range(3):
            rows = sorted(r.id for r in cql.execute("SELECT id FROM users WHERE birth_year = 42"))
            if rows == ['Bob', 'Tom'] or attempt == 2:
                break
        assert rows == ['Bob', 'Tom']
        cql.execute("DROP INDEX users_birth_year_idx")
        with pytest.raises(InvalidRequest):
            cql.execute("SELECT id FROM users WHERE birth_year = 42")
