#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Tests of the eventually-consistent TRUNCATE of tablet tables, which deletes the data
written at or before its timestamp, like a DELETE of every partition.
See docs/dev/eventually-consistent-truncate.md.
"""
import logging

from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_view
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)


async def select_pks(cql, table: str, cl: ConsistencyLevel = ConsistencyLevel.ALL, where: str = "") -> set[int]:
    rows = await cql.run_async(SimpleStatement(f"SELECT pk FROM {table} {where}", consistency_level=cl))
    return {row.pk for row in rows}


async def test_truncate_using_timestamp_with_views(manager: ScyllaClusterManager):
    """
    TRUNCATE ... USING TIMESTAMP deletes the data written at or before the timestamp,
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

        await cql.run_async(f"TRUNCATE {table} USING TIMESTAMP {truncated_at}")

        assert await select_pks(cql, table) == {100}
        assert await select_pks(cql, f"{ks}.mv") == {100}
        assert await select_pks(cql, table, where="WHERE v = 100") == {100}
        assert await select_pks(cql, table, where="WHERE v = 1") == set()


async def test_truncate_with_replica_down(manager: ScyllaClusterManager):
    """
    TRUNCATE does not require all the table's replicas to be alive: a replica which is
    down when the table is truncated observes the truncation when it comes back.
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

        await cql.run_async(f"TRUNCATE {table}")
        assert await select_pks(cql, table, cl=ConsistencyLevel.QUORUM) == set()

        await manager.server_start(servers[2].server_id)
        await manager.server_sees_other_server(servers[0].ip_addr, servers[2].ip_addr)

        # Read the data of the replica which was down, through it.
        cql2 = await manager.get_cql_exclusive(servers[2])
        assert await select_pks(cql2, table, cl=ConsistencyLevel.ONE) == set()
        assert await select_pks(cql, table) == set()
