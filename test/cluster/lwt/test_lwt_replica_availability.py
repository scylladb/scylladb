#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


# An LWT needs only a quorum of replicas: with RF=3 it must succeed with one
# replica down, fail with Unavailable with two down, and work again once
# enough replicas are back. Reproduces CASSANDRA-8640, where LWT required
# all replicas to be alive.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_lwt_replica_availability(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, auto_rack_dc="dc1", config={"tablets_mode_for_new_keyspaces": "disabled"})
    # Coordinate everything on the first server, which is never stopped.
    cql = await manager.get_cql_exclusive(servers[0])
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (k int PRIMARY KEY, v int)")

        async def lwt_insert(k):
            rows = await cql.run_async(f"INSERT INTO {ks}.test (k, v) VALUES ({k}, {k}) IF NOT EXISTS")
            assert rows[0].applied

        await lwt_insert(0)

        await manager.server_stop_gracefully(servers[2].server_id)
        await lwt_insert(1)

        await manager.server_stop_gracefully(servers[1].server_id)
        # No retries, so that the driver reports the Unavailable error itself.
        insert = SimpleStatement(f"INSERT INTO {ks}.test (k, v) VALUES (2, 2) IF NOT EXISTS", retry_policy=FallthroughRetryPolicy())
        with pytest.raises(Unavailable):
            await cql.run_async(insert)

        await manager.server_start(servers[1].server_id)
        await manager.server_sees_other_server(servers[0].ip_addr, servers[1].ip_addr)
        await lwt_insert(3)

        await manager.server_start(servers[2].server_id)
        await manager.server_sees_other_server(servers[0].ip_addr, servers[2].ip_addr)
        await lwt_insert(4)

        # A replica which was down missed some writes, so read at QUORUM.
        select = SimpleStatement(f"SELECT k FROM {ks}.test", consistency_level=ConsistencyLevel.QUORUM)
        assert sorted(row.k for row in await cql.run_async(select)) == [0, 1, 3, 4]
