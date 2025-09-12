#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import pytest

from cassandra import ConsistencyLevel

from test.cluster.test_hints import await_sync_point, create_sync_point
from test.pylib.manager_client import ManagerClient

@pytest.mark.asyncio
async def test_write_to_hinted_handoff_for_views(manager: ManagerClient):
    """
    Verify that view updates work correctly: they're stored and replayed.

    1. Create a 2-node cluster and a keyspace with RF=1. Create a table
       and a materialized view.
    2. Stop node 2.
    3. Insert data. Some of the mutations may fail if the partition key
       belongs to node 2; ignore them. About half of the successful mutations
       should produce view hints.
    4. Revive node 2.
    5. Wait for the view hints to be sent to node 2.
    6. Query data from the view. We should see all of the successful mutations
       there.
    """

    # We want to test view hints specifically, so let's disable regular hints.
    config = {"hinted_handoff_enabled": False}
    s1, s2 = await manager.servers_add(2, config=config, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"}
    ])

    cql, _ = await manager.get_ready_cql([s1])

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv AS SELECT * FROM ks.t "
                        "WHERE pk IS NOT NULL AND ck IS NOT NULL "
                        "AND v IS NOT NULL PRIMARY KEY ((ck, pk), v)")

    await manager.server_stop_gracefully(s2.server_id)

    stmt = cql.prepare("INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)")
    stmt.consistency_level = ConsistencyLevel.ONE

    # If the replica for the base table turns out to be the dead node, ignore it.
    # Note that the replicas for the base table and the replicas for the view
    # may be different.
    async def insert_data(i):
        try:
            await cql.run_async(stmt, (i, i, i))
            return i
        except:
            return None

    indices = await asyncio.gather(*[insert_data(i) for i in range(1000)])
    indices = [index for index in indices if index is not None]

    # Hints are written asynchronously, so let's give the node a bit of time.
    await asyncio.sleep(1)

    sync_point1 = create_sync_point(s1)

    await manager.server_start(s2.server_id, wait_others=1)

    await_sync_point(s1, sync_point1, 120)

    cql, _ = await manager.get_ready_cql([s1, s2])

    rows = await cql.run_async("SELECT * FROM ks.mv")
    # Normally, if a mutation fails, it doesn't mean that it wasn't applied;
    # for instance, the consistency level might've just not been satisfied.
    # Due to that, the number of rows in the base table could, under normal
    # circumstances, be greater than `len(indices)`. That's impossible here,
    # though, because we use RF=1.
    assert len(rows) == len(indices)
