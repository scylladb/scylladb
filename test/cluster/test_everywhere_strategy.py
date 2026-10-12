#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time

from cassandra import ConsistencyLevel  # type: ignore

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts
import pytest


# Ported from scylla-dtest everywhere_strategy_test.py::TestEverywhereConsistency::test_bootstrap_second_dc.
# Bootstrapping the first node of a new DC streams it all the data of an
# EverywhereStrategy keyspace: with the old DC's node down, the new node
# alone returns every row.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_bootstrap_second_dc(manager: ScyllaClusterManager):
    node1 = await manager.server_add(property_file={"dc": "dc1", "rack": "r1"})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'EverywhereStrategy'} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        insert = cql.prepare(f"INSERT INTO {ks}.t (pk) VALUES (?)")
        insert.consistency_level = ConsistencyLevel.ALL
        for k in range(100):
            await cql.run_async(insert, [k])

        node2 = await manager.server_add(property_file={"dc": "dc2", "rack": "r1"})
        await manager.server_stop_gracefully(node1.server_id)
        cql2 = await manager.get_cql_exclusive(node2)
        try:
            # Prepared, as in the dtest.
            select = cql2.prepare(f"select pk from {ks}.t")
            select.consistency_level = ConsistencyLevel.LOCAL_ONE
            rows = await cql2.run_async(select)
            assert {r.pk for r in rows} == set(range(100))
        finally:
            cql2.cluster.shutdown()
        await manager.server_start(node1.server_id)
        await wait_for_cql_and_get_hosts(cql, [node1, node2], time.time() + 60)
