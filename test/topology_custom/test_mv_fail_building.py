#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import pytest
import time
from test.topology.conftest import skip_mode
from test.pylib.manager_client import ManagerClient

from cassandra.cluster import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore


async def wait_for_view(cql, name, node_count):
    deadline = time.time() + 120
    while time.time() < deadline:
        done = await cql.run_async(f"SELECT COUNT(*) FROM system_distributed.view_build_status WHERE status = 'SUCCESS' AND view_name = '{name}' ALLOW FILTERING")
        if done[0][0] == node_count:
            return
        else:
            time.sleep(0.2)
    raise Exception("Timeout waiting for views to build")


# This test makes sure that even if the view building encounter errors, the view building is eventually finished
# and the view is consistent with the base table.
# Reproduces the scenario in #19261
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_mv_fail_building(manager: ManagerClient) -> None:
    node_count = 3
    servers = await manager.servers_add(node_count)
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}")
    await cql.run_async(f"CREATE TABLE ks.tab (key int, c int, PRIMARY KEY (key, c))")
    # Insert initial rows for building an index
    for i in range(10):
        await cql.run_async(f"INSERT INTO ks.tab (key, c) VALUES ({i}, 0)")

    for s in servers:
        await manager.api.enable_injection(s.ip_addr, 'view_building_failure', one_shot=True)

    await cql.run_async(f"CREATE INDEX tab_by_c ON ks.tab (c)")

    # Insert more rows while building an index which is delayed by the 'view_building_failure' injection.
    for i in range(10, 20):
        await cql.run_async(f"INSERT INTO ks.tab (key, c) VALUES ({i}, 0)")
    await wait_for_view(cql, "tab_by_c_index", node_count)

    # Verify that all rows were inserted to the view by reading from the index
    rows = await cql.run_async(SimpleStatement(f"SELECT * FROM ks.tab WHERE c = 0", consistency_level=ConsistencyLevel.ALL))
    base_rows = await cql.run_async(SimpleStatement(f"SELECT * FROM ks.tab", consistency_level=ConsistencyLevel.ALL))
    assert sorted(rows) == sorted(base_rows)

    await cql.run_async(f"DROP KEYSPACE ks")
