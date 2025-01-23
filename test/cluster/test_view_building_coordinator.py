#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import logging
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for
from test.cluster.util import new_test_keyspace


logger = logging.getLogger(__name__)

async def wait_for_view_is_built(cql, ks, view):
    async def check_table():
        result = await cql.run_async(f"SELECT * FROM system.built_views WHERE keyspace_name='{ks}' AND view_name='{view}'")
        return True if len(result) == 1 else None
    await wait_for(check_table, time.time() + 60)

@pytest.mark.asyncio
@pytest.mark.parametrize("rf", [1, 2])
async def test_tablet_view_building(manager: ManagerClient, rf):
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    rows = 1024
    async with new_test_keyspace(manager, f"WITH replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tbl(id int primary key, v1 int, v2 int)")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.tbl(id, v1, v2) VALUES ({k}, {k}, {k});") for k in range(rows)])
        
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.tbl WHERE v1 IS NOT NULL primary key(id, v1)")
        await wait_for_view_is_built(cql, ks, "mv")
        
        tasks_count = (await cql.run_async(f"SELECT count(*) FROM system.view_building_coordinator_tasks"))[0].count
        assert tasks_count == 0

        rows_in_view = (await cql.run_async(f"SELECT count(*) FROM {ks}.mv"))[0].count
        assert rows == rows_in_view
