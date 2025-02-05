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


logger = logging.getLogger(__name__)

async def wait_for_view_is_built(cql, ks, view):
    async def check_table():
        result = await cql.run_async(f"SELECT * FROM system.built_tablet_views WHERE keyspace_name='{ks}' AND view_name='{view}'")
        if len(result) == 1:
            return True
        else:
            return None
    await wait_for(check_table, time.time() + 60)

@pytest.mark.asyncio
async def test_tablet_view_building(manager: ManagerClient):
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    async def validate_view_is_built(replication_factor, rows):
        ks = f"ks{replication_factor}"
        await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}")
        await cql.run_async(f"CREATE TABLE {ks}.tbl(id int primary key, v1 int, v2 int)")
        stmt = cql.prepare(f"INSERT INTO {ks}.tbl(id, v1, v2) VALUES (?, ?, ?)")
        for i in range(rows):
            await cql.run_async(stmt, [i, i, i])
        
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.tbl WHERE v1 IS NOT NULL primary key(id, v1)")
        await wait_for_view_is_built(cql, ks, "mv")
        rows_in_view = (await cql.run_async(f"SELECT count(*) FROM {ks}.mv"))[0].count

        assert rows == rows_in_view
    await validate_view_is_built(1, 10)
    await validate_view_is_built(2, 10)