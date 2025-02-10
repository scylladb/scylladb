#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import random
import pytest
import logging
import time
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier, inject_error_one_shot
from test.pylib.util import wait_for
from test.cluster.util import new_test_keyspace
from test.cluster.conftest import skip_mode


logger = logging.getLogger(__name__)

async def wait_for_view_is_built(cql, ks, view):
    async def check_table():
        result = await cql.run_async(f"SELECT * FROM system.built_views WHERE keyspace_name='{ks}' AND view_name='{view}'")
        return True if len(result) == 1 else None
    await wait_for(check_table, time.time() + 60)

async def get_tablet_count(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str, is_view: bool):
    host = manager.cql.cluster.metadata.get_host(server.ip_addr)

    # read_barrier is needed to ensure that local tablet metadata on the queried node
    # reflects the finalized tablet movement.
    await read_barrier(manager.api, server.ip_addr)

    if is_view:
        table_id = await manager.get_view_id(keyspace_name, table_name)
    else:
        table_id = await manager.get_table_id(keyspace_name, table_name)
    rows = await manager.cql.run_async(f"SELECT tablet_count FROM system.tablets where "
                                       f"table_id = {table_id}", host=host)
    return rows[0].tablet_count

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

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_view_building_with_tablet_split(manager: ManagerClient):
    cmdline = [
        '--logger-log-level', 'storage_service=debug',
        '--logger-log-level', 'table=debug',
        '--logger-log-level', 'load_balancer=debug',
        '--logger-log-level', 'view_building_coordinator=debug',
        '--target-tablet-size-in-bytes', '10000',
    ]
    server = await manager.server_add(config={
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
    }, cmdline=cmdline)
    cql = manager.get_cql()

    await manager.api.disable_tablet_balancing(server.ip_addr)

    keys = 250
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int, c blob)")
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.test(pk, v, c) VALUES (?, ?, ?)")
        await asyncio.gather(*[cql.run_async(insert_stmt, [pk, pk, random.randbytes(100)]) for pk in range(keys)])

        await manager.api.enable_injection(server.ip_addr, "view_building_worker_pause_before_consume", one_shot=True)
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.test WHERE v IS NOT NULL primary key(pk, v)")

        await manager.api.flush_keyspace(server.ip_addr, ks)
        await inject_error_one_shot(manager.api, server.ip_addr, "tablet_allocator_shuffle")
        await manager.api.enable_injection(server.ip_addr, "tablet_load_stats_refresh_before_rebalancing", False)

        await manager.api.enable_tablet_balancing(server.ip_addr)
        await manager.api.disable_injection(server.ip_addr, "view_building_worker_pause_before_consume")
        await wait_for_view_is_built(cql, ks, 'mv')
        rows = await cql.run_async(f"SELECT * FROM {ks}.mv")
        assert len(list(rows)) == keys
