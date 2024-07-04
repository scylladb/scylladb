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
from test.pylib.util import wait_for_view

from cassandra.cluster import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

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

# Reproduces #18929
# Test view build operations running during node shutdown and view drain.
# Verify the drain order is correct and the view build doesn't fail with
# database write failures.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_mv_build_during_shutdown(manager: ManagerClient):
    server = await manager.server_add()

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")

    for i in range(100):
        await cql.run_async(f"insert into ks.t (pk, v) values ({i}, {i+1})")

    # Start building two views. The first is delayed by the injection, and the second
    # view build is queued, waiting on the view builder semaphore.
    await manager.api.enable_injection(server.ip_addr, "delay_before_get_view_natural_endpoint", one_shot=True)
    cql.run_async("CREATE materialized view ks.t_view1 AS select pk, v from ks.t where v is not null primary key (v, pk)")
    cql.run_async("CREATE materialized view ks.t_view2 AS select pk, v from ks.t where v is not null primary key (v, pk)")

    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    # Start node shutdown. this will drain and abort the running view build.
    # As we continue and drain the view building of view1 and view2 we will
    # have writes to the database, running during the draining phase.
    # If the drain order is correct it should succeed without errors.
    await manager.server_stop_gracefully(server.server_id)

    # Verify no db write errors during the shutdown
    occurrences = await log.grep(expr="exception during mutation write", from_mark=mark)
    assert len(occurrences) == 0
