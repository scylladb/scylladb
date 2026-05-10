#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.util import Duration
import pytest

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.cluster.util import reconnect_driver
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for


NANOS_PER_SECOND = 1_000_000_000

V1_SERVICE_LEVELS = {
    "sl_v1_interactive": (Duration(0, 0, 30 * NANOS_PER_SECOND), "interactive", 1000),
    "sl_v1_batch": (Duration(0, 0, 60 * NANOS_PER_SECOND), "batch", 500)}


def assert_service_levels(rows, expected):
    rows_by_name = {row.service_level: row for row in rows}
    assert rows_by_name.keys() == expected.keys()

    for name, (timeout, workload_type, shares) in expected.items():
        row = rows_by_name[name]
        assert row.timeout == timeout
        assert row.workload_type == workload_type
        assert row.shares == shares


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode="release", reason="error injection is disabled in release mode")
async def test_self_heals_service_levels_v1_after_restart(manager: ManagerClient, scale_timeout: callable):
    """Reproduces a raft-topology cluster created before service levels were initialized as v2."""

    service_level_ver_query = "SELECT value FROM system.scylla_local WHERE key = 'service_level_version'"
    service_level_list_query = "LIST ALL SERVICE LEVELS"
    service_levels_v1_query = SimpleStatement("SELECT service_level, timeout, workload_type, shares FROM system_distributed.service_levels",
        consistency_level=ConsistencyLevel.ONE)
    
    config = {
        **auth_config,
        "error_injections_at_startup": ["skip_service_levels_v2_initialization"]}

    server = await manager.server_add(config=config)
    cql = manager.get_cql()

    #create the service_levels v1 table because in 2026.2 its removed 
    await cql.run_async("""
        CREATE TABLE IF NOT EXISTS system_distributed.service_levels (
            service_level text PRIMARY KEY,
            timeout duration,
            workload_type text,
            shares int)""")

    async def service_levels_version_initialized():
        rows = await cql.run_async(service_level_ver_query)
        if not rows:
            return None
        return rows

    version_rows = await wait_for(service_levels_version_initialized, time.time() + scale_timeout(30))
    assert version_rows[0].value == "1"

    insert_service_level = cql.prepare("INSERT INTO system_distributed.service_levels (service_level, timeout, workload_type, shares) VALUES (?, ?, ?, ?)")
    insert_service_level.consistency_level = ConsistencyLevel.ONE

    for name, (timeout, workload_type, shares) in V1_SERVICE_LEVELS.items():
        await cql.run_async(insert_service_level, (name, timeout, workload_type, shares))

    assert_service_levels(await cql.run_async(service_levels_v1_query), V1_SERVICE_LEVELS)
    assert await cql.run_async(service_level_list_query) == []

    await manager.server_stop_gracefully(server.server_id)
    await manager.server_update_config(server.server_id, "error_injections_at_startup", [])
    await manager.server_start(server.server_id)
    cql = await reconnect_driver(manager)
    await manager.api.reload_raft_topology_state(server.ip_addr)

    expected_service_levels = {**V1_SERVICE_LEVELS, "driver": (None, "batch", 200)}

    async def service_levels_v2_healed():
        version_rows = await cql.run_async(service_level_ver_query)
        if not version_rows or version_rows[0].value != "2":
            return None

        service_levels = await cql.run_async(service_level_list_query)
        service_level_names = {row.service_level for row in service_levels}
        if service_level_names != expected_service_levels.keys():
            return None
        assert_service_levels(service_levels, expected_service_levels)
        return True

    await wait_for(service_levels_v2_healed, time.time() + scale_timeout(30))
