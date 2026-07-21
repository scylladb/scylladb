#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.util import Duration
import pytest

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.cluster.util import reconnect_driver
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts


NANOS_PER_SECOND = 1_000_000_000

V1_SERVICE_LEVELS = {
    "sl_v1_interactive": (Duration(0, 0, 30 * NANOS_PER_SECOND), "interactive", 1000),
    "sl_v1_batch": (Duration(0, 0, 60 * NANOS_PER_SECOND), "batch", 500)}

SERVICE_LEVEL_VER_QUERY = "SELECT value FROM system.scylla_local WHERE key = 'service_level_version'"


def assert_service_levels(rows, expected):
    rows_by_name = {row.service_level: row for row in rows}
    assert rows_by_name.keys() == expected.keys()

    for name, (timeout, workload_type, shares) in expected.items():
        row = rows_by_name[name]
        assert row.timeout == timeout
        assert row.workload_type == workload_type
        assert row.shares == shares


async def create_system_distributed_service_levels(cql):
    await cql.run_async("""
        CREATE TABLE IF NOT EXISTS system_distributed.service_levels (
            service_level text PRIMARY KEY,
            timeout duration,
            workload_type text,
            shares int)""")


@pytest.mark.skip_mode(mode="release", reason="error injection is disabled in release mode")
async def test_self_heals_service_levels_v1_after_restart(manager: ManagerClient, scale_timeout: callable):
    """Reproduces a raft-topology cluster created before service levels were initialized as v2."""

    service_level_list_query = "LIST ALL SERVICE LEVELS"
    service_levels_v1_query = SimpleStatement("SELECT service_level, timeout, workload_type, shares FROM system_distributed.service_levels",
        consistency_level=ConsistencyLevel.ONE)

    config = {
        **auth_config,
        "error_injections_at_startup": ["skip_service_levels_v2_initialization"]}

    server = await manager.server_add(config=config)
    cql = manager.get_cql()

    await create_system_distributed_service_levels(cql)

    async def service_levels_version_initialized():
        rows = await cql.run_async(SERVICE_LEVEL_VER_QUERY)
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
        version_rows = await cql.run_async(SERVICE_LEVEL_VER_QUERY)
        if not version_rows or version_rows[0].value != "2":
            return None

        service_levels = await cql.run_async(service_level_list_query)
        service_level_names = {row.service_level for row in service_levels}
        if service_level_names != expected_service_levels.keys():
            return None
        assert_service_levels(service_levels, expected_service_levels)
        return True

    await wait_for(service_levels_v2_healed, time.time() + scale_timeout(30))


async def _validate_host_service_level_ver(hosts, cql, ver):
    for host in hosts:
        version_rows = await cql.run_async(SERVICE_LEVEL_VER_QUERY, host=host)
        if not version_rows:
            raise RuntimeError("No service level returned from query, aborting!")
        assert version_rows[0].value == str(ver)


@pytest.mark.skip_mode(mode="release", reason="error injection is disabled in release mode")
async def test_self_heal_waits_for_unavailable_replicas(manager: ManagerClient, scale_timeout: callable):
    """Verify SCYLLADB-3337: service level migration should wait for all nodes to be available"""

    config = {
        **auth_config,
        "error_injections_at_startup": ["skip_service_levels_v2_initialization"]}

    servers = await manager.servers_add(3, config=config)
    cql = manager.get_cql()

    await create_system_distributed_service_levels(cql)

    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + scale_timeout(30))
    await _validate_host_service_level_ver(hosts, cql, 1)

    restarting_server, unavailable_server = servers[:2]
    await manager.server_stop_gracefully(restarting_server.server_id)
    await manager.server_stop_gracefully(unavailable_server.server_id)
    await manager.server_update_config(restarting_server.server_id, "error_injections_at_startup", [])

    restarting_log = await manager.server_open_log(restarting_server.server_id)
    log_mark = await restarting_log.mark()
    restart_task = asyncio.create_task(manager.server_start(restarting_server.server_id, timeout=scale_timeout(180)))

    await restarting_log.wait_for(
        "Cannot self-heal service levels version because not all replicas are alive",
        from_mark=log_mark,
        timeout=scale_timeout(60))
    assert not restart_task.done()

    await manager.server_start(unavailable_server.server_id, timeout=scale_timeout(30))
    await restart_task

    cql = await reconnect_driver(manager)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + scale_timeout(30))
    await _validate_host_service_level_ver(hosts, cql, 2)
