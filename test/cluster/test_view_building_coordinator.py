#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import asyncio
import pytest
import time
import logging

from test.cluster.conftest import skip_mode
from test.pylib.util import wait_for_view, wait_for_first_completed, gather_safely, wait_for
from test.pylib.internal_types import ServerInfo, HostID
from test.pylib.tablets import get_tablet_replicas, get_tablet_replica
from test.cluster.mv.tablets.test_mv_tablets import pin_the_only_tablet
from test.cluster.util import new_test_keyspace, get_topology_coordinator, trigger_stepdown
from test.pylib.scylla_cluster import ReplaceConfig

from cassandra.cluster import Session, ConsistencyLevel, EXEC_PROFILE_DEFAULT # type: ignore
from cassandra.cqltypes import Int32Type # type: ignore
from cassandra.policies import FallthroughRetryPolicy # type: ignore
from cassandra.query import SimpleStatement, BoundStatement # type: ignore

logger = logging.getLogger(__name__)

VIEW_BUILDING_COORDINATOR_PAUSE_MAIN_LOOP = "view_building_coordinator_pause_main_loop"
VIEW_BUILDING_WORKER_PAUSE_BUILD_RANGE_TASK = "view_building_worker_pause_build_range_task"

ROW_COUNT = 1000
ROWS_PER_PARTITION = 10

cmdline_loggers = [
    '--logger-log-level', 'storage_service=debug',
    '--logger-log-level', 'raft_topology=debug',
    '--logger-log-level', 'view_building_coordinator=debug',
    '--logger-log-level', 'view_building_worker=debug',
]

async def mark_all_servers(manager: ManagerClient) -> list[int]:
    servers = await manager.running_servers()
    logs = await asyncio.gather(*(manager.server_open_log(s.server_id) for s in servers))
    return await asyncio.gather(*(l.mark() for l in logs))

async def pause_view_build_coordinator(manager: ManagerClient):
    """Pause view build coordinator."""
    servers = await manager.running_servers()
    await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, VIEW_BUILDING_COORDINATOR_PAUSE_MAIN_LOOP, one_shot=True) for s in servers))

async def unpause_view_build_coordinator(manager: ManagerClient):
    """Unpause the view build coordinator."""
    servers = await manager.running_servers()
    await asyncio.gather(*(manager.api.message_injection(s.ip_addr, VIEW_BUILDING_COORDINATOR_PAUSE_MAIN_LOOP) for s in servers))
    await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, VIEW_BUILDING_COORDINATOR_PAUSE_MAIN_LOOP) for s in servers))

async def pause_view_building_tasks(manager: ManagerClient, token: int | None = None, pause_all: bool = True):
    servers = await manager.running_servers()
    params = {}
    if token is not None:
        params["token"] = token
    await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, VIEW_BUILDING_WORKER_PAUSE_BUILD_RANGE_TASK, one_shot=pause_all, parameters=params) for s in servers))

async def unpause_view_building_tasks(manager: ManagerClient):
    servers = await manager.running_servers()
    await asyncio.gather(*(manager.api.message_injection(s.ip_addr, VIEW_BUILDING_WORKER_PAUSE_BUILD_RANGE_TASK) for s in servers))
    await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, VIEW_BUILDING_WORKER_PAUSE_BUILD_RANGE_TASK) for s in servers))

async def wait_for_message_on_any_server(manager: ManagerClient, message: str, marks: list[int]):
    servers = await manager.running_servers()
    logs = await asyncio.gather(*(manager.server_open_log(s.server_id) for s in servers))
    assert len(servers) == len(marks)
    await wait_for_first_completed([l.wait_for(message, from_mark=m, timeout=60) for l, m in zip(logs, marks)])

async def wait_for_some_view_build_tasks_to_get_stuck(manager: ManagerClient, marks: list[int]):
    return await wait_for_message_on_any_server(manager, "do_build_range: paused, waiting for message", marks)

async def disable_tablet_load_balancing_on_all_servers(manager: ManagerClient):
    servers = await manager.running_servers()
    await asyncio.gather(*(manager.api.disable_tablet_balancing(s.ip_addr) for s in servers))

async def enable_tablet_load_balancing_on_all_servers(manager: ManagerClient):
    servers = await manager.running_servers()
    await asyncio.gather(*(manager.api.enable_tablet_balancing(s.ip_addr) for s in servers))

async def populate_base_table(cql: Session, ks: str, tbl: str):
    for i in range(ROW_COUNT):
        await cql.run_async(f"INSERT INTO {ks}.{tbl} (key, c, v) VALUES ({i // ROWS_PER_PARTITION}, {i % ROWS_PER_PARTITION}, '{i}')")

async def check_view_contents(cql: Session, ks: str, table: str, view: str, partition_list: list[int] | None = None, clustering_key: str = "c"):
    where_clause = ""
    if partition_list:
        where_clause = f"WHERE key in ({','.join(map(str, partition_list))})"
    expected_rows = set(await cql.run_async(f"SELECT key, {clustering_key}, v FROM {ks}.{table} {where_clause}"))
    rows = set(await cql.run_async(f"SELECT key, {clustering_key}, v FROM {ks}.{view}"))
    assert rows == expected_rows

#############
### TESTS ###
#############

@pytest.mark.asyncio
async def test_build_no_data(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                         "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")
        await wait_for_view(cql, 'mv_cf_view', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view")

@pytest.mark.asyncio
async def test_build_one_view(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                         "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")
        await wait_for_view(cql, 'mv_cf_view', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view")

@pytest.mark.asyncio
async def test_build_filtered_view(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers)
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key in (1, 4, 9) AND v IS NOT NULL PRIMARY KEY (c, key, v) ")
        await wait_for_view(cql, 'mv_cf_view', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view", partition_list=[1, 4, 9])


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_build_two_views(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql, _ = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        await pause_view_build_coordinator(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view1 AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view2 AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await unpause_view_build_coordinator(manager)

        await wait_for_view(cql, 'mv_cf_view1', node_count)
        await wait_for_view(cql, 'mv_cf_view2', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view1")
        await check_view_contents(cql, ks, "tab", "mv_cf_view2")

@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_add_view_while_build_in_progress(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql, _ = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view1 AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view2 AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await unpause_view_building_tasks(manager)

        await wait_for_view(cql, 'mv_cf_view1', node_count)
        await wait_for_view(cql, 'mv_cf_view2', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view1")
        await check_view_contents(cql, ks, "tab", "mv_cf_view2")

@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_remove_some_view_while_build_in_progress(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers)
    cql, _ = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view1 AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view2 AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        await cql.run_async(f"DROP MATERIALIZED VIEW {ks}.mv_cf_view2")

        await unpause_view_building_tasks(manager)

        await wait_for_view(cql, 'mv_cf_view1', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view1")

@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_abort_building_by_remove_view(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers)
    cql, _ = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        await cql.run_async(f"DROP MATERIALIZED VIEW {ks}.mv_cf_view")

        await unpause_view_building_tasks(manager)

        views = await cql.run_async(f"SELECT * FROM system_schema.views WHERE keyspace_name = '{ks}'")
        assert len(views) == 0

@pytest.mark.parametrize("change", ["add", "rename"])
@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_alter_base_schema_while_build_in_progress(manager: ManagerClient, change: str):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers)
    cql, _ = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        if change == "add":
            await cql.run_async(f"ALTER TABLE {ks}.tab ADD u text")
        elif change == "rename":
            await cql.run_async(f"ALTER TABLE {ks}.tab RENAME c TO renamed_c")

        await unpause_view_building_tasks(manager)

        await wait_for_view(cql, 'mv_cf_view', node_count)
        if change == "add":
            await check_view_contents(cql, ks, "tab", "mv_cf_view")
            added_column = await cql.run_async(f"SELECT u FROM {ks}.mv_cf_view")
            assert added_column[0].u is None  # The new column should be present but not populated
        elif change == "rename":
            await check_view_contents(cql, ks, "tab", "mv_cf_view", clustering_key="renamed_c")

@pytest.mark.parametrize("change", ["increase", "decrease"])
@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_change_rf_while_build_in_progress(manager: ManagerClient, change: str):
    node_count = 4
    servers = await manager.servers_add(node_count, config={"rf_rack_valid_keyspaces": "false", "enable_tablets": "true"}, cmdline=cmdline_loggers)
    cql, _ = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    rf = 3
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        new_rf = rf + 1 if change == "increase" else rf - 1
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': {new_rf}}}")

        await unpause_view_building_tasks(manager)

        await wait_for_view(cql, 'mv_cf_view', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view")

@pytest.mark.parametrize("operation", ["add", "remove", "decommission", "replace"])
@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_node_operation_during_view_building(manager: ManagerClient, operation: str):
    node_count = 4 if operation == "remove" or operation == "decommission" else 3
    servers = await manager.servers_add(node_count, config={"rf_rack_valid_keyspaces": "false", "enable_tablets": "true"}, cmdline=cmdline_loggers)
    cql, _ = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")      
        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        if operation == "add":
            await manager.server_add(config={"rf_rack_valid_keyspaces": "false", "enable_tablets": "true"}, cmdline=cmdline_loggers)
            node_count = node_count + 1
        elif operation == "remove":
            await manager.server_stop_gracefully(servers[-1].server_id)
            await manager.remove_node(servers[0].server_id, servers[-1].server_id)
            node_count = node_count - 1
        elif operation == "decommission":
            await manager.decommission_node(servers[-1].server_id)
            node_count = node_count - 1
        elif operation == "replace":
            await manager.server_stop_gracefully(servers[-1].server_id)
            replace_cfg = ReplaceConfig(replaced_id = servers[-1].server_id, reuse_ip_addr = False, use_host_id = True)
            await manager.server_add(replace_cfg, config={"rf_rack_valid_keyspaces": "false", "enable_tablets": "true"}, cmdline=cmdline_loggers)

        await unpause_view_building_tasks(manager)
        await wait_for_view(cql, 'mv_cf_view', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view")

@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_leader_change_while_building(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    host_ids = [await manager.get_host_id(s.server_id) for s in servers]
    cql, _ = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view1 AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        coord = await get_topology_coordinator(manager)
        coord_idx = host_ids.index(coord)
        await trigger_stepdown(manager, servers[coord_idx])

        await unpause_view_building_tasks(manager)

        await wait_for_view(cql, 'mv_cf_view1', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view1")

@pytest.mark.asyncio
@pytest.mark.xfail
@skip_mode("release", "error injections are not supported in release mode")
async def test_truncate_while_building(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql, _ = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view1 AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        await cql.run_async(f"TRUNCATE {ks}.tab")

        await unpause_view_building_tasks(manager)

        await wait_for_view(cql, 'mv_cf_view1', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view1")

@pytest.mark.parametrize("view_action", ["finish_build", "drop_while_building"])
@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_scylla_views_builds_in_progress(manager: ManagerClient, view_action):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql, hosts = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    async def check_scylla_views_builds_in_progress(expect_zero_rows: bool):
        async def check():
            for h in hosts:
                result = await cql.run_async("SELECT * FROM system.scylla_views_builds_in_progress", host=h)
                has_zero_rows = len(result) == 0
                if has_zero_rows != expect_zero_rows:
                    return None
            return True
        await wait_for(check, time.time() + 60)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view1 AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        await check_scylla_views_builds_in_progress(expect_zero_rows=False)

        if view_action == "drop_while_building":
            await cql.run_async(f"DROP MATERIALIZED VIEW {ks}.mv_cf_view1")

        await unpause_view_building_tasks(manager)

        if view_action == "finish_build":
            await wait_for_view(cql, 'mv_cf_view1', node_count)
            await check_view_contents(cql, ks, "tab", "mv_cf_view1")

        await check_scylla_views_builds_in_progress(expect_zero_rows=True)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_view_building_while_tablet_streaming_fail(manager: ManagerClient):
    servers = [await manager.server_add(cmdline=cmdline_loggers)]
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "tab")

        servers.append(await manager.server_add(cmdline=cmdline_loggers))
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, 'tab', tablet_token)
        await manager.api.enable_injection(servers[0].ip_addr, "stream_tablet_fail", one_shot=True)
        await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, VIEW_BUILDING_WORKER_PAUSE_BUILD_RANGE_TASK) for s in servers))
        await manager.api.move_tablet(servers[0].ip_addr, ks, "tab", replica[0], replica[1], s1_host_id, 0, tablet_token)

        await wait_for_view(cql, 'mv_cf_view', 2)
        await check_view_contents(cql, ks, "tab", "mv_cf_view")

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_view_building_failure(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        await manager.api.enable_injection(servers[1].ip_addr, "do_build_range_fail", one_shot=False)
        s1_log = await manager.server_open_log(servers[1].server_id)
        s1_mark = await s1_log.mark()

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                         "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await s1_log.wait_for(f"do_build_range failed due to error injection", from_mark=s1_mark, timeout=60)
        await manager.api.disable_injection(servers[1].ip_addr, "do_build_range_fail")

        await wait_for_view(cql, 'mv_cf_view', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view")

# Reproduces scylladb/scylladb#25912
@pytest.mark.asyncio
async def test_concurrent_tablet_migrations(manager: ManagerClient):
    # This test created 2 nodes per DC (node1/dc1, node2/dc1, node1/dc2, node2/dc2)
    # and the initial tablet value in the keyspace is 1.
    # Load balancing is turned off at first.
    #
    # This test makes sure that all tablets (for base table and view) are on node1
    # in both DCs.
    # Then, when load balancing is enabled again, load balancer should generate 2 migrations
    # for the same table:
    # - in dc1: tablet of base table node1 -> node2
    # - in dc2: tablet of base table node1 -> node2
    # Since these are 2 migrations of different tablets but in the same table,
    # one of them is overwritten by the second one and this leads to scylladb/scylladb#25912

    servers = {
        "dc1": {
            "rack1": await manager.server_add(property_file={'dc': 'dc1', 'rack': 'rack1'}, cmdline=cmdline_loggers),
            "rack2": await manager.server_add(property_file={'dc': 'dc1', 'rack': 'rack2'}, cmdline=cmdline_loggers)
        },
        "dc2": {
            "rack1": await manager.server_add(property_file={'dc': 'dc2', 'rack': 'rack1'}, cmdline=cmdline_loggers),
            "rack2": await manager.server_add(property_file={'dc': 'dc2', 'rack': 'rack2'}, cmdline=cmdline_loggers)
        }
    }

    cql = manager.get_cql()
    await disable_tablet_load_balancing_on_all_servers(manager)

    host_ids = {
        dc: {rack: await manager.get_host_id(s.server_id) for rack, s in dc_servers.items()} for dc, dc_servers in servers.items()
    }

    def get_host_id_of_rack1_in_the_same_dc(my_host_id):
        if my_host_id in host_ids["dc1"].values():
            return host_ids["dc1"]["rack1"]
        elif my_host_id in host_ids["dc2"].values():
            return host_ids["dc2"]["rack1"]
        assert False, "host id not found in any dc"

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1} AND tablets = {'initial': 1}") as ks:
        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE TABLE {ks}.base (pk int, ck int, PRIMARY KEY (pk, ck))")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT pk, ck FROM {ks}.base WHERE ck IS NOT NULL PRIMARY KEY (ck, pk)")

        # Make sure both tablets are on rack1 in both DCs
        for table in ["base", "mv"]:
            replicas = await get_tablet_replicas(manager, servers["dc1"]["rack1"], ks, table, 0)
            for replica in replicas:
                if replica[0] not in [host_ids["dc1"]["rack1"], host_ids["dc2"]["rack1"]]:
                    target_host = get_host_id_of_rack1_in_the_same_dc(replica[0])
                    await manager.api.move_tablet(servers["dc1"]["rack1"].ip_addr, ks, table, replica[0], replica[1], target_host, 0, 0, force=True)

        # Enable load balancing, wait for balancing is finished and resume work on view building
        await enable_tablet_load_balancing_on_all_servers(manager)
        await manager.api.quiesce_topology(servers["dc1"]["rack1"].ip_addr)
        await unpause_view_building_tasks(manager)

        await wait_for_view(cql, "mv", 4)
