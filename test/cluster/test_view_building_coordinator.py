#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import os
import asyncio
import pytest
import time
import logging
import re

from test.cluster.conftest import skip_mode
from test.pylib.util import wait_for_view, wait_for_first_completed, gather_safely, wait_for
from test.pylib.internal_types import ServerInfo, HostID
from test.pylib.tablets import get_all_tablet_replicas, get_tablet_replica, get_tablet_replicas, get_tablet_count
from test.cluster.mv.tablets.test_mv_tablets import pin_the_only_tablet
from test.cluster.util import new_test_keyspace, get_topology_coordinator, trigger_stepdown, wait_for_cql_and_get_hosts
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
    '--logger-log-level', 'load_balancer=debug',
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
    if change == "increase":
        node_count = 2
        rack_layout = ["rack1", "rack2"]
    elif change == "decrease":
        node_count = 3
        rack_layout = ["rack1", "rack1", "rack2"]
    else:
        assert False

    property_file = [{"dc": "dc1", "rack": rack} for rack in rack_layout]
    servers = await manager.servers_add(node_count, config={"enable_tablets": "true"}, cmdline=cmdline_loggers,
                                        property_file=property_file)
    cql, _ = await manager.get_ready_cql(servers)
    await disable_tablet_load_balancing_on_all_servers(manager)

    rf = node_count - 1

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")

        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        new_rf = rf + 1 if change == "increase" else rf - 1
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': {new_rf}}}")

        await unpause_view_building_tasks(manager)

        await wait_for_view(cql, 'mv_cf_view', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view")

@pytest.mark.parametrize("operation", ["add", "remove", "decommission", "replace"])
@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_node_operation_during_view_building(manager: ManagerClient, operation: str):
    if operation == "remove" or operation == "decommission":
        node_count = 4
        rack_layout = ["rack1", "rack2", "rack3", "rack3"]
    else:
        node_count = 3
        rack_layout = ["rack1", "rack2", "rack3"]

    property_file = [{"dc": "dc1", "rack": rack} for rack in rack_layout]
    servers = await manager.servers_add(node_count, config={"enable_tablets": "true"},
                                        cmdline=cmdline_loggers,
                                        property_file=property_file)

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
            property_file = servers[-1].property_file()
            await manager.server_add(config={"enable_tablets": "true"}, cmdline=cmdline_loggers, property_file=property_file)
            node_count = node_count + 1
        elif operation == "remove":
            await manager.server_stop_gracefully(servers[-1].server_id)
            await manager.remove_node(servers[0].server_id, servers[-1].server_id)
            node_count = node_count - 1
        elif operation == "decommission":
            await manager.decommission_node(servers[-1].server_id)
            node_count = node_count - 1
        elif operation == "replace":
            property_file = servers[-1].property_file()
            await manager.server_stop_gracefully(servers[-1].server_id)
            replace_cfg = ReplaceConfig(replaced_id = servers[-1].server_id, reuse_ip_addr = False, use_host_id = True)
            await manager.server_add(replace_cfg, config={"enable_tablets": "true"}, cmdline=cmdline_loggers,
                                     property_file=property_file)

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
    """
    The test creates a situation where a single tablet is replicated across
    multiple DCs / racks, and all those tablet replicas are eligible for migration.
    As described in scylladb/scylladb#25912, the tablet load balancer computes
    plans for each DC independently and, in each DC, it may decide to migrate
    a replica of the same tablet. However, the system.tablets schema only allows
    migration of only one replica of a tablet at a time, so only one of those
    generated migrations will "win" and will actually proceed. This confused
    the view build coordinator because it reacted to all of the proposed migrations
    by cancelling the view building task associated with the to-be-migrated base
    replica, but the tasks aborted due to the migrations that "lost" may never
    be resumed, resulting in a view which never gets built.

    Scenario:
    - Create a 3 node cluster, with two DCs, two racks in the first DC and one rack
      in the second DC. One node in each rack
    - Create a keyspace with one replica in each rack
    - Pause tablet load balancing and view building
    - Within that keyspace, create a base table and a colocated view with two tablets
    - Add one more node to each rack (cluster has 6 nodes in total afterwards)
    - Unpause tablet load balancing
    - Wait until tablets are evenly distributed
    - Unpause view building
    - Wait until the view is built

    At the moment when tablet load balancing is unpaused, the tablet load balancer
    should be in a situation where it should consider moving a replica of a tablet
    in each rack, generating 3 conflicting plans.
    """

    rack_property_files = [
        {'dc': 'dc1', 'rack': 'rack1'},
        {'dc': 'dc1', 'rack': 'rack2'},
        {'dc': 'dc2', 'rack': 'rack3'},
    ]

    servers = []
    for property_file in rack_property_files:
        servers.append(await manager.server_add(property_file=property_file, cmdline=cmdline_loggers))

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)
    await pause_view_building_tasks(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1} AND tablets = {'initial': 2}") as ks:
        cql = manager.get_cql()

        # The base and the view are colocated, so their tablets are always migrated together.
        # With 2 tablets per table and RF=3, we will have 2 tablet groups (base+view) per node.
        await cql.run_async(f"CREATE TABLE {ks}.base (pk int, ck int, v int, PRIMARY KEY (pk, ck))")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT pk, ck, v FROM {ks}.base WHERE ck IS NOT NULL AND v IS NOT NULL PRIMARY KEY (pk, v, ck)")

        async def get_nodes_which_are_tablet_replicas():
            all_tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'base')
            return frozenset(r[0] for tablet in all_tablet_replicas for r in tablet.replicas)

        # Add one more node to each rack. Load balancing is disabled so new nodes should not get new tablets.
        for property_file in rack_property_files:
            servers.append(await manager.server_add(property_file=property_file, cmdline=cmdline_loggers))
        assert len(await get_nodes_which_are_tablet_replicas()) == 3

        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        # The effect of unpausing the balancer should be that all replicas are distributed evenly between nodes
        # (1 base + 1 view tablet for each node).
        async def tablets_are_evenly_distributed():
            if len(await get_nodes_which_are_tablet_replicas()) == 6:
                return True

        await wait_for(tablets_are_evenly_distributed, time.time() + 60)

        await unpause_view_building_tasks(manager)
        await wait_for_view(cql, "mv", len(servers))

async def get_table_dir(manager: ManagerClient, server: ServerInfo, ks: str, table: str):
    workdir = await manager.server_get_workdir(server.server_id)
    ks_dir = os.path.join(workdir, "data", ks)

    table_pattern = re.compile(f"{table}-")
    for root, dirs, files in os.walk(ks_dir):
        for d in dirs:
            if table_pattern.match(d):
                return os.path.join(root, d)

async def delete_table_sstables(manager: ManagerClient, server: ServerInfo, ks: str, table: str):
    table_dir = await get_table_dir(manager, server, ks, table)
    for root, dirs, files in os.walk(table_dir):
        for file in files:
            path = os.path.join(root, file)
            os.remove(path)
        break # break unconditionally here to remove only files in `table_dir`

async def assert_row_count_on_host(cql, host, ks, table, row_count):
    stmt = SimpleStatement(f"SELECT * FROM {ks}.{table}", consistency_level = ConsistencyLevel.LOCAL_ONE)
    rows = await cql.run_async(stmt, host=host)
    assert len(rows) == row_count

# Reproducer for issue scylla-enterprise#4572
# This test triggers file-based streaming of staging sstables
# and expects that view updates are generated after the streaming is finished.
# Staging sstables are created by removing table's normal sstables and repairing it.
# Then processing staging sstables is prevented using error injection and the tablet is
# migrated to a new node, which will receive the staging sstables via file streaming.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_file_streaming(manager: ManagerClient):
    node_count = 2
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
    ])
    cql, hosts = await manager.get_ready_cql(servers)
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}} AND tablets = {{'initial': 1}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v int, PRIMARY KEY (key))")

        # Populate the base table
        rows = 1000
        for i in range(rows):
            await cql.run_async(f"INSERT INTO {ks}.tab (key, c, v) VALUES ({i}, {i}, 1)")

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.tab "
                        "WHERE key IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, key) ")
        await wait_for_view(cql, 'mv', node_count)

        # Flush on node0
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "tab")
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "mv")

        # Delete sstables
        await delete_table_sstables(manager, servers[0], ks, "tab")
        await delete_table_sstables(manager, servers[0], ks, "mv")

        # Restart node0
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.server_start(servers[0].server_id)

        # Assert that node0 has no data for base table and MV
        hosts = await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 30)
        await manager.server_stop_gracefully(servers[1].server_id)
        await assert_row_count_on_host(cql, hosts[0], ks, "tab", 0)
        await assert_row_count_on_host(cql, hosts[0], ks, "mv", 0)
        await manager.server_start(servers[1].server_id)

        # Repair the base table
        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()
        await manager.api.enable_injection(servers[0].ip_addr, "view_update_generator_consume_staging_sstable", one_shot=False)
        await manager.api.repair(servers[0].ip_addr, ks, "tab")
        await s0_log.wait_for(f"Processing {ks} failed for table tab", from_mark=s0_mark, timeout=60)
        await s0_log.wait_for(f"Finished user-requested repair for tablet keyspace={ks}", from_mark=s0_mark, timeout=60)

        await manager.server_stop_gracefully(servers[1].server_id)
        await assert_row_count_on_host(cql, hosts[0], ks, "tab", 1000)
        await assert_row_count_on_host(cql, hosts[0], ks, "mv", 0)
        await manager.server_start(servers[1].server_id)

        # Add node3
        await manager.api.disable_tablet_balancing(servers[0].ip_addr)
        new_server = await manager.server_add(cmdline=cmdline_loggers + ['--logger-log-level', 'view_update_generator=trace'], property_file={"dc": "dc1", "rack": "r1"})

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        tablet_token = 0 # Doesn't matter since there is one tablet
        async def get_tablet_replica_for_s0():
            replicas = await get_tablet_replicas(manager, servers[0], ks, "tab", tablet_token)
            for replica in replicas:
                if replica[0] == s0_host_id:
                    return replica
            return None

        s3_log = await manager.server_open_log(new_server.server_id)
        s3_mark = await s3_log.mark()

        # Move tablet from node0 to node3
        src_replica = await get_tablet_replica_for_s0()
        if not src_replica:
            assert False, "no replica"
        s3_host_id = await manager.get_host_id(new_server.server_id)
        await manager.api.move_tablet(servers[0].ip_addr, ks, "tab", src_replica[0], src_replica[1], s3_host_id, 0, tablet_token)
        # Without the fix for issue scylla-enterprise#4572 following line times out and ...
        await s3_log.wait_for(f"Processed {ks}.tab:", from_mark=s3_mark, timeout=60)

        new_hosts = await wait_for_cql_and_get_hosts(cql, [new_server], time.time() + 30)
        await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
        await assert_row_count_on_host(cql, new_hosts[0], ks, "tab", 1000)
        # Start node0 here because view replica might not be migrated to the new node, since they are not colocated
        await manager.server_start(servers[0].server_id)
        # ... this check fails because the mv has 0 rows on the new host
        await assert_row_count_on_host(cql, new_hosts[0], ks, "mv", 1000)
        await manager.server_start(servers[1].server_id)

# Reproducer for issue scylladb#26244
# Purpose of this test is to check if staging sstables managed by `view_building_worker`
# correctly reacts to tablet merge.
#
# This test starts with 2 node cluster, a table (RF=2) with tablet count = 2 and a view.
# Scenario:
# - create staging sstables by removing sstables on node0
#   but prevent its processing by view update generator using error injection
# - suspend view building coordinator using error injection
# - create a new node 2, migrate both tablet replicas from node0 to node2
#   - at this point both staging sstables are migrated to the new node
#     and view building tasks are created for them
# - trigger tablet merge on the base table (from 2 to 1)
# - restore view building coordinator by disabling error injection
# - staging sstables should be processed now
# - the view should be consistent with the base table
#
# This test fails because of 2 reasons:
# - #1 (no support for tablet migration in staging sstables)
#   When staging sstables are migrated to node2,
#   sstable for tablet1 is on shard1 (in the view building worker) and sstable for tablet2 in on shard2.
#   When tablet merge is starting, firstly the tablet2 is migrated to shard1,
#   but its sstable stays on shard2 in the worker.
# - #2 (no support for tablet merge in staging sstables)
#   Even if both staging sstables would be on the same shard, the sstables are stored in a map indexed by
#   tablet's last token. So when there are 2 tablets, the map is
#   [last token of tablet1] -> [sstable for tablet1]
#   [last token of tablet2] -> [sstable for tablet2]
#   However after tablet merge, view building tasks of those sstables are merged into one task,
#   but the map stays the same, so one sstable won't be processed
#   because last token after tablet merge = last token of tablet2 before merge
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.skip(reason="#26244")
async def test_staging_sstables_with_tablet_merge(manager: ManagerClient):
    node_count = 2
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
    ], config={
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        'error_injections_at_startup': ['allow_tablet_merge_with_views'],
    })
    cql, hosts = await manager.get_ready_cql(servers)
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v int, PRIMARY KEY (key)) WITH tablets = {{'min_tablet_count': 2}}")

        assert await get_tablet_count(manager, servers[0], ks, "tab") == 2
        # Populate the base table
        rows = 1000
        for i in range(rows):
            await cql.run_async(f"INSERT INTO {ks}.tab (key, c, v) VALUES ({i}, {i}, 1)")

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.tab "
                        "WHERE key IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, key)")
        await wait_for_view(cql, 'mv', node_count)
        assert await get_tablet_count(manager, servers[0], ks, "tab") == 2

        # Flush on node0
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "tab")
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "mv")

        # Delete sstables
        await delete_table_sstables(manager, servers[0], ks, "tab")
        await delete_table_sstables(manager, servers[0], ks, "mv")

        # Restart node0
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.server_start(servers[0].server_id)

        # Assert that node0 has no data for base table and MV
        hosts = await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 30)
        await manager.server_stop_gracefully(servers[1].server_id)
        await assert_row_count_on_host(cql, hosts[0], ks, "tab", 0)
        await assert_row_count_on_host(cql, hosts[0], ks, "mv", 0)
        await manager.server_start(servers[1].server_id)

        # Repair the base table
        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()
        await manager.api.enable_injection(servers[0].ip_addr, "view_update_generator_consume_staging_sstable", one_shot=False)
        await manager.api.repair(servers[0].ip_addr, ks, "tab")
        await s0_log.wait_for(f"Processing {ks} failed for table tab", from_mark=s0_mark, timeout=60)
        await s0_log.wait_for(f"Finished user-requested repair for tablet keyspace={ks}", from_mark=s0_mark, timeout=60)

        await manager.server_stop_gracefully(servers[1].server_id)
        await assert_row_count_on_host(cql, hosts[0], ks, "tab", 1000)
        await assert_row_count_on_host(cql, hosts[0], ks, "mv", 0)
        await manager.server_start(servers[1].server_id)

        # Add node3
        await manager.api.disable_tablet_balancing(servers[0].ip_addr)
        new_server = await manager.server_add(cmdline=cmdline_loggers + ['--logger-log-level', 'view_update_generator=trace'], property_file={"dc": "dc1", "rack": "r1"}, config={
            'tablet_load_stats_refresh_interval_in_seconds': 1,
            'error_injections_at_startup': ['allow_tablet_merge_with_views'],
        })

        await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, "view_building_coordinator_skip_main_loop", one_shot=False) for s in servers + [new_server]))
        s3_log = await manager.server_open_log(new_server.server_id)
        s3_mark = await s3_log.mark()

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        replicas = await get_all_tablet_replicas(manager, servers[0], ks, "tab")
        for tablet_replica in replicas:
            def get_tablet_replica_for_s0():
                for r in tablet_replica.replicas:
                    if r[0] == s0_host_id:
                        return r
                return None
            src_replica = get_tablet_replica_for_s0()

            if not src_replica:
                assert False, "no replica"

            s3_host_id = await manager.get_host_id(new_server.server_id)
            await manager.api.move_tablet(servers[0].ip_addr, ks, "tab", src_replica[0], src_replica[1], s3_host_id, src_replica[1], tablet_replica.last_token)

        async def tablet_count_is(expected_tablet_count):
            new_tablet_count = await get_tablet_count(manager, servers[0], ks, 'tab')
            if new_tablet_count == expected_tablet_count:
                return True

        await manager.api.enable_tablet_balancing(servers[0].ip_addr)
        assert await get_tablet_count(manager, servers[0], ks, "tab") == 2
        await cql.run_async(f"ALTER TABLE {ks}.tab WITH tablets = {{'min_tablet_count': 1}}")
        await wait_for(lambda: tablet_count_is(1), time.time() + 60)

        await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, "view_building_coordinator_skip_main_loop") for s in servers + [new_server]))
        # The test fails here because of no support for tablet migration for staging sstables stored in view_building_worker. (#1)
        await s3_log.wait_for(f"Processed {ks}.tab:", from_mark=s3_mark, timeout=60)
        await asyncio.sleep(1)

        new_hosts = await wait_for_cql_and_get_hosts(cql, [new_server], time.time() + 30)
        await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
        await assert_row_count_on_host(cql, new_hosts[0], ks, "tab", 1000)
        await manager.server_start(servers[0].server_id)
        # And also fails here because not all staging sstables are processed after tablet merge. (#2)
        await assert_row_count_on_host(cql, new_hosts[0], ks, "mv", 1000)
        await manager.server_start(servers[1].server_id)
