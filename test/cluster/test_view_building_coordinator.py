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
from test.pylib.util import wait_for_view, wait_for_first_completed, gather_safely
from test.pylib.internal_types import ServerInfo, HostID
from test.pylib.tablets import get_tablet_replicas
from test.cluster.mv.tablets.test_mv_tablets import pin_the_only_tablet
from test.cluster.util import new_test_keyspace

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

async def pause_view_building_tasks(manager: ManagerClient, token: int | None = None):
    servers = await manager.running_servers()
    params = {}
    if token is not None:
        params["token"] = token
    await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, VIEW_BUILDING_WORKER_PAUSE_BUILD_RANGE_TASK, one_shot=True, parameters=params) for s in servers))

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

async def check_view_contents(cql: Session, ks: str, table: str, view: str, partition_list: list[int] | None = None):
    where_clause = ""
    if partition_list:
        where_clause = f"WHERE key in ({','.join(map(str, partition_list))})"
    expected_rows = set(await cql.run_async(f"SELECT key, c, v FROM {ks}.{table} {where_clause}"))
    rows = set(await cql.run_async(f"SELECT key, c, v FROM {ks}.{view}"))
    assert rows == expected_rows

#############
### TESTS ###
#############

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
async def test_alter_base_schema_while_build_in_progress(manager: ManagerClient):
    node_count = 3
    servers = await manager.servers_add(node_count, cmdline=cmdline_loggers)
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")
 
        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)
 
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")
         
        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)
 
        await cql.run_async(f"ALTER TABLE {ks}.tab ADD u text")
 
        await unpause_view_building_tasks(manager)
 
        await wait_for_view(cql, 'mv_cf_view', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view")

@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_increase_rf_while_build_in_progress(manager: ManagerClient):
    node_count = 4
    servers = await manager.servers_add(node_count, config={"rf_rack_valid_keyspaces": "false", "enable_tablets": "true"}, cmdline=cmdline_loggers)
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': true}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await populate_base_table(cql, ks, "tab")

        marks = await mark_all_servers(manager)
        await pause_view_building_tasks(manager)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v) ")
        
        await wait_for_some_view_build_tasks_to_get_stuck(manager, marks)

        await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 2}}")

        await unpause_view_building_tasks(manager)

        await wait_for_view(cql, 'mv_cf_view', node_count)
        await check_view_contents(cql, ks, "tab", "mv_cf_view")
