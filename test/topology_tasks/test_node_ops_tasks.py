#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from functools import partial
from typing import Optional
from test.pylib.internal_types import IPAddress, ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import InjectionHandler, inject_error_one_shot
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import wait_for
from test.topology_tasks.task_manager_client import TaskManagerClient

import asyncio
import logging
import pytest
import time

from test.topology_tasks.task_manager_types import TaskID, TaskStats, TaskStatus

logger = logging.getLogger(__name__)

async def get_status_allow_peer_connection_failure(tm: TaskManagerClient, node_ip: IPAddress, task_id: TaskID) -> Optional[TaskStatus]:
    ret = await tm.api.client.get_json(f"/task_manager/task_status/{task_id}", host = node_ip, allow_failed = True)
    resp_status = ret.get("code", 200)
    if resp_status == 200:
        assert(type(ret) == dict)
        return TaskStatus(**ret)
    else:
        assert resp_status == 500 and "seastar::rpc::closed_error (connection is closed)" in ret["message"]
        return None

async def compare_status_on_all_servers(task_id: TaskID, tm: TaskManagerClient, servers: list[ServerInfo]) -> TaskStatus:
    statuses = [await get_status_allow_peer_connection_failure(tm, server.ip_addr, task_id) for server in servers]
    statuses = [s for s in statuses if s is not None]
    assert statuses, "No statuses to compare"
    assert all(status.id == statuses[0].id and status.start_time == statuses[0].start_time for status in statuses)
    return statuses[0]

async def get_new_virtual_tasks_list(tm: TaskManagerClient, module_name: str, server: ServerInfo,
                                     previous_vts: list[TaskID]) -> list[TaskStats]:
    return [task for task in await tm.list_tasks(server.ip_addr, module_name)
                     if task.kind == "cluster" and task.task_id not in previous_vts]

async def get_new_virtual_tasks_statuses(tm: TaskManagerClient, module_name: str, servers: list[ServerInfo],
                                     previous_vts: list[TaskID], expected_task_num: int) -> list[TaskStatus]:
    vts_list = await get_new_virtual_tasks_list(tm, module_name, servers[0], previous_vts)
    assert len(vts_list) == expected_task_num, "Wrong cluster tasks number"

    return [await compare_status_on_all_servers(stats.task_id, tm, servers) for stats in vts_list]

def check_virtual_task_status(virtual_task: TaskStatus, expected_state: str, expected_type: str, expected_children_num: int) -> None:
    assert virtual_task.state == expected_state
    assert virtual_task.type == expected_type or virtual_task.type == ""
    assert virtual_task.kind == "cluster"
    assert virtual_task.scope == "cluster"
    assert not virtual_task.is_abortable
    assert virtual_task.parent_id == "none"
    assert len(virtual_task.children_ids) == expected_children_num

def check_regular_task_status(task: TaskStatus, expected_state: str, expected_type: str, expected_scope: str,
                              expected_parent_id: str, expected_children_num: int) -> None:
    assert task.state == expected_state
    assert task.type == expected_type
    assert task.kind == "node"
    assert task.scope == expected_scope
    assert not task.is_abortable
    assert task.parent_id == expected_parent_id
    assert len(task.children_ids) == expected_children_num

async def check_bootstrap_tasks_tree(tm: TaskManagerClient, module_name: str, servers: list[ServerInfo],
                          previous_vts: list[TaskID] = []) -> tuple[list[ServerInfo], list[TaskID]]:
    virtual_tasks = await get_new_virtual_tasks_statuses(tm, module_name, servers, previous_vts, len(servers))

    # No streaming task for first node bootstrap.
    bootstrap_with_streaming = [status.id for status in virtual_tasks if status.children_ids]
    assert len(bootstrap_with_streaming) == len(virtual_tasks) - 1, "All but one tasks should have children"

    for virtual_task in virtual_tasks:
        if virtual_task.id in bootstrap_with_streaming:
            check_virtual_task_status(virtual_task, "done", "join", 1)

            child = await tm.get_task_status(virtual_task.children_ids[0]["node"], virtual_task.children_ids[0]["task_id"])
            check_regular_task_status(child, "done", "bootstrap: streaming", "node", virtual_task.id, 0)
        else:
            check_virtual_task_status(virtual_task, "done", "join", 0)

    return (servers, [vt.id for vt in virtual_tasks])

async def check_replace_tasks_tree(manager: ManagerClient, tm: TaskManagerClient, module_name: str, servers: list[ServerInfo],
                          previous_vts: list[TaskID]) -> tuple[list[ServerInfo], list[TaskID]]:
    assert servers, "No servers available"

    replaced_server = servers[0]
    logger.info(f"Stopping node {replaced_server}")
    await manager.server_stop_gracefully(replaced_server.server_id)

    logger.info(f"Replacing node {replaced_server}")
    replace_cfg = ReplaceConfig(replaced_id = replaced_server.server_id, reuse_ip_addr = False, use_host_id = False)
    replacing_server = await manager.server_add(replace_cfg)

    servers = servers[1:] + [replacing_server]
    virtual_tasks = await get_new_virtual_tasks_statuses(tm, module_name, servers, previous_vts, 1)
    virtual_task = virtual_tasks[0]
    check_virtual_task_status(virtual_task, "done", "replace", 1)

    child = await tm.get_task_status(virtual_task.children_ids[0]["node"], virtual_task.children_ids[0]["task_id"])
    check_regular_task_status(child, "done", "replace: streaming", "node", virtual_task.id, 0)

    return servers, previous_vts + [virtual_task.id]

async def check_rebuild_tasks_tree(manager: ManagerClient, tm: TaskManagerClient, module_name: str, servers: list[ServerInfo],
                          previous_vts: list[TaskID]) -> tuple[list[ServerInfo], list[TaskID]]:
    async def _all_alive():
        if len(servers) == len(await manager.running_servers()):
            return True
    assert servers, "No servers available"

    logger.info(f"Rebuilding node {servers[0]}")
    rebuilt_server = servers[0]
    await manager.api.rebuild_node(rebuilt_server.ip_addr, 60)
    await wait_for(_all_alive, time.time() + 60)

    virtual_tasks = await get_new_virtual_tasks_statuses(tm, module_name, servers, previous_vts, 1)
    virtual_task = virtual_tasks[0]
    check_virtual_task_status(virtual_task, "done", "rebuild", 1)

    child = await tm.get_task_status(virtual_task.children_ids[0]["node"], virtual_task.children_ids[0]["task_id"])
    check_regular_task_status(child, "done", "rebuild: streaming", "node", virtual_task.id, 0)

    return servers, previous_vts + [virtual_task.id]

async def check_remove_node_tasks_tree(manager: ManagerClient, tm: TaskManagerClient,module_name: str, servers: list[ServerInfo],
                          previous_vts: list[TaskID]) -> tuple[list[ServerInfo], list[TaskID]]:
    assert servers, "No servers available"

    removed_server = servers[0]
    removing_server = servers[1]
    logger.info(f"Stopping node {removed_server}")
    await manager.server_stop_gracefully(removed_server.server_id)

    logger.info(f"Removing node {removed_server} using {removing_server}")
    await manager.remove_node(removing_server.server_id, removed_server.server_id)

    servers = servers[1:]

    virtual_tasks = await get_new_virtual_tasks_statuses(tm, module_name, servers, previous_vts, 1)
    virtual_task = virtual_tasks[0]
    check_virtual_task_status(virtual_task, "done", "remove", len(servers))

    child = await tm.get_task_status(virtual_task.children_ids[0]["node"], virtual_task.children_ids[0]["task_id"])
    check_regular_task_status(child, "done", "removenode: streaming", "node", virtual_task.id, 0)

    return servers, previous_vts + [virtual_task.id]

async def poll_for_task(tm: TaskManagerClient, module_name: str, server: ServerInfo, expected_kind: str, expected_type: str):
    async def _get_streaming_tasks(server: ServerInfo) -> list[TaskStats]:
        return [stats for stats in await tm.list_tasks(server.ip_addr, module_name) if stats.kind == expected_kind and stats.type == expected_type]

    async def _has_streaming_tasks(server: ServerInfo):
        if len(await _get_streaming_tasks(server)) > 0:
            return True

    await wait_for(partial(_has_streaming_tasks, server), time.time() + 100.)  # Wait until streaming task is created.

    streaming_tasks = await _get_streaming_tasks(server)
    assert len(streaming_tasks) == 1

    return streaming_tasks[0]


async def check_decommission_tasks_tree(manager: ManagerClient, tm: TaskManagerClient,module_name: str, servers: list[ServerInfo],
                          previous_vts: list[TaskID]) -> tuple[list[ServerInfo], list[TaskID]]:
    async def _check_virtual_task(decommissioned_server: ServerInfo, handler):
        logger.info("Checking top level decommission node task")

        await poll_for_task(tm, module_name, decommissioned_server, "node", "decommission: streaming")

        virtual_tasks = await get_new_virtual_tasks_statuses(tm, module_name, servers, previous_vts, 1)
        virtual_task = virtual_tasks[0]
        check_virtual_task_status(virtual_task, "running", "leave", 1)

        child = await tm.get_task_status(virtual_task.children_ids[0]["node"], virtual_task.children_ids[0]["task_id"])
        check_regular_task_status(child, "running", "decommission: streaming", "node", virtual_task.id, 0)

        await handler.message()

    assert servers, "No servers available"

    decommissioned_server = servers[0]
    injection = "streaming_task_impl_decommission_run"
    handler = await inject_error_one_shot(manager.api, decommissioned_server.ip_addr, injection)
    logger.info(f"Decommissioning node {decommissioned_server}")
    await asyncio.gather(*(manager.decommission_node(decommissioned_server.server_id),
                           _check_virtual_task(decommissioned_server, handler)))

    servers = servers[1:]
    vts_list = await get_new_virtual_tasks_list(tm, module_name, servers[0], previous_vts)
    return servers, previous_vts + [vts_list[0].task_id]

@pytest.mark.asyncio
async def test_node_ops_tasks_tree(manager: ManagerClient):
    """Test node ops task manager tasks."""
    module_name = "node_ops"
    tm = TaskManagerClient(manager.api)

    servers = [await manager.server_add() for _ in range(3)]
    assert module_name in await tm.list_modules(servers[0].ip_addr), "node_ops module wasn't registered"

    servers, vt_ids = await check_bootstrap_tasks_tree(tm, module_name, servers)
    servers, vt_ids = await check_replace_tasks_tree(manager, tm, module_name, servers, vt_ids)
    servers, vt_ids = await check_rebuild_tasks_tree(manager, tm, module_name, servers, vt_ids)
    servers, vt_ids = await check_remove_node_tasks_tree(manager, tm, module_name, servers, vt_ids)
    servers, vt_ids = await check_decommission_tasks_tree(manager, tm, module_name, servers, vt_ids)

@pytest.mark.asyncio
async def test_node_ops_tasks_ttl(manager: ManagerClient):
    """Test node ops virtual tasks' ttl."""
    module_name = "node_ops"
    tm = TaskManagerClient(manager.api)

    servers = [await manager.server_add() for _ in range(2)]
    [await tm.set_task_ttl(server.ip_addr, 3) for server in servers]
    time.sleep(3)
    await get_new_virtual_tasks_statuses(tm, module_name, servers, [], expected_task_num=0)

@pytest.mark.asyncio
async def test_node_ops_task_wait(manager: ManagerClient):
    """Test node ops virtual task's wait."""
    async def _decommission(manager: ManagerClient, server: ServerInfo):
        await manager.decommission_node(server.server_id)

    async def _wait_for_task(tm: TaskManagerClient, module_name: str, server: ServerInfo, handler: InjectionHandler):
        task = await poll_for_task(tm, module_name, servers[1], "cluster", "leave")
        assert task.state == "running"

        await handler.message()

        status = await tm.wait_for_task(server.ip_addr, task.task_id)
        assert status.state == "done"

    module_name = "node_ops"
    tm = TaskManagerClient(manager.api)

    servers = [await manager.server_add() for _ in range(2)]
    injection = "streaming_task_impl_decommission_run"
    handler = await inject_error_one_shot(manager.api, servers[0].ip_addr, injection)

    decommission_task = asyncio.create_task(
        _decommission(manager, servers[0]))

    waiting_task = asyncio.create_task(
        _wait_for_task(tm, module_name, servers[1], handler))

    await decommission_task
    await waiting_task
