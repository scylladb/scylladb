#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from functools import partial
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import wait_for
from test.topology_tasks.task_manager_client import TaskManagerClient
from test.topology_tasks.task_manager_types import TaskStatus, TaskID
from typing import Optional

import asyncio
import logging
import pytest
import time

logger = logging.getLogger(__name__)


async def check_single_child(tm: TaskManagerClient, server: ServerInfo, parent_status: TaskStatus, expected_state: str, 
                             expected_type: str, expected_scope: str) -> None:
    assert parent_status.children_ids, "No child task was created"
    assert len(parent_status.children_ids) == 1, "More than one child task was created"

    child_id = parent_status.children_ids[0]
    child_status = await tm.get_task_status(server.ip_addr, child_id)

    assert child_status.state == expected_state, f"Wrong state {child_status.state}; expected {expected_state}"
    assert child_status.type == expected_type, f"Wrong type {child_status.type}; expected {expected_type}"
    assert child_status.scope == expected_scope, f"Wrong scope {child_status.scope}; expected {expected_scope}"

    assert parent_status.sequence_number == child_status.sequence_number, f"Child task with id {child_id} did not inherit parent's sequence number"
    assert child_status.parent_id == parent_status.id, f"Parent id of task with id {child_id} is not set"

async def check_top_level_task(tm: TaskManagerClient, server: ServerInfo, module_name: str, expected_state: str,
                               expected_type: str) -> TaskStatus:
    tasks = [stats for stats in await tm.list_tasks(server.ip_addr, module_name) if stats.scope == "node"]
    assert tasks, "A task wasn't created"
    assert len(tasks) == 1, "More than one task was created"

    status = await tm.wait_for_task(server.ip_addr, tasks[0].task_id)
    assert status.state == expected_state, "Task failed"
    assert status.type == expected_type, "Wrong task type"

    return status

async def check_bootstrap(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo],
                          module_name: str, raft: bool) -> list[ServerInfo]:
    logger.info("Bootstrapping one node")
    bootstrapped_server = await prepare_server(manager, raft)

    logger.info("Checking top level bootstrap task")
    status = await check_top_level_task(tm, bootstrapped_server, module_name, "done", "bootstrap")
    await check_single_child(tm, bootstrapped_server, status, "done", "bootstrap", "raft entry" if raft else "gossiper entry")

    servers.append(bootstrapped_server)
    return servers

async def check_replace(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo],
                        module_name: str, raft: bool) -> list[ServerInfo]:
    assert servers, "No servers available"

    replaced_server = servers[0]
    logger.info(f"Stopping node {replaced_server}")
    await manager.server_stop_gracefully(replaced_server.server_id)

    logger.info(f"Replacing node {replaced_server}")
    replace_cfg = ReplaceConfig(replaced_id = replaced_server.server_id, reuse_ip_addr = False, use_host_id = False)
    replacing_server = await prepare_server(manager, raft, replace_cfg)

    logger.info("Checking top level replace task")
    status = await check_top_level_task(tm, replacing_server, module_name, "done", "bootstrap")
    await check_single_child(tm, replacing_server, status, "done", "replace", "raft entry" if raft else "gossiper entry")

    servers = servers[1:] + [replacing_server]
    return servers

async def check_rebuild(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo],
                        module_name: str, raft: bool) -> list[ServerInfo]:
    async def _all_alive():
        if len(servers) == len(await manager.running_servers()):
            return True
    assert servers, "No servers available"

    logger.info(f"Rebuilding node {servers[0]}")
    rebuilt_server = servers[0]
    await manager.api.rebuild_node(rebuilt_server.ip_addr, 60)
    await wait_for(_all_alive, time.time() + 60)

    logger.info("Checking top level rebuild task")
    status = await check_top_level_task(tm, rebuilt_server, module_name, "done", "rebuild")
    await check_single_child(tm, rebuilt_server, status, "done", "rebuild", "raft entry" if raft else "gossiper entry")

    return servers

async def check_remove_node(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo],
                            module_name: str, raft: bool) -> list[ServerInfo]:
    assert servers, "No servers available"

    removed_server = servers[0]
    removing_server = servers[1]
    logger.info(f"Stopping node {removed_server}")
    await manager.server_stop_gracefully(removed_server.server_id)

    logger.info(f"Removing node {removed_server} using {removing_server}")
    await manager.remove_node(removing_server.server_id, removed_server.server_id)

    logger.info("Checking top level remove node task")
    status = await check_top_level_task(tm, removing_server, module_name, "done", "removenode")
    await check_single_child(tm, removing_server, status, "done", "removenode", "raft entry" if raft else "gossiper entry")

    return servers[1:]

async def check_decommission(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo],
                             module_name: str, raft: bool) -> list[ServerInfo]:
    async def _check_top_level_decommission_task(server: ServerInfo, handler):
        async def _get_tasks():
            if await tm.list_tasks(server.ip_addr, module_name):
                return True

        async def _get_children(parent_id: TaskID):
            status = await tm.get_task_status(server.ip_addr, parent_id)
            if status.children_ids:
                return True

        logger.info("Checking top level decommission node task")
        await wait_for(_get_tasks, time.time() + 100.)  # Wait until task is created.
        tasks = await tm.list_tasks(server.ip_addr, module_name)
        assert tasks, "Decommission task wasn't created"
        assert len(tasks) == 1, "More than one decommision task was created"

        status = await tm.get_task_status(server.ip_addr, tasks[0].task_id)
        assert status.type == "decommission", "Wrong task type"
        assert status.state == "running", "Wrong task state"

        await wait_for(partial(_get_children, status.id), time.time() + 100.)
        await check_single_child(tm, server, status, "running", "decommission", "raft entry" if raft else "gossiper entry")

        await handler.message()

    assert servers, "No servers available"

    decommissioned_server = servers[0]
    await tm.drain_module_tasks(decommissioned_server.ip_addr, module_name)
    injection = "node_ops_raft_decommission_task_impl_run" if raft else "node_ops_gossiper_decommission_task_impl_run"
    handler = await inject_error_one_shot(manager.api, decommissioned_server.ip_addr, injection)
    logger.info(f"Decommissioning node {decommissioned_server}")
    await asyncio.gather(*(manager.decommission_node(decommissioned_server.server_id),
                           _check_top_level_decommission_task(decommissioned_server, handler)))

    servers = servers[1:]
    return servers

async def prepare_server(manager: ManagerClient, raft: bool, replace_cfg: Optional[ReplaceConfig] = None) -> ServerInfo:
    if raft:
        return await manager.server_add(replace_cfg=replace_cfg)
    else:
        cfg = {'consistent_cluster_management': False,
            'experimental_features': [],
            'enable_user_defined_functions': False}
        return await manager.server_add(config=cfg, replace_cfg=replace_cfg)

async def check_node_ops_tasks(manager: ManagerClient, raft: bool) -> None:
    module_name = "node_ops"
    tm = TaskManagerClient(manager.api)

    servers = [await prepare_server(manager, raft) for _ in range(2)]
    assert module_name in await tm.list_modules(servers[0].ip_addr), "node_ops module wasn't registered"
    [await tm.drain_module_tasks(s.ip_addr, module_name) for s in servers]  # Get rid of bootstrap tasks.

    servers = await check_bootstrap(manager, tm, servers, module_name, raft)
    servers = await check_replace(manager, tm, servers, module_name, raft)
    servers = await check_rebuild(manager, tm, servers, module_name, raft)
    servers = await check_remove_node(manager, tm, servers, module_name, raft)
    await check_decommission(manager, tm, servers, module_name, raft)

@pytest.mark.asyncio
async def test_node_ops_tasks_raft_enabled(manager: ManagerClient):
    """Test node ops task manager tasks with raft enabled."""
    await check_node_ops_tasks(manager, True)

@pytest.mark.asyncio
async def test_node_ops_tasks_raft_disabled(manager: ManagerClient):
    """Test node ops task manager tasks with raft disabled."""
    await check_node_ops_tasks(manager, False)
