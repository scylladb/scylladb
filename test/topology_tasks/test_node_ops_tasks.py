#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import wait_for
from test.topology_tasks.task_manager_client import TaskManagerClient

import asyncio
import logging
import pytest
import time

logger = logging.getLogger(__name__)


async def check_top_level_task(tm: TaskManagerClient, server: ServerInfo, module_name: str, expected_state: str,
                               expected_type: str) -> None:
    tasks = await tm.list_tasks(server.ip_addr, module_name)
    assert tasks, "A task wasn't created"
    assert len(tasks) == 1, "More than one task was created"

    status = await tm.wait_for_task(server.ip_addr, tasks[0].task_id)
    assert status.state == expected_state, "Task failed"
    assert status.type == expected_type, "Wrong task type"

async def check_bootstrap(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo],
                          module_name: str) -> list[ServerInfo]:
    logger.info("Bootstrapping one node")
    bootstrapped_server = await manager.server_add()

    logger.info("Checking top level bootstrap task")
    await check_top_level_task(tm, bootstrapped_server, module_name, "done", "bootstrap")

    servers.append(bootstrapped_server)
    return servers

async def check_replace(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo],
                        module_name: str) -> list[ServerInfo]:
    assert servers, "No servers available"

    replaced_server = servers[0]
    logger.info(f"Stopping node {replaced_server}")
    await manager.server_stop_gracefully(replaced_server.server_id)

    logger.info(f"Replacing node {replaced_server}")
    replace_cfg = ReplaceConfig(replaced_id = replaced_server.server_id, reuse_ip_addr = False, use_host_id = False)
    replacing_server = await manager.server_add(replace_cfg)

    logger.info("Checking top level replace task")
    await check_top_level_task(tm, replacing_server, module_name, "done", "bootstrap")

    servers = servers[1:] + [replacing_server]
    return servers

async def check_rebuild(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo],
                        module_name: str) -> list[ServerInfo]:
    async def _all_alive():
        if len(servers) == len(await manager.running_servers()):
            return True
    assert servers, "No servers available"

    logger.info(f"Rebuilding node {servers[0]}")
    rebuilt_server = servers[0]
    await manager.api.rebuild_node(rebuilt_server.ip_addr, 60)
    await wait_for(_all_alive, time.time() + 60)

    logger.info("Checking top level rebuild task")
    await check_top_level_task(tm, rebuilt_server, module_name, "done", "rebuild")

    return servers

async def check_remove_node(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo],
                            module_name: str) -> list[ServerInfo]:
    assert servers, "No servers available"

    removed_server = servers[0]
    removing_server = servers[1]
    logger.info(f"Stopping node {removed_server}")
    await manager.server_stop_gracefully(removed_server.server_id)

    logger.info(f"Removing node {removed_server} using {removing_server}")
    await manager.remove_node(removing_server.server_id, removed_server.server_id)

    logger.info("Checking top level remove node task")
    await check_top_level_task(tm, removing_server, module_name, "done", "removenode")

    return servers[1:]

async def check_decommission(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo],
                             module_name: str) -> list[ServerInfo]:
    async def _check_top_level_decommission_task(server: ServerInfo, handler):
        async def _get_tasks():
            if await tm.list_tasks(server.ip_addr, module_name):
                return True

        logger.info("Checking top level decommission node task")
        await wait_for(_get_tasks, time.time() + 100.)  # Wait until task is created.
        tasks = await tm.list_tasks(server.ip_addr, module_name)
        assert tasks, "Decommission task wasn't created"
        assert len(tasks) == 1, "More than one decommision task was created"

        status = await tm.get_task_status(server.ip_addr, tasks[0].task_id)
        assert status.type == "decommission", "Wrong task type"
        assert status.state == "running", "Wrong task state"

        await handler.message()

    assert servers, "No servers available"

    decommissioned_server = servers[0]
    await tm.drain_module_tasks(decommissioned_server.ip_addr, module_name)
    handler = await inject_error_one_shot(manager.api, decommissioned_server.ip_addr,
                                          "node_ops_start_decommission_task_impl_run")
    logger.info(f"Decommissioning node {decommissioned_server}")
    await asyncio.gather(*(manager.decommission_node(decommissioned_server.server_id),
                           _check_top_level_decommission_task(decommissioned_server, handler)))

    servers = servers[1:]
    return servers

@pytest.mark.asyncio
async def test_node_ops_tasks(manager: ManagerClient):
    """Test node ops task manager tasks."""
    module_name = "node_ops"
    tm = TaskManagerClient(manager.api)

    servers = await manager.running_servers()
    assert module_name in await tm.list_modules(servers[0].ip_addr), "node_ops module wasn't registered"
    [await tm.drain_module_tasks(s.ip_addr, module_name) for s in servers]  # Get rid of bootstrap tasks.

    servers = await check_bootstrap(manager, tm, servers, module_name)
    servers = await check_replace(manager, tm, servers, module_name)
    servers = await check_rebuild(manager, tm, servers, module_name)
    servers = await check_remove_node(manager, tm, servers, module_name)
    await check_decommission(manager, tm, servers, module_name)
