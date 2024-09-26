#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.repair import create_table_insert_data_for_repair
from test.topology.conftest import skip_mode
from test.topology_experimental_raft.test_tablets import inject_error_on
from test.topology_tasks.task_manager_client import TaskManagerClient
from test.topology_tasks.task_manager_types import TaskStatus, TaskStats

async def wait_tasks_created(tm: TaskManagerClient, server: ServerInfo, module_name: str, expected_number: int):
    async def get_user_repair_tasks():
        return [task for task in await tm.list_tasks(server.ip_addr, module_name) if task.kind == "cluster" and task.type == "user_repair" and task.keyspace == "test"]

    repair_tasks = await get_user_repair_tasks()
    while len(repair_tasks) != expected_number:
        repair_tasks = await get_user_repair_tasks()
    return repair_tasks

async def check_and_abort_repair_task(tm: TaskManagerClient, servers: list[ServerInfo], module_name: str):
    def check_repair_task_status(status: TaskStatus, states: list[str]):
        assert status.scope == "table"
        assert status.kind == "cluster"
        assert status.type == "user_repair"
        assert status.keyspace == "test"
        assert status.table == "test"
        assert status.is_abortable
        assert not status.children_ids
        assert status.state in states

    # Wait until user repair task is created.
    repair_tasks = await wait_tasks_created(tm, servers[0], module_name, 1)

    task = repair_tasks[0]
    assert task.scope == "table"
    assert task.keyspace == "test"
    assert task.table == "test"
    assert task.state in ["created", "running"]

    status = await tm.get_task_status(servers[0].ip_addr, task.task_id)

    check_repair_task_status(status, ["created", "running"])

    async def wait_for_task():
        status_wait = await tm.wait_for_task(servers[0].ip_addr, task.task_id)
        check_repair_task_status(status_wait, ["done"])

    async def abort_task():
        await tm.abort_task(servers[0].ip_addr, task.task_id)

    await asyncio.gather(wait_for_task(), abort_task())

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_repair_task(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)

    servers, cql, hosts, table_id = await create_table_insert_data_for_repair(manager)
    assert module_name in await tm.list_modules(servers[0].ip_addr), "tablets module wasn't registered"

    async def repair_task():
        token = -1
        # Keep retring tablet repair.
        await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
        await manager.api.tablet_repair(servers[0].ip_addr, "test", "test", token)

    await asyncio.gather(repair_task(), check_and_abort_repair_task(tm, servers, module_name))

async def check_repair_task_list(tm: TaskManagerClient, servers: list[ServerInfo], module_name: str):
    def get_task_with_id(repair_tasks, task_id):
        tasks_with_id1 = [task for task in repair_tasks if task.task_id == task_id]
        assert len(tasks_with_id1) == 1
        return tasks_with_id1[0]

    # Wait until user repair tasks are created.
    repair_tasks0 = await wait_tasks_created(tm, servers[0], module_name, len(servers))
    repair_tasks1 = await wait_tasks_created(tm, servers[1], module_name, len(servers))
    repair_tasks2 = await wait_tasks_created(tm, servers[2], module_name, len(servers))

    assert len(repair_tasks0) == len(repair_tasks1), f"Different number of repair virtual tasks on nodes {servers[0].server_id} and {servers[1].server_id}"
    assert len(repair_tasks0) == len(repair_tasks2), f"Different number of repair virtual tasks on nodes {servers[0].server_id} and {servers[2].server_id}"

    for task0 in repair_tasks0:
        task1 = get_task_with_id(repair_tasks1, task0.task_id)
        task2 = get_task_with_id(repair_tasks2, task0.task_id)

        assert task0.table in ["test", "test2", "test3"]
        assert task0.table == task1.table, f"Inconsistent table for task {task0.task_id}"
        assert task0.table == task2.table, f"Inconsistent table for task {task0.task_id}"

        for task in [task0, task1, task2]:
            assert task.state in ["created", "running"]
            assert task.type == "user_repair"
            assert task.kind == "cluster"
            assert task.scope == "table"
            assert task.keyspace == "test"

        await tm.abort_task(servers[0].ip_addr, task0.task_id)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_repair_task_list(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)

    servers, cql, hosts, table_id = await create_table_insert_data_for_repair(manager)
    assert module_name in await tm.list_modules(servers[0].ip_addr), "tablets module wasn't registered"

    # Create other tables.
    await cql.run_async("CREATE TABLE test.test2 (pk int PRIMARY KEY, c int) WITH tombstone_gc = {'mode':'repair'};")
    await cql.run_async("CREATE TABLE test.test3 (pk int PRIMARY KEY, c int) WITH tombstone_gc = {'mode':'repair'};")
    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test2 (pk, c) VALUES ({k}, {k});") for k in keys])
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test3 (pk, c) VALUES ({k}, {k});") for k in keys])

    async def run_repair(server_id, table_name):
        token = -1
        await manager.api.tablet_repair(servers[server_id].ip_addr, "test", table_name, token)

    await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)

    await asyncio.gather(run_repair(0, "test"), run_repair(1, "test2"), run_repair(2, "test3"), check_repair_task_list(tm, servers, module_name))
