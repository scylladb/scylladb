#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import random
from typing import Optional
import pytest

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.repair import create_table_insert_data_for_repair, get_tablet_task_id
from test.pylib.tablets import get_all_tablet_replicas
from test.cluster.conftest import skip_mode
from test.cluster.util import create_new_test_keyspace, new_test_keyspace
from test.cluster.test_tablets2 import inject_error_on
from test.cluster.tasks.task_manager_client import TaskManagerClient
from test.cluster.tasks.task_manager_types import TaskStatus, TaskStats
from test.cluster.tasks import extra_scylla_cmdline_options

async def enable_injection(manager: ManagerClient, servers: list[ServerInfo], injection: str):
    for server in servers:
        await manager.api.enable_injection(server.ip_addr, injection, False)

async def disable_injection(manager: ManagerClient, servers: list[ServerInfo], injection: str):
    for server in servers:
        await manager.api.disable_injection(server.ip_addr, injection)

async def message_injection(manager: ManagerClient, servers: list[ServerInfo], injection: str):
    for server in servers:
        await manager.api.message_injection(server.ip_addr, injection)

async def wait_tasks_created(tm: TaskManagerClient, server: ServerInfo, module_name: str, expected_number: int, type: str, keyspace: str, table: Optional[str] = None):
    async def get_tasks():
        tasks = [task for task in await tm.list_tasks(server.ip_addr, module_name) if task.kind == "cluster" and task.type == type and task.keyspace == keyspace]
        return [task for task in tasks if not table or table == task.table]

    tasks = await get_tasks()
    while len(tasks) != expected_number:
        tasks = await get_tasks()
    return tasks

def check_task_status(status: TaskStatus, states: list[str], type: str, scope: str, abortable: bool, keyspace: str, table: str = "test", possible_child_num: list[int] = [0]):
    assert status.scope == scope
    assert status.kind == "cluster"
    assert status.type == type
    assert status.keyspace == keyspace
    assert status.table == table
    assert status.is_abortable == abortable
    assert len(status.children_ids) in possible_child_num
    assert status.state in states

async def check_and_abort_repair_task(manager: ManagerClient, tm: TaskManagerClient, servers: list[ServerInfo], module_name: str, keyspace: str):
    # Wait until user repair task is created.
    repair_tasks = await wait_tasks_created(tm, servers[0], module_name, 1, "user_repair", keyspace=keyspace)

    task = repair_tasks[0]
    assert task.scope == "table"
    assert task.keyspace == keyspace
    assert task.table == "test"
    assert task.state in ["created", "running"]

    status = await tm.get_task_status(servers[0].ip_addr, task.task_id)

    check_task_status(status, ["created", "running"], "user_repair", "table", True, keyspace)

    log = await manager.server_open_log(servers[0].server_id)
    mark = await log.mark()

    async def wait_for_task():
        status_wait = await tm.wait_for_task(servers[0].ip_addr, task.task_id)
        check_task_status(status_wait, ["done"], "user_repair", "table", True, keyspace)

    async def abort_task():
        await log.wait_for('tablet_virtual_task: wait until tablet operation is finished', from_mark=mark)
        await tm.abort_task(servers[0].ip_addr, task.task_id)

    await asyncio.gather(wait_for_task(), abort_task())

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_repair_task(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)

    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)
    assert module_name in await tm.list_modules(servers[0].ip_addr), "tablets module wasn't registered"

    async def repair_task():
        token = -1
        # Keep retring tablet repair.
        await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

    await asyncio.gather(repair_task(), check_and_abort_repair_task(manager, tm, servers, module_name, ks))

async def check_repair_task_list(tm: TaskManagerClient, servers: list[ServerInfo], module_name: str, keyspace: str):
    def get_task_with_id(repair_tasks, task_id):
        tasks_with_id1 = [task for task in repair_tasks if task.task_id == task_id]
        assert len(tasks_with_id1) == 1
        return tasks_with_id1[0]

    # Wait until user repair tasks are created.
    repair_tasks0 = await wait_tasks_created(tm, servers[0], module_name, len(servers), "user_repair", keyspace=keyspace)
    repair_tasks1 = await wait_tasks_created(tm, servers[1], module_name, len(servers), "user_repair", keyspace=keyspace)
    repair_tasks2 = await wait_tasks_created(tm, servers[2], module_name, len(servers), "user_repair", keyspace=keyspace)

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
            assert task.keyspace == keyspace

        await tm.abort_task(servers[0].ip_addr, task0.task_id)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_repair_task_list(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)

    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)
    assert module_name in await tm.list_modules(servers[0].ip_addr), "tablets module wasn't registered"

    # Create other tables.
    await cql.run_async(f"CREATE TABLE {ks}.test2 (pk int PRIMARY KEY, c int) WITH tombstone_gc = {{'mode':'repair'}};")
    await cql.run_async(f"CREATE TABLE {ks}.test3 (pk int PRIMARY KEY, c int) WITH tombstone_gc = {{'mode':'repair'}};")
    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test2 (pk, c) VALUES ({k}, {k});") for k in keys])
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test3 (pk, c) VALUES ({k}, {k});") for k in keys])

    async def run_repair(server_id, table_name):
        token = -1
        await manager.api.tablet_repair(servers[server_id].ip_addr, ks, table_name, token)

    await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)

    await asyncio.gather(run_repair(0, "test"), run_repair(1, "test2"), run_repair(2, "test3"), check_repair_task_list(tm, servers, module_name, ks))

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_repair_task_children(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)
    injection = "repair_tablet_repair_task_impl_run"

    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)
    for server in servers:
        tm.set_task_ttl(server.ip_addr, 3600)
    assert module_name in await tm.list_modules(servers[0].ip_addr), "tablets module wasn't registered"

    log = await manager.server_open_log(servers[0].server_id)
    mark = await log.mark()

    async def repair_task():
        token = -1
        # Keep retring tablet repair.
        await inject_error_on(manager, injection, servers)
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

    async def resume_repair():
        await log.wait_for('tablet_virtual_task: wait until tablet operation is finished', from_mark=mark)
        await message_injection(manager, servers, injection)

    async def check_children():
        # Wait until user repair task is created.
        repair_tasks = await wait_tasks_created(tm, servers[0], module_name, 1, "user_repair", ks)
        status = await tm.wait_for_task(servers[0].ip_addr, repair_tasks[0].task_id)

        assert len(status.children_ids) == 1
        child = status.children_ids[0]
        child_status = await tm.get_task_status(child["node"], child["task_id"])
        assert child_status.parent_id == repair_tasks[0].task_id

    await asyncio.gather(repair_task(), resume_repair(), check_children())

async def prepare_migration_test(manager: ManagerClient):
    servers = []
    host_ids = []

    async def make_server():
        s = await manager.server_add(cmdline=extra_scylla_cmdline_options)
        servers.append(s)
        host_ids.append(await manager.get_host_id(s.server_id))
        await manager.api.disable_tablet_balancing(s.ip_addr)

    await make_server()
    cql = manager.get_cql()
    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}")
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
    await make_server()

    await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({1}, {1});")

    return (ks, servers, host_ids)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_migration_task(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)
    ks, servers, host_ids = await prepare_migration_test(manager)

    injection = "handle_tablet_migration_end_migration"

    async def move_tablet(old_replica, new_replica):
        await manager.api.enable_injection(servers[0].ip_addr, injection, False)
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", old_replica[0], old_replica[1], new_replica[0], new_replica[1], 0)

    async def check(type):
        # Wait until migration task is created.
        migration_tasks = await wait_tasks_created(tm, servers[0], module_name, 1, type, keyspace=ks)

        assert len(migration_tasks) == 1
        status = await tm.get_task_status(servers[0].ip_addr, migration_tasks[0].task_id)
        check_task_status(status, ["created", "running"], type, "tablet", False, keyspace=ks)

        await manager.api.disable_injection(servers[0].ip_addr, injection)

    replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
    assert len(replicas) == 1 and len(replicas[0].replicas) == 1

    intranode_migration_src = replicas[0].replicas[0]
    intranode_migration_dst = (intranode_migration_src[0], 1 - intranode_migration_src[1])
    await asyncio.gather(move_tablet(intranode_migration_src, intranode_migration_dst), check("intranode_migration"))

    migration_src = intranode_migration_dst
    assert migration_src[0] != host_ids[1]
    migration_dst = (host_ids[1], 0)
    await asyncio.gather(move_tablet(migration_src, migration_dst), check("migration"))

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_migration_task_list(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)
    ks, servers, host_ids = await prepare_migration_test(manager)
    injection = "handle_tablet_migration_end_migration"

    async def move_tablet(server, old_replica, new_replica):
        await manager.api.move_tablet(server.ip_addr, ks, "test", old_replica[0], old_replica[1], new_replica[0], new_replica[1], 0)

    async def check_migration_task_list(type: str):
        # Wait until migration tasks are created.
        migration_tasks0 = await wait_tasks_created(tm, servers[0], module_name, 1, type, keyspace=ks)
        migration_tasks1 = await wait_tasks_created(tm, servers[1], module_name, 1, type, keyspace=ks)

        assert len(migration_tasks0) == len(migration_tasks1), f"Different number of migration virtual tasks on nodes {servers[0].server_id} and {servers[1].server_id}"
        assert len(migration_tasks0) == 1, f"Wrong number of migration virtual tasks"

        task0 = migration_tasks0[0]
        task1 = migration_tasks1[0]
        assert task0.task_id == task1.task_id

        for task in [task0, task1]:
            assert task.state in ["created", "running"]
            assert task.type == type
            assert task.kind == "cluster"
            assert task.scope == "tablet"
            assert task.table == "test"
            assert task.keyspace == ks

        await disable_injection(manager, servers, injection)

    replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
    assert len(replicas) == 1 and len(replicas[0].replicas) == 1

    intranode_migration_src = replicas[0].replicas[0]
    intranode_migration_dst = (intranode_migration_src[0], 1 - intranode_migration_src[1])
    await enable_injection(manager, servers, injection)
    await asyncio.gather(move_tablet(servers[0], intranode_migration_src, intranode_migration_dst), check_migration_task_list("intranode_migration"))

    migration_src = intranode_migration_dst
    assert migration_src[0] != host_ids[1]
    migration_dst = (host_ids[1], 0)
    await enable_injection(manager, servers, injection)
    await asyncio.gather(move_tablet(servers[0], migration_src, migration_dst), check_migration_task_list("migration"))

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_migration_task_failed(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)
    ks, servers, host_ids = await prepare_migration_test(manager)

    wait_injection = "stream_tablet_wait"
    throw_injection = "stream_tablet_move_to_cleanup"

    async def move_tablet(old_replica, new_replica):
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", old_replica[0], old_replica[1], new_replica[0], new_replica[1], 0)

    async def wait_for_task(task_id, type):
        status = await tm.wait_for_task(servers[0].ip_addr, task_id)
        check_task_status(status, ["failed"], type, "tablet", False, keyspace=ks)

    async def resume_migration(log, mark):
        await log.wait_for('tablet_virtual_task: wait until tablet operation is finished', from_mark=mark)
        await disable_injection(manager, servers, wait_injection)

    async def check(type, log, mark):
        # Wait until migration task is created.
        migration_tasks = await wait_tasks_created(tm, servers[0], module_name, 1, type, keyspace=ks)
        assert len(migration_tasks) == 1

        await asyncio.gather(wait_for_task(migration_tasks[0].task_id, type), resume_migration(log, mark))

    await enable_injection(manager, servers, wait_injection)
    await enable_injection(manager, servers, throw_injection)

    log = await manager.server_open_log(servers[0].server_id)
    mark = await log.mark()

    replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
    assert len(replicas) == 1 and len(replicas[0].replicas) == 1

    src = replicas[0].replicas[0]
    dst = (src[0], 1 - src[1])
    await asyncio.gather(move_tablet(src, dst), check("intranode_migration", log, mark))

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_repair_task_info_is_none_when_no_running_repair(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)
    token = -1

    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)
    assert module_name in await tm.list_modules(servers[0].ip_addr), "tablets module wasn't registered"

    async def check_none():
        tablet_task_id = await get_tablet_task_id(cql, hosts[0], table_id, token)
        assert tablet_task_id is None

    await check_none()

    async def repair_task():
        await enable_injection(manager, servers, "repair_tablet_fail_on_rpc_call")
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

    async def wait_and_check_none():
        task = (await wait_tasks_created(tm, servers[0], module_name, 1,"user_repair", keyspace=ks))[0]
        await disable_injection(manager, servers, "repair_tablet_fail_on_rpc_call")
        status = await tm.wait_for_task(servers[0].ip_addr, task.task_id)
        await check_none()

    await asyncio.gather(repair_task(), wait_and_check_none())

async def prepare_split(manager: ManagerClient, server: ServerInfo, keyspace: str, table: str, keys: list[int]):
    await manager.api.disable_tablet_balancing(server.ip_addr)

    cql = manager.get_cql()
    insert = cql.prepare(f"INSERT INTO {keyspace}.{table}(pk, c) VALUES(?, ?)")
    for pk in keys:
        value = random.randbytes(1000)
        cql.execute(insert, [pk, value])

    await manager.api.flush_keyspace(server.ip_addr, keyspace)

async def prepare_merge(manager: ManagerClient, server: ServerInfo, keyspace: str, table: str, keys: list[int]):
    await manager.api.disable_tablet_balancing(server.ip_addr)

    cql = manager.get_cql()
    await asyncio.gather(*[cql.run_async(f"DELETE FROM {keyspace}.{table} WHERE pk={k};") for k in keys])

    await manager.api.flush_keyspace(server.ip_addr, keyspace)

async def enable_tablet_balancing_and_wait(manager: ManagerClient, server: ServerInfo, message: str):
    s1_log = await manager.server_open_log(server.server_id)
    s1_mark = await s1_log.mark()

    await manager.api.enable_tablet_balancing(server.ip_addr)

    await s1_log.wait_for(message, from_mark=s1_mark)

#common cmdline for server_add command in all tests below
cmdline = ['--target-tablet-size-in-bytes', '30000', ]
cmdline.extend(extra_scylla_cmdline_options)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_resize_task(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)
    servers = [await manager.server_add(cmdline=cmdline, config={
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
    })]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    table1 = "test1"
    table2 = "test2"
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as keyspace:
        await cql.run_async(f"CREATE TABLE {keyspace}.{table1} (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1;")
        await cql.run_async(f"CREATE TABLE {keyspace}.{table2} (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1;")

        total_keys = 60
        keys = range(total_keys)
        await prepare_split(manager, servers[0], keyspace, table1, keys)
        await enable_tablet_balancing_and_wait(manager, servers[0], "Detected tablet split for table")
        await wait_tasks_created(tm, servers[0], module_name, 0, "split", keyspace, table1)

        await prepare_split(manager, servers[0], keyspace, table2, keys)
        await prepare_merge(manager, servers[0], keyspace, table1, keys[:-1])
        await manager.api.keyspace_compaction(servers[0].ip_addr, keyspace)

        injection = "tablet_split_finalization_postpone"
        await enable_injection(manager, servers, injection)
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        async def wait_and_check_status(server, type, keyspace, table):
            task = (await wait_tasks_created(tm, server, module_name, 1, type, keyspace, table))[0]
            status = await tm.get_task_status(server.ip_addr, task.task_id)
            check_task_status(status, ["running"], type, "table", False, keyspace, table, [0, 1, 2])

        await wait_and_check_status(servers[0], "split", keyspace, table2)
        await wait_and_check_status(servers[0], "merge", keyspace, table1)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_resize_list(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)
    servers = [await manager.server_add(cmdline=cmdline, config={
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
    })]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    table1 = "test1"
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as keyspace:
        await cql.run_async(f"CREATE TABLE {keyspace}.{table1} (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1;")

        total_keys = 60
        keys = range(total_keys)
        await prepare_split(manager, servers[0], keyspace, table1, keys)

        servers.append(await manager.server_add(cmdline=cmdline, config={
            'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
        }))

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        injection = "tablet_split_finalization_postpone"
        compaction_injection = "split_sstable_rewrite"
        await enable_injection(manager, servers, injection)
        await manager.api.enable_injection(servers[0].ip_addr, compaction_injection, one_shot=True)

        await manager.api.enable_tablet_balancing(servers[0].ip_addr)
        task0 = (await wait_tasks_created(tm, servers[0], module_name, 1, "split", keyspace, table1))[0]
        task1 = (await wait_tasks_created(tm, servers[1], module_name, 1, "split", keyspace, table1))[0]

        assert task0.task_id == task1.task_id

        for task in [task0, task1]:
            assert task.state == "running"
            assert task.type == "split"
            assert task.kind == "cluster"
            assert task.scope == "table"
            assert task.table == table1
            assert task.keyspace == keyspace

        await s1_log.wait_for("split_sstable_rewrite: waiting", from_mark=s1_mark)
        await manager.api.message_injection(servers[0].ip_addr, "split_sstable_rewrite")

        status1 = await tm.get_task_status(servers[1].ip_addr, task0.task_id)
        status0 = await tm.get_task_status(servers[0].ip_addr, task0.task_id)
        assert len(status0.children_ids) == 2
        assert status0.children_ids == status1.children_ids

        await disable_injection(manager, servers, injection)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'debug mode is too time-sensitive')
async def test_tablet_resize_revoked(manager: ManagerClient):
    module_name = "tablets"
    tm = TaskManagerClient(manager.api)
    servers = [await manager.server_add(cmdline=cmdline, config={
        'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
    })]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    table1 = "test1"
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as keyspace:
        await cql.run_async(f"CREATE TABLE {keyspace}.{table1} (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1;")

        total_keys = 60
        keys = range(total_keys)
        await prepare_split(manager, servers[0], keyspace, table1, keys)

        injection = "tablet_split_finalization_postpone"
        await enable_injection(manager, servers, injection)

        await manager.api.enable_tablet_balancing(servers[0].ip_addr)
        task0 = (await wait_tasks_created(tm, servers[0], module_name, 1, "split", keyspace, table1))[0]

        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        async def revoke_resize(log, mark):
            await log.wait_for('tablet_virtual_task: wait until tablet operation is finished', from_mark=mark)
            await asyncio.gather(*[cql.run_async(f"DELETE FROM {keyspace}.{table1} WHERE pk={k};") for k in keys])

            await manager.api.flush_keyspace(servers[0].ip_addr, keyspace)

        async def wait_for_task(task_id):
            status = await tm.wait_for_task(servers[0].ip_addr, task_id)
            check_task_status(status, ["suspended"], "split", "table", False, keyspace, table1, [0, 1, 2])

        await asyncio.gather(revoke_resize(log, mark), wait_for_task(task0.task_id))

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_task_sees_latest_state(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)

    token = -1
    async def repair_task():
        await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
        # Check failed repair request can be deleted
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

    async def del_repair_task():
        tablet_task_id = None
        while tablet_task_id is None:
            tablet_task_id = await get_tablet_task_id(cql, hosts[0], table_id, token)

        await manager.api.abort_task(servers[0].ip_addr, tablet_task_id)

    await asyncio.gather(repair_task(), del_repair_task())
