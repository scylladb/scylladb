#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.topology_tasks.task_manager_types import TaskID, TaskStats, TaskStatus
from test.pylib.internal_types import IPAddress
from test.pylib.rest_client import ScyllaRESTAPIClient

import asyncio
from typing import Optional

class TaskManagerClient():
    """Async Task Manager client"""

    def __init__(self, api: ScyllaRESTAPIClient):
        self.api = api

    async def list_modules(self, node_ip: IPAddress) -> list[str]:
        """Get the list of supported modules."""
        modules = await self.api.client.get_json("/task_manager/list_modules", host=node_ip)
        assert(type(modules) == list)
        return modules

    async def list_tasks(self, node_ip: IPAddress, module_name: str, internal: bool = False,
                        keyspace: Optional[str] = None, table: Optional[str] = None) -> list[TaskStats]:
        """Get the list of tasks stats in one module."""
        args = { "internal": str(internal) }
        if keyspace:
            args["keyspace"] = keyspace
        if table:
            args["table"] = table
        stats_list = await self.api.client.get_json(f"/task_manager/list_module_tasks/{module_name}", params=args,
                                                    host=node_ip)
        assert(type(stats_list) == list)
        return [TaskStats(**stats_dict) for stats_dict in stats_list]

    async def get_task_status(self, node_ip: IPAddress, task_id: TaskID) -> TaskStatus:
        """Get status of one task."""
        status = await self.api.client.get_json(f"/task_manager/task_status/{task_id}", host=node_ip)
        assert(type(status) == dict)
        return TaskStatus(**status)

    async def abort_task(self, node_ip: IPAddress, task_id: TaskID) -> None:
        """Abort a task."""
        await self.api.client.post(f"/task_manager/abort_task/{task_id}", host=node_ip)

    async def wait_for_task(self, node_ip: IPAddress, task_id: TaskID) -> TaskStatus:
        """Wait for a task and get its status."""
        status = await self.api.client.get_json(f"/task_manager/wait_task/{task_id}", host=node_ip)
        assert(type(status) == dict)
        return TaskStatus(**status)

    async def set_task_ttl(self, node_ip: IPAddress, ttl: int) -> int:
        """Set task ttl and get old value."""
        old_ttl = await self.api.client.post_json("/task_manager/ttl", params={ "ttl": str(ttl) }, host=node_ip)
        assert(type(old_ttl) == int)
        return old_ttl

    async def get_task_status_recursively(self, node_ip: IPAddress, task_id: TaskID) -> list[TaskStatus]:
        """Get status of a task and all its descendants."""
        status_list = await self.api.client.get_json(f"/task_manager/task_status_recursive/{task_id}", host=node_ip)
        assert(type(status_list) == list)
        return [TaskStatus(**status_dict) for status_dict in status_list]

    async def drain_module_tasks(self, node_ip: IPAddress, module_name: str, internal: bool = False) -> None:
        """Drain tasks of one module."""
        tasks = await self.list_tasks(node_ip, module_name, internal=internal)
        await asyncio.gather(*(self.api.client.get(f"/task_manager/wait_task/{stats.task_id}", host=node_ip,
                                                allow_failed=True) for stats in tasks))
        await asyncio.gather(*(self.api.client.get(f"/task_manager/task_status/{stats.task_id}", host=node_ip,
                                                allow_failed=True) for stats in tasks))
