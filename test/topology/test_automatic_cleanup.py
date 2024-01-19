#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster import ReplaceConfig
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_no_cleanup_when_unnecessary(request, manager: ManagerClient):
    """The test runs two bootstraps and checks that there is no cleanup in between.
       Then it runs a decommission and checks that cleanup runs automatically and then
       it runs one more decommission and checks that no cleanup runs again.
       Second part checks manual cleanup triggering. It adds a node. Triggers cleanup
       through the REST API, checks that is runs, decommissions a node and check that the
       cleanup did not run again.
    """
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.server_add()
    await manager.server_add()
    matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0

    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[4].server_id)
    matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 4

    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[3].server_id)
    matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0

    await manager.server_add()
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.api.client.post("/storage_service/cleanup_all", servers[0].ip_addr)
    matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 3

    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[3].server_id)
    matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0

