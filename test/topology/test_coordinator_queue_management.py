#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
import pytest
import logging
import asyncio

from test.pylib.util import gather_safely

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_coordinator_queue_management(manager: ManagerClient):
    """This test creates a 5 node cluster with 2 down nodes (A and B). After that it
       creates a queue of 3 topology operation: bootstrap, removenode A and removenode B
       with ignore_nodes=A. Check that all operation manage to complete.
       Then it downs one node and creates a queue with two requests:
       bootstrap and decommission. Since none can proceed both should be canceled.
    """
    await manager.server_add()
    await manager.server_add()
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.server_stop_gracefully(servers[3].server_id)
    await manager.server_stop_gracefully(servers[4].server_id)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[3].ip_addr)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[4].ip_addr)

    inj = 'topology_coordinator_pause_before_processing_backlog'
    [await manager.api.enable_injection(s.ip_addr, inj, one_shot=True) for s in servers[:3]]

    s3_id = await manager.get_host_id(servers[3].server_id)
    tasks = [asyncio.create_task(manager.server_add()),
             asyncio.create_task(manager.remove_node(servers[0].server_id, servers[3].server_id)),
             asyncio.create_task(manager.remove_node(servers[1].server_id, servers[4].server_id, [s3_id]))]

    search = [asyncio.create_task(l.wait_for("received request to join from host_id", m) for l, m in zip(logs[:3], marks[:3]))]
    done, pending = await asyncio.wait(search, return_when = asyncio.FIRST_COMPLETED)
    for t in pending: t.cancel()

    [await l.wait_for("raft_topology - removenode: wait for completion", m) for l, m in zip(logs[:2], marks[:2])]

    [await manager.api.message_injection(s.ip_addr, inj) for s in servers[:3]]

    await gather_safely(*tasks)

    servers = await manager.running_servers()
    await manager.server_stop_gracefully(servers[3].server_id)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[3].ip_addr)

    [await manager.api.enable_injection(s.ip_addr, inj, one_shot=True) for s in servers[:3]]

    s = await manager.server_add(start=False)

    tasks = [asyncio.create_task(manager.server_start(s.server_id, expected_error="request canceled because some required nodes are dead")),
             asyncio.create_task(manager.decommission_node(servers[1].server_id, expected_error="Decommission failed. See earlier errors"))]

    search = [asyncio.create_task(l.wait_for("received request to join from host_id", m) for l, m in zip(logs[:3], marks[:3]))]
    done, pending = await asyncio.wait(search, return_when = asyncio.FIRST_COMPLETED)
    for t in pending: t.cancel()

    logs[1].wait_for("raft_topology - decommission: wait for completion", marks[1])

    [await manager.api.message_injection(s.ip_addr, inj) for s in servers[:3]]

    await gather_safely(*tasks)
