#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import logging
import asyncio

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_long_join(manager: ManagerClient) -> None:
    """The test checks that join works even if expiring entries are dropped
       between placement of the join request and its processing"""
    s1 = await manager.server_add()
    inj = 'topology_coordinator_pause_before_processing_backlog'
    await manager.api.enable_injection(s1.ip_addr, inj, one_shot=True)
    s2 = await manager.server_add(start=False)
    task = asyncio.create_task(manager.server_start(s2.server_id))
    await manager.server_sees_other_server(s1.ip_addr, s2.ip_addr, interval=300)
    await manager.api.enable_injection(s1.ip_addr, "handle_node_transition_drop_expiring", one_shot=True)
    await manager.api.message_injection(s1.ip_addr, inj)
    await asyncio.gather(task)

@pytest.mark.asyncio
async def test_long_join_drop_entries_on_bootstrapping(manager: ManagerClient) -> None:
    """The test checks that join works even if expiring entries are dropped
       on the joining node between placement of the join request and its processing"""
    servers = await manager.servers_add(2)
    inj = 'topology_coordinator_pause_before_processing_backlog'
    [await manager.api.enable_injection(s.ip_addr, inj, one_shot=True) for s in servers]
    s = await manager.server_add(start=False,  config={
        'error_injections_at_startup': ['pre_server_start_drop_expiring']
    })
    task = asyncio.create_task(manager.server_start(s.server_id))
    log = await manager.server_open_log(s.server_id)
    await log.wait_for("init - starting gossiper")
    servers.append(s)
    await manager.servers_see_each_other(servers, interval=300)
    await manager.api.enable_injection(s.ip_addr, 'join_node_response_drop_expiring', one_shot=True)
    [await manager.api.message_injection(s.ip_addr, inj) for s in servers[:-1]]
    await asyncio.gather(task)
