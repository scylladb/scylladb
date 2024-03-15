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

from test.pylib.util import gather_safely

logger = logging.getLogger(__name__)

async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, True) for s in servers]
    await gather_safely(*errs)

@pytest.mark.asyncio
async def test_topology_streaming_failure(request, manager: ManagerClient):
    """Fail streaming while doing a topology operation"""
    # decommission failure
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.api.enable_injection(servers[2].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.decommission_node(servers[2].server_id, expected_error="Decommission failed. See earlier errors")
    servers = await manager.running_servers()
    assert len(servers) == 3
    matches = [await log.grep("raft_topology - rollback.*after decommissioning failure, moving transition state to rollback to normal",
               from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
    # bootstrap failure
    marks = [await log.mark() for log in logs]
    servers = await manager.running_servers()
    s = await manager.server_add(start=False, config={
        'error_injections_at_startup': ['stream_ranges_fail']
    })
    await manager.server_start(s.server_id, expected_error="Bootstrap failed. See earlier errors")
    servers = await manager.running_servers()
    assert s not in servers
    matches = [await log.grep("raft_topology - rollback.*after bootstrapping failure, moving transition state to left token ring",
               from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
    # bootstrap failure in raft barrier
    marks = [await log.mark() for log in logs]
    servers = await manager.running_servers()
    s = await manager.server_add(start=False)
    await manager.api.enable_injection(servers[1].ip_addr, 'raft_topology_barrier_fail', one_shot=True)
    await manager.server_start(s.server_id, expected_error="Bootstrap failed. See earlier errors")
    servers = await manager.running_servers()
    assert s not in servers
    matches = [await log.grep("raft_topology - rollback.*after bootstrapping failure, moving transition state to left token ring",
               from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
    # rebuild failure
    marks = [await log.mark() for log in logs]
    servers = await manager.running_servers()
    await manager.api.enable_injection(servers[1].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.rebuild_node(servers[1].server_id, expected_error="rebuild failed:")
    # replace failure
    marks = [await log.mark() for log in logs]
    servers = await manager.running_servers()
    await manager.server_stop_gracefully(servers[2].server_id)
    downed_server_id = servers[2].server_id
    replace_cfg = ReplaceConfig(replaced_id = servers[2].server_id, reuse_ip_addr = False, use_host_id = True)
    s = await manager.server_add(start=False, replace_cfg=replace_cfg, config={
        'error_injections_at_startup': ['stream_ranges_fail']
    })
    await manager.server_start(s.server_id, expected_error="Replace failed. See earlier errors")
    servers = await manager.running_servers()
    assert s not in servers
    matches = [await log.grep("raft_topology - rollback.*after replacing failure, moving transition state to left token ring",
               from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
    # remove failure
    marks = [await log.mark() for log in logs]
    await manager.api.enable_injection(servers[1].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.remove_node(servers[0].server_id, downed_server_id, expected_error="Removenode failed. See earlier errors")
    matches = [await log.grep("raft_topology - rollback.*after removing failure, moving transition state to rollback to normal",
               from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
