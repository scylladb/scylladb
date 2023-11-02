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

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_topology_streaming_failure(request, manager: ManagerClient):
    """Fail streaming while doing a topology operation"""
    # decommission failure
    servers = await manager.running_servers()
    await manager.api.enable_injection(servers[2].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.decommission_node(servers[2].server_id, expected_error="Decommission failed. See earlier errors")
    servers = await manager.running_servers()
    assert len(servers) == 3
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    matches = [await log.grep("storage_service - rollback.*after decommissioning failure to state normal") for log in logs]
    assert sum(len(x) for x in matches) == 1
    # remove failure
    await manager.server_add()
    servers = await manager.running_servers()
    await manager.server_stop_gracefully(servers[3].server_id)
    await manager.api.enable_injection(servers[2].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.remove_node(servers[0].server_id, servers[3].server_id, expected_error="Removenode failed. See earlier errors")
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    matches = [await log.grep("storage_service - rollback.*after removing failure to state normal") for log in logs]
    assert sum(len(x) for x in matches) == 1
    await manager.server_start(servers[3].server_id)
    # bootstrap failure
    servers = await manager.running_servers()
    s = await manager.server_add(start=False, config={
        'error_injections_at_startup': ['stream_ranges_fail']
    })
    await manager.server_start(s.server_id, expected_error="Bootstrap failed. See earlier errors")
    servers = await manager.running_servers()
    assert s not in servers
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    matches = [await log.grep("storage_service - rollback.*after bootstrapping failure to state left_token_ring") for log in logs]
    assert sum(len(x) for x in matches) == 1
    # replace failure
    await manager.server_stop_gracefully(servers[2].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[2].server_id, reuse_ip_addr = False, use_host_id = True)
    s = await manager.server_add(start=False, replace_cfg=replace_cfg, config={
        'error_injections_at_startup': ['stream_ranges_fail']
    })
    await manager.server_start(s.server_id, expected_error="Replace failed. See earlier errors")
    servers = await manager.running_servers()
    assert s not in servers
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    matches = [await log.grep("storage_service - rollback.*after replacing failure to state left_token_ring") for log in logs]
    assert sum(len(x) for x in matches) == 1
