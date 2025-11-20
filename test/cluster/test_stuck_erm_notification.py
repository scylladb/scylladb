#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import time
import pytest
import logging

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_stuck_erm_notification(request, manager: ManagerClient):
    """Test that erm that is held for too long result in a notification"""
    rf_rack_cfg = {'rf_rack_valid_keyspaces': False}
    cfg = {'tablets_mode_for_new_keyspaces': 'enabled'} | rf_rack_cfg

    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3, config=cfg)

    await manager.api.enable_injection(servers[1].ip_addr, 'raft_topology_barrier_and_drain_hold_erm', True)
    logger.info(f"Decommissioning node {servers[0]}")
    await manager.decommission_node(servers[0].server_id)

    log = await manager.server_open_log(servers[1].server_id)
    await log.wait_for("topology version .* held for .* past expiry", timeout=60);
