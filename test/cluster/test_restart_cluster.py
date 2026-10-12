#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Test clusters can restart fine after all nodes are stopped gracefully
"""

import logging
import time

import pytest
from test import DEBUG_MODES
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


async def test_restart_cluster(manager: ScyllaClusterManager) -> None:
    """Test that cluster can restart fine after all nodes are stopped gracefully"""
    servers = await manager.servers_add(3)
    cql = manager.get_cql()

    logger.info(f"Servers {servers}, gracefully stopping servers {[s.server_id for s in servers]} to check if all will go up")
    for s in servers:
        await manager.server_stop_gracefully(s.server_id)

    logger.info(f"Starting servers {[s.server_id for s in servers]}")
    for s in servers:
        await manager.server_start(s.server_id)

    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_uptime_resets_after_restart(manager: ScyllaClusterManager) -> None:
    """Test that /system/uptime_ms counts from the last start of the node, not the first one"""
    server = await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled"})

    async def get_uptime_ms() -> int:
        return int(await manager.api.client.get_text("/system/uptime_ms", host=server.ip_addr))

    uptime_before_restart = await get_uptime_ms()
    restart_time = time.monotonic()
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_start(server.server_id)
    uptime = await get_uptime_ms()
    # Without a reset the uptime would exceed the time since the restart by the node's boot time.
    assert uptime <= (time.monotonic() - restart_time) * 1000, \
        f"uptime {uptime} ms (was {uptime_before_restart} ms before restart) is longer than the time since the restart"
