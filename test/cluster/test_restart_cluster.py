#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
"""
Test clusters can restart fine after all nodes are stopped gracefully
"""

import logging
import time

import pytest
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_restart_cluster(manager: ManagerClient) -> None:
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
