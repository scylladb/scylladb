#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.topology.util import check_token_ring_and_group0_consistency

import pytest
import logging

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_topology_ops(request, manager: ManagerClient):
    """Test basic topology operations using the topology coordinator."""
    logger.info("Bootstrapping first node")
    servers = [await manager.server_add()]

    logger.info(f"Restarting node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_start(servers[0].server_id)

    logger.info("Bootstrapping other nodes")
    servers += [await manager.server_add(), await manager.server_add()]

    logger.info(f"Restarting node {servers[0]} when other nodes have bootstrapped")
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_start(servers[0].server_id)

    logger.info(f"Stopping node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)

    logger.info(f"Replacing node {servers[0]}")
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = False)
    servers = servers[1:] + [await manager.server_add(replace_cfg)]
    await check_token_ring_and_group0_consistency(manager)

    logger.info(f"Stopping node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)

    logger.info(f"Removing node {servers[0]} using {servers[1]}")
    await manager.remove_node(servers[1].server_id, servers[0].server_id)
    await check_token_ring_and_group0_consistency(manager)
    servers = servers[1:]

    logger.info(f"Decommissioning node {servers[0]}")
    await manager.decommission_node(servers[0].server_id)
    await check_token_ring_and_group0_consistency(manager)
