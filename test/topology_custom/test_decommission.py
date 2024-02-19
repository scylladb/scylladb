#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import logging

import pytest
from test.pylib.manager_client import ManagerClient
from test.topology.util import check_token_ring_and_group0_consistency

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_decommissioned_node_cant_rejoin(request, manager: ManagerClient):
    # This a regression test for #17282.

    logger.info("Bootstrapping the leader node")
    servers = [await manager.server_add()]

    logger.info(f"Bootstrapping the second node")
    servers += [await manager.server_add()]

    # It's important that we decommission a node which is not a leader.
    # We want to check the case when after restart the node needs
    # to communicate with other nodes to discover a leader.
    logger.info(f"Decommissioning node {servers[1]}")
    await manager.decommission_node(servers[1].server_id)
    await check_token_ring_and_group0_consistency(manager)
    logger.info(f"Attempting to start the node {servers[1]} after it was decommissioned")
    await manager.server_start(servers[1].server_id,
                               expected_error='This node was decommissioned and will not rejoin the ring')
    logger.info(f"Got the expected error")
