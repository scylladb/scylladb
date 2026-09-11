#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test clusters can restart fine after an RPC IP address change.
"""

import time
import asyncio
import logging

import pytest

from test.topology.util import wait_for_token_ring_and_group0_consistency


logger = logging.getLogger(__name__)


@pytest.fixture
async def two_node_cluster(manager):
    logger.info(f"Booting initial 2-node cluster")
    servers = [(await manager.server_add()).server_id for _ in range(2)]
    for node in servers:
        await manager.server_start(node)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    return servers


@pytest.mark.asyncio
async def test_change_ip_for_all_nodes(two_node_cluster, manager, random_tables):
    """Reproduce problem with changing IPs for all nodes in a cluster."""

    table = await random_tables.add_table(ncolumns=5)

    logger.info("Change IP addresses for all servers simultaneously")

    await asyncio.gather(*[manager.server_stop_gracefully(server_id) for server_id in two_node_cluster])
    await asyncio.gather(*[manager.server_change_ip(server_id) for server_id in two_node_cluster])
    for server_id in two_node_cluster:
        await manager.server_start(server_id)

    await table.add_column()
    await random_tables.verify_schema()
