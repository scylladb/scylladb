#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import pytest
import asyncio

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import check_token_ring_and_group0_consistency, alter_auto_rf_keyspaces

logger = logging.getLogger(__name__)


async def test_left_node_notification(manager: ScyllaClusterManager) -> None:
    """
    Create a 3-node multi-DC cluster with 2 nodes in dc1 and 1 node in dc2.
    Then decommission both dc1 nodes, ensuring the topology remains consistent,
    and the remaining node belongs to dc2, and only two 'left the cluster'
    notifications were issued.
    """
    # Bootstrap 2 nodes in dc1
    logger.info("Bootstrapping dc1 nodes")
    dc1_node_a = await manager.server_add(property_file={"dc": "dc1", "rack": "r1"})
    dc1_node_b = await manager.server_add(property_file={"dc": "dc1", "rack": "r2"})

    # Bootstrap 1 node in dc2 with storage_service debug logging
    logger.info("Bootstrapping dc2 node with storage_service=debug")
    dc2_node = await manager.server_add(cmdline=["--logger-log-level", "storage_service=debug"],
                                        property_file={"dc": "dc2", "rack": "r1"})

    # The system keyspaces start out in dc1 only. Move them to dc2 so that removing the
    # dc1 nodes does not fail with "zero replica after the removal" or "can not find new
    # node in local dc".
    cql = manager.get_cql()
    await alter_auto_rf_keyspaces(cql, "{'class': 'NetworkTopologyStrategy', 'dc1': 0, 'dc2': 1}")

    # Ensure ring and group0 are consistent before operations
    await check_token_ring_and_group0_consistency(manager)

    # Decommission both dc1 nodes
    logger.info(f"Decommissioning dc1 node {dc1_node_b}")
    await manager.decommission_node(dc1_node_b.server_id)
    await check_token_ring_and_group0_consistency(manager)

    logger.info(f"Decommissioning dc1 node {dc1_node_a}")
    await manager.decommission_node(dc1_node_a.server_id)
    await check_token_ring_and_group0_consistency(manager)

    # Verify only dc2 node remains running
    running = await manager.running_servers()
    assert len(running) == 1, f"Expected 1 running server, found {len(running)}: {running}"
    assert running[0].datacenter == "dc2", f"Remaining node should be in dc2, got {running[0].datacenter}"
    logger.info("Successfully decommissioned both dc1 nodes; dc2 node remains running")

    # Check the remaining node's log contains exactly two 'Notify node … has left the cluster'
    log = await manager.server_open_log(dc2_node.server_id)
    left_msgs = await log.grep(r"Notify node .* has left the cluster")
    assert len(left_msgs) == 2, f"Expected exactly 2 'left the cluster' notifications, got {len(left_msgs)}"
