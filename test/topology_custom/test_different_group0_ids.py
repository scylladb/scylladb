#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient

import asyncio
import pytest


@pytest.mark.asyncio
async def test_different_group0_ids(manager: ManagerClient):
    """
    The reproducer for #14448.

    The test starts two nodes with different group0_ids. The second node
    is restarted and tries to join the cluster consisting of the first node.
    gossip_digest_syn message should be rejected by the first node, so
    the second node will not be able to join the cluster.

    This test uses repair-based node operations to make this test easier.
    If the second node successfully joins the cluster, their tokens metadata
    will be merged and the repair service will allow to decommission the second node.
    If not - decommissioning the second node will fail with an exception
    "zero replica after the removal" thrown by the repair service.
    """

    # Consistent topology changes are disabled to use repair based node operations.
    scylla_a = await manager.server_add(config={'force_gossip_topology_changes': True})
    scylla_b = await manager.server_add(start=False, config={'force_gossip_topology_changes': True})
    await manager.server_start(scylla_b.server_id, seeds=[scylla_b.ip_addr])

    await manager.server_stop(scylla_b.server_id)
    await manager.server_start(scylla_b.server_id, seeds=[scylla_a.ip_addr, scylla_b.ip_addr])

    log_file_a = await manager.server_open_log(scylla_a.server_id)
    log_file_b = await manager.server_open_log(scylla_b.server_id)

    # Wait for a gossip round to finish
    _, pending = await asyncio.wait([
            asyncio.create_task(log_file_b.wait_for(f'InetAddress {scylla_a.ip_addr} is now UP')), # The second node joins the cluster
            asyncio.create_task(log_file_a.wait_for(f'Group0Id mismatch')) # The first node discards gossip from the second node
        ], return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()

    # Check if decommissioning the second node fails.
    # Repair service throws a runtime exception "zero replica after the removal"
    # when it tries to remove the only one node from the cluster.
    # If it is not thrown, it means that the second node successfully send a gossip
    # to the first node and they merged their tokens metadata.
    with pytest.raises(Exception, match='zero replica after the removal'):
        await manager.decommission_node(scylla_b.server_id)
