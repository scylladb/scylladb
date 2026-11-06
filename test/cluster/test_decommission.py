#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import time

import pytest
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import run_nodetool
from test.cluster.util import wait_for_token_ring_and_group0_consistency

logger = logging.getLogger(__name__)


async def test_decommissioned_node_cant_rejoin(request, manager: ScyllaClusterManager):
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
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    logger.info(f"Attempting to start the node {servers[1]} after it was decommissioned")
    await manager.server_start(servers[1].server_id,
                               expected_error='This node was decommissioned and will not rejoin the ring')
    logger.info(f"Got the expected error")


# Ported from scylla-dtest nodetool_test.py::TestNodetool::test_decommission_after_drain_is_invalid.
# On three nodes (ccm's populate([3]): dc1, RAC1), "nodetool decommission"
# of a node drained with "nodetool drain" fails with "Node in DRAINED state"
# (CASSANDRA-8741).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_decommission_after_drain_is_rejected(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config={"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256},
                                        property_file={"dc": "dc1", "rack": "RAC1"})
    node = servers[0]
    log = await manager.server_open_log(node.server_id)
    mark = await log.mark()
    await run_nodetool(manager, node, "drain")
    # ccm's drain(block_on_log=True).
    await log.wait_for("DRAINED", from_mark=mark, timeout=60)
    out, err, rc = await run_nodetool(manager, node, "decommission", check=False)
    assert rc != 0
    assert "Node in DRAINED state" in out or "Node in DRAINED state" in err
