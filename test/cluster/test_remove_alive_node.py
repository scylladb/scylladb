#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from test import DEBUG_MODES
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
import asyncio
import logging
import pytest


async def test_removing_alive_node_fails(manager: ScyllaClusterManager) -> None:
    """
    Test verifying that an attempt to remove an alive node fails as expected.
    It uses a 3-node cluster:
    srv1 - the topology coordinator,
    srv2 - the removenode initiator,
    srv3 - the node being removed.
    """
    logging.info("Bootstrapping nodes")
    srv1 = await manager.server_add()
    srv2 = await manager.server_add()
    srv3 = await manager.server_add()
    await manager.server_sees_other_server(srv2.ip_addr, srv3.ip_addr)

    # srv2 considers srv3 alive. The removenode operation should fail on the initiator
    # side (in storage_service::raft_removenode).
    logging.info(f"Removing {srv3} initiated by {srv2}")
    await manager.remove_node(srv2.server_id, srv3.server_id, [],
                              "the node being removed is alive, maybe you should use decommission instead?", False)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_removing_decommissioned_node_fails(manager: ScyllaClusterManager) -> None:
    """
    Test verifying that removenode of a node which has already been
    decommissioned fails because its host id is no longer in the cluster.
    """
    srv1, srv2 = await manager.servers_add(2)
    await manager.decommission_node(srv2.server_id)

    logging.info(f"Removing decommissioned {srv2} initiated by {srv1}")
    await manager.remove_node(srv1.server_id, srv2.server_id,
                              expected_error="is not found in the cluster", wait_dead=False)
