#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
import asyncio
import logging
import pytest


@pytest.mark.asyncio
async def test_removing_alive_node_fails(manager: ManagerClient) -> None:
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
