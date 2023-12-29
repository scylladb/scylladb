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
    srv1 has a much bigger failure detector timeout than srv2 to create a scenario
    where srv2 considers srv3 dead, but srv1 still considers srv3 alive.
    """
    logging.info("Bootstrapping nodes")
    srv1 = await manager.server_add(config={'failure_detector_timeout_in_ms': 300000})
    srv2 = await manager.server_add(config={'failure_detector_timeout_in_ms': 2000})
    srv3 = await manager.server_add()
    await manager.server_sees_other_server(srv2.ip_addr, srv3.ip_addr)

    # srv2 considers srv3 alive. The removenode operation should fail on the initiator
    # side (in storage_service::raft_removenode).
    logging.info(f"Removing {srv3} initiated by {srv2}")
    await manager.remove_node(srv2.server_id, srv3.server_id, [],
                              "the node being removed is alive, maybe you should use decommission instead?", False)

    logging.info(f"Stopping {srv3}")
    await manager.server_stop(srv3.server_id)
    await manager.server_not_sees_other_server(srv2.ip_addr, srv3.ip_addr)

    log_file1 = await manager.server_open_log(srv1.server_id)

    # srv2 considers srv3 dead, but srv1 still considers srv3 alive. The removenode
    # operation should fail on the topology coordinator side (in
    # topology_coordinator::handle_node_transition).
    logging.info(f"Removing {srv3} initiated by {srv2}")
    await manager.remove_node(srv2.server_id, srv3.server_id, [], "Removenode failed. See earlier errors", False)
    await log_file1.wait_for("raft topology: rejected removenode operation for node", timeout=60)
