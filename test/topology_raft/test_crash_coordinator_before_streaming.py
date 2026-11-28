#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import logging
import re
import time

import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.topology.conftest import skip_mode
from test.topology.util import (check_token_ring_and_group0_consistency, wait_for_token_ring_and_group0_consistency,
                                get_coordinator_host, get_coordinator_host_ids, wait_new_coordinator_elected)


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_kill_coordinator_during_op(manager: ManagerClient) -> None:
    """ Kill coordinator with error injection while topology operation is running for cluster: decommission,
    bootstrap, removenode, replace.

    1. Find the coordinator node.
    2. Inject an error to abort coordinator before streaming starts
    3. Start the operation on target node
    4. Wait for the operation to abort
    5. Check if the new coordinator has been elected
    6. Start the old coordinator node

    Topology operation is expected to fail and cluster is rolled back
    to previous state.

    | Operation | Coverage |
    | Decommission | done |
    | Removenode | done |
    | Bootstrap | done|
    | Replace | done|
    """
    # Decrease the failure detector threshold so we don't have to wait for too long.
    config = {
        'failure_detector_timeout_in_ms': 2000
    }
    nodes = [await manager.server_add(config=config) for _ in range(5)]
    coordinators_ids = await get_coordinator_host_ids(manager)
    assert len(coordinators_ids) == 1, "At least 1 coordinator id should be found"

    # kill coordinator during decommission
    logger.debug("Kill coordinator during decommission")
    coordinator_host = await get_coordinator_host(manager)
    other_nodes = [srv for srv in nodes if srv.server_id != coordinator_host.server_id]
    await manager.api.enable_injection(coordinator_host.ip_addr, "crash_coordinator_before_stream", one_shot=True)
    await manager.decommission_node(server_id=other_nodes[-1].server_id, expected_error="Decommission failed. See earlier errors")
    await wait_new_coordinator_elected(manager, 2, time.time() + 60)
    await manager.server_restart(coordinator_host.server_id, wait_others=1)
    await manager.servers_see_each_other(await manager.running_servers())
    await check_token_ring_and_group0_consistency(manager)

    # kill coordinator during removenode
    logger.debug("Kill coordinator during removenode")
    nodes = await manager.running_servers()
    coordinator_host = await get_coordinator_host(manager)
    other_nodes = [srv for srv in nodes if srv.server_id != coordinator_host.server_id]
    working_srv_id = other_nodes[0].server_id
    node_to_remove_srv_id = other_nodes[-1].server_id
    logger.debug("Stop node with srv_id %s", node_to_remove_srv_id)
    await manager.server_stop_gracefully(node_to_remove_srv_id)
    await manager.api.enable_injection(coordinator_host.ip_addr, "crash_coordinator_before_stream", one_shot=True)
    logger.debug("Start removenode with srv_id %s from node with srv_id %s", node_to_remove_srv_id, working_srv_id)
    await manager.remove_node(working_srv_id,
                              node_to_remove_srv_id,
                              expected_error="Removenode failed. See earlier errors")

    await wait_new_coordinator_elected(manager, 3, time.time() + 60)
    await manager.others_not_see_server(server_ip=coordinator_host.ip_addr)
    logger.debug("Start old coordinator node with srv_id %s", coordinator_host.server_id)
    await manager.server_restart(coordinator_host.server_id, wait_others=1)
    await manager.servers_see_each_other(await manager.running_servers())
    logger.debug("Remove node with srv_id %s from node with srv_id %s because it was banned in a previous attempt", node_to_remove_srv_id, working_srv_id)
    await manager.remove_node(working_srv_id, node_to_remove_srv_id)
    await check_token_ring_and_group0_consistency(manager)
    logger.debug("Restore number of nodes in cluster")
    await manager.server_add()

    # kill coordinator during bootstrap
    logger.debug("Kill coordinator during bootstrap")
    nodes = await manager.running_servers()
    coordinator_host = await get_coordinator_host(manager)
    other_nodes = [srv for srv in nodes if srv.server_id != coordinator_host.server_id]
    new_node = await manager.server_add(start=False)
    await manager.api.enable_injection(coordinator_host.ip_addr, "crash_coordinator_before_stream", one_shot=True)
    await manager.server_start(new_node.server_id,
                               expected_error="Startup failed: std::runtime_error")
    await wait_new_coordinator_elected(manager, 4, time.time() + 60)
    await manager.server_restart(coordinator_host.server_id, wait_others=1)
    await manager.servers_see_each_other(await manager.running_servers())
    await check_token_ring_and_group0_consistency(manager)

    # kill coordinator during replace
    logger.debug("Kill coordinator during replace")
    nodes = await manager.running_servers()
    coordinator_host = await get_coordinator_host(manager)
    other_nodes = [srv for srv in nodes if srv.server_id != coordinator_host.server_id]
    node_to_replace_srv_id = other_nodes[-1].server_id
    await manager.server_stop_gracefully(node_to_replace_srv_id)
    await manager.api.enable_injection(coordinator_host.ip_addr, "crash_coordinator_before_stream", one_shot=True)
    replace_cfg = ReplaceConfig(replaced_id = node_to_replace_srv_id, reuse_ip_addr = False, use_host_id = True)
    new_node = await manager.server_add(start=False, replace_cfg=replace_cfg)
    await manager.server_start(new_node.server_id, expected_error="Replace failed. See earlier errors")
    await wait_new_coordinator_elected(manager, 5, time.time() + 60)
    logger.debug("Start old coordinator node")
    await manager.others_not_see_server(server_ip=coordinator_host.ip_addr)
    await manager.server_restart(coordinator_host.server_id, wait_others=1)
    await manager.servers_see_each_other(await manager.running_servers())
    logger.debug("Replaced node is already non-voter and will be banned after restart. Remove it")
    coordinator_host = await get_coordinator_host(manager)
    await manager.remove_node(coordinator_host.server_id, node_to_replace_srv_id)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 60)
    await check_token_ring_and_group0_consistency(manager)
