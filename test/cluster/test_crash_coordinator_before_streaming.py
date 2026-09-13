#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import time

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_cluster import ReplaceConfig
from test.cluster.util import (BANNED_NOTIFICATION, check_token_ring_and_group0_consistency,
                               wait_for_token_ring_and_group0_consistency, get_coordinator_host,
                               get_coordinator_host_ids, wait_new_coordinator_elected,
                               wait_for_no_pending_topology_transition)


logger = logging.getLogger(__name__)


async def _start_cluster_and_kill_coordinator(manager: ScyllaClusterManager, failure_detector_timeout: int):
    config = {
        'failure_detector_timeout_in_ms': failure_detector_timeout,
        # Raise the raft direct failure detector threshold above its 2s default.
        # A shard 0 stall in a loaded test environment (SCYLLADB-2121) otherwise
        # gets a live voter marked dead, the group0 leader steps down and the
        # topology coordinator moves to another node before the injection fires.
        'error_injections_at_startup': [
            {
                'name': 'raft-group-registry-fd-threshold-in-ms',
                'value': '5000'
            }
        ]
    }
    cmdline = ['--logger-log-level', 'raft_topology=trace']
    nodes = [await manager.server_add(config=config, cmdline=cmdline) for _ in range(5)]
    coordinators_ids = await get_coordinator_host_ids(manager)
    assert len(coordinators_ids) == 1, "At least 1 coordinator id should be found"
    manager.ignore_cores_log_patterns.append("crash_coordinator_before_stream: aborting")
    coordinator_host = await get_coordinator_host(manager)
    return config, cmdline, nodes, coordinator_host


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_kill_coordinator_during_decommission(manager: ScyllaClusterManager, failure_detector_timeout: int, scale_timeout: callable) -> None:
    """ Kill coordinator with error injection while decommission is running. Topology operation is
    expected to fail and cluster is rolled back to previous state. """
    _config, _cmdline, nodes, coordinator_host = await _start_cluster_and_kill_coordinator(manager, failure_detector_timeout)
    other_nodes = [srv for srv in nodes if srv.server_id != coordinator_host.server_id]
    previous_coordinator_id = await manager.get_host_id(coordinator_host.server_id)
    await manager.api.enable_injection(coordinator_host.ip_addr, "crash_coordinator_before_stream", one_shot=True)
    await manager.decommission_node(server_id=other_nodes[-1].server_id, expected_error="Decommission failed. See earlier errors")
    await wait_new_coordinator_elected(manager, previous_coordinator_id, time.time() + scale_timeout(60))
    await wait_for_no_pending_topology_transition(manager, time.time() + scale_timeout(60))
    await manager.server_restart(coordinator_host.server_id, wait_others=1)
    await manager.servers_see_each_other(await manager.running_servers())
    await check_token_ring_and_group0_consistency(manager)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_kill_coordinator_during_removenode(manager: ScyllaClusterManager, failure_detector_timeout: int, scale_timeout: callable) -> None:
    """ Kill coordinator with error injection while removenode is running. Topology operation is
    expected to fail and cluster is rolled back to previous state. """
    _config, _cmdline, nodes, coordinator_host = await _start_cluster_and_kill_coordinator(manager, failure_detector_timeout)
    other_nodes = [srv for srv in nodes if srv.server_id != coordinator_host.server_id]
    working_srv_id = other_nodes[0].server_id
    node_to_remove_srv_id = other_nodes[-1].server_id
    logger.debug("Stop node with srv_id %s", node_to_remove_srv_id)
    await manager.server_stop_gracefully(node_to_remove_srv_id)
    previous_coordinator_id = await manager.get_host_id(coordinator_host.server_id)
    await manager.api.enable_injection(coordinator_host.ip_addr, "crash_coordinator_before_stream", one_shot=True)
    logger.debug("Start removenode with srv_id %s from node with srv_id %s", node_to_remove_srv_id, working_srv_id)
    await manager.remove_node(working_srv_id,
                              node_to_remove_srv_id,
                              expected_error="Removenode failed. See earlier errors")

    await wait_new_coordinator_elected(manager, previous_coordinator_id, time.time() + scale_timeout(60))
    await wait_for_no_pending_topology_transition(manager, time.time() + scale_timeout(60))

    await manager.others_not_see_server(server_ip=coordinator_host.ip_addr)
    logger.debug("Start old coordinator node with srv_id %s", coordinator_host.server_id)
    await manager.server_restart(coordinator_host.server_id, wait_others=1)
    await manager.servers_see_each_other(await manager.running_servers())
    logger.debug("Remove node with srv_id %s from node with srv_id %s because it was banned in a previous attempt", node_to_remove_srv_id, working_srv_id)
    await manager.remove_node(working_srv_id, node_to_remove_srv_id)
    await wait_for_no_pending_topology_transition(manager, time.time() + scale_timeout(60))
    await manager.servers_see_each_other(await manager.running_servers())
    await check_token_ring_and_group0_consistency(manager)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_kill_coordinator_during_bootstrap(manager: ScyllaClusterManager, failure_detector_timeout: int, scale_timeout: callable) -> None:
    """ Kill coordinator with error injection while bootstrap is running. Topology operation is
    expected to fail and cluster is rolled back to previous state. """
    config, cmdline, nodes, coordinator_host = await _start_cluster_and_kill_coordinator(manager, failure_detector_timeout)
    new_node = await manager.server_add(start=False, config=config, cmdline=cmdline)
    previous_coordinator_id = await manager.get_host_id(coordinator_host.server_id)
    await manager.api.enable_injection(coordinator_host.ip_addr, "crash_coordinator_before_stream", one_shot=True)
    await manager.server_start(new_node.server_id, expected_error=f"Startup failed: std::runtime_error|{BANNED_NOTIFICATION}")
    await wait_new_coordinator_elected(manager, previous_coordinator_id, time.time() + scale_timeout(60))
    await wait_for_no_pending_topology_transition(manager, time.time() + scale_timeout(60))
    await manager.server_restart(coordinator_host.server_id, wait_others=1)
    await manager.servers_see_each_other(await manager.running_servers())
    await check_token_ring_and_group0_consistency(manager)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_kill_coordinator_during_replace(manager: ScyllaClusterManager, failure_detector_timeout: int, scale_timeout: callable) -> None:
    """ Kill coordinator with error injection while replace is running. Topology operation is
    expected to fail and cluster is rolled back to previous state. """
    config, cmdline, nodes, coordinator_host = await _start_cluster_and_kill_coordinator(manager, failure_detector_timeout)
    other_nodes = [srv for srv in nodes if srv.server_id != coordinator_host.server_id]
    node_to_replace_srv_id = other_nodes[-1].server_id
    await manager.server_stop_gracefully(node_to_replace_srv_id)
    previous_coordinator_id = await manager.get_host_id(coordinator_host.server_id)
    await manager.api.enable_injection(coordinator_host.ip_addr, "crash_coordinator_before_stream", one_shot=True)
    replace_cfg = ReplaceConfig(replaced_id = node_to_replace_srv_id, reuse_ip_addr = False, use_host_id = True)
    new_node = await manager.server_add(start=False, config=config, replace_cfg=replace_cfg, cmdline=cmdline)
    await manager.server_start(new_node.server_id, expected_error=f"Replace failed. See earlier errors|{BANNED_NOTIFICATION}")
    await wait_new_coordinator_elected(manager, previous_coordinator_id, time.time() + scale_timeout(60))
    await wait_for_no_pending_topology_transition(manager, time.time() + scale_timeout(60))
    logger.debug("Start old coordinator node")
    await manager.others_not_see_server(server_ip=coordinator_host.ip_addr)
    await manager.server_restart(coordinator_host.server_id, wait_others=1)
    await manager.servers_see_each_other(await manager.running_servers())
    logger.debug("Replaced node is already non-voter and will be banned after restart. Remove it")
    coordinator_host = await get_coordinator_host(manager)
    await manager.remove_node(coordinator_host.server_id, node_to_replace_srv_id)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + scale_timeout(60))
    await check_token_ring_and_group0_consistency(manager)
