#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
"""
Tests that are specific to the raft-based cluster feature implementation.
"""
import asyncio
import time

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_feature
from test.cluster import test_cluster_features
import pytest


@pytest.mark.asyncio
async def test_rolling_upgrade_happy_path(manager: ManagerClient) -> None:
    await manager.servers_add(3, auto_rack_dc="dc1")
    await test_cluster_features.test_rolling_upgrade_happy_path(manager)


@pytest.mark.asyncio
async def test_downgrade_after_partial_upgrade(manager: ManagerClient) -> None:
    await manager.servers_add(3, auto_rack_dc="dc1")
    await test_cluster_features.test_downgrade_after_partial_upgrade(manager)


@pytest.mark.asyncio
async def test_joining_old_node_fails(manager: ManagerClient) -> None:
    await manager.servers_add(3, auto_rack_dc="dc1")
    await test_cluster_features.test_joining_old_node_fails(manager)


@pytest.mark.asyncio
async def test_downgrade_after_successful_upgrade_fails(manager: ManagerClient) -> None:
    await manager.servers_add(3, auto_rack_dc="dc1")
    await test_cluster_features.test_downgrade_after_successful_upgrade_fails(manager)


@pytest.mark.asyncio
async def test_partial_upgrade_can_be_finished_with_removenode(manager: ManagerClient) -> None:
    await manager.servers_add(3, auto_rack_dc="dc1")
    await test_cluster_features.test_partial_upgrade_can_be_finished_with_removenode(manager)


@pytest.mark.asyncio
async def test_cannot_disable_cluster_feature_after_all_declare_support(manager: ManagerClient) -> None:
    """Upgrade all nodes to support the test cluster feature, but suppress
       the topology coordinator and prevent it from enabling the feature.
       Try to downgrade one of the nodes - it should fail because of the
       missing feature. Unblock the topology coordinator, restart the node
       and observe that the feature was enabled.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")

    # Rolling restart so that all nodes support the feature - but do not
    # allow enabling yet
    for srv in servers:
        await manager.server_update_config(srv.server_id, 'error_injections_at_startup', [
            'raft_topology_suppress_enabling_features',
            'features_enable_test_feature',
        ])
        await manager.server_restart(srv.server_id)

    # Try to downgrade one node
    await manager.server_update_config(servers[0].server_id, 'error_injections_at_startup', [])
    await manager.server_stop(servers[0].server_id)
    await manager.server_start(servers[0].server_id,
                               expected_error="Feature 'TEST_ONLY_FEATURE' was previously supported by all nodes in the cluster")
    
    # Unblock enabling features on nodes
    for srv in servers[1:]:
        await manager.api.disable_injection(srv.ip_addr, 'raft_topology_suppress_enabling_features')
    
    # Re-enable the feature again and restart
    await manager.server_update_config(servers[0].server_id, 'error_injections_at_startup', [
        'features_enable_test_feature',
    ])
    await manager.server_start(servers[0].server_id)

    # Nodes should start supporting the feature
    cql = cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await asyncio.gather(*(wait_for_feature('TEST_ONLY_FEATURE', cql, h, time.time() + 60) for h in hosts))

@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_simulate_upgrade_legacy_to_raft_listener_registration(manager: ManagerClient):
    """
    We simulate an upgrade from legacy mode to Raft. Our goal is
    to make sure that the cluster successfully reaches the state
    where it can start the upgrade procedure.

    This test effectively reproduces the problem described
    in scylladb/scylladb#18049.
    """

    # We need this so that the first logs we wait for appear.
    cmdline = ["--logger-log-level", "raft_topology=debug"]
    # Tablets and legacy mode are incompatible with each other.
    config = {"force_gossip_topology_changes": True,
              "tablets_mode_for_new_keyspaces": "disabled"}
    
    error_injection = { "name": "suppress_features", "value": "SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES"}
    bad_config = config | {"error_injections_at_startup": [error_injection]}

    # We need to bootstrap the nodes one-by-one.
    # We can't do it concurrently without Raft.
    s1 = await manager.server_add(cmdline=cmdline, config=bad_config)
    s2 = await manager.server_add(cmdline=cmdline, config=bad_config)

    # Simulate upgrading node 1.
    await manager.server_stop_gracefully(s1.server_id)
    await manager.server_update_config(s1.server_id, "error_injections_at_startup", [])

    log = await manager.server_open_log(s1.server_id)
    mark = await log.mark()

    await manager.server_start(s1.server_id)

    # The node should block after this.
    await log.wait_for("Waiting for cluster feature `SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES`", from_mark=mark)
    mark = await log.mark()

    # Simulate upgrading node 2.
    await manager.server_stop_gracefully(s2.server_id)
    await manager.server_update_config(s2.server_id, "error_injections_at_startup", [])
    await manager.server_start(s2.server_id)

    # If everything went smoothly, we'll get to this.
    await log.wait_for("The cluster is ready to start upgrade to the raft topology")
