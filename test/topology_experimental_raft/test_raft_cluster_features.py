#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Tests that are specific to the raft-based cluster feature implementation.
"""
import logging
import asyncio
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_feature
from test.topology import test_cluster_features
import pytest


@pytest.mark.asyncio
async def test_rolling_upgrade_happy_path(manager: ManagerClient) -> None:
    await manager.servers_add(3)
    await test_cluster_features.test_rolling_upgrade_happy_path(manager)


@pytest.mark.asyncio
async def test_downgrade_after_partial_upgrade(manager: ManagerClient) -> None:
    await manager.servers_add(3)
    await test_cluster_features.test_downgrade_after_partial_upgrade(manager)


@pytest.mark.asyncio
async def test_joining_old_node_fails(manager: ManagerClient) -> None:
    await manager.servers_add(3)
    await test_cluster_features.test_joining_old_node_fails(manager)


@pytest.mark.asyncio
async def test_downgrade_after_successful_upgrade_fails(manager: ManagerClient) -> None:
    await manager.servers_add(3)
    await test_cluster_features.test_downgrade_after_successful_upgrade_fails(manager)


@pytest.mark.asyncio
async def test_partial_upgrade_can_be_finished_with_removenode(manager: ManagerClient) -> None:
    await manager.servers_add(3)
    await test_cluster_features.test_partial_upgrade_can_be_finished_with_removenode(manager)


@pytest.mark.asyncio
async def test_cannot_disable_cluster_feature_after_all_declare_support(manager: ManagerClient) -> None:
    """Upgrade all nodes to support the test cluster feature, but suppress
       the topology coordinator and prevent it from enabling the feature.
       Try to downgrade one of the nodes - it should fail because of the
       missing feature. Unblock the topology coordinator, restart the node
       and observe that the feature was enabled.
    """
    servers = await manager.servers_add(3)

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
