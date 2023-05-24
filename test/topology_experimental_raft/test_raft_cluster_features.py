#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Tests the cluster feature functionality, with raft.
"""
from test.pylib.manager_client import ManagerClient
import test.topology.test_cluster_features
import pytest


async def setup_cluster(manager: ManagerClient):
    for i in range(3):
        await manager.server_add()


@pytest.mark.asyncio
async def test_cluster_upgrade(manager: ManagerClient) -> None:
    """Simulates an upgrade of a cluster by doing a rolling restart
       and marking the test-only feature as supported on restarted nodes.
    """
    await setup_cluster(manager)
    await test.topology.test_cluster_features.test_cluster_upgrade(manager)


@pytest.mark.asyncio
async def test_partial_cluster_upgrade_then_downgrade(manager: ManagerClient) -> None:
    """Simulates an partial of a cluster by enabling the test features in all
       but one nodes, then downgrading them.
    """
    await setup_cluster(manager)
    await test.topology.test_cluster_features.test_partial_cluster_upgrade_then_downgrade(manager)


@pytest.mark.asyncio
async def test_reject_joining_node_without_support_for_enabled_features(manager: ManagerClient) -> None:
    """Upgrades the cluster to enable a new feature, then tries to add
       another node - which should fail because it doesn't support one
       of the enabled features.
    """
    await setup_cluster(manager)
    await test.topology.test_cluster_features.test_reject_joining_node_without_support_for_enabled_features(manager)


@pytest.mark.asyncio
async def test_reject_replacing_node_without_support_for_enabled_features(manager: ManagerClient) -> None:
    """Upgrades the cluster to enable a new feature, then tries to replace
       an existing node with another node - which should fail because
       it doesn't support one of the enabled features.
    """
    await setup_cluster(manager)
    await test.topology.test_cluster_features.test_reject_replacing_node_without_support_for_enabled_features(manager)
