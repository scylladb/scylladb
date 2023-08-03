#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Tests that are specific to the raft-based cluster feature implementation.
"""
from test.pylib.manager_client import ManagerClient
from test.topology.test_cluster_downgrade_after_successful_upgrade_fails import test_downgrade_after_successful_upgrade_fails
import pytest


@pytest.mark.asyncio
async def test_downgrade_after_successful_upgrade_fails_experimental(manager: ManagerClient) -> None:
    for _ in range(3):
        await manager.server_add()
    await test_downgrade_after_successful_upgrade_fails(manager)
