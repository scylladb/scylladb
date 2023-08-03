#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
from test.topology.test_cluster_rolling_upgrade_happy_path import test_rolling_upgrade_happy_path
import pytest


@pytest.mark.asyncio
async def test_rolling_upgrade_happy_path_experimental(manager: ManagerClient) -> None:
    for _ in range(3):
        await manager.server_add()
    await test_rolling_upgrade_happy_path(manager)
