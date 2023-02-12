#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test rejoin of a server after it was stopped suddenly (crash-like)
"""
from test.pylib.manager_client import ManagerClient
import pytest


@pytest.mark.asyncio
async def test_start_after_sudden_stop(manager: ManagerClient, random_tables) -> None:
    """Tests a server can rejoin the cluster after being stopped suddenly"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_stop(servers[0].server_id)
    await table.add_column()
    await manager.server_start(servers[0].server_id)
    await random_tables.verify_schema()
