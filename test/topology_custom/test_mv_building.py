#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import pytest
from test.pylib.manager_client import ManagerClient

# A sanity check test ensures that starting and shutting down Scylla when view building is
# disabled is conducted properly and we don't run into any issues.
@pytest.mark.asyncio
async def test_start_scylla_with_view_building_disabled(manager: ManagerClient):
    server = await manager.server_add(config={"view_building": "false"})
    await manager.server_stop_gracefully(server_id=server.server_id)

    # Make sure there have been no errors.
    log = await manager.server_open_log(server.server_id)
    res = await log.grep(r"ERROR.*\[shard [0-9]+:[a-z]+\]")
    assert len(res) == 0
