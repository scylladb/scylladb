# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest
from test.pylib.util import wait_for


@pytest.mark.asyncio
async def test_config_live_updates(manager):
    server = await manager.server_add()
    server_log = await manager.server_open_log(server.server_id)

    await manager.server_update_config(server.server_id, "uninitialized_connections_semaphore_cpu_concurrency", 16)
    await server_log.wait_for("Updating uninitialized_connections_semaphore_cpu_concurrency from 8 to 16 due to config update")
