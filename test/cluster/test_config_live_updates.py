# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest
from test.pylib.util import wait_for


@pytest.mark.asyncio
async def test_config_live_updates(manager):
    config = {
        "maintenance_socket": "ignore"  # bring back the default value
    }
    server = await manager.server_add(config=config)
    server_log = await manager.server_open_log(server.server_id)

    await manager.server_update_config(server.server_id, "permissions_validity_in_ms", 20000)
    await server_log.wait_for("Updating loading cache; max_size: 1000, expiry: 20000ms, refresh: 100ms")

    await manager.server_update_config(server.server_id, "permissions_update_interval_in_ms", 30000)
    await server_log.wait_for("Updating loading cache; max_size: 1000, expiry: 20000ms, refresh: 30000ms")

    await manager.server_update_config(server.server_id, "uninitialized_connections_semaphore_cpu_concurrency", 16)
    await server_log.wait_for("Updating uninitialized_connections_semaphore_cpu_concurrency from 8 to 16 due to config update")
