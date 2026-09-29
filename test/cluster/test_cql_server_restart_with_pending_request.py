#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import contextlib
import time

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cql_server_restart_with_pending_request(manager: ScyllaClusterManager) -> None:
    """Re-enabling the CQL server must not wait for a request that outlived the previous server's drain."""
    server = await manager.server_add(config={'request_timeout_on_shutdown_in_seconds': 1})
    cql = manager.get_cql()
    injection = "transport_cql_request_pause"

    await manager.api.enable_injection(server.ip_addr, injection, one_shot=False)
    query = None
    try:
        query = asyncio.ensure_future(cql.run_async("SELECT * FROM system.local"))
        await manager.api.wait_for_injection_enter(server.ip_addr, injection)

        # The paused request outlives the 1s drain timeout.
        await manager.api.client.delete("/storage_service/native_transport", host=server.ip_addr)
        await manager.api.client.post("/storage_service/native_transport", host=server.ip_addr, timeout=5)
    finally:
        await manager.api.disable_injection(server.ip_addr, injection)
        if query is not None:
            with contextlib.suppress(Exception):
                await query

    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 30)
