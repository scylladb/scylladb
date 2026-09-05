# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
system.clients caches a node-wide snapshot for a short TTL. That snapshot must
not hold on to cql_server's client-option cache entries: disabling the native
transport destroys cql_server, whose ~loading_shared_values() asserts if any
entry is still referenced. Scan, then disable binary inside the TTL.
"""

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.mark.asyncio
async def test_system_clients_scan_then_disablebinary(manager: ScyllaClusterManager):
    server = (await manager.servers_add(1, cmdline=['--smp=2']))[0]
    cql, _ = await manager.get_ready_cql([server])

    # The driver's own connections carry driver_name/version, i.e. cache entries.
    rows = await cql.run_async("SELECT * FROM system.clients")
    assert any(r.driver_name for r in rows)
    # Snapshot still alive here (500ms TTL); this used to trip the assert.
    await manager.api.client.delete("/storage_service/native_transport", host=server.ip_addr)
    await manager.api.client.post("/storage_service/native_transport", host=server.ip_addr)

    manager.driver_close()
    await manager.driver_connect()
    cql, _ = await manager.get_ready_cql([server])
    assert await cql.run_async("SELECT * FROM system.clients")
