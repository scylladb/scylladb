# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Disabling binary destroys cql_server, whose option cache asserts if the system.clients snapshot
still references its entries.
"""

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_system_clients_scan_then_disablebinary(manager: ScyllaClusterManager):
    server = (await manager.servers_add(1, cmdline=['--smp=2']))[0]
    cql, _ = await manager.get_ready_cql([server])
    # Keep the snapshot past its release timer so disablebinary always overlaps it.
    await manager.api.enable_injection(server.ip_addr, "clients_table_keep_snapshot", one_shot=False)

    # The driver's own connections carry driver_name/version, i.e. cache entries.
    rows = await cql.run_async("SELECT * FROM system.clients")
    assert any(r.driver_name for r in rows)
    # Snapshot still retained here (injection); this used to trip the assert.
    await manager.api.client.delete("/storage_service/native_transport", host=server.ip_addr)
    await manager.api.client.post("/storage_service/native_transport", host=server.ip_addr)
    await manager.api.disable_injection(server.ip_addr, "clients_table_keep_snapshot")

    manager.driver_close()
    await manager.driver_connect()
    cql, _ = await manager.get_ready_cql([server])
    assert await cql.run_async("SELECT * FROM system.clients")
