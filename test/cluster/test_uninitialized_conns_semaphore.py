#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio

import pytest

from test.pylib.manager_client import ManagerClient

CQL_PORT = 9042
SHARD_AWARE_PORT = 19042


@pytest.mark.asyncio
async def test_uninitialized_conns_sempahore_one(manager: ManagerClient):
    """Verify that CQL queries work when uninitialized_connections_semaphore_cpu_concurrency is set to 1."""
    config = {
        "uninitialized_connections_semaphore_cpu_concurrency": 1,
        "native_transport_port": CQL_PORT,
        "native_shard_aware_transport_port": SHARD_AWARE_PORT,
    }
    server = await manager.server_add(config=config)
    cql, _ = await manager.get_ready_cql([server])

    await cql.run_async("SELECT release_version FROM system.local")

    for port in [CQL_PORT, SHARD_AWARE_PORT]:
        reader, writer = await asyncio.open_connection(server.ip_addr, port)
        writer.close()
        await writer.wait_closed()
