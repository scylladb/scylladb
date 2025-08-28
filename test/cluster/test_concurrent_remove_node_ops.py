#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import pytest

from test.pylib.manager_client import ManagerClient


@pytest.mark.asyncio
async def test_concurrent_removenode(manager: ManagerClient):
    await manager.servers_add(5)
    servers = await manager.running_servers()
    assert len(servers) >= 5

    await manager.server_stop_gracefully(servers[2].server_id)
    await manager.server_stop_gracefully(servers[1].server_id)

    ignore_nodes = [servers[1].ip_addr, servers[2].ip_addr]
    await asyncio.gather(*[manager.remove_node(servers[0].server_id, servers[2].server_id, ignore_dead=ignore_nodes),
            manager.remove_node(servers[0].server_id, servers[1].server_id, ignore_dead=ignore_nodes)])
