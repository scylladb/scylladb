#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient
import pytest

@pytest.mark.asyncio
async def test_gossiper_endpoints(manager: ManagerClient) -> None:
    servers = await manager.running_servers()
    servers_ips = [await manager.get_host_ip(server.server_id) for server in servers]

    alive_endpoints_lists = [await manager.api.get_alive_endpoints(ip) for ip in servers_ips]
    alive_endpoints_num = len(alive_endpoints_lists[0])
    assert all(len(alive_endpoints) == alive_endpoints_num for alive_endpoints in alive_endpoints_lists), "Servers see different number of alive endpoints"

    down_endpoints_lists = [await manager.api.get_down_endpoints(ip) for ip in servers_ips]
    assert not any(down_endpoints_lists), "Some servers see a down endpoint"

    down_server_index = 1
    down_server_ip = servers_ips[down_server_index]
    await manager.server_stop(servers[down_server_index].server_id)
    for ip in servers_ips:
        if ip != down_server_ip:
            await manager.server_not_sees_other_server(ip, down_server_ip)

            alive_endpoints = await manager.api.get_alive_endpoints(ip)
            assert len(alive_endpoints) == alive_endpoints_num - 1, "Incorrect number of alive endpoints"
            assert down_server_ip not in alive_endpoints, "Stopped server is marked as alive"

            down_endpoints = await manager.api.get_down_endpoints(ip)
            assert len(down_endpoints) == 1, "Incorrect number of down endpoints"
            assert down_server_ip in down_endpoints, "Stopped server isn't marked as dead"
