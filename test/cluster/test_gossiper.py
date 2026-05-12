#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

from test.pylib.manager_client import ManagerClient
import pytest

logger = logging.getLogger(__name__)

@pytest.mark.prepare_3_racks_cluster
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
    await manager.server_stop(servers[down_server_index].server_id, convict=True)
    for ip in servers_ips:
        if ip != down_server_ip:
            await manager.server_not_sees_other_server(ip, down_server_ip)

            alive_endpoints = await manager.api.get_alive_endpoints(ip)
            assert len(alive_endpoints) == alive_endpoints_num - 1, "Incorrect number of alive endpoints"
            assert down_server_ip not in alive_endpoints, "Stopped server is marked as alive"

            down_endpoints = await manager.api.get_down_endpoints(ip)
            assert len(down_endpoints) == 1, "Incorrect number of down endpoints"
            assert down_server_ip in down_endpoints, "Stopped server isn't marked as dead"


@pytest.mark.asyncio
async def test_natural_failure_detection(manager: ManagerClient, failure_detector_timeout) -> None:
    """Verify that the failure detector detects a killed node as DOWN
    without using the convict mechanism, relying on natural detection."""
    servers = await manager.servers_add(3, config={'failure_detector_timeout_in_ms': failure_detector_timeout})
    down_server = servers[0]
    down_server_ip = down_server.ip_addr

    logger.info(f"Stopping {down_server} without conviction")
    await manager.server_stop(down_server.server_id, convict=False)

    logger.info(f"Waiting for other nodes to detect {down_server} as DOWN via natural failure detection")
    await manager.others_not_see_server(down_server_ip)

    for srv in servers:
        if srv.server_id == down_server.server_id:
            continue
        down_endpoints = await manager.api.get_down_endpoints(srv.ip_addr)
        assert down_server_ip in down_endpoints, f"Server {srv} did not detect {down_server} as down"
