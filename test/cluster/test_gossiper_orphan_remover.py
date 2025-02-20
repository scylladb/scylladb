#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import time
import pytest
import logging
from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_crashed_node_substitution(manager: ManagerClient):
    servers = await manager.servers_add(3, config={
        'error_injections_at_startup': ['fast_orphan_removal_fiber']
    })

    cmdline = [
        '--logger-log-level', 'gossip=debug',
    ]
    failed_server = await manager.server_add(start=False, config={
        'error_injections_at_startup': ['crash_before_group0_join']}, cmdline=cmdline)
    task = asyncio.create_task(manager.server_start(failed_server.server_id, expected_error="deliberately crashed for orphan remover test"))

    log = await manager.server_open_log(failed_server.server_id)
    await log.wait_for("finished do_send_ack2_msg")
    await manager.api.message_injection(failed_server.ip_addr, 'crash_before_group0_join')
    
    await task
    
    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)

    gossiper_eps = live_eps + down_eps
    post_crash_servers = await manager.running_servers()
    server_eps = [s.ip_addr for s in post_crash_servers]

    assert len(gossiper_eps) == (len(server_eps)+1)

    orphan_ip = [ip for ip in gossiper_eps if ip not in server_eps][0]
    assert failed_server.ip_addr == orphan_ip 

    [await manager.api.enable_injection(s.ip_addr, 'speedup_orphan_removal', one_shot=False) for s in servers]
    [await manager.api.message_injection(s.ip_addr, 'fast_orphan_removal_fiber') for s in servers]

    log = await manager.server_open_log(servers[0].server_id)
    await log.wait_for(f"Finished to force remove node {orphan_ip}")

    post_wait_live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    post_wait_down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)

    assert len(post_wait_live_eps + post_wait_down_eps) == len(server_eps)
    assert orphan_ip not in (post_wait_live_eps + post_wait_down_eps)
