#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import time
import pytest
import logging
from functools import partial
from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode
from test.pylib.util import wait_for
from test.pylib.internal_types import ServerInfo

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_crashed_node_substitution(manager: ManagerClient):
    """Test that a node which crashed after starting gossip but before joining group0
    (an 'orphan' node) is eventually removed from gossip by the gossiper_orphan_remover_fiber.

    The scenario:
    1. Start 3 nodes with the 'fast_orphan_removal_fiber' injection enabled. This freezes
       the gossiper_orphan_remover_fiber on each node before it enters its polling loop,
       so it cannot remove any orphan until explicitly unblocked.
    2. Start a 4th node with the 'crash_before_group0_join' injection enabled. This node
       starts gossip normally but blocks inside pre_server_start(), just before sending
       the join RPC to the topology coordinator. It never joins group0.
    3. Wait until the 4th node's gossip state has fully propagated to all 3 running peers,
       then trigger its crash via the injection. At this point all peers see it as an orphan:
       present in gossip but absent from the group0 topology.
    4. Assert the orphan is visible in gossip (live or down) on the surviving nodes.
    5. Unblock the gossiper_orphan_remover_fiber on all 3 nodes (via message_injection) and
       enable the 'speedup_orphan_removal' injection so the fiber removes the orphan immediately
       without waiting for the normal 60-second age threshold.
    6. Wait for the 'Finished to force remove node' log line confirming removal, then assert
       the orphan is no longer present in gossip.
    """
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
    failed_id = await manager.get_host_id(failed_server.server_id)

    # Wait until the failed server's gossip state has propagated to all running peers.
    # "finished do_send_ack2_msg" only guarantees that one peer completed a gossip round
    # with the failed server; other nodes learn about it only in subsequent gossip rounds.
    # Querying gossip before propagation completes would cause the assertion below to fail
    # because the orphan node would not yet appear as live or down on every peer.
    async def gossip_has_node(server: ServerInfo):
        live = await manager.api.client.get_json("/gossiper/endpoint/live", host=server.ip_addr)
        down = await manager.api.client.get_json("/gossiper/endpoint/down", host=server.ip_addr)
        return True if failed_server.ip_addr in live + down else None

    for s in servers:
        await wait_for(partial(gossip_has_node, s), deadline=time.time() + 30)

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
    await log.wait_for(f"Finished to force remove node {failed_id}")

    post_wait_live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    post_wait_down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)

    assert len(post_wait_live_eps + post_wait_down_eps) == len(server_eps)
    assert orphan_ip not in (post_wait_live_eps + post_wait_down_eps)
