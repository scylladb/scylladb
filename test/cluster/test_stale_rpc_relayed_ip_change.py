#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Regression test for SCYLLADB-6568: stale host_id RPC clients must be evicted
# when a node learns a peer's new IP through gossip before it handles the
# peer's CLIENT_ID.

import asyncio
import logging
import time

import pytest
from test.cluster.test_stale_rpc_connection_on_ip_change import BlackholeServer
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

INJECTION = "delay_client_id_before_map_update"


async def wait_until(pred, timeout: float, what: str):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if await pred():
            return
        await asyncio.sleep(0.5)
    raise TimeoutError(f"timed out waiting for {what}")


@pytest.mark.xfail(reason="SCYLLADB-6568: stale host_id clients survive a relayed IP change")
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.asyncio
async def test_direct_fd_recovers_after_relayed_ip_change(manager: ScyllaClusterManager):
    servers = await manager.servers_add(1)
    servers += await manager.servers_add(2, seeds=[servers[0].ip_addr])
    s0, s1, s2 = servers
    old_ip = s1.ip_addr
    host_id1 = await manager.get_host_id(s1.server_id)

    for srv in servers:
        await manager.server_stop_gracefully(srv.server_id)
    new_ip = await manager.server_change_ip(s1.server_id)
    logger.info(f"Node 1 IP {old_ip} -> {new_ip}")

    # Node 0 restarts with node 1's old IP from system.peers and connects to the
    # blackhole on it. Node 2 starts only after that, so the connection is node 0's.
    blackhole = BlackholeServer(old_ip, [7000, 7001])
    await blackhole.start()
    start = None
    try:
        await manager.server_start(s0.server_id)

        async def blackhole_connected():
            return len(blackhole._tasks) > 0
        await wait_until(blackhole_connected, 30, "node 0 to connect to the blackhole")
        logger.info(f"Blackhole accepted {len(blackhole._tasks)} connections")
        await manager.server_start(s2.server_id)

        # Hold every CLIENT_ID from node 1's new IP on node 0, so node 0 learns
        # the new IP from gossip before it handles node 1's CLIENT_ID.
        await manager.api.enable_injection(s0.ip_addr, INJECTION, one_shot=False,
                                           parameters={"target_addr": new_ip})

        # Node 1 is down until it starts, so node 0 cannot log it alive before this mark.
        log0 = await manager.server_open_log(s0.server_id)
        mark = await log0.mark()
        logger.info("Starting node 1 at the new IP, seeded by node 2")
        start = asyncio.create_task(manager.server_start(s1.server_id, seeds=[s2.ip_addr]))

        # With the fix node 0 marks node 1 alive as soon as it learns the new IP,
        # so the new IP may show up in either list.
        async def node0_knows_new_ip():
            return (new_ip in await manager.api.get_down_endpoints(s0.ip_addr)
                    or new_ip in await manager.api.get_alive_endpoints(s0.ip_addr))
        await wait_until(node0_knows_new_ip, 45, "node 0 to learn the new IP through gossip")
        logger.info("Node 0 learned the new IP through gossip; releasing CLIENT_ID")

        await manager.api.message_injection(s0.ip_addr, INJECTION)
        await start

        # Without the fix node 0 pings node 1 through the client to the old IP
        # until the blackhole goes away. Check gossip and the direct failure detector.
        # Both checks share one 60 s budget.
        start_t = time.time()
        await manager.server_sees_other_server(s0.ip_addr, new_ip, interval=60)
        await log0.wait_for(f"marking Raft server {host_id1} as alive", from_mark=mark,
                            timeout=max(1, start_t + 60 - time.time()))
        logger.info(f"Node 0 sees node 1 alive after {time.time() - start_t:.1f}s")
    finally:
        try:
            # On failure, release held CLIENT_IDs before their 60 s timeout aborts node 0.
            # CLIENT_ID is no_wait, so node 1 can finish starting while node 0 still holds it.
            if start is not None:
                try:
                    await manager.api.disable_injection(s0.ip_addr, INJECTION)
                finally:
                    await asyncio.gather(start, return_exceptions=True)
        finally:
            await blackhole.stop()
