#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Regression test for SCYLLADB-2127: direct_failure_detector should recover
# after a node's IP changes, not get stuck on a stale cached RPC connection.

import asyncio
import logging
import time

import pytest
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


class BlackholeServer:
    """A TCP server that accepts connections but never responds.
    Simulates a blackholed IP in Kubernetes where the old pod IP is
    routable but nothing processes the traffic."""

    def __init__(self, ip: str, ports: list[int]):
        self.ip = ip
        self.ports = ports
        self._servers = []
        self._tasks = []

    async def start(self):
        for port in self.ports:
            srv = await asyncio.start_server(
                self._handle_client, self.ip, port,
                reuse_address=True)
            self._servers.append(srv)
        logger.info(f"Blackhole server listening on {self.ip}:{self.ports}")

    async def _handle_client(self, reader, writer):
        # Track the task so we can cancel it on stop
        self._tasks.append(asyncio.current_task())
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            writer.close()

    async def stop(self):
        # Cancel all handler tasks to close accepted connections
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        for srv in self._servers:
            srv.close()
            await srv.wait_closed()
        logger.info(f"Blackhole server on {self.ip} stopped")


@pytest.mark.asyncio
async def test_direct_fd_recovers_after_ip_change(manager: ManagerClient):
    """Reproduces SCYLLADB-2127: when a node restarts with a new IP (e.g. K8s
    pod rescheduling), the other nodes' direct_failure_detector should recover
    within a reasonable time, not get stuck on stale cached RPC connections.

    The scenario:
    1. Start a 2-node cluster, both nodes UP
    2. Stop both nodes
    3. Change node 1's IP
    4. Start a blackhole server on node 1's old IP
    5. Restart node 0 — it reads system.peers with node 1's old IP
       and creates a cached RPC connection to the blackholed address
    6. Restart node 1 with new IP — gossip updates node 0's address_map
    7. Without fix: node 0's direct_fd is stuck pinging old (blackholed) IP
       via the stale cached RPC connection for ~10 minutes
    8. Assert node 0 sees node 1 as alive within 60 seconds
    """

    # Start node 0 first as the explicit seed
    logger.info("Starting a 2-node cluster")
    servers = await manager.servers_add(1)
    servers += await manager.servers_add(1, seeds=[servers[0].ip_addr])

    # servers_add waits for the node to be fully up (SERVING state),
    # which means system.peers is already populated on node 0.
    # No need to wait for CQL separately.

    # Record old IP for node 1
    old_ip = servers[1].ip_addr
    logger.info(f"Node 1 old IP: {old_ip}")

    # Stop both nodes
    logger.info("Stopping all nodes")
    for srv in servers:
        await manager.server_stop_gracefully(srv.server_id)

    # Change IP of node 1
    new_ip = await manager.server_change_ip(servers[1].server_id)
    logger.info(f"Node 1 new IP: {new_ip}")

    # Start blackhole on old IP. When node 0 restarts, it reads system.peers
    # with node 1's old IP and creates a cached RPC connection to the blackhole.
    # The blackhole accepts TCP but never responds — exactly like K8s where old
    # pod IPs are routable but the pod is gone.
    blackhole = BlackholeServer(old_ip, [7000, 7001])
    await blackhole.start()

    try:
        # Restart node 0. It reads system.peers with node 1's old IP →
        # creates a cached RPC connection to the blackholed address.
        # server_start waits for the node to be fully up, at which point
        # it has already loaded peers and initiated RPC connections.
        logger.info("Restarting node 0")
        await manager.server_start(servers[0].server_id)

        # Mark the log position so we can later verify the fix was triggered
        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        # Restart node 1 with new IP. It contacts node 0 via gossip,
        # which updates node 0's address_map with node 1's new IP.
        # BUG: messaging_service doesn't invalidate cached RPC connections
        # when address_map changes → direct_fd still uses stale connection.
        logger.info("Restarting node 1 with new IP")
        await manager.server_start(servers[1].server_id)

        # Update local server info with new IP for REST API calls
        servers[1] = servers[1]._replace(ip_addr=new_ip, rpc_address=new_ip)

        # The key assertion: node 0 should see node 1 as alive within 60s.
        # Without the fix, node 0's direct_failure_detector is stuck pinging
        # old (blackholed) IP via cached RPC connection → node 1 appears dead
        # for ~10 minutes.
        logger.info("Waiting for node 0 to see node 1 as alive (60s timeout)")
        start = time.time()
        await manager.server_sees_other_server(servers[0].ip_addr, new_ip, interval=60)
        elapsed = time.time() - start
        logger.info(f"Node 0 sees node 1 as alive after {elapsed:.1f}s")

    finally:
        await blackhole.stop()
