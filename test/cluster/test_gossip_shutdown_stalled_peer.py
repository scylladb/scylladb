#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("stop_method", ["graceful_stop", "stop_gossiping_api"])
async def test_graceful_shutdown_with_new_connection_to_stalled_peer(manager: ScyllaClusterManager, stop_method: str) -> None:
    """Stop gossip on node A right after it opened a new RPC connection to a stalled node B.

    Reproducer for SCYLLADB-4693. B is SIGSTOPped, so its kernel accepts TCP
    connections but the process never answers. A convicts B, which marks it
    DOWN at once instead of waiting for the failure detector. Marking B DOWN
    drops A's RPC clients to B, and A's next gossip round opens a new
    connection to B whose protocol negotiation never completes. The SYN sent
    on it is awaited under A's _background_msg gate, and without a fix
    gossiper::shutdown() waits on that gate forever.

    Gossip is stopped either by a graceful shutdown of A or through the
    stop_gossiping REST API.
    """
    cmdline = ["--logger-log-level", "gossip=trace"]
    node_a, node_b = await manager.servers_add(2, cmdline=cmdline, auto_rack_dc="dc")
    node_b_host_id = await manager.get_host_id(node_b.server_id)

    log = await manager.server_open_log(node_a.server_id)

    logger.info(f"Pausing {node_b}")
    await manager.server_pause(node_b.server_id)
    try:
        logger.info(f"Convicting {node_b} on {node_a}")
        await manager.api.convict(node_a.ip_addr, node_b_host_id)
        mark = await log.mark()

        logger.info(f"Waiting for {node_a} to send a SYN to {node_b} on a new connection")
        await log.wait_for(f"Sending a GossipDigestSyn to {node_b_host_id}", from_mark=mark, timeout=60)

        if stop_method == "graceful_stop":
            logger.info(f"Gracefully stopping {node_a}")
            await manager.server_stop_gracefully(node_a.server_id, timeout=30)
        else:
            logger.info(f"Stopping gossip on {node_a} through the REST API")
            await manager.api.client.delete("/storage_service/gossiping", host=node_a.ip_addr, timeout=30)
    finally:
        # Lets a stuck send complete, so that a failed run does not make the
        # teardown wait for the graceful stop timeout.
        await manager.server_unpause(node_b.server_id)


async def test_graceful_shutdown_with_unanswered_connection_to_dead_peer(manager: ScyllaClusterManager) -> None:
    """Gracefully stop node A while a live peer B is dead behind an address that accepts connections and never answers.

    B is killed and the test listens on B's address in its place, accepting
    every connection and reading nothing. B is still live in A's view when
    the stop starts, so the stop announces the shutdown to it. A's old
    connections to B died with B, so the GossipShutdown goes out on a new
    connection whose protocol negotiation never completes. The send must
    expire at its deadline and the stop must finish.
    """
    node_a, node_b = await manager.servers_add(2, auto_rack_dc="dc")
    node_b_host_id = await manager.get_host_id(node_b.server_id)

    log = await manager.server_open_log(node_a.server_id)

    logger.info(f"Killing {node_b}")
    await manager.server_stop(node_b.server_id, convict=False)

    connected = asyncio.Event()
    # A StreamWriter closes its connection when garbage collected.
    writers: list[asyncio.StreamWriter] = []

    async def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)
        connected.set()

    logger.info(f"Listening on {node_b.ip_addr}:7000 in place of {node_b}")
    listener = await asyncio.start_server(accept, host=node_b.ip_addr, port=7000)
    try:
        logger.info(f"Waiting for {node_a} to connect")
        await asyncio.wait_for(connected.wait(), timeout=60)
        mark = await log.mark()

        logger.info(f"Gracefully stopping {node_a}")
        await manager.server_stop_gracefully(node_a.server_id, timeout=60)
    finally:
        # Fails a stuck send, so that a failed run does not make the teardown
        # wait for the graceful stop timeout. The connections are closed
        # through the listener: one accepted a moment ago has not reached
        # accept() yet, and wait_closed() waits for every one of them.
        listener.close()
        listener.abort_clients()
        await asyncio.wait_for(listener.wait_closed(), timeout=30)

    assert await log.grep(f"Sending a GossipShutdown to {node_b_host_id}", from_mark=mark), \
        f"{node_a} did not announce its shutdown to {node_b}, so the scenario did not hold"
    assert await log.grep(f"GossipShutdown to {node_b_host_id} was not sent before the deadline", from_mark=mark), \
        f"the GossipShutdown from {node_a} to {node_b} did not expire, so the send was not stuck"
