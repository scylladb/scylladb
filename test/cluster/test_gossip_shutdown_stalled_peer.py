#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import scale_timeout_by_mode

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



STALL_INJECTION = "messaging_service_stall_gossip_syn"


@pytest.mark.parametrize("stalled", [
    pytest.param(True, id="stalled_syn_send", marks=[
        pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode"),
        pytest.mark.skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-6334",
                             reason="gossiper::shutdown() waits for a gossip SYN stuck at the head of an RPC connection"),
    ]),
    pytest.param(False, id="control"),
])
async def test_graceful_shutdown_with_stalled_syn_send(manager: ScyllaClusterManager, build_mode: str, stalled: bool) -> None:
    """Stop node A while a gossip SYN to node B sits at the head of an established RPC connection.

    Idea: a frame at the front of an RPC outgoing queue is being written to the
    socket and no timeout or cancel can withdraw it, so when a GOSSIP_DIGEST_SYN
    is that frame, the abort requested by gossiper::shutdown() does not resolve
    its send and the _background_msg gate never drains. In the field the write
    blocks because the socket's send buffer is full: the node started while a
    network partition cut it off from B, so it never marked B UP, never marked
    it DOWN, never dropped the connection, and kept gossiping into it until the
    kernel buffer filled. The stop then lasts until TCP gives up on the
    connection, about 16 minutes, longer than systemd's stop timeout.

    Setup: A's RPC client connection to B is reopened under the
    messaging_service_stall_gossip_syn injection, which stalls the write of the
    first GOSSIP_DIGEST_SYN frame to B inside the socket sink, in place of a
    full send buffer. Once A logs the stall, A is stopped gracefully and the
    stop must finish within stop_timeout. Without the stall (control) it does.
    """
    stop_timeout = scale_timeout_by_mode(build_mode, 60)
    node_a, node_b = await manager.servers_add(2, auto_rack_dc="dc")
    log = await manager.server_open_log(node_a.server_id)

    if stalled:
        await manager.api.enable_injection(node_a.ip_addr, STALL_INJECTION, one_shot=False, parameters={"ip": node_b.ip_addr})
        mark = await log.mark()
        # The injection wraps connections opened after it is enabled.
        await manager.api.inject_disconnect(node_a.ip_addr, node_b.ip_addr)
        logger.info(f"Waiting for a gossip SYN from {node_a} to {node_b} to stall")
        await log.wait_for(f"stalling a GOSSIP_DIGEST_SYN send to {node_b.ip_addr}", from_mark=mark, timeout=60)

    logger.info(f"Gracefully stopping {node_a}")
    try:
        await manager.server_stop_gracefully(node_a.server_id, timeout=stop_timeout)
    except TimeoutError:
        if stalled:
            # Lets the stalled send complete, as the TCP timeout eventually
            # would, so the stop finishes and the teardown is clean.
            await manager.api.message_injection(node_a.ip_addr, STALL_INJECTION)
        stall = f" with a gossip SYN send to {node_b} stalled" if stalled else ""
        pytest.fail(f"graceful stop of {node_a} did not finish in {stop_timeout}s{stall}")
