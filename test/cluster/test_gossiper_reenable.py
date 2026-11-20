#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


async def test_gossip_reenable_all_shards(manager: ScyllaClusterManager) -> None:
    """A node whose gossip was stopped and started again must handle gossip messages on every shard.

    Reproducer for SCYLLADB-6473. Stopping gossip closes the gate of incoming
    gossip messages on every shard, and starting it again reopened that gate on
    shard 0 only. The node then dropped every gossip message that arrived on
    another shard.

    A message is handled on the shard that owns the connection it arrives on.
    That shard is the source port of the connection modulo the number of
    shards of the receiver, and a sender picks a source port that equals its
    own shard modulo its own number of shards. Gossip messages are sent from
    shard 0, so between nodes with the same number of shards they always
    arrive on shard 0, which hides the bug. Hence node B runs with one shard:
    its source ports are then arbitrary and its messages arrive on a random
    shard of node A.

    Since the test cannot choose that shard, it repeats the stop and start of
    gossip on node A until a message from B arrives on a shard other than 0.
    Each stop makes node B mark A as down and drop its connections to A, so
    B's messages arrive on a new connection in each round. A round takes a few
    milliseconds and fails to hit another shard with a probability of 1 / shards.
    """
    shards = 4
    rounds = 20

    node_a = await manager.server_add(cmdline=["--smp", str(shards), "--logger-log-level", "gossip=debug"])
    node_b = await manager.server_add(cmdline=["--smp", "1"])
    node_b_host_id = await manager.get_host_id(node_b.server_id)

    log = await manager.server_open_log(node_a.server_id)

    for i in range(rounds):
        # A announces the stop of its gossip only to the nodes it sees as alive.
        await manager.servers_see_each_other([node_a, node_b])

        logger.info(f"Round {i}: stopping gossip on {node_a}")
        await manager.api.client.delete("/storage_service/gossiping", host=node_a.ip_addr)
        await manager.server_not_sees_other_server(node_b.ip_addr, node_a.ip_addr)

        mark = await log.mark()
        logger.info(f"Round {i}: starting gossip on {node_a}")
        await manager.api.client.post("/storage_service/gossiping", host=node_a.ip_addr)

        # B is the only other node, so every gossip message A receives comes from it.
        mark, matches = await log.wait_for(r"\[shard +(\d+):.*\] gossip - Received GOSSIP_", from_mark=mark, timeout=30)
        shard = int(matches[0][1].group(1))

        logger.info(f"Round {i}: {node_a} received a gossip message on shard {shard}, waiting until it handles one")
        await log.wait_for(f"gossip - (Queue|Process) gossip (syn msg|ack msg digests) from node {node_b_host_id}",
                           from_mark=mark, timeout=30)

        if shard != 0:
            return

    logger.warning(f"All gossip messages arrived on shard 0 of {node_a} in {rounds} rounds, so the test verified nothing")


async def test_gossip_reenable_new_generation(manager: ScyllaClusterManager) -> None:
    """A node whose gossip was stopped and started again must publish its state with a new generation.

    Reproducer for SCYLLADB-4276. A node that stops its gossip is marked as
    shut down by its peers, and they ignore its state until it comes with
    a higher generation. Starting gossip again built a new local state without
    the host ID, so that state was dropped with a warning together with its
    new generation. The node relied on bumping the generation of the old state
    in place, on shard 0 only, which a concurrent update of the local state
    could overwrite. The peers then saw the node as down until its restart.
    """
    node_a, node_b = await manager.servers_add(2, auto_rack_dc="dc")
    log = await manager.server_open_log(node_a.server_id)

    await manager.servers_see_each_other([node_a, node_b])
    old_generation = await manager.api.get_gossip_generation_number(node_b.ip_addr, node_a.ip_addr)

    logger.info(f"Stopping gossip on {node_a}")
    await manager.api.client.delete("/storage_service/gossiping", host=node_a.ip_addr)
    await manager.server_not_sees_other_server(node_b.ip_addr, node_a.ip_addr)

    mark = await log.mark()
    logger.info(f"Starting gossip on {node_a}")
    await manager.api.client.post("/storage_service/gossiping", host=node_a.ip_addr)

    # A new local state starts with version 0. If it was dropped, the old state with its old version is logged.
    matches = await log.grep(r"Gossip started with local state: HeartBeatState = { generation = (\d+), version = 0 }",
                             from_mark=mark)
    assert len(matches) == 1
    new_generation = int(matches[0][1].group(1))
    assert new_generation > old_generation
    assert not await log.grep("attempting to add a state with empty host id", from_mark=mark)

    await manager.server_sees_other_server(node_b.ip_addr, node_a.ip_addr)
    assert await manager.api.get_gossip_generation_number(node_b.ip_addr, node_a.ip_addr) == new_generation
