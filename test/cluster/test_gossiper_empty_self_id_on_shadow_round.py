#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#


from aiohttp import ServerDisconnectedError
import pytest
import asyncio
import logging

from test.cluster.conftest import skip_mode
from test.cluster.util import get_coordinator_host
from test.pylib.manager_client import ManagerClient


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.xfail(reason="https://github.com/scylladb/scylladb/issues/25831")
async def test_gossiper_empty_self_id_on_shadow_round(manager: ManagerClient):
    """
    Test gossiper race condition on bootstrap that can lead to an empty self host ID sent in replies to other nodes.
      1. Enable gossiper_publish_local_state_pause on one of the nodes to pause gossiper in
        `gossiper::start_gossiping` when it has just created a self node entry in `_endpoint_state_map`
         with an empty state.
      2. Start nodes normally to allow them join the cluster.
      3. Restart 1st node, to that will pause in `gossiper::start_gossiping`.
      4. After it pauses, start second node and make sure it's making a gossip shadow round. At this step
         if a fix is not in place, the second node will receive an empty host ID (which is a race).
      5. After shadow round on the 2nd node done, unpause the 1st node.
      6. Make sure nodes started successfully.
    """

    cmdline = [
        '--logger-log-level=gossip=debug'
    ]

    cfg = {
        'error_injections_at_startup': [
            {
                'name': 'gossiper_publish_local_state_pause'
            }
        ]
    }

    logging.info("Starting cluster normally")
    node1 = await manager.server_add(cmdline=cmdline, start=False, config=cfg)
    manager.server_add(cmdline=cmdline, start=False)
    node1_log = await manager.server_open_log(node1.server_id)
    node2 = await manager.server_add(cmdline=cmdline, start=False, seeds=[node1.ip_addr])
    node2_log = await manager.server_open_log(node2.server_id)
    task1 = asyncio.create_task(manager.server_start(node1.server_id))
    task2 = asyncio.create_task(manager.server_start(node2.server_id))
    await node1_log.wait_for("gossiper_publish_local_state_pause: waiting for message")
    await manager.api.message_injection(node1.ip_addr, 'gossiper_publish_local_state_pause')
    await task1
    await task2

    logging.info("Stopping cluster")
    await manager.server_stop_gracefully(node1.server_id)
    await manager.server_stop_gracefully(node2.server_id)

    # Remember logs
    paused_node_mark = await node1_log.mark()
    reading_node_mark = await node2_log.mark()

    logging.info("Restarting cluster")
    # Start first node and make sure it's paused on gossiper_publish_local_state_pause
    task1 = asyncio.create_task(
        manager.server_start(node1.server_id, wait_interval=120))
    await node1_log.wait_for("gossiper_publish_local_state_pause: waiting for message", from_mark=paused_node_mark)
    logging.info("Found gossiper_publish_local_state_pause")
    # After the first node started and paused in start_gossiping, start the second node/
    task2 = asyncio.create_task(manager.server_start(node2.server_id))
    # Make sure the 2nd node received a `get_endpoint_states` request response.
    await node2_log.wait_for(f"Got get_endpoint_states response from {node1.ip_addr}", from_mark=reading_node_mark)
    # Unpause the 1st node.
    await manager.api.message_injection(node1.ip_addr, 'gossiper_publish_local_state_pause')
    await task1
    await task2
