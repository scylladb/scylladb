#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import logging

import pytest
import asyncio
from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode

logger = logging.getLogger(__name__)


@pytest.fixture
def raft_op_timeout(mode):
    return 5000 if mode == 'debug' else 1000


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'aarch64/debug is unpredictably slow', platform_key='aarch64')
async def test_cannot_add_new_node(manager: ManagerClient, raft_op_timeout: int) -> None:
    # This test makes sure that trying to add a new node fails with timeout
    # if the majority of the cluster is not available.
    # To exercise this, we start with a cluster of four nodes. This setup lets us check two situations:
    # one where the new node's join request goes to the leader of the cluster, and
    # another where it goes to a non-leader. Initially, the first node we start
    # becomes the leader. Then, we shut down the last two nodes.
    # This means the new node's request could be handled by either of the
    # first two nodes, depending on which one responds first to the discovery request
    # in persistent_discovery::run.
    # In the second case we rely on a leader in Raft to steps down
    # if an election timeout elapses without a successful round of heartbeats to a majority
    # of its cluster (fsm::tick_leader).
    # This is important since execute_read_barrier_on_leader doesn't take the abort_source,
    # it just returns 'not_a_leader' and read_barrier rechecks the abort_source in the
    # loop inside do_on_leader_with_retries.

    config = {
        'error_injections_at_startup': [
            {
                'name': 'group0-raft-op-timeout-in-ms',
                'value': raft_op_timeout
            },
            {
                'name': 'raft-group-registry-fd-threshold-in-ms',
                'value': '500'
            }
        ]
    }
    logger.info("starting a first node (the leader)")
    servers = [await manager.server_add(config=config)]

    logger.info("starting a second node (a follower)")
    servers += [await manager.server_add(config=config)]

    logger.info("starting other two nodes")
    servers += await manager.servers_add(servers_num=2)

    logger.info("stopping the last two nodes")
    await asyncio.gather(manager.server_stop_gracefully(servers[2].server_id),
                         manager.server_stop_gracefully(servers[3].server_id))

    logger.info("starting a fifth node with no quorum")
    await manager.server_add(expected_error="raft operation [read_barrier] timed out, there is no raft quorum",
                             timeout=60)

    logger.info("done")
