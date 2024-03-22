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
from test.pylib.rest_client import inject_error_one_shot, InjectionHandler

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


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'aarch64/debug is unpredictably slow', platform_key='aarch64')
async def test_quorum_lost_during_node_join(manager: ManagerClient, raft_op_timeout: int) -> None:
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

    logger.info("starting a second node (the follower)")
    servers += [await manager.server_add()]

    logger.info(f"injecting join-node-before-add-entry into the leader node {servers[0]}")
    injection_handler = await inject_error_one_shot(manager.api, servers[0].ip_addr, 'join-node-before-add-entry')

    logger.info("starting a third node")
    third_node_future = asyncio.create_task(manager.server_add(
        seeds=[servers[0].ip_addr],
        expected_error="raft operation [add_entry] timed out, there is no raft quorum",
        timeout=60))

    logger.info(f"waiting for the leader node {servers[0]} to start handling the join request")
    log_file = await manager.server_open_log(servers[0].server_id)
    await log_file.wait_for("join-node-before-add-entry injection hit",
                            timeout=60)

    logger.info("stopping the second node")
    await manager.server_stop_gracefully(servers[1].server_id)

    logger.info("release join-node-before-add-entry injection")
    await injection_handler.message()

    logger.info("waiting for third node joining process to fail")
    await third_node_future


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'aarch64/debug is unpredictably slow', platform_key='aarch64')
async def test_quorum_lost_during_node_join_response_handler(manager: ManagerClient, raft_op_timeout: int) -> None:
    logger.info("starting a first node (the leader)")
    servers = [await manager.server_add()]

    logger.info("starting a second node (the follower)")
    servers += [await manager.server_add()]

    logger.info("adding a third node")
    servers += [await manager.server_add(config={
        'error_injections_at_startup': [
            {
                'name': 'group0-raft-op-timeout-in-ms',
                'value': raft_op_timeout
            },
            {
                'name': 'raft-group-registry-fd-threshold-in-ms',
                'value': '500'
            },
            {
                'name': 'join-node-response_handler-before-read-barrier'
            }
        ]
    }, start=False)]

    logger.info("starting a third node")
    third_node_future = asyncio.create_task(
        manager.server_start(servers[2].server_id,
                             expected_error="raft operation [read_barrier] timed out, there is no raft quorum",
                             timeout=60))

    logger.info(f"waiting for the third node {servers[2]} to hit join-node-response_handler-before-read-barrier")
    log_file = await manager.server_open_log(servers[2].server_id)
    await log_file.wait_for("join-node-response_handler-before-read-barrier injection hit", timeout=60)

    logger.info("stopping the second node")
    await manager.server_stop_gracefully(servers[1].server_id)

    logger.info("release join-node-response_handler-before-read-barrier injection")
    injection_handler = InjectionHandler(manager.api,
                                         'join-node-response_handler-before-read-barrier',
                                         servers[2].ip_addr)
    await injection_handler.message()

    logger.info("waiting for third node joining process to fail")
    await third_node_future


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'aarch64/debug is unpredictably slow', platform_key='aarch64')
async def test_cannot_run_operations(manager: ManagerClient, raft_op_timeout: int) -> None:
    logger.info("starting a first node (the leader)")
    servers = [await manager.server_add(config={
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
    })]

    logger.info("starting a second node (a follower)")
    servers += [await manager.server_add()]

    logger.info('create keyspace and table')
    await manager.get_cql().run_async("create keyspace ks "
                                      "with replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    await manager.get_cql().run_async('create table ks.test_table (pk int primary key)')

    logger.info("stopping the second node")
    await manager.server_stop_gracefully(servers[1].server_id)

    logger.info("attempting removenode for the second node")
    await manager.remove_node(servers[0].server_id, servers[1].server_id,
                              expected_error="raft operation [read_barrier] timed out, there is no raft quorum",
                              timeout=60)

    logger.info("attempting decommission_node for the first node")
    await manager.decommission_node(servers[0].server_id,
                                    expected_error="raft operation [read_barrier] timed out, there is no raft quorum",
                                    timeout=60)

    logger.info("attempting rebuild_node for the first node")
    await manager.rebuild_node(servers[0].server_id,
                               expected_error="raft operation [read_barrier] timed out, there is no raft quorum",
                               timeout=60)

    with pytest.raises(Exception, match="raft operation \[read_barrier\] timed out, "
                                        "there is no raft quorum, total voters count 2, alive voters count 1"):
        await manager.get_cql().run_async('drop table ks.test_table', timeout=60)

    logger.info("done")
