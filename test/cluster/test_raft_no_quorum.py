#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging

import pytest
import asyncio
from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode
from test.pylib.rest_client import inject_error_one_shot, InjectionHandler, read_barrier
from test.cluster.util import create_new_test_keyspace

logger = logging.getLogger(__name__)


@pytest.fixture(name="raft_op_timeout")  # avoid the W0621:redefined-outer-name pylint warning
def fixture_raft_op_timeout(build_mode):
    return 10000 if build_mode == 'debug' else 1000


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'aarch64/debug is unpredictably slow', platform_key='aarch64')
async def test_cannot_add_new_node(manager: ManagerClient, raft_op_timeout: int) -> None:
    # This test makes sure that trying to add a new node fails with timeout
    # if the majority of the cluster is not available.
    # To exercise this, we start with a cluster of five nodes. This setup lets us check two situations:
    # one where the new node's join request goes to the leader of the cluster, and
    # another where it goes to a non-leader. Initially, the first node we start
    # becomes the leader. Then, we shut down the last three nodes.
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
        'direct_failure_detector_ping_timeout_in_ms': 300,
        'group0_raft_op_timeout_in_ms': raft_op_timeout,
        'error_injections_at_startup': [
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

    logger.info("starting other three nodes")
    servers += await manager.servers_add(servers_num=3)

    logger.info("stopping the last three nodes")
    await asyncio.gather(manager.server_stop_gracefully(servers[2].server_id),
                         manager.server_stop_gracefully(servers[3].server_id),
                         manager.server_stop_gracefully(servers[4].server_id))

    logger.info("starting a sixth node with no quorum")
    await manager.server_add(expected_error="raft operation [read_barrier] timed out, there is no raft quorum",
                             timeout=60)

    logger.info("done")


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'aarch64/debug is unpredictably slow', platform_key='aarch64')
async def test_quorum_lost_during_node_join(manager: ManagerClient, raft_op_timeout: int) -> None:
    config = {
        'group0_raft_op_timeout_in_ms': raft_op_timeout,
        'error_injections_at_startup': [
            {
                'name': 'raft-group-registry-fd-threshold-in-ms',
                'value': '500'
            }
        ]
    }
    logger.info("starting a first node (the leader)")
    servers = [await manager.server_add(config=config)]

    logger.info("starting second and third nodes (followers)")
    servers += await manager.servers_add(servers_num=2)

    logger.info(f"injecting join-node-before-add-entry into the leader node {servers[0]}")
    injection_handler = await inject_error_one_shot(manager.api, servers[0].ip_addr, 'join-node-before-add-entry')

    logger.info("starting a fourth node")
    fourth_node_future = asyncio.create_task(manager.server_add(
        seeds=[servers[0].ip_addr],
        expected_error="raft operation [add_entry] timed out, there is no raft quorum",
        timeout=60))

    logger.info(f"waiting for the leader node {servers[0]} to start handling the join request")
    log_file = await manager.server_open_log(servers[0].server_id)
    await log_file.wait_for("join-node-before-add-entry: waiting", timeout=60)

    logger.info("stopping the second and third nodes")
    await asyncio.gather(manager.server_stop_gracefully(servers[1].server_id),
                         manager.server_stop_gracefully(servers[2].server_id))

    logger.info("release join-node-before-add-entry injection")
    await injection_handler.message()

    logger.info("waiting for fourth node joining process to fail")
    await fourth_node_future


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'aarch64/debug is unpredictably slow', platform_key='aarch64')
async def test_quorum_lost_during_node_join_response_handler(manager: ManagerClient, raft_op_timeout: int) -> None:
    logger.info("starting a first node (the leader)")
    servers = [await manager.server_add()]

    logger.info("starting second and third nodes (followers)")
    servers += await manager.servers_add(servers_num=2)

    logger.info("adding a fourth node")
    servers += [await manager.server_add(config={
        'group0_raft_op_timeout_in_ms': raft_op_timeout,
        'error_injections_at_startup': [
            {
                'name': 'raft-group-registry-fd-threshold-in-ms',
                'value': '500'
            },
            {
                'name': 'join-node-response_handler-before-read-barrier'
            }
        ]
    }, start=False)]

    logger.info("starting a fourth node")
    fourth_node_future = asyncio.create_task(
        manager.server_start(servers[3].server_id,
                             expected_error="raft operation [read_barrier] timed out, there is no raft quorum",
                             timeout=60))

    logger.info(
        f"waiting for the fourth node {servers[3]} to hit join-node-response_handler-before-read-barrier")
    log_file = await manager.server_open_log(servers[3].server_id)
    await log_file.wait_for("join-node-response_handler-before-read-barrier: waiting", timeout=60)

    logger.info("stopping the second and third nodes")
    await asyncio.gather(manager.server_stop_gracefully(servers[1].server_id),
                         manager.server_stop_gracefully(servers[2].server_id))

    logger.info("release join-node-response_handler-before-read-barrier injection")
    injection_handler = InjectionHandler(manager.api,
                                         'join-node-response_handler-before-read-barrier',
                                         servers[3].ip_addr)
    await injection_handler.message()

    logger.info("waiting for fourth node joining process to fail")
    await fourth_node_future


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'aarch64/debug is unpredictably slow', platform_key='aarch64')
async def test_cannot_run_operations(manager: ManagerClient, raft_op_timeout: int) -> None:
    logger.info("starting a first node (the leader)")
    servers = [await manager.server_add(config={
        'group0_raft_op_timeout_in_ms': raft_op_timeout,
        'error_injections_at_startup': [
            {
                'name': 'raft-group-registry-fd-threshold-in-ms',
                'value': '500'
            }
        ]
    })]

    logger.info("starting second and third nodes (followers)")
    servers += await manager.servers_add(servers_num=2)

    logger.info('create keyspace and table')
    ks = await create_new_test_keyspace(manager.get_cql(), "with replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    await manager.get_cql().run_async(f'create table {ks}.test_table (pk int primary key)')

    logger.info("stopping the second and third nodes")
    await asyncio.gather(manager.server_stop_gracefully(servers[1].server_id),
                         manager.server_stop_gracefully(servers[2].server_id))

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

    with pytest.raises(Exception, match="raft operation \\[read_barrier\\] timed out, "
                                        "there is no raft quorum, total voters count 3, alive voters count 1"):
        await manager.get_cql().run_async(f'drop table {ks}.test_table', timeout=60)

    logger.info("done")


@pytest.mark.asyncio
@skip_mode('release', 'dev mode is sufficient for this test')
@skip_mode('debug', 'dev mode is sufficient for this test')
async def test_can_restart(manager: ManagerClient, raft_op_timeout: int) -> None:
    """
    Test that restarts work without group 0 quorum. Stop all five nodes and restart them one by one.

    The purpose of this test is to catch regressions that introduce a group 0 quorum requirement on the restart path.
    This could happen if we added code that, for example, adds a group 0 command or executes a group 0 read barrier.

    Note that a restarting node can add a group 0 command if it's upgrading (see e.g.
    system_distributed_keyspace::create_tables that can add a new table). However, we can safely assume that nodes never
    upgrade in quorum loss scenarios.
    """
    logger.info("Adding servers")
    servers = await manager.servers_add(5)

    logger.info(f"Stopping {servers}")
    await asyncio.gather(*(manager.server_stop(srv.server_id) for srv in servers))

    # This ensures the read barriers below fail quickly without group 0 quorum.
    logger.info(f"Decreasing group0_raft_op_timeout_in_ms on {servers}")
    await asyncio.gather(*(manager.server_update_config(srv.server_id, 'group0_raft_op_timeout_in_ms', raft_op_timeout)
                           for srv in servers))

    logger.info(f"Restarting {servers[:2]} with no group 0 quorum")
    for idx, srv in enumerate(servers[:2]):
        await manager.server_start(srv.server_id)
        with pytest.raises(Exception, match="raft operation \\[read_barrier\\] timed out, "
                                            "there is no raft quorum, total voters count 5, "
                                            f"alive voters count {idx + 1}"):
            await read_barrier(manager.api, srv.ip_addr)

    # Increase the timeout back to 300s to ensure the new group 0 leader is elected before the first read barrier below
    # times out.
    await asyncio.gather(*(manager.server_update_config(srv.server_id, 'group0_raft_op_timeout_in_ms', 300000)
                           for srv in servers))

    logger.info(f"Restarting {servers[2:]} with group 0 quorum")
    for srv in servers[2:]:
        await manager.server_start(srv.server_id)
        await read_barrier(manager.api, srv.ip_addr)
