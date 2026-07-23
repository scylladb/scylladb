#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import random
import pytest

from cassandra.policies import WhiteListRoundRobinPolicy

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.cluster.conftest import cluster_con
from test.cluster.util import (get_coordinator_host_ids, get_current_group0_config,
                                ensure_group0_leader_on)


GROUP0_VOTERS_LIMIT = 5


async def get_number_of_voters(manager: ManagerClient, srv: ServerInfo):
    group0_members = await get_current_group0_config(manager, srv)
    return len([m for m in group0_members if m[1]])


# Make sure the algorithm works with different cluster sizes.
# (3, 1, 1) specifically checks that the largest DC doesn't get 2+ voters
# and keep half or more of all voters, which would make losing that DC unsafe.
@pytest.mark.parametrize('dc1_nodes,dc2_nodes,dc3_nodes', [
    pytest.param(3, 1, 1),
    pytest.param(6, 3, 3,
                 marks=pytest.mark.skip_mode(mode='debug', reason='larger topology case is too slow in debug on minipcs')),
])
@pytest.mark.parametrize('stop_gracefully', [True, False])
async def test_raft_voters_multidc_kill_dc(
        manager: ManagerClient, dc1_nodes: int, dc2_nodes: int, dc3_nodes: int, stop_gracefully: bool):
    """
    Test the basic functionality of limited voters in a multi-DC cluster.

    All DCs should now have the same number of voters so the majority shouldn't be lost when one DC out of 3 goes out,
    even if the DC is disproportionally larger than the other two.

    Arrange:
        - create 2 smaller DCs and one large DC
        - the large DC has significantly more nodes than each smaller DC
    Act:
        - kill all the nodes in the large DC
        - this would cause the loss of majority in the cluster without the limited voters feature
    Assert:
        - test the group0 didn't lose the majority by sending a read barrier request to one of the smaller DCs
        - this should work as the large DC should now have the same number of voters as each of the smaller DCs
          (so the majority shouldn't be lost in this scenario)
    """

    config = {
        'endpoint_snitch': 'GossipingPropertyFileSnitch',
    }
    dc_setup = [
        {
            'property_file': {'dc': 'dc1', 'rack': 'rack1'},
            'num_nodes': dc1_nodes,
        },
        {
            'property_file': {'dc': 'dc2', 'rack': 'rack2'},
            'num_nodes': dc2_nodes,
        },
        {
            'property_file': {'dc': 'dc3', 'rack': 'rack3'},
            'num_nodes': dc3_nodes,
        },
    ]

    # Arrange: create DCs / servers

    dc_servers = []
    for dc in dc_setup:
        logging.info(f"Creating {dc['property_file']['dc']} with {dc['num_nodes']} nodes")
        dc_servers.append(await manager.servers_add(dc['num_nodes'], config=config,
                                                    property_file=dc['property_file']))

    assert len(dc_servers) == len(dc_setup)

    logging.info('Creating connections to all DCs')
    dc_cqls = []
    for servers in dc_servers:
        dc_cqls.append(cluster_con([servers[0].ip_addr],
                                   load_balancing_policy=WhiteListRoundRobinPolicy([servers[0].ip_addr])).connect())

    assert len(dc_cqls) == len(dc_servers)

    # Act: Kill all nodes in dc1

    logging.info('Killing all nodes in dc1')
    if stop_gracefully:
        await asyncio.gather(*(manager.server_stop_gracefully(srv.server_id) for srv in dc_servers[0]))
    else:
        await asyncio.gather(*(manager.server_stop(srv.server_id, convict=False) for srv in dc_servers[0]))

    # Assert: Verify that the majority has not been lost (we can change the topology)

    logging.info('Executing read barrier')
    await read_barrier(manager.api, dc_servers[1][0].ip_addr)


async def test_raft_limited_voters_retain_coordinator(manager: ManagerClient):
    """
    Test that the topology coordinator is retained as a voter when possible.

    This test ensures that if a voter needs to be removed and the topology coordinator
    is among the candidates for removal, the coordinator is prioritized to remain a voter.

    Arrange:
        - Create 2 DCs with 3 nodes each.
        - Retrieve the current topology coordinator.
    Act:
        - Add a third DC with 1 node.
        - This will require removing one voter from either DC1 or DC2.
    Assert:
        - Verify that the topology coordinator is retained as a voter.
    """

    # Arrange: Create a 3-node cluster with the limited voters feature disabled

    dc_setup = [
        {
            'property_file': {'dc': 'dc1', 'rack': 'rack1'},
            'num_nodes': 3,
        },
        {
            'property_file': {'dc': 'dc2', 'rack': 'rack2'},
            'num_nodes': 3,
        },
    ]

    dc_servers = []
    for dc in dc_setup:
        logging.info(
            f"Creating {dc['property_file']['dc']} with {dc['num_nodes']} nodes")
        dc_servers.append(await manager.servers_add(dc['num_nodes'],
                                                    property_file=dc['property_file']))

    assert len(dc_servers) == len(dc_setup)

    coordinator_ids = await get_coordinator_host_ids(manager)
    assert coordinator_ids, "At least 1 coordinator id should be found"

    coordinator_id = coordinator_ids[0]

    # Act: Add a new DC with one node

    logging.info('Adding a new DC with one node')
    await manager.servers_add(1, property_file={'dc': 'dc3', 'rack': 'rack3'})

    # Assert: Verify that the topology coordinator is still the voter

    group0_members = await get_current_group0_config(manager, dc_servers[0][0])
    assert any(m[0] == coordinator_id and m[1] for m in group0_members), \
        f"The coordinator {coordinator_id} should be a voter (but is not)"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_raft_voters_stop_leader_keeps_quorum(manager: ManagerClient, build_mode: str):
    """
    Gracefully stopping the group0 leader must not leave the cluster without
    quorum (SCYLLADB-3026).

    On shutdown the leader announces STATUS=SHUTDOWN and drops out of its own
    get_live_members() while staying group0 coordinator. A voter refresh in that
    window runs update_nodes() with self absent, so calc_voters_max caps voters at
    1: a healthy node is demoted and the dead leader keeps its vote, leaving
    voters = {dead_leader, 1 alive} — permanent no-quorum.
    """

    # 3-node cluster. 1s tablet-load-stats refresh lands the periodic voter
    # refresh inside the short shutdown window (default 60s is too coarse).
    config = {'tablet_load_stats_refresh_interval_in_seconds': 1}
    servers = await manager.servers_add(3, config=config)
    assert len(servers) == 3

    # Pin the group0 leader so we know which node drives the refresh.
    leader = servers[0]
    await ensure_group0_leader_on(manager, leader)

    leader_host_id = await manager.get_host_id(leader.server_id)
    survivor = servers[1]
    logging.info(f"Leader: {leader} (host_id={leader_host_id}), survivor: {survivor}")

    # Pause the leader right after it announces STATUS=SHUTDOWN (self excluded from
    # its own live members) but before the topology coordinator fiber is aborted.
    injection = 'gossiper_pause_after_shutdown_announce'
    await manager.api.enable_injection(leader.ip_addr, injection, one_shot=True)

    leader_log = await manager.server_open_log(leader.server_id)
    leader_mark = await leader_log.mark()

    # Drain via REST (mirrors the field incident). Drain commits STATUS=SHUTDOWN
    # while the node is still alive, so self is excluded from get_live_members() in
    # the window. (A plain SIGTERM aborts gossip before the STATUS commits, so the
    # bug does not fire.) Drain blocks in the injection, so run it as a task.
    drain_task = asyncio.create_task(
        manager.api.client.post("/storage_service/drain", host=leader.ip_addr))

    await leader_log.wait_for(injection, from_mark=leader_mark)
    logging.info("Leader paused in the post-shutdown-announce window")

    # In-window the periodic refresher wakes the voter refresher; with the bug,
    # update_nodes() demotes a healthy node while the leader keeps its vote. Wait
    # for the demotion to commit before releasing (so the corrupt config is durable).
    # The fixed binary never demotes, so time out and proceed.
    try:
        await leader_log.wait_for("are now non-voters", from_mark=leader_mark, timeout=30)
        logging.info("Leader demoted a node during the shutdown window (bug reproduced)")
    except asyncio.TimeoutError:
        logging.info("No demotion observed in the shutdown window (expected on the fixed binary)")

    # Release the pause; drain stops gossip and the peers mark the leader dead.
    await manager.api.message_injection(leader.ip_addr, injection)
    await drain_task

    # Stop the process so the cluster sees the leader as permanently down.
    await manager.server_stop_gracefully(leader.server_id)

    # A read barrier on a survivor must succeed. With the bug the config is
    # {dead_leader (voter), survivor (voter), 1 non-voter} — 2 voters, 1 alive —
    # and the barrier fails with "there is no raft quorum, total voters count 2,
    # alive voters count 1". Short timeout so a quorum loss surfaces fast.
    barrier_timeout = 10 if build_mode == 'debug' else 5
    await read_barrier(manager.api, survivor.ip_addr, timeout=barrier_timeout)
