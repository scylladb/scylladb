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
from test.cluster.util import get_coordinator_host_ids, get_current_group0_config


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
