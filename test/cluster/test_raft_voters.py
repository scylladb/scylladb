#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import pytest

from cassandra.policies import WhiteListRoundRobinPolicy

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.cluster.conftest import cluster_con


@pytest.mark.asyncio
@pytest.mark.skip(reason='issue #18793')
# Make sure the algorithm works with different number of nodes.
# Here with the "num_nodes == 1" we test that we'll only have one voter per DC, despite DC having two nodes
# (the DC1 must not have 2 voters otherwise losing it would result in the raft majority loss).
@pytest.mark.parametrize('num_nodes', [1, 3])
async def test_raft_voters_multidc_kill_dc(manager: ManagerClient, num_nodes: int):
    """
    Test the basic functionality of limited voters in a multi-DC cluster.

    All DCs should now have the same number of voters so the majority shouldn't be lost when one DC out of 3 goes out,
    even if the DC is disproportionally larger than the other two.

    Arrange:
        - create 2 smaller DCs and one large DC
        - the sum of nodes in the smaller DCs is equal to the number of nodes of the large DC
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
            # The large DC has 2x the number of nodes of the smaller DCs
            'num_nodes': 2 * num_nodes,
        },
        {
            'property_file': {'dc': 'dc2', 'rack': 'rack2'},
            'num_nodes': num_nodes,
        },
        {
            'property_file': {'dc': 'dc3', 'rack': 'rack3'},
            'num_nodes': num_nodes,
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
        dc_cqls.append(cluster_con([servers[0].ip_addr], 9042, False,
                                   load_balancing_policy=WhiteListRoundRobinPolicy([servers[0].ip_addr])).connect())

    assert len(dc_cqls) == len(dc_servers)

    # Act: Kill all nodes in dc1

    logging.info('Killing all nodes in dc1')
    await asyncio.gather(*(manager.server_stop_gracefully(srv.server_id) for srv in dc_servers[0]))

    # Assert: Verify that the majority has not been lost (we can change the topology)

    logging.info('Executing read barrier')
    await read_barrier(manager.api, dc_servers[1][0].ip_addr)
