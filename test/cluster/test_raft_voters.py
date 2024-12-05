#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import random
import pytest

from cassandra.policies import WhiteListRoundRobinPolicy

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.cluster.conftest import cluster_con, skip_mode
from test.cluster.util import get_coordinator_host_ids, get_current_group0_config


GROUP0_VOTERS_LIMIT = 5


async def get_number_of_voters(manager: ManagerClient, srv: ServerInfo):
    group0_members = await get_current_group0_config(manager, srv)
    return len([m for m in group0_members if m[1]])


@pytest.mark.asyncio
# Make sure the algorithm works with different number of nodes.
# Here with the "num_nodes == 1" we test that we'll only have one voter per DC, despite DC having two nodes
# (the DC1 must not have 2 voters otherwise losing it would result in the raft majority loss).
@pytest.mark.parametrize('num_nodes', [1, 3])
@pytest.mark.parametrize('stop_gracefully', [True, False])
async def test_raft_voters_multidc_kill_dc(manager: ManagerClient, num_nodes: int, stop_gracefully: bool):
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
    if stop_gracefully:
        await asyncio.gather(*(manager.server_stop_gracefully(srv.server_id) for srv in dc_servers[0]))
    else:
        await asyncio.gather(*(manager.server_stop(srv.server_id) for srv in dc_servers[0]))

    # Assert: Verify that the majority has not been lost (we can change the topology)

    logging.info('Executing read barrier')
    await read_barrier(manager.api, dc_servers[1][0].ip_addr)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_raft_limited_voters_upgrade(manager: ManagerClient):
    """
    Test that the limited voters feature works correctly during the upgrade.

    The scenario being tested here is that we first have a cluster that doesn't
    have the limited voters feature enabled, and then we enable it during the
    upgrade.

    We first upgrade all nodes except the coordinator, and then the coordinator
    itself. When the feature is not available on the coordinator, and would be
    enabled on the other nodes, the other nodes wouldn't become voters until the
    coordinator is upgraded as well. But that couldn't be done because the
    coordinator would be the last voter, thus the majority would be lost.

    Therefore we use the feature flags and only enable the feature after all
    nodes are upgraded (all nodes report that they support the feature).

    Arrange:
        - create a 7-node cluster with the limited voters feature disabled
          (more nodes than the voters limit of 5)
    Act:
        - upgrade all nodes in the random order (the topology coordinator at random place)
    Assert:
        - the feature has been enabled and the cluster works correctly
          (i.e. has enough voters to e.g. add a new server)
    """

    # Arrange: Create a 3-node cluster with the limited voters feature disabled

    cfg = {
        "error_injections_at_startup": [
            {
                "name": "suppress_features",
                "value": "GROUP0_LIMITED_VOTERS"
            },
        ]
    }
    servers = await manager.servers_add(7, config=cfg)

    # Check that all servers are voters (the feature is disabled)
    num_voters = await get_number_of_voters(manager, servers[0])
    assert num_voters == len(
        servers), f"The number of voters should be equal to the number of servers (but is {num_voters})"

    # Shuffle the servers for random order of servers and topology coordinator
    # (the topology coordinator being at the random place)
    random.shuffle(servers)

    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]

    # Act: Perform a rolling restart with the feature enabled to upgrade all the servers
    #      (topology coordinator at a random place).

    async def upgrade_server(srv):
        await manager.server_update_config(srv.server_id, "error_injections_at_startup", [])

    logging.info('Upgrading all nodes in this order: %s', [srv.server_id for srv in servers])
    await manager.rolling_restart(servers, with_down=upgrade_server)

    # Assert: Verify that the feature has been enabled and the majority has not been lost
    # (we can add another server to the cluster)

    logging.info('Waiting for the GROUP0_LIMITED_VOTERS feature to be enabled')
    await asyncio.gather(*(log.wait_for("Feature GROUP0_LIMITED_VOTERS is enabled", from_mark=mark, timeout=60)
                           for log, mark in zip(logs, marks)))

    logging.info('Adding a new server to the cluster')
    await manager.server_add()

    num_voters = await get_number_of_voters(manager, servers[0])
    assert num_voters == GROUP0_VOTERS_LIMIT, f"The number of voters should be limited to {GROUP0_VOTERS_LIMIT} (but is {num_voters})"


@pytest.mark.asyncio
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
