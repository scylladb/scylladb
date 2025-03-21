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
from test.cluster.conftest import cluster_con, skip_mode
from test.cluster.util import get_coordinator_host


@pytest.mark.asyncio
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
        - create a 3-node cluster with the limited voters feature disabled
    Act:
        - upgrade all nodes except the coordinator, with the feature enabled
        - upgrade the leader node
    Assert:
        - the feature has been enabled and the cluster works correctly
          (i.e. has enough voters to execute e.g. a read barrier)
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
    servers = await manager.servers_add(3, config=cfg)

    async def upgrade_server(srv):
        await manager.server_stop_gracefully(srv.server_id)
        await asyncio.gather(*(manager.server_not_sees_other_server(s.ip_addr, srv.ip_addr)
                               for s in servers if s.server_id != srv.server_id))
        await manager.server_update_config(srv.server_id, "error_injections_at_startup", [])
        await manager.server_start(srv.server_id)
        await asyncio.gather(*(manager.server_sees_other_server(s.ip_addr, srv.ip_addr)
                               for s in servers if s.server_id != srv.server_id))

    # Act: Upgrade all nodes except the coordinator, with the feature enabled

    logging.info('Upgrading all nodes except the coordinator')
    coordinator_host = await get_coordinator_host(manager)
    for srv in servers:
        if srv.server_id != coordinator_host.server_id:
            await upgrade_server(srv)

    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]

    # Act: Upgrade the coordinator node

    logging.info('Upgrading the leader node')
    await upgrade_server(coordinator_host)

    # Assert: Verify that the feature has been enabled and the majority has not been lost
    # (we can execute a read barrier)

    logging.info('Waiting for the GROUP0_LIMITED_VOTERS feature to be enabled')
    await asyncio.gather(*(log.wait_for("Feature GROUP0_LIMITED_VOTERS is enabled", mark, timeout=60)
                           for log, mark in zip(logs, marks)))

    logging.info('Executing read barrier')
    await read_barrier(manager.api, servers[0].ip_addr)
