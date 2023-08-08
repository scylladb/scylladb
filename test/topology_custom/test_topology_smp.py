#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test functionality on the cluster with different values of the --smp parameter on the nodes.
"""
import logging
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import unique_name
from test.topology.util import wait_for_token_ring_and_group0_consistency
import pytest
from pytest import FixtureRequest

logger = logging.getLogger(__name__)


# Checks basic functionality on the cluster with different values of the --smp parameter on the nodes.
@pytest.mark.asyncio
async def test_nodes_with_different_smp(request: FixtureRequest, manager: ManagerClient) -> None:
    # In this test it's more convenient to start with a fresh cluster.

    # When the node starts it tries to communicate with others
    # by sending group0_peer_exchange message to them.
    # This message can be handled on arbitrary shard of the target node.
    # The method manager.server_add waits for node to start, which can only happen
    # if this message has been handled correctly.
    #
    # Note: messaging_service is initialized with server_socket::load_balancing_algorithm::port
    # policy, this means that the shard for message will be chosen as client_port % smp::count.
    # The client port in turn is chosen as rand() * smp::count + current_shard
    # (posix_socket_impl::find_port_and_connect).
    # If this succeeds to occupy a free port in 5 tries and smp::count is the same
    # on both nodes, then it's guaranteed that the message will be
    # processed on the same shard as the calling code.
    # In the general case, we cannot assume that this same shard guarantee holds.
    logger.info(f'Adding --smp=3 server')
    await manager.server_add(cmdline=['--smp', '3'])
    logger.info(f'Adding --smp=4 server')
    await manager.server_add(cmdline=['--smp', '4'])
    logger.info(f'Adding --smp=5 server')
    await manager.server_add(cmdline=['--smp', '5'])

    await wait_for_token_ring_and_group0_consistency(manager)

    logger.info(f'Creating new tables')
    tables = RandomTables(request.node.name, manager, unique_name(), 3)
    await tables.add_tables(ntables=4, ncolumns=5)
    await tables.verify_schema()
