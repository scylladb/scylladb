#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

"""
Test clusters can restart fine after an RPC IP address change.
"""

from __future__ import annotations

import time
import asyncio
import logging
from typing import TYPE_CHECKING

import pytest

from test.topology.util import wait_for_token_ring_and_group0_consistency

if TYPE_CHECKING:
    from test.pylib.random_tables import RandomTables
    from test.pylib.internal_types import ServerNum
    from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)


@pytest.fixture
async def two_nodes_cluster(manager: ManagerClient) -> list[ServerNum]:
    logger.info(f"Booting initial 2-nodes cluster")
    servers = [srv.server_id for srv in await manager.servers_add(2)]
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    return servers


@pytest.mark.asyncio
async def test_change_rpc_address(two_nodes_cluster: list[ServerNum],
                                  manager: ManagerClient,
                                  random_tables: RandomTables) -> None:
    """Sequentially stop two nodes, change their RPC IPs and start, check the cluster is functional."""

    table = await random_tables.add_table(ncolumns=5)

    # Scenario 1
    # ----------
    logger.info("Change RPC IP addresses sequentially")
    for server_id in two_nodes_cluster:
        logger.info("Change RPC IP address for server %s", server_id)

        # There is an issue with Python driver (probably related to scylladb/python-driver#170 and/or
        # scylladb/python-driver#230) which can cause reconnect failure after stopping one node.
        # As a workaround, close the connection and reconnect after starting the server.
        manager.driver_close()

        await manager.server_stop_gracefully(server_id)
        await manager.server_change_rpc_address(server_id)
        await manager.server_start(server_id)

        # Connect the Python driver back with updated IP address.
        await manager.driver_connect()

        await table.add_column()
        await random_tables.verify_schema()

    await wait_for_token_ring_and_group0_consistency(manager=manager, deadline=time.time() + 30)

    # Scenario 2
    # ----------
    logger.info("Change RPC IP addresses for both servers simultaneously")

    manager.driver_close()

    await asyncio.gather(*[manager.server_stop_gracefully(server_id) for server_id in two_nodes_cluster])
    await asyncio.gather(*[manager.server_change_rpc_address(server_id) for server_id in two_nodes_cluster])
    for server_id in two_nodes_cluster:
        await manager.server_start(server_id)

    # Connect the Python driver back with updated IP addresses.
    await manager.driver_connect()

    await table.add_column()
    await random_tables.verify_schema()

    await wait_for_token_ring_and_group0_consistency(manager=manager, deadline=time.time() + 30)
