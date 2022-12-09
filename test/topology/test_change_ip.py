#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test clusters can restart fine after an IP address change.
"""

from test.pylib.driver_util import reconnect_driver
from test.pylib.util import wait_for_cql_and_get_hosts
import asyncio
import logging
import time
import pytest

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_change_two(manager, random_tables):
    """Stop two nodes, change their IPs and start, check the cluster is
    funcitonal"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    s_1 = servers[1].server_id
    s_2 = servers[2].server_id
    logger.info("Gracefully stopping servers %s and %s to change ips", s_1, s_2)
    await manager.server_stop_gracefully(s_1)
    await manager.server_stop_gracefully(s_2)
    await manager.server_change_ip(s_1)
    await manager.server_change_ip(s_2)
    await manager.server_start(s_1)
    await manager.server_start(s_2)
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_change_all(manager, random_tables):
    """Stop all nodes in the cluster and restart with a different IP address"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    stops = []
    for server in servers:
        logger.info("Gracefully stopping server %s to change ip", server.server_id)
        stops.append(manager.server_stop_gracefully(server.server_id))
    await asyncio.gather(*stops)

    starts = []
    for server in servers:
        logger.info("Starting server %s", server.server_id)
        await manager.server_change_ip(server.server_id)
        starts.append(manager.server_start(server.server_id))
    await asyncio.gather(*starts)
    # Despite hacks in server_start() to refresh the connection list, if
    # we shut down the entire cluster, they don't work.
    cql = await reconnect_driver(manager)
    # The server list must be refreshed since the old list has old IPs
    servers = await manager.running_servers()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 600)
    await table.add_column()
    await random_tables.verify_schema()
