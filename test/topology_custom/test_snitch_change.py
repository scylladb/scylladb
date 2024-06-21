#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import logging
import asyncio
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_snitch_change(manager: ManagerClient) -> None:
    """ The test changes snitch from simple to GossipingPropertyFileSnitch one and checks
        that DC and rack names change accordingly"""
    s1 = await manager.server_add(property_file = {'dc': 'DC1', 'rack' : 'R1'})
    s2 = await manager.server_add(property_file = {'dc': 'DC1', 'rack' : 'R1'})
    cql = manager.get_cql()
    res = await cql.run_async(f"SELECT data_center,rack From system.peers")
    assert res[0].data_center == "datacenter1" and res[0].rack == "rack1"
    await manager.server_update_config(s1.server_id, 'endpoint_snitch', 'GossipingPropertyFileSnitch')
    await manager.server_update_config(s2.server_id, 'endpoint_snitch', 'GossipingPropertyFileSnitch')
    await manager.server_stop_gracefully(s1.server_id)
    await manager.server_stop_gracefully(s2.server_id)
    tasks = [asyncio.create_task(manager.server_start(s1.server_id)), asyncio.create_task(manager.server_start(s2.server_id))]
    await asyncio.gather(*tasks)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, await manager.running_servers(), time.time() + 60)
    res = await cql.run_async(f"SELECT data_center,rack From system.peers")
    assert res[0].data_center == "DC1" and res[0].rack == "R1"
