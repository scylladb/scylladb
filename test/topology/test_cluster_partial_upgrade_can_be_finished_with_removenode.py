#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Tests the cluster feature functionality.
"""
import logging
import asyncio
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_feature, get_supported_features, get_enabled_features
from test.topology.util import reconnect_driver
from test.topology.util import TEST_FEATURE_NAME, change_support_for_test_feature_and_restart
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.skip(reason="issue #14194")
@pytest.mark.asyncio
async def test_partial_upgrade_can_be_finished_with_removenode(manager: ManagerClient) -> None:
    """Upgrades all but one node in the cluster to enable the test feature.
       Then, the last one is shut down and removed via `nodetool removenode`.
       After that, the test only feature should be enabled on all nodes.
    """
    # Restart all servers but the last one at once and mark the test feature as supported
    servers = await manager.running_servers()
    await change_support_for_test_feature_and_restart(manager, servers[:-1], enable=True)

    # All servers should see each other as alive
    for srv in servers:
        await manager.server_sees_others(srv.server_id, len(servers) - 1)

    # The feature should not be enabled yet on any node
    cql = manager.get_cql()
    for srv in servers:
        host = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60))[0]
        assert TEST_FEATURE_NAME not in await get_enabled_features(cql, host)

    # Remove the last node
    await manager.server_stop_gracefully(servers[-1].server_id)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[-1].ip_addr)
    await manager.remove_node(servers[0].server_id, servers[-1].server_id)

    # The feature should eventually become enabled
    hosts = await wait_for_cql_and_get_hosts(cql, servers[:-1], time.time() + 60)
    logging.info(f"Waiting until {TEST_FEATURE_NAME} is enabled on all nodes")
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 300) for h in hosts))
