#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Tests the cluster feature functionality upgrade
"""
import logging
import asyncio
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_feature
from test.topology.util import reconnect_driver
from test.topology.util import TEST_FEATURE_NAME, change_support_for_test_feature_and_restart
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_joining_old_node_fails(manager: ManagerClient) -> None:
    """Upgrades the cluster to enable a new feature. Then, it first tries to
       add a new node without the feature, and then replace an existing node
       with a new node without the feature. The new node should fail to join
       the cluster in both cases
    """
    # Restart all servers at once and mark the test feature as supported
    servers = await manager.running_servers()
    await change_support_for_test_feature_and_restart(manager, servers, enable=True)

    # Workaround for scylladb/python-driver#230 - the driver might not
    # reconnect after all nodes are stopped at once.
    cql = await reconnect_driver(manager)

    # Wait until the feature is considered enabled by all nodes
    cql = manager.cql
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info("Waiting until %s is enabled on all nodes", TEST_FEATURE_NAME)
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 60)
                           for h in hosts))

    # Try to add a node that doesn't support the feature - should fail
    new_server_info = await manager.server_add(start=False)
    await manager.server_start(new_server_info.server_id, expected_error="Feature check failed")

    # Try to replace with a node that doesn't support the feature - should fail
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id=servers[0].server_id, reuse_ip_addr=False, use_host_id=False)
    new_server_info = await manager.server_add(start=False, replace_cfg=replace_cfg)
    await manager.server_start(new_server_info.server_id, expected_error="Feature check failed")
