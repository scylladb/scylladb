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
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_feature
from test.topology.util import reconnect_driver
from test.topology.util import TEST_FEATURE_NAME, change_support_for_test_feature_and_restart
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_downgrade_after_successful_upgrade_fails(manager: ManagerClient) -> None:
    """Upgrades the cluster to enable the test feature. Then, shuts down all nodes,
       disables support for the feature, then restarts all nodes. All nodes
       should fail to start as none of them support the test feature.
    """

    # Restart all servers at once and mark the test feature as supported
    servers = await manager.running_servers()
    await change_support_for_test_feature_and_restart(manager, servers, enable=True)

    # Workaround for scylladb/python-driver#230 - the driver might not
    # reconnect after all nodes are stopped at once.
    cql = await reconnect_driver(manager)

    # Wait until the feature is considered enabled by all nodes
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info("Waiting until %s is enabled on all nodes", TEST_FEATURE_NAME)
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 60)
                           for h in hosts))

    # Stop all servers to disable the feature, then restart - all should fail
    await change_support_for_test_feature_and_restart(manager, servers, enable=False, \
            expected_error=f"Feature '{TEST_FEATURE_NAME}' was previously enabled in the cluster")
