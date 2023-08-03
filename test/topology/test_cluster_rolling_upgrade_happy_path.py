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
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_feature, get_supported_features, \
                            get_enabled_features
from test.topology.util import TEST_FEATURE_NAME, change_support_for_test_feature_and_restart
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_rolling_upgrade_happy_path(manager: ManagerClient) -> None:
    """Simulates an upgrade of a cluster by doing a rolling restart
       and marking the test-only feature as supported on restarted nodes.
    """
    servers = await manager.running_servers()

    cql = manager.cql
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    for srv in servers:
        host = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60))[0]

        # The feature should not be advertised as supported until we enable it
        # via error injection.
        assert TEST_FEATURE_NAME not in await get_supported_features(cql, host)

        # Until all nodes are updated to support the test feature, that feature
        # should not be considered enabled by any node.
        for host in hosts:
            assert TEST_FEATURE_NAME not in await get_enabled_features(cql, host)

        await change_support_for_test_feature_and_restart(manager, [srv], enable=True)

        host = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60))[0]
        assert TEST_FEATURE_NAME in await get_supported_features(cql, host)

    # The feature should become enabled on all nodes soon.
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info("Waiting until %s is enabled on all nodes", TEST_FEATURE_NAME)
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 60)
                           for h in hosts))
