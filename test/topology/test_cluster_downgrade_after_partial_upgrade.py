#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Tests the cluster feature functionality downgrade after partial upgrade.
"""
import logging
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts, get_supported_features, \
                            get_enabled_features
from test.topology.util import TEST_FEATURE_NAME, change_support_for_test_feature_and_restart
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_downgrade_after_partial_upgrade(manager: ManagerClient) -> None:
    """Simulates a partial upgrade of a cluster by enabling the test features
       in all nodes but one, then downgrading the upgraded nodes.
    """
    servers = await manager.running_servers()
    upgrading_servers = servers[1:] if len(servers) > 0 else []

    cql = manager.cql

    # Upgrade
    for srv in upgrading_servers:
        await change_support_for_test_feature_and_restart(manager, [srv], enable=True)
        host = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60))[0]
        assert TEST_FEATURE_NAME in await get_supported_features(cql, host)

    # There is one node that is not upgraded. The feature should not be enabled.
    for srv in servers:
        assert TEST_FEATURE_NAME not in await get_enabled_features(cql, host)

    # Downgrade, in reverse order
    for srv in upgrading_servers[::-1]:
        await change_support_for_test_feature_and_restart(manager, [srv], enable=False)
        host = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60))[0]
        assert TEST_FEATURE_NAME not in await get_supported_features(cql, host)
