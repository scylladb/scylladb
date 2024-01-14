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
from typing import Set, Optional, List

from cassandra.cluster import Session  # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host        # type: ignore # pylint: disable=no-name-in-module

from test.pylib.manager_client import ManagerClient, ServerInfo
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_feature, get_supported_features, get_enabled_features
from test.topology.util import reconnect_driver
import pytest


logger = logging.getLogger(__name__)


TEST_FEATURE_NAME = "TEST_ONLY_FEATURE"
TEST_FEATURE_ENABLE_ERROR_INJECTION = "features_enable_test_feature"
ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY = "error_injections_at_startup"


async def get_error_injections_enabled_at_startup(manager: ManagerClient, srv: ServerInfo) -> Set[str]:
    # TODO: An "error injection enabled at startup" might not be a string, but a dictionary
    #       with some options. The tests in this module only use strings, but it's worth
    #       keeping that in mind here in case dicts start being used.
    config = await manager.server_get_config(srv.server_id)
    injections = config.get(ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY) or []
    assert isinstance(injections, list)
    assert all(isinstance(inj, str) for inj in injections)
    return set(injections)


async def change_support_for_test_feature_and_restart(manager: ManagerClient, srvs: [ServerInfo], enable: bool, expected_error: Optional[str] = None) -> None:
    """Stops all provided nodes, changes their support for test-only feature
       and restarts them.
    """
    logging.info(f"Reconfiguring and restarting nodes {srvs} to {'enable' if enable else 'disable'} support for {TEST_FEATURE_NAME}")

    async def adjust_feature_in_config(manager: ManagerClient, srv: ServerInfo, enable: bool):
        injections = await get_error_injections_enabled_at_startup(manager, srv)
        if enable:
            injections.add(TEST_FEATURE_ENABLE_ERROR_INJECTION)
        else:
            injections.remove(TEST_FEATURE_ENABLE_ERROR_INJECTION)
        await manager.server_update_config(srv.server_id, ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY, list(injections))

    await asyncio.gather(*(manager.server_stop(srv.server_id) for srv in srvs))
    await asyncio.gather(*(adjust_feature_in_config(manager, srv, enable) for srv in srvs))
    await asyncio.gather(*(manager.server_start(srv.server_id, expected_error) for srv in srvs))


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
    logging.info(f"Waiting until {TEST_FEATURE_NAME} is enabled on all nodes")
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 60) for h in hosts))


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

    await manager.servers_see_each_other(servers)

    # Workaround for scylladb/python-driver#230 - the driver might not
    # reconnect after all nodes are stopped at once.
    cql = await reconnect_driver(manager)

    # Wait until the feature is considered enabled by all nodes
    cql = manager.cql
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f"Waiting until {TEST_FEATURE_NAME} is enabled on all nodes")
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 60) for h in hosts))

    # Try to add a node that doesn't support the feature - should fail
    new_server_info = await manager.server_add(start=False)
    await manager.server_start(new_server_info.server_id, expected_error="Feature check failed")

    # Try to replace with a node that doesn't support the feature - should fail
    await manager.server_stop_gracefully(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id=servers[0].server_id, reuse_ip_addr=False, use_host_id=False)
    new_server_info = await manager.server_add(start=False, replace_cfg=replace_cfg)
    await manager.server_start(new_server_info.server_id, expected_error="Feature check failed")


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
    logging.info(f"Waiting until {TEST_FEATURE_NAME} is enabled on all nodes")
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 60) for h in hosts))

    # Stop all servers to disable the feature, then restart - all should fail
    await change_support_for_test_feature_and_restart(manager, servers, enable=False, \
            expected_error=f"Feature '{TEST_FEATURE_NAME}' was previously enabled in the cluster")


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
    await manager.server_stop(servers[-1].server_id)
    await manager.remove_node(servers[0].server_id, servers[-1].server_id)

    # The feature should eventually become enabled
    hosts = await wait_for_cql_and_get_hosts(cql, servers[:-1], time.time() + 60)
    logging.info(f"Waiting until {TEST_FEATURE_NAME} is enabled on all nodes")
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 300) for h in hosts))
