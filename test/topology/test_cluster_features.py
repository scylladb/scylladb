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


async def turn_on_test_feature_and_restart(manager: ManagerClient, srv: ServerInfo) -> None:
    logging.info(f"Reconfiguring and restarting node {srv} to support {TEST_FEATURE_NAME}")
    await manager.server_stop_gracefully(srv.server_id)
    injections = await get_error_injections_enabled_at_startup(manager, srv)
    injections.add(TEST_FEATURE_ENABLE_ERROR_INJECTION)
    await manager.server_update_config(srv.server_id, ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY, list(injections))
    await manager.server_start(srv.server_id)


async def turn_off_test_feature_and_restart(manager: ManagerClient, srv: ServerInfo, expected_error: Optional[str] = None) -> None:
    logging.info(f"Reconfiguring and restarting node {srv} to disable support of {TEST_FEATURE_NAME}")
    await manager.server_stop_gracefully(srv.server_id)
    injections = await get_error_injections_enabled_at_startup(manager, srv)
    injections.remove(TEST_FEATURE_ENABLE_ERROR_INJECTION)
    await manager.server_update_config(srv.server_id, ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY, list(injections))
    await manager.server_start(srv.server_id, expected_error)


@pytest.mark.asyncio
async def test_cluster_upgrade(manager: ManagerClient) -> None:
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

        await turn_on_test_feature_and_restart(manager, srv)
        cql = await reconnect_driver(manager)

        host = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60))[0]
        assert TEST_FEATURE_NAME in await get_supported_features(cql, host)

    # The feature should become enabled on all nodes soon.
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f"Waiting until {TEST_FEATURE_NAME} is enabled on all nodes")
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 60) for h in hosts))


@pytest.mark.asyncio
async def test_partial_cluster_upgrade_then_downgrade(manager: ManagerClient) -> None:
    """Simulates an partial of a cluster by enabling the test features in all
       but one nodes, then downgrading them.
    """
    servers = await manager.running_servers()
    upgrading_servers = servers[1:] if len(servers) > 0 else []

    cql = manager.cql

    # Upgrade
    for srv in upgrading_servers:
        await turn_on_test_feature_and_restart(manager, srv)
        cql = await reconnect_driver(manager)
        host = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60))[0]
        assert TEST_FEATURE_NAME in await get_supported_features(cql, host)

    # There is one node that is not upgraded. The feature should not be enabled.
    for srv in servers:
        cql = await reconnect_driver(manager)
        host = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60))[0]
        assert TEST_FEATURE_NAME not in await get_enabled_features(cql, host)

    # Downgrade, in reverse order
    for srv in upgrading_servers[::-1]:
        await turn_off_test_feature_and_restart(manager, srv)
        cql = await reconnect_driver(manager)
        host = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60))[0]
        assert TEST_FEATURE_NAME not in await get_supported_features(cql, host)


@pytest.mark.asyncio
async def test_reject_joining_node_without_support_for_enabled_features(manager: ManagerClient) -> None:
    """Upgrades the cluster to enable a new feature, then tries to add
       another node - which should fail because it doesn't support one
       of the enabled features.
    """
    servers = await manager.running_servers()
    await asyncio.gather(*(turn_on_test_feature_and_restart(manager, srv) for srv in servers))
    cql = await reconnect_driver(manager)

    cql = manager.cql
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f"Waiting until {TEST_FEATURE_NAME} is enabled on all nodes")
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 60) for h in hosts))

    new_server_info = await manager.server_add(start=False)
    await manager.server_start(new_server_info.server_id, expected_error="Feature check failed")


@pytest.mark.asyncio
async def test_reject_replacing_node_without_support_for_enabled_features(manager: ManagerClient) -> None:
    """Upgrades the cluster to enable a new feature, then tries to replace
       an existing node with another node - which should fail because
       it doesn't support one of the enabled features.
    """
    servers = await manager.running_servers()
    await asyncio.gather(*(turn_on_test_feature_and_restart(manager, srv) for srv in servers))
    cql = await reconnect_driver(manager)

    cql = manager.cql
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f"Waiting until {TEST_FEATURE_NAME} is enabled on all nodes")
    await asyncio.gather(*(wait_for_feature(TEST_FEATURE_NAME, cql, h, time.time() + 60) for h in hosts))

    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id=servers[0].server_id, reuse_ip_addr=False, use_host_id=False)
    new_server_info = await manager.server_add(start=False, replace_cfg=replace_cfg)
    await manager.server_start(new_server_info.server_id, expected_error="Feature check failed")
