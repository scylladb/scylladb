#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import asyncio
import logging
import pytest
import time
from typing import Set, Optional, List
from cassandra.cluster import Session  # type: ignore # pylint: disable=no-name-in-module
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts, read_barrier


logger = logging.getLogger(__name__)


TEST_FEATURE_NAME = "TEST_ONLY_FEATURE"
TEST_FEATURE_ENABLE_ERROR_INJECTION = "features_enable_test_feature"
ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY = "error_injections_at_startup"


async def reconnect_driver(manager: ManagerClient) -> Session:
    """Workaround for scylladb/python-driver#170 and scylladb/python-driver#230:
       the existing driver session may not reconnect, create a new one.
    """
    logging.info(f"Reconnecting driver")
    manager.driver_close()
    await manager.driver_connect()
    cql = manager.cql
    assert(cql)
    return cql


async def get_token_ring_host_ids(manager: ManagerClient, srv: ServerInfo) -> set[str]:
    """Get the host IDs of normal token owners known by `srv`."""
    token_endpoint_map = await manager.api.client.get_json("/storage_service/tokens_endpoint", srv.ip_addr)
    normal_endpoints = {e["value"] for e in token_endpoint_map}
    logger.info(f"Normal endpoints' IPs by {srv}: {normal_endpoints}")
    host_id_map = await manager.api.client.get_json('/storage_service/host_id', srv.ip_addr)
    all_host_ids = {e["value"] for e in host_id_map}
    logger.info(f"All host IDs by {srv}: {all_host_ids}")
    normal_host_ids = {e["value"] for e in host_id_map if e["key"] in normal_endpoints}
    logger.info(f"Normal endpoints' host IDs by {srv}: {normal_host_ids}")
    return normal_host_ids


async def get_current_group0_config(manager: ManagerClient, srv: ServerInfo) -> set[tuple[str, bool]]:
    """Get the current Raft group 0 configuration known by `srv`.
       The first element of each tuple is the Raft ID of the node (which is equal to the Host ID),
       the second element indicates whether the node is a voter.
     """
    assert manager.cql
    host = (await wait_for_cql_and_get_hosts(manager.cql, [srv], time.time() + 60))[0]
    await read_barrier(manager.cql, host)
    group0_id = (await manager.cql.run_async(
        "select value from system.scylla_local where key = 'raft_group0_id'",
        host=host))[0].value
    config = await manager.cql.run_async(
        f"select server_id, can_vote from system.raft_state where group_id = {group0_id} and disposition = 'CURRENT'",
        host=host)
    result = {(str(m.server_id), bool(m.can_vote)) for m in config}
    logger.info(f"Group 0 members by {srv}: {result}")
    return result


async def check_token_ring_and_group0_consistency(manager: ManagerClient) -> None:
    """Ensure that the normal token owners and group 0 members match
       according to each currently running server.
    """
    servers = await manager.running_servers()
    for srv in servers:
        group0_members = await get_current_group0_config(manager, srv)
        group0_ids = {m[0] for m in group0_members}
        token_ring_ids = await get_token_ring_host_ids(manager, srv)
        assert token_ring_ids == group0_ids


async def wait_for_token_ring_and_group0_consistency(manager: ManagerClient, deadline: float) -> None:
    """Weaker version of the above check; the token ring is not immediately updated after
    bootstrap/replace/decommission - the normal tokens of the new node propagate through gossip.
    Take this into account and wait for the equality condition to hold, with a timeout.
    """
    servers = await manager.running_servers()
    for srv in servers:
        group0_members = await get_current_group0_config(manager, srv)
        group0_ids = {m[0] for m in group0_members}
        async def token_ring_matches():
            token_ring_ids = await get_token_ring_host_ids(manager, srv)
            diff = token_ring_ids ^ group0_ids
            if diff:
                logger.warning(f"Group 0 members and token ring members don't yet match" \
                               f" according to {srv}, symmetric difference: {diff}")
                return None
            return True
        await wait_for(token_ring_matches, deadline, period=.5)


async def get_error_injections_enabled_at_startup(manager: ManagerClient, srv: ServerInfo) -> Set[str]:
    # TODO: An "error injection enabled at startup" might not be a string, but a dictionary
    #       with some options. The tests in this module only use strings, but it's worth
    #       keeping that in mind here in case dicts start being used.
    config = await manager.server_get_config(srv.server_id)
    injections = config.get(ERROR_INJECTIONS_AT_STARTUP_CONFIG_KEY) or []
    assert isinstance(injections, list)
    assert all(isinstance(inj, str) for inj in injections)
    return set(injections)


async def change_support_for_test_feature_and_restart(manager: ManagerClient, srvs: List[ServerInfo], enable: bool, expected_error: Optional[str] = None) -> None:
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

    await asyncio.gather(*(manager.server_stop_gracefully(srv.server_id) for srv in srvs))
    await asyncio.gather(*(adjust_feature_in_config(manager, srv, enable) for srv in srvs))
    await asyncio.gather(*(manager.server_start(srv.server_id, expected_error) for srv in srvs))

