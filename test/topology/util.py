#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import logging
import functools
import pytest
import time
from cassandra.cluster import Session  # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host        # type: ignore # pylint: disable=no-name-in-module
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts, read_barrier


logger = logging.getLogger(__name__)


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


async def restart(manager: ManagerClient, server: ServerInfo) -> None:
    logging.info(f"Stopping {server} gracefully")
    await manager.server_stop_gracefully(server.server_id)
    logging.info(f"Restarting {server}")
    await manager.server_start(server.server_id)
    logging.info(f"{server} restarted")


async def wait_for_upgrade_state(state: str, cql: Session, host: Host, deadline: float) -> None:
    """Wait until group 0 upgrade state reaches `state` on `host`, using `cql` to query it.  Warning: if the
       upgrade procedure may progress beyond `state` this function may not notice when it entered `state` and
       then time out.  Use it only if either `state` is the last state or the conditions of the test don't allow
       the upgrade procedure to progress beyond `state` (e.g. a dead node causing the procedure to be stuck).
    """
    async def reached_state():
        rs = await cql.run_async("select value from system.scylla_local where key = 'group0_upgrade_state'", host=host)
        if rs:
            value = rs[0].value
            if value == state:
                return True
            else:
                logging.info(f"Upgrade not yet in state {state} on server {host}, state: {value}")
        else:
            logging.info(f"Upgrade not yet in state {state} on server {host}, no state was written")
        return None
    await wait_for(reached_state, deadline)


async def wait_until_upgrade_finishes(cql: Session, host: Host, deadline: float) -> None:
    await wait_for_upgrade_state('use_post_raft_procedures', cql, host, deadline)


async def enter_recovery_state(cql: Session, host: Host) -> None:
    await cql.run_async(
            "update system.scylla_local set value = 'recovery' where key = 'group0_upgrade_state'",
            host=host)


async def delete_raft_data(cql: Session, host: Host) -> None:
    await cql.run_async("truncate table system.discovery", host=host)
    await cql.run_async("truncate table system.group0_history", host=host)
    await cql.run_async("delete value from system.scylla_local where key = 'raft_group0_id'", host=host)


async def delete_upgrade_state(cql: Session, host: Host) -> None:
    await cql.run_async("delete from system.scylla_local where key = 'group0_upgrade_state'", host=host)


async def delete_raft_data_and_upgrade_state(cql: Session, host: Host) -> None:
    await delete_raft_data(cql, host)
    await delete_upgrade_state(cql, host)


def log_run_time(f):
    @functools.wraps(f)
    async def wrapped(*args, **kwargs):
        start = time.time()
        res = await f(*args, **kwargs)
        logging.info(f"{f.__name__} took {int(time.time() - start)} seconds.")
        return res
    return wrapped
