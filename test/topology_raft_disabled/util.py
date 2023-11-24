#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Utilities for Raft upgrade tests
"""
import logging
import functools
import time
from typing import Optional
from cassandra.cluster import NoHostAvailable, Session  # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host                         # type: ignore # pylint: disable=no-name-in-module
from test.pylib.rest_client import ScyllaRESTAPIClient
from test.pylib.manager_client import ManagerClient, IPAddress, ServerInfo
from test.pylib.util import wait_for


async def restart(manager: ManagerClient, server: ServerInfo) -> None:
    logging.info(f"Stopping {server} gracefully")
    await manager.server_stop_gracefully(server.server_id)
    logging.info(f"Restarting {server}")
    await manager.server_start(server.server_id)
    logging.info(f"{server} restarted")


async def enable_raft(manager: ManagerClient, server: ServerInfo) -> None:
    config = await manager.server_get_config(server.server_id)
    logging.info(f"Updating config of server {server}")
    await manager.server_update_config(server.server_id, 'consistent_cluster_management', 'True')


async def enable_raft_and_restart(manager: ManagerClient, server: ServerInfo) -> None:
    await enable_raft(manager, server)
    await restart(manager, server)


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


async def wait_for_gossip_gen_increase(api: ScyllaRESTAPIClient, gen: int, node_ip: IPAddress,
                                       target_ip: IPAddress, deadline: float):
    """Wait until the generation number of `target_ip` increases above `gen` from the point of view of `node_ip`.
       Can be used to wait until `node_ip` gossips with `target_ip` after `target_ip` was restarted
       by saving the generation number of `target_ip` before restarting it and then calling this function
       (nodes increase their generation numbers when they restart).
    """
    async def gen_increased() -> Optional[int]:
        curr_gen = await api.get_gossip_generation_number(node_ip, target_ip)
        if curr_gen <= gen:
            logging.info(f"Gossip generation number of {target_ip} is {curr_gen} <= {gen} according to {node_ip}")
            return None
        return curr_gen
    gen = await wait_for(gen_increased, deadline)
    logging.info(f"Gossip generation number of {target_ip} is reached {gen} according to {node_ip}")


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
