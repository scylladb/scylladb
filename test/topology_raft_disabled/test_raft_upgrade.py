#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest
import logging
import time
import functools
from typing import Callable, Awaitable, Optional, TypeVar, Generic

from cassandra.cluster import NoHostAvailable, Session  # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host                         # type: ignore # pylint: disable=no-name-in-module

from test.pylib.manager_client import ManagerClient, IPAddress, ServerInfo
from test.pylib.random_tables import RandomTables
from test.pylib.rest_client import ScyllaRESTAPIClient, inject_error_one_shot
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts


async def reconnect_driver(manager: ManagerClient) -> Session:
    """Workaround for scylladb/python-driver#170:
       the existing driver session may not reconnect, create a new one.
    """
    logging.info(f"Reconnecting driver")
    manager.driver_close()
    await manager.driver_connect()
    cql = manager.cql
    assert(cql)
    return cql


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


async def delete_raft_data(cql: Session, host: Host) -> None:
    await cql.run_async("truncate table system.discovery", host=host)
    await cql.run_async("truncate table system.group0_history", host=host)
    await cql.run_async("delete value from system.scylla_local where key = 'raft_group0_id'", host=host)


def log_run_time(f):
    @functools.wraps(f)
    async def wrapped(*args, **kwargs):
        start = time.time()
        res = await f(*args, **kwargs)
        logging.info(f"{f.__name__} took {int(time.time() - start)} seconds.")
        return res
    return wrapped


@pytest.mark.asyncio
@log_run_time
@pytest.mark.replication_factor(1)
async def test_raft_upgrade_basic(manager: ManagerClient, random_tables: RandomTables):
    """
    kbr-: the test takes about 7 seconds in dev mode on my laptop.
    """
    servers = await manager.running_servers()
    cql = manager.cql
    assert(cql)

    # system.group0_history should either not exist or there should be no entries in it before upgrade.
    if await cql.run_async("select * from system_schema.tables where keyspace_name = 'system' and table_name = 'group0_history'"):
        assert(not (await cql.run_async("select * from system.group0_history")))

    logging.info(f"Enabling Raft on {servers} and restarting")
    await asyncio.gather(*(enable_raft_and_restart(manager, srv) for srv in servers))
    cql = await reconnect_driver(manager)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Driver reconnected, hosts: {hosts}. Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    logging.info("Upgrade finished. Creating a new table")
    table = await random_tables.add_table(ncolumns=5)

    logging.info("Checking group0_history")
    rs = await cql.run_async("select * from system.group0_history")
    assert(rs)
    logging.info(f"group0_history entry description: '{rs[0].description}'")
    assert(table.full_name in rs[0].description)

    logging.info("Booting new node")
    await manager.server_add(config={
        'consistent_cluster_management': True
    })


@pytest.mark.asyncio
@log_run_time
@pytest.mark.replication_factor(1)
async def test_recover_stuck_raft_upgrade(manager: ManagerClient, random_tables: RandomTables):
    """
    We enable Raft on every server and the upgrade procedure starts.  All servers join group 0. Then one
    of them fails, the rest enter 'synchronize' state.  We assume the failed server cannot be recovered.
    We cannot just remove it at this point; it's already part of group 0, `remove_from_group0` will wait
    until upgrade procedure finishes - but the procedure is stuck.  To proceed we enter RECOVERY state on
    the other servers, remove the failed one, and clear existing Raft data. After leaving RECOVERY the
    remaining nodes will restart the procedure, establish a new group 0 and finish upgrade.

    kbr-: the test takes about 26 seconds in dev mode on my laptop.
    """
    servers = await manager.running_servers()
    srv1, *others = servers

    logging.info(f"Enabling Raft on {srv1} and restarting")
    await enable_raft_and_restart(manager, srv1)

    # TODO error injection should probably be done through ScyllaClusterManager (we may need to mark the cluster as dirty).
    # In this test the cluster is dirty anyway due to a restart so it's safe.
    await inject_error_one_shot(manager.api, srv1.ip_addr, 'group0_upgrade_before_synchronize')
    logging.info(f"Enabling Raft on {others} and restarting")
    await asyncio.gather(*(enable_raft_and_restart(manager, srv) for srv in others))
    cql = await reconnect_driver(manager)

    logging.info(f"Cluster restarted, waiting until driver reconnects to {others}")
    hosts = await wait_for_cql_and_get_hosts(cql, others, time.time() + 60)
    logging.info(f"Driver reconnected, hosts: {hosts}")

    logging.info(f"Waiting until {hosts} enter 'synchronize' state")
    await asyncio.gather(*(wait_for_upgrade_state('synchronize', cql, h, time.time() + 60) for h in hosts))
    logging.info(f"{hosts} entered synchronize")

    # TODO ensure that srv1 failed upgrade - look at logs?
    # '[shard 0] raft_group0_upgrade - Raft upgrade failed: std::runtime_error (error injection before group 0 upgrade enters synchronize).'

    logging.info(f"Setting recovery state on {hosts}")
    for host in hosts:
        await cql.run_async(
                "update system.scylla_local set value = 'recovery' where key = 'group0_upgrade_state'",
                host=host)

    logging.info(f"Restarting {others}")
    await asyncio.gather(*(restart(manager, srv) for srv in others))
    cql = await reconnect_driver(manager)

    logging.info(f"{others} restarted, waiting until driver reconnects to them")
    hosts = await wait_for_cql_and_get_hosts(cql, others, time.time() + 60)

    logging.info(f"Checking if {hosts} are in recovery state")
    for host in hosts:
        rs = await cql.run_async(
                "select value from system.scylla_local where key = 'group0_upgrade_state'",
                host=host)
        assert rs[0].value == 'recovery'

    logging.info("Creating a table while in recovery state")
    table = await random_tables.add_table(ncolumns=5)

    logging.info(f"Stopping {srv1}")
    await manager.server_stop_gracefully(srv1.server_id)

    logging.info(f"Removing {srv1} using {others[0]}")
    await manager.remove_node(others[0].server_id, srv1.server_id)

    logging.info(f"Deleting Raft data and upgrade state on {hosts} and restarting")
    for host in hosts:
        await delete_raft_data(cql, host)
        await cql.run_async("delete from system.scylla_local where key = 'group0_upgrade_state'", host=host)

    await asyncio.gather(*(restart(manager, srv) for srv in others))
    cql = await reconnect_driver(manager)

    logging.info(f"Cluster restarted, waiting until driver reconnects to {others}")
    hosts = await wait_for_cql_and_get_hosts(cql, others, time.time() + 60)

    logging.info(f"Driver reconnected, hosts: {hosts}, waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    logging.info("Checking if previously created table still exists")
    await cql.run_async(f"select * from {table.full_name}")


@pytest.mark.asyncio
@log_run_time
@pytest.mark.replication_factor(1)
async def test_recovery_after_majority_loss(manager: ManagerClient, random_tables: RandomTables):
    """
    We successfully upgrade a cluster. Eventually however all servers but one fail - group 0
    is left without a majority. We create a new group 0 by entering RECOVERY, using `removenode`
    to get rid of the other servers, clearing Raft data and restarting. The Raft upgrade procedure
    runs again to establish a single-node group 0. We also verify that schema changes performed
    using the old group 0 are still there.
    Note: in general there's no guarantee that all schema changes will be present; the minority
    used to recover group 0 might have missed them. However in this test the driver waits
    for schema agreement to complete before proceeding, so we know that every server learned
    about the schema changes.

    kbr-: the test takes about 22 seconds in dev mode on my laptop.
    """
    servers = await manager.running_servers()

    logging.info(f"Enabling Raft on {servers} and restarting")
    await asyncio.gather(*(enable_raft_and_restart(manager, srv) for srv in servers))
    cql = await reconnect_driver(manager)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Driver reconnected, hosts: {hosts}. Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    logging.info("Upgrade finished. Creating a bunch of tables")
    tables = await asyncio.gather(*(random_tables.add_table(ncolumns=5) for _ in range(5)))

    srv1, *others = servers

    logging.info(f"Killing all nodes except {srv1}")
    await asyncio.gather(*(manager.server_stop_gracefully(srv.server_id) for srv in others))

    logging.info(f"Entering recovery state on {srv1}")
    host1 = next(h for h in hosts if h.address == srv1.ip_addr)
    await cql.run_async("update system.scylla_local set value = 'recovery' where key = 'group0_upgrade_state'", host=host1)
    await restart(manager, srv1)
    cql = await reconnect_driver(manager)

    logging.info("Node restarted, waiting until driver connects")
    host1 = (await wait_for_cql_and_get_hosts(cql, [srv1], time.time() + 60))[0]

    for i in range(len(others)):
        to_remove = others[i]
        ignore_dead_ips = [srv.ip_addr for srv in others[i+1:]]
        logging.info(f"Removing {to_remove} using {srv1} with ignore_dead: {ignore_dead_ips}")
        await manager.remove_node(srv1.server_id, to_remove.server_id, ignore_dead_ips)

    logging.info(f"Deleting old Raft data and upgrade state on {host1} and restarting")
    await delete_raft_data(cql, host1)
    await cql.run_async("delete from system.scylla_local where key = 'group0_upgrade_state'", host=host1)
    await restart(manager, srv1)
    cql = await reconnect_driver(manager)

    logging.info("Node restarted, waiting until driver connects")
    host1 = (await wait_for_cql_and_get_hosts(cql, [srv1], time.time() + 60))[0]

    logging.info(f"Driver reconnected, host: {host1}. Waiting until upgrade finishes.")
    await wait_until_upgrade_finishes(cql, host1, time.time() + 60)

    logging.info("Checking if previously created tables still exist")
    await asyncio.gather(*(cql.run_async(f"select * from {t.full_name}") for t in tables))

    logging.info("Creating another table")
    await random_tables.add_table(ncolumns=5)

    logging.info("Booting new node")
    await manager.server_add(config={
        'consistent_cluster_management': True
    })
