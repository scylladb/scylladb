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

from cassandra.cluster import NoHostAvailable, Session
from cassandra.pool import Host

from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables


T = TypeVar('T')


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


async def restart(manager: ManagerClient, srv: str) -> None:
    logging.info(f"Stopping {srv} gracefully")
    await manager.server_stop_gracefully(srv)
    logging.info(f"Restarting {srv}")
    await manager.server_start(srv)
    logging.info(f"{srv} restarted")


async def enable_raft(manager: ManagerClient, srv: str) -> None:
    config = await manager.server_get_config(srv)
    features = config['experimental_features']
    assert(type(features) == list)
    features.append('raft')
    logging.info(f"Updating config of server {srv}")
    await manager.server_update_config(srv, 'experimental_features', features)


async def enable_raft_and_restart(manager: ManagerClient, srv: str) -> None:
    await enable_raft(manager, srv)
    await restart(manager, srv)


async def wait_for(pred: Callable[[], Awaitable[Optional[T]]], deadline: float) -> T:
    while True:
        assert(time.time() < deadline), "Deadline exceeded, failing test."
        res = await pred()
        if res is not None:
            return res
        await asyncio.sleep(1)


async def wait_for_cql(cql: Session, host: Host, deadline: float) -> None:
    async def cql_ready():
        try:
            await cql.run_async("select * from system.local", host=host)
        except NoHostAvailable:
            logging.info(f"Driver not connected to {host} yet")
            return None
        return True
    await wait_for(cql_ready, deadline)


async def wait_for_cql_and_get_hosts(cql: Session, ips: list[str], deadline: float) -> list[Host]:
    """Wait until every ip in `ips` is available through `cql` and translate `ips` to `Host`s."""
    ip_set = set(ips)
    async def get_hosts() -> Optional[list[Host]]:
        hosts = cql.cluster.metadata.all_hosts()
        remaining = ip_set - {h.address for h in hosts}
        if not remaining:
            return hosts

        logging.info(f"Driver hasn't yet learned about hosts: {remaining}")
        return None
    hosts = await wait_for(get_hosts, deadline)

    # Take only hosts from `ip_set` (there may be more)
    hosts = [h for h in hosts if h.address in ip_set]
    await asyncio.gather(*(wait_for_cql(cql, h, deadline) for h in hosts))

    return hosts


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
async def test_raft_upgrade_basic(manager: ManagerClient, random_tables: RandomTables):
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
