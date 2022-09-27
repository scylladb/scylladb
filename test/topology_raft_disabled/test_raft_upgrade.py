#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest
import logging
import time

from cassandra.cluster import NoHostAvailable

from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables

@pytest.mark.asyncio
async def test_raft_upgrade_basic(manager: ManagerClient, random_tables: RandomTables):
    start = time.time()
    servers = await manager.running_servers()

    cql = manager.cql
    assert(cql)

    # system.group0_history should either not exist or there should be no entries in it before upgrade.
    if await cql.run_async("select * from system_schema.tables where keyspace_name = 'system' and table_name = 'group0_history'"):
        assert(not (await cql.run_async("select * from system.group0_history")))

    async def restart(srv):
        logging.info(f"Stopping {srv} gracefully")
        await manager.server_stop_gracefully(srv)
        logging.info(f"Restarting {srv}")
        await manager.server_start(srv)
        logging.info(f"{srv} restarted")

    restarts = []
    for srv in servers:
        config = await manager.server_get_config(srv)
        features = config['experimental_features']
        assert(type(features) == list)
        features.append('raft')
        logging.info(f"Updating config of server {srv}")
        await manager.server_update_config(srv, 'experimental_features', features)
        restarts.append(restart(srv))

    await asyncio.gather(*restarts)

    # Workaround for scylladb/python-driver#170: the existing driver session may not reconnect, create a new one
    logging.info(f"Reconnecting driver")
    manager.driver_close()
    await manager.driver_connect()
    cql = manager.cql

    deadline = time.time() + 300
    # Using `servers` doesn't work for the `host` parameter in `cql.run_async` (we need objects of type `Host`).
    # Get the host list from the driver.
    hosts = cql.cluster.metadata.all_hosts()
    logging.info(f"Driver hosts: {hosts}")

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    for host in hosts:
        while True:
            assert(time.time() < deadline), "Deadline exceeded, failing test."
            try:
                await cql.run_async("select * from system.local", host=host)
            except NoHostAvailable:
                logging.info(f"Driver not connected to {host} yet")
            else:
                break
            await asyncio.sleep(1)

    logging.info("Driver reconnected, waiting until upgrade finishes")
    for host in hosts:
        while True:
            assert(time.time() < deadline), "Deadline exceeded, failing test."
            rs = await cql.run_async("select value from system.scylla_local where key = 'group0_upgrade_state'", host=host)
            if rs:
                value = rs[0].value
                if value == 'use_post_raft_procedures':
                    break
                else:
                    logging.info(f"Upgrade not finished yet on server {host}, state: {value}")
            else:
                logging.info(f"Upgrade not finished yet on server {host}, no upgrade state written")
            await asyncio.sleep(1)

    logging.info("Upgrade finished. Creating a new table")

    table = await random_tables.add_table(ncolumns=5)
    table_name = table.full_name

    logging.info("Checking group0_history")
    rs = await cql.run_async("select * from system.group0_history")
    assert(rs)
    logging.info(f"group0_history entry description: '{rs[0].description}'")
    assert(table_name in rs[0].description)

    logging.info(f"{test_raft_upgrade_basic.__name__} took {int(time.time() - start)} seconds.")
