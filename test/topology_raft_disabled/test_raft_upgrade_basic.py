#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest
import logging
import time
from typing import Callable, Awaitable, Optional, TypeVar, Generic

from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts, wait_for_feature
from test.topology.util import reconnect_driver
from test.topology_raft_disabled.util import restart, enable_raft, \
        enable_raft_and_restart, wait_for_upgrade_state, wait_until_upgrade_finishes, \
        delete_raft_data, log_run_time


@pytest.mark.asyncio
@log_run_time
@pytest.mark.replication_factor(1)
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

    logging.info("Booting new node")
    await manager.server_add(config={
        'consistent_cluster_management': True
    })
