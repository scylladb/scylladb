#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest
import logging
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver, enter_recovery_state, \
        wait_until_upgrade_finishes, delete_raft_data_and_upgrade_state, log_run_time


@pytest.mark.asyncio
@log_run_time
async def test_raft_recovery_basic(request, manager: ManagerClient):
    cfg = {'enable_user_defined_functions': False,
           'force_gossip_topology_changes': True}
    cmd = ['--logger-log-level', 'raft=trace']

    servers = [await manager.server_add(config=cfg, cmdline=cmd) for _ in range(3)]
    cql = manager.cql
    assert(cql)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Setting recovery state on {hosts}")
    await asyncio.gather(*(enter_recovery_state(cql, h) for h in hosts))
    await asyncio.gather(*(manager.server_restart(srv.server_id) for srv in servers))
    cql = await reconnect_driver(manager)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f"Driver reconnected, hosts: {hosts}")

    logging.info(f"Deleting Raft data and upgrade state on {hosts}")
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))

    logging.info(f"Restarting {servers}")
    await asyncio.gather(*(manager.server_restart(srv.server_id) for srv in servers))
    cql = await reconnect_driver(manager)

    logging.info(f"Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Driver reconnected, hosts: {hosts}. Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    logging.info("Upgrade finished. Creating a new table")
    random_tables = RandomTables(request.node.name, manager, unique_name(), 1)
    table = await random_tables.add_table(ncolumns=5)

    logging.info("Checking group0_history")
    rs = await cql.run_async("select * from system.group0_history")
    assert(rs)
    logging.info(f"group0_history entry description: '{rs[0].description}'")
    assert(table.full_name in rs[0].description)

    logging.info("Booting new node")
    await manager.server_add(config=cfg, cmdline=cmd)
