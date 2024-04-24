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
async def test_recovery_after_majority_loss(request, manager: ManagerClient):
    """
    All initial servers but one fail - group 0 is left without a majority. We create a new group
    0 by entering RECOVERY, using `removenode` to get rid of the other servers, clearing Raft
    data and restarting. The Raft upgrade procedure runs to establish a single-node group 0. We
    also verify that schema changes performed using the old group 0 are still there.
    Note: in general there's no guarantee that all schema changes will be present; the minority
    used to recover group 0 might have missed them. However in this test the driver waits
    for schema agreement to complete before proceeding, so we know that every server learned
    about the schema changes.
    """
    cfg = {'enable_user_defined_functions': False,
           'force_gossip_topology_changes': True}
    servers = [await manager.server_add(config=cfg) for _ in range(3)]

    logging.info("Waiting until driver connects to every server")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Creating a bunch of tables")
    random_tables = RandomTables(request.node.name, manager, unique_name(), 1)
    tables = await asyncio.gather(*(random_tables.add_table(ncolumns=5) for _ in range(5)))

    srv1, *others = servers

    logging.info(f"Killing all nodes except {srv1}")
    await asyncio.gather(*(manager.server_stop_gracefully(srv.server_id) for srv in others))

    logging.info(f"Entering recovery state on {srv1}")
    host1 = next(h for h in hosts if h.address == srv1.ip_addr)
    await enter_recovery_state(cql, host1)
    await manager.server_restart(srv1.server_id)
    cql = await reconnect_driver(manager)

    logging.info("Node restarted, waiting until driver connects")
    host1 = (await wait_for_cql_and_get_hosts(cql, [srv1], time.time() + 60))[0]

    for i in range(len(others)):
        to_remove = others[i]
        ignore_dead_ips = [srv.ip_addr for srv in others[i+1:]]
        logging.info(f"Removing {to_remove} using {srv1} with ignore_dead: {ignore_dead_ips}")
        await manager.remove_node(srv1.server_id, to_remove.server_id, ignore_dead_ips)

    logging.info(f"Deleting old Raft data and upgrade state on {host1} and restarting")
    await delete_raft_data_and_upgrade_state(cql, host1)
    await manager.server_restart(srv1.server_id)
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
    await manager.server_add(config=cfg)
