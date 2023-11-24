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
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver
from test.topology_raft_disabled.util import restart, enable_raft_and_restart, enter_recovery_state, \
        wait_for_upgrade_state, wait_until_upgrade_finishes, delete_raft_data_and_upgrade_state, log_run_time


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
    await asyncio.gather(*(enter_recovery_state(cql, h) for h in hosts))

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
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))

    await asyncio.gather(*(restart(manager, srv) for srv in others))
    cql = await reconnect_driver(manager)

    logging.info(f"Cluster restarted, waiting until driver reconnects to {others}")
    hosts = await wait_for_cql_and_get_hosts(cql, others, time.time() + 60)

    logging.info(f"Driver reconnected, hosts: {hosts}, waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    logging.info("Checking if previously created table still exists")
    await cql.run_async(f"select * from {table.full_name}")
