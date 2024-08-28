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
from test.topology.conftest import skip_mode
from test.topology.util import (delete_raft_data_and_upgrade_state, enter_recovery_state, log_run_time,
                                reconnect_driver, wait_for_upgrade_state, wait_until_upgrade_finishes)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@log_run_time
async def test_recover_stuck_raft_recovery(request, manager: ManagerClient):
    """
    After creating a cluster, we enter RECOVERY state on every server. Then, we delete the Raft data
    and the upgrade state on all servers. We restart them and the upgrade procedure starts. One of the
    servers fails, the rest enter 'synchronize' state. We assume the failed server cannot be recovered.
    We cannot just remove it at this point; it's already part of group 0, `remove_from_group0` will wait
    until upgrade procedure finishes - but the procedure is stuck.  To proceed we enter RECOVERY state on
    the other servers, remove the failed one, and clear existing Raft data. After leaving RECOVERY the
    remaining nodes will restart the procedure, establish a new group 0 and finish upgrade.
    """
    cfg = {'enable_user_defined_functions': False,
           'force_gossip_topology_changes': True}
    servers = [await manager.server_add(config=cfg) for _ in range(3)]
    srv1, *others = servers

    logging.info("Waiting until driver connects to every server")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Setting recovery state on {hosts}")
    await asyncio.gather(*(enter_recovery_state(cql, h) for h in hosts))
    await asyncio.gather(*(manager.server_restart(srv.server_id) for srv in servers))
    cql = await reconnect_driver(manager)

    logging.info(f"Cluster restarted, waiting until driver reconnects to {others}")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f"Driver reconnected, hosts: {hosts}")

    logging.info(f"Deleting Raft data and upgrade state on {hosts}")
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))

    logging.info(f"Stopping {servers}")
    await asyncio.gather(*(manager.server_stop_gracefully(srv.server_id) for srv in servers))

    logging.info(f"Starting {srv1} with injected group 0 upgrade error")
    await manager.server_update_config(srv1.server_id, 'error_injections_at_startup', ['group0_upgrade_before_synchronize'])
    await manager.server_start(srv1.server_id)

    logging.info(f"Starting {others}")
    await asyncio.gather(*(manager.server_start(srv.server_id) for srv in others))
    cql = await reconnect_driver(manager)

    logging.info(f"Cluster restarted, waiting until driver reconnects to {others}")
    hosts = await wait_for_cql_and_get_hosts(cql, others, time.time() + 60)
    logging.info(f"Driver reconnected, hosts: {hosts}")

    logging.info(f"Waiting until {hosts} enter 'synchronize' state")
    await asyncio.gather(*(wait_for_upgrade_state('synchronize', cql, h, time.time() + 60) for h in hosts))
    logging.info(f"{hosts} entered synchronize")

    log_file1 = await manager.server_open_log(srv1.server_id)
    logging.info(f"Checking if Raft upgrade procedure failed on {srv1}")
    await log_file1.wait_for("error injection before group 0 upgrade enters synchronize")

    logging.info(f"Setting recovery state on {hosts}")
    await asyncio.gather(*(enter_recovery_state(cql, h) for h in hosts))

    logging.info(f"Restarting {others}")
    await manager.rolling_restart(others)

    logging.info(f"{others} restarted, waiting until driver reconnects to them")
    hosts = await wait_for_cql_and_get_hosts(cql, others, time.time() + 60)

    logging.info(f"Checking if {hosts} are in recovery state")
    for host in hosts:
        rs = await cql.run_async(
                "select value from system.scylla_local where key = 'group0_upgrade_state'",
                host=host)
        assert rs[0].value == 'recovery'

    logging.info("Creating a table while in recovery state")
    random_tables = RandomTables(request.node.name, manager, unique_name(), 1)
    table = await random_tables.add_table(ncolumns=5)

    logging.info(f"Stopping {srv1}")
    await manager.server_stop_gracefully(srv1.server_id)

    logging.info(f"Removing {srv1} using {others[0]}")
    await manager.remove_node(others[0].server_id, srv1.server_id)

    logging.info(f"Deleting Raft data and upgrade state on {hosts}")
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))

    logging.info(f"Restarting {others}")
    await manager.rolling_restart(others)

    logging.info(f"Cluster restarted, waiting until driver reconnects to {others}")
    hosts = await wait_for_cql_and_get_hosts(cql, others, time.time() + 60)

    logging.info(f"Driver reconnected, hosts: {hosts}, waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    logging.info("Checking if previously created table still exists")
    await cql.run_async(f"select * from {table.full_name}")
