#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import logging
import pytest
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver, enter_recovery_state, \
        delete_raft_data_and_upgrade_state, log_run_time, wait_until_upgrade_finishes as wait_until_schema_upgrade_finishes, \
        wait_until_topology_upgrade_finishes, delete_raft_topology_state, wait_for_cdc_generations_publishing, \
        check_system_topology_and_cdc_generations_v3_consistency


@pytest.mark.asyncio
@log_run_time
async def test_topology_recovery_after_majority_loss(request, manager: ManagerClient):
    servers = await manager.servers_add(3)
    servers += await manager.servers_add(2, config={'join_ring': False})
    cql = manager.cql
    assert(cql)

    # Currently python driver ignores zero-token nodes, so we skip them here.
    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers[:-2], time.time() + 60)

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
    await delete_raft_topology_state(cql, host1)
    await delete_raft_data_and_upgrade_state(cql, host1)
    await manager.server_restart(srv1.server_id)
    cql = await reconnect_driver(manager)

    logging.info("Node restarted, waiting until driver connects")
    host1 = (await wait_for_cql_and_get_hosts(cql, [srv1], time.time() + 60))[0]

    logging.info("Waiting until upgrade to raft schema finishes.")
    await wait_until_schema_upgrade_finishes(cql, host1, time.time() + 60)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(host1.address)

    logging.info("Waiting until upgrade to raft topology finishes")
    await wait_until_topology_upgrade_finishes(manager, host1.address, time.time() + 60)

    logging.info("Waiting for CDC generations publishing")
    await wait_for_cdc_generations_publishing(cql, [host1], time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, [host1])

    logging.info("Add two more nodes")
    servers = [srv1] + await manager.servers_add(2)
    hosts = await wait_for_cql_and_get_hosts(cql, servers[:-2], time.time() + 60)

    logging.info("Waiting for the new CDC generations publishing")
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)
