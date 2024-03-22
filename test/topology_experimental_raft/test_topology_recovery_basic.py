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
from test.topology.util import reconnect_driver, restart, enter_recovery_state, \
        delete_raft_data_and_upgrade_state, log_run_time, start_writes_to_cdc_table, wait_until_upgrade_finishes as wait_until_schema_upgrade_finishes, \
        wait_until_topology_upgrade_finishes, delete_raft_topology_state, wait_for_cdc_generations_publishing, \
        check_system_topology_and_cdc_generations_v3_consistency


@pytest.mark.asyncio
@log_run_time
async def test_topology_recovery_basic(request, manager: ManagerClient):
    servers = await manager.servers_add(3)
    cql = manager.cql
    assert(cql)

    finish_writes_and_verify = await start_writes_to_cdc_table(cql)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Restarting hosts {hosts} in recovery mode")
    await asyncio.gather(*(enter_recovery_state(cql, h) for h in hosts))
    # Restart sequentially, as it tests how nodes operating in legacy mode
    # react to raft topology mode nodes and vice versa
    for srv in servers:
        await restart(manager, srv)
    cql = await reconnect_driver(manager)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f"Driver reconnected, hosts: {hosts}")

    logging.info(f"Deleting Raft data and upgrade state on {hosts}")
    await asyncio.gather(*(delete_raft_topology_state(cql, h) for h in hosts))
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))

    logging.info(f"Restarting hosts {hosts}")
    for srv in servers:
        await restart(manager, srv)
    cql = await reconnect_driver(manager)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Waiting until upgrade to raft schema finishes")
    await asyncio.gather(*(wait_until_schema_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    logging.info("Checking the topology upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    logging.info("Waiting until all nodes see others as alive")
    await manager.servers_see_each_other(servers)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Waiting for CDC generations publishing")
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)

    logging.info("Booting new node")
    servers += [await manager.server_add()]
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Waiting for the new CDC generation publishing")
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)

    logging.info("Checking correctness of data in system_distributed.cdc_streams_descriptions_v2")
    await finish_writes_and_verify(cql)
