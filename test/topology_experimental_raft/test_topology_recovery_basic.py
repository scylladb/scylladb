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
        check_system_topology_and_cdc_generations_v3_consistency, start_writes_to_cdc_table, wait_until_last_generation_is_in_use


@pytest.mark.asyncio
@log_run_time
async def test_topology_recovery_basic(request, mode: str, manager: ManagerClient):
    # Increase ring delay to ensure nodes learn about CDC generations before they start operating.
    cfg = {'ring_delay_ms': 15000 if mode == 'debug' else 5000}

    servers = await manager.servers_add(3, config=cfg)
    cql = manager.cql
    assert(cql)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    restart_writes, stop_writes_and_verify = await start_writes_to_cdc_table(cql)

    logging.info(f"Restarting hosts {hosts} in recovery mode")
    await asyncio.gather(*(enter_recovery_state(cql, h) for h in hosts))

    # If we restarted nodes before the last generation was in use, some writes
    # could fail. After restart, nodes load only the last generation. If it's
    # not active yet, writes with lower timestamps would fail.
    await wait_until_last_generation_is_in_use(cql)

    logging.debug("Sleeping for 1 second to make sure there are writes to the CDC table in all 3 generations")
    await asyncio.sleep(1)

    # Restart sequentially, as it tests how nodes operating in legacy mode
    # react to raft topology mode nodes and vice versa
    await manager.rolling_restart(servers)

    await stop_writes_and_verify()

    cql = await reconnect_driver(manager)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f"Driver reconnected, hosts: {hosts}")

    restart_writes(cql)

    logging.info(f"Deleting Raft data and upgrade state on {hosts}")
    await asyncio.gather(*(delete_raft_topology_state(cql, h) for h in hosts))
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))

    logging.info(f"Restarting hosts {hosts}")
    await manager.rolling_restart(servers)

    # FIXME: We must reconnect the driver before performing CQL queries below, for example
    # in wait_until_schema_upgrade_finishes. Unfortunately, it forces us to stop writing to
    # a CDC table first. Reconnecting the driver would close the session used to send the
    # writes, and some writes could time out on the client.
    # Once https://github.com/scylladb/python-driver/issues/295 is fixed, we can remove
    # all calls to reconnect_driver, restart_writes and leave only the last call to
    # stop_writes_and_verify.
    await stop_writes_and_verify()

    cql = await reconnect_driver(manager)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    restart_writes(cql)

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
    servers += [await manager.server_add(config=cfg)]
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Waiting for the new CDC generation publishing")
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)

    await wait_until_last_generation_is_in_use(cql)

    logging.debug("Sleeping for 1 second to make sure there are writes to the CDC table in the last generation")
    await asyncio.sleep(1)

    logging.info("Checking correctness of data in system_distributed.cdc_streams_descriptions_v2")
    await stop_writes_and_verify()
