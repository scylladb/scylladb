#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import pytest
import time

from typing import List

from test.pylib.log_browsing import ScyllaLogFile
from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import gather_safely
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.conftest import skip_mode
from test.cluster.util import reconnect_driver, enter_recovery_state, \
        delete_raft_data_and_upgrade_state, log_run_time, wait_until_upgrade_finishes as wait_until_schema_upgrade_finishes, \
        wait_until_topology_upgrade_finishes, delete_raft_topology_state, wait_for_cdc_generations_publishing, \
        check_system_topology_and_cdc_generations_v3_consistency

async def wait_for_log_on_any_node(logs: List[ScyllaLogFile], marks: List[int], pattern: str):
    """
    Waits until a given line appears on any node in the cluster.
    """
    assert len(logs) == len(marks)
    async with asyncio.TaskGroup() as tg:
        log_watch_tasks = [tg.create_task(l.wait_for(pattern)) for l, m in zip(logs, marks)]
        _, pending = await asyncio.wait(log_watch_tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'test performs many topology changes')
@log_run_time
async def test_topology_upgrade_stuck(request, manager: ManagerClient):
    """
    Simulates a situation where upgrade procedure gets stuck due to majority
    loss: we have one upgraded node, one not upgraded node, and three nodes
    permanently down. Then, it verifies that it's possible to perform recovery
    procedure and redo the upgrade after the issue is resolved.
    """

    # First, force the first node to start in legacy mode
    cfg = {'force_gossip_topology_changes': True, 'enable_tablets': False}

    servers = [await manager.server_add(config=cfg) for _ in range(5)]
    to_be_upgraded_node, to_be_isolated_node, *to_be_shutdown_nodes = servers

    logging.info("Waiting until driver connects to every server")
    cql, hosts = await manager.get_ready_cql(servers)
    removed_hosts = hosts[2:]

    logging.info("Checking the upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    logging.info("Enabling error injection which will cause the topology coordinator to get stuck")
    await gather_safely(*(manager.api.enable_injection(s.ip_addr, "topology_coordinator_fail_to_build_state_during_upgrade", one_shot=False) for s in servers))

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade gets stuck due to error injection")
    logs = await gather_safely(*(manager.server_open_log(s.server_id) for s in servers))
    marks = [l.mark() for l in logs]
    await wait_for_log_on_any_node(logs, marks, "failed to build topology coordinator state due to error injection")

    logging.info("Isolate one of the nodes via error injection")
    await manager.api.enable_injection(to_be_isolated_node.ip_addr, "raft_drop_incoming_append_entries", one_shot=False)

    logging.info("Disable the error injection that causes upgrade to get stuck")
    marks = [l.mark() for l in logs]
    await gather_safely(*(manager.api.disable_injection(s.ip_addr, "topology_coordinator_fail_to_build_state_during_upgrade") for s in servers))

    logging.info("Wait for the topology coordinator to observe upgrade as finished")
    await wait_for_log_on_any_node(logs, marks, "upgrade to raft topology has finished")

    logging.info("Shut down three nodes to simulate quorum loss")
    await gather_safely(*(manager.server_stop(s.server_id) for s in to_be_shutdown_nodes))

    logging.info("Disable the error injection that causes node to be isolated")
    await manager.api.disable_injection(to_be_isolated_node.ip_addr, "raft_drop_incoming_append_entries")

    logging.info("Checking that not all nodes finished upgrade")
    upgraded_count = 0
    for s in [to_be_upgraded_node, to_be_isolated_node]:
        status = await manager.api.raft_topology_upgrade_status(s.ip_addr)
        if status == "done":
            upgraded_count += 1
    assert upgraded_count != 2

    logging.info(f"Only {upgraded_count}/2 nodes finished upgrade, which was expected")

    servers, others = [to_be_upgraded_node, to_be_isolated_node], to_be_shutdown_nodes

    logging.info(f"Obtaining hosts for nodes {servers}")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Restarting hosts {hosts} in recovery mode")
    await gather_safely(*(enter_recovery_state(cql, h) for h in hosts))
    await manager.rolling_restart(servers)
    cql = await reconnect_driver(manager)

    await manager.servers_see_each_other(servers)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f"Driver reconnected, hosts: {hosts}")

    for i in range(len(others)):
        to_remove = others[i]
        ignore_dead_ips = [srv.ip_addr for srv in others[i+1:]]
        logging.info(f"Removing {to_remove} using {servers[0]} with ignore_dead: {ignore_dead_ips}")
        await manager.remove_node(servers[0].server_id, to_remove.server_id, ignore_dead_ips)

    logging.info(f"Deleting Raft data and upgrade state on {hosts}")
    await gather_safely(*(delete_raft_topology_state(cql, h) for h in hosts))
    await gather_safely(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))

    logging.info(f"Restarting hosts {hosts}")
    await manager.rolling_restart(servers)
    cql = await reconnect_driver(manager)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Waiting until upgrade to raft schema finishes")
    await gather_safely(*(wait_until_schema_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    logging.info("Checking the topology upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    logging.info("Waiting until all nodes see others as alive")
    await manager.servers_see_each_other(servers)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await gather_safely(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Waiting for CDC generations publishing")
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts, ignored_hosts=removed_hosts)

    logging.info("Booting three new nodes")
    servers += await gather_safely(*(manager.server_add() for _ in range(3)))
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Waiting for the new CDC generation publishing")
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts, ignored_hosts=removed_hosts)
