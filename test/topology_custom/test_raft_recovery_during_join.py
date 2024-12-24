#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import logging
import time
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode
from test.topology.util import check_system_topology_and_cdc_generations_v3_consistency, \
        check_token_ring_and_group0_consistency, delete_discovery_state_and_group0_id, delete_raft_group_data, \
        reconnect_driver, wait_for_cdc_generations_publishing


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_raft_recovery_during_join(manager: ManagerClient):
    """
    Test that the Raft-based recovery procedure works correctly if majority has been lost in the write_both_read_new
    state caused by a join request.

    The purpose of this test is to check that the Raft-based recovery procedure works correctly if majority has been
    lost during an ongoing work of the topology coordinator. During the procedure, the new topology coordinator starts
    on the leader of the new group 0. We must ensure it finishes its work cleanly (successfully or with a rollback). Of
    course, there are more possible cases to test, but for now, this is the only test.

    1. Start a cluster with 5 nodes (called nodes 1-5).
    2. Enable the delay_node_bootstrap error injection on the topology coordinator to ensure it gets stuck in
    write_both_read_new while handling the next join request.
    3. Add node 6.
    4. Kill node 6 once the topology coordinator reaches write_both_read_new.
    5. Kill nodes 3-5 causing a permanent group 0 majority loss.
    6. Unblock the topology coordinator to allow a graceful shutdown (it would hang otherwise).
    7. Run the recovery procedure to recreate group 0 with nodes 1, 2 as the only members. The new topology coordinator
    starts on node 1 and resumes work. Since the join request passed the point of no return, it will succeed, and node 6
    will join the topology.
    8. Remove nodes 3-6 from topology using the standard removenode procedure.
    9. Add a new node (a sanity check verifying that the cluster is functioning properly).
    """
    logging.info('Adding the first server')
    coordinator = await manager.server_add()

    logging.info('Adding four servers')
    servers = [coordinator] + await manager.servers_add(4)
    live_servers = servers[:2]
    dead_servers = servers[2:]
    logging.info(f'Servers to survive majority loss: {live_servers}, servers to be killed: {dead_servers}')

    cql, _ = await manager.get_ready_cql(servers)

    first_group0_id = (await cql.run_async(
            "SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'"))[0].value

    coordinator_log = await manager.server_open_log(coordinator.server_id)

    logging.info(f'Blocking the topology coordinator in write_both_read_new on server {coordinator}')
    await manager.api.enable_injection(coordinator.ip_addr, 'delay_node_bootstrap', one_shot=False)

    logging.info('Adding a server that will crash before the join topology operation completes')
    failed_server = await manager.server_add(start=False, config={
            'error_injections_at_startup': ['crash_before_topology_request_completion']})
    task = asyncio.create_task(manager.server_start(failed_server.server_id,
                               expected_error='Crashed in crash_before_topology_request_completion'))

    logging.info(f'Waiting until the topology coordinator blocks in write_both_read_new on node server {coordinator}')
    await coordinator_log.wait_for("delay_node_bootstrap: waiting for message")

    logging.info(f'Crashing {failed_server}')
    await manager.api.message_injection(failed_server.ip_addr, 'crash_before_topology_request_completion')
    await task
    dead_servers.append(failed_server)

    logging.info(f'Killing {dead_servers}')
    for srv in dead_servers:
        await manager.server_stop(server_id=srv.server_id)

    logging.info(f'Unblocking the topology coordinator on server {coordinator}')
    await manager.api.message_injection(coordinator.ip_addr, 'delay_node_bootstrap')

    cql, hosts = await manager.get_ready_cql(live_servers)

    logging.info('Starting the recovery procedure')

    logging.info(f'Restarting {live_servers}')
    await manager.rolling_restart(live_servers)

    logging.info(f'Deleting the persistent discovery state and group 0 ID on {live_servers}')
    for h in hosts:
        await delete_discovery_state_and_group0_id(cql, h)

    recovery_leader_id = await manager.get_host_id(live_servers[0].server_id)
    logging.info(f'Setting recovery leader to {live_servers[0].server_id} on {live_servers}')
    for srv in live_servers:
        await manager.server_update_config(srv.server_id, 'recovery_leader', recovery_leader_id)

    logging.info(f'Restarting {live_servers}')
    await manager.rolling_restart(live_servers)

    logging.info(f'Removing {dead_servers}')
    for i, being_removed in enumerate(dead_servers):
        ignored = [dead_srv.ip_addr for dead_srv in dead_servers[i + 1:]]
        initiator = live_servers[i % 2]
        await manager.remove_node(initiator.server_id, being_removed.server_id, ignored)

    logging.info(f'Unsetting the recovery_leader config option on {live_servers}')
    for srv in live_servers:
        await manager.server_update_config(srv.server_id, 'recovery_leader', '')

    cql = await reconnect_driver(manager)
    hosts = await wait_for_cql_and_get_hosts(cql, live_servers, time.time() + 60)

    logging.info(f'Deleting persistent data of group 0 {first_group0_id} on {live_servers}')
    for h in hosts:
        await delete_raft_group_data(first_group0_id, cql, h)

    logging.info('Performing consistency checks after the recovery procedure')
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)
    await check_token_ring_and_group0_consistency(manager)
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)

    logging.info('Adding a new server')
    new_server = await manager.server_add()

    hosts = await wait_for_cql_and_get_hosts(cql, live_servers + [new_server], time.time() + 60)

    logging.info(f'Performing consistency checks after adding {new_server}')
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)
    await check_token_ring_and_group0_consistency(manager)
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)
