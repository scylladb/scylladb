#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import logging
import time
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import check_system_topology_and_cdc_generations_v3_consistency, \
        check_token_ring_and_group0_consistency, delete_discovery_state_and_group0_id, delete_raft_group_data, \
        reconnect_driver, wait_for_cdc_generations_publishing
from test.topology_custom.test_group0_schema_versioning import get_group0_schema_version, get_local_schema_version


@pytest.mark.asyncio
async def test_raft_recovery_entry_lose(manager: ManagerClient):
    """
    Test that the Raft-based recovery procedure works correctly if some committed group 0 entry has been permanently
    lost (it has been committed only by dead nodes).

    1. Start a cluster with 5 nodes
    2. Create a scenario where nodes have the following group 0 states:
    - node 1: v1,
    - node 2: v2,
    - nodes 3-5: v3.
    3. Kill nodes 3-5 causing a permanent group 0 majority loss. We also permanently lose v3. After recovering majority,
    nodes 1-2 should have v2, which is the only safe option.
    4. Run the recovery procedure to recreate group 0 with nodes 1, 2 as the only members. We make sure node 2
    becomes the leader of new group 0 since it has a newer group 0 state. When node 1 joins new group 0, it receives
    a group 0 snapshot with v2 from node 2 and applies it. If node 1 became the leader, node 1 would end up with v1 and
    node 2 would end up with v2. The snapshot transfer from node 1 to node 2 would not do anything due to mutations from
    v2 having later timestamps.
    5. Check that node 1 has moved its group 0 state to v2.
    6. Remove nodes 3-5 from topology using the standard removenode procedure.
    7. Add a new node (a sanity check verifying that the cluster is functioning properly).
    """
    logging.info('Adding initial servers')
    servers = await manager.servers_add(5)
    live_servers = servers[:2]
    dead_servers = servers[2:]
    logging.info(f'Servers to survive majority loss: {live_servers}, servers to be killed: {dead_servers}')

    cql, _ = await manager.get_ready_cql(servers)

    first_group0_id = (await cql.run_async(
            "SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'"))[0].value

    logging.info(f'Stopping {live_servers[0].server_id} to keep its group 0 state in v1')
    await manager.server_stop(live_servers[0].server_id)

    logging.info('Creating keyspace ks1, moving the group 0 state to v2')
    await cql.run_async(
            "CREATE KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")

    logging.info(f'Stopping {live_servers[1].server_id} to keep its group 0 state in v2')
    await manager.server_stop(live_servers[1].server_id)

    logging.info('Creating keyspace ks2, moving the group 0 state to v3')
    await cql.run_async(
            "CREATE KEYSPACE ks2 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")

    cql = await reconnect_driver(manager)
    hosts = await wait_for_cql_and_get_hosts(cql, dead_servers, time.time() + 60)

    v_group0 = await get_group0_schema_version(cql, hosts[0])
    logging.info(f'Found group 0 schema version {v_group0}')

    logging.info(f'Killing {dead_servers}')
    for srv in dead_servers:
        await manager.server_stop(server_id=srv.server_id)

    logging.info(f'Starting {live_servers}')
    for srv in live_servers:
        await manager.server_start(srv.server_id)

    cql = await reconnect_driver(manager)
    hosts = await wait_for_cql_and_get_hosts(cql, live_servers, time.time() + 60)

    v_node1 = await get_local_schema_version(cql, hosts[0])
    logging.info(f'Found schema version {v_node1} on {live_servers[0].server_id}')
    v_node2 = await get_local_schema_version(cql, hosts[1])
    logging.info(f'Found schema version {v_node2} on {live_servers[1].server_id}')
    assert v_group0 != v_node1 and v_node1 != v_node2 and v_node2 != v_group0

    logging.info('Starting the recovery procedure')

    logging.info(f'Restarting {live_servers}')
    await manager.rolling_restart(live_servers)

    logging.info(f'Deleting the persistent discovery state and group 0 ID on {live_servers}')
    for h in hosts:
        await delete_discovery_state_and_group0_id(cql, h)

    # FIXME: check that the way to identify the leader works when it's implemented (whatever it will be).

    recovery_leader_id = await manager.get_host_id(live_servers[1].server_id)
    logging.info(f'Setting recovery leader to {live_servers[1].server_id} on {live_servers}')
    for srv in live_servers:
        await manager.server_update_config(srv.server_id, 'recovery_leader', recovery_leader_id)

    # Restart twice to check that noninitial restarts with recovery_leader work. Noninitial restarts should not break
    # the recovery procedure. Note that nodes join the new group 0 only during the first restart with
    # recovery_leader, so the following restarts have a different execution path.
    logging.info(f'Restarting {live_servers[::-1]} twice')
    await manager.rolling_restart(live_servers[::-1])
    await manager.rolling_restart(live_servers[::-1])

    cql = await reconnect_driver(manager)
    hosts = await wait_for_cql_and_get_hosts(cql, live_servers, time.time() + 60)

    new_v_group0 = await get_group0_schema_version(cql, hosts[1])
    logging.info(f'Found new group 0 schema version {new_v_group0}')
    new_v_node1 = await get_local_schema_version(cql, hosts[0])
    logging.info(f'Found new schema version {new_v_node1} on {live_servers[0].server_id}')
    new_v_node2 = await get_local_schema_version(cql, hosts[1])
    logging.info(f'Found new schema version {new_v_node2} on {live_servers[1].server_id}')
    assert v_group0 != new_v_group0 and new_v_group0 == new_v_node1 and new_v_node1 == new_v_node2

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
