#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import logging
import time
import pytest

from cassandra.cluster import ConsistencyLevel
from cassandra.policies import WhiteListRoundRobinPolicy

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts
from test.cluster.conftest import cluster_con
from test.cluster.util import check_system_topology_and_cdc_generations_v3_consistency, \
        check_token_ring_and_group0_consistency, delete_discovery_state_and_group0_id, delete_raft_group_data, \
        reconnect_driver, start_writes, wait_for_cdc_generations_publishing


@pytest.mark.asyncio
@pytest.mark.parametrize("remove_dead_nodes_with", ["remove", "replace"])
async def test_raft_recovery_user_data(manager: ManagerClient, remove_dead_nodes_with: str):
    """
    Test that the Raft-based recovery procedure works correctly with the user data. It involves testing:
    - client requests during the procedure (mainly availability),
    - removing/replacing dead nodes during the procedure in the presence of client requests and tablets on dead nodes.

    1. Start a cluster with two dcs, dc1 and dc2, containing three nodes each.
    2. Start sending writes with CL=LOCAL_QUORUM to a table with RF=3, NetworkTopologyStrategy and tablets.
    3. Kill all nodes from dc2 causing a permanent group 0 majority loss.
    4. Run the recovery procedure to recreate group 0 with nodes from dc2 as new members. Writes sent to dc1 should
    continue succeeding since at least two nodes are alive at any point during the recovery procedure (it involves only
    rolling restarts), every node in dc1 is a replica (3 nodes, RF=3) and two nodes make a local quorum in dc1.
    5. Remove nodes from dc2 from topology using remove or replace, depending on the value of the remove_dead_nodes_with
    parameter. For remove, we must do two additional steps to make it work:
    - Mark all dead nodes as permanently dead.
    - Decrease RF of the user keyspace, which uses tablets, to 0 in dc2.
    6. For remove, add 3 nodes to dc2 and bring back RF of the user keyspace to 3 in dc2, which brings back dc2 to its
    previous state. For replace, add a single node (a sanity check verifying that the cluster is functioning properly).
    7. Stop sending writes.
    """
    # Currently, the constraints imposed by `rf_rack_valid_keyspaces` are quite strict
    # and adjusting this test to working with it may require significant changes in the test.
    # Let's disable the option explicitly until we do that.
    rf_rack_cfg = {'rf_rack_valid_keyspaces': False}
    # Workaround for flakiness from https://github.com/scylladb/scylladb/issues/23565.
    hints_cfg = {'hinted_handoff_enabled': False}
    # Decrease failure_detector_timeout_in_ms from the default 20 s to speed up some graceful shutdowns in the test.
    # Shutting down the CQL server can hang for failure_detector_timeout_in_ms in the presence of dead nodes and
    # CQL requests.
    cfg = {
        'endpoint_snitch': 'GossipingPropertyFileSnitch',
        'tablets_mode_for_new_keyspaces': 'enabled',
        'failure_detector_timeout_in_ms': 2000,
    } | rf_rack_cfg | hints_cfg

    property_file_dc1 = {'dc': 'dc1', 'rack': 'rack1'}
    property_file_dc2 = {'dc': 'dc2', 'rack': 'rack2'}

    logging.info('Adding servers that will survive majority loss to dc1')
    live_servers = await manager.servers_add(3, config=cfg, property_file=property_file_dc1)
    logging.info('Adding servers that will be killed to dc2')
    dead_servers = await manager.servers_add(3, config=cfg, property_file=property_file_dc2)
    logging.info(f'Servers to survive majority loss: {live_servers}, servers to be killed: {dead_servers}')

    cql, _ = await manager.get_ready_cql(live_servers + dead_servers)
    hosts = await wait_for_cql_and_get_hosts(cql, live_servers, time.time() + 60)
    dead_hosts = await wait_for_cql_and_get_hosts(cql, dead_servers, time.time() + 60)

    first_group0_id = (await cql.run_async(
            "SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'"))[0].value

    rf: int = 3
    ks_name = unique_name()
    finish_writes = await start_writes(cql, rf, ConsistencyLevel.LOCAL_QUORUM, concurrency=5,
                                       ks_name=ks_name, node_shutdowns=True)

    # Send some writes before we kill nodes.
    logging.info('Sleeping for 1 s')
    await asyncio.sleep(1)

    logging.info(f'Killing {dead_servers}')
    for srv in dead_servers:
        await manager.server_stop(server_id=srv.server_id)

    logging.info('Starting the recovery procedure')

    logging.info(f'Restarting {live_servers}')
    await manager.rolling_restart(live_servers)

    logging.info(f'Deleting the persistent discovery state and group 0 ID on {live_servers}')
    for h in hosts:
        await delete_discovery_state_and_group0_id(cql, h)

    # FIXME: use the API to find the recovery leader here when it is implemented. A background operation like a tablet
    # migration could change the group 0 state just before losing the majority. Then, node 0 could be an incorrect
    # recovery leader because there could be another node in dc1 with a newer group 0 state.
    recovery_leader_id = await manager.get_host_id(live_servers[0].server_id)
    logging.info(f'Setting recovery leader to {live_servers[0].server_id} on {live_servers}')
    for srv in live_servers:
        await manager.server_update_config(srv.server_id, 'recovery_leader', recovery_leader_id)

    logging.info(f'Restarting {live_servers}')
    await manager.rolling_restart(live_servers)

    # We reconnect the driver before we send ALTER KEYSPACE requests below (if remove_dead_nodes_with == "remove") due
    # to https://github.com/scylladb/python-driver/issues/295. We must finish sending writes before reconnecting.
    await finish_writes()

    await reconnect_driver(manager)
    cql, hosts = await manager.get_ready_cql(live_servers)
    recovery_leader_host = [h for h in hosts if h.address == live_servers[0].ip_addr][0]

    # Ensure we keep sending writes only to dc1 for now. At the end of the test, if remove_dead_nodes_with == "remove",
    # we add 3 new nodes to dc2 and increase RF in dc2 from 0 to 3. We increase RF by 1 in each of the 3 steps. When
    # we increase RF from 0 to 1 in the first step, writes send to dc2 would fail (RF=1, only 1 pending replica in dc2,
    # but CL=LOCAL_QUORUM requires 1 normal replica). Writes would also fail after the second step (RF=2, 1 normal
    # replica, 1 pending replica, CL=LOCAL_QUORUM requires 2 normal replicas). So, we can start sending writes to dc2
    # only after increasing RF to 3, which we do - see finish_writes_dc2.
    dc1_cql = cluster_con(
            [srv.ip_addr for srv in live_servers], 9042, False,
            load_balancing_policy=WhiteListRoundRobinPolicy([srv.ip_addr for srv in live_servers])).connect()
    finish_writes_dc1 = await start_writes(dc1_cql, rf, ConsistencyLevel.LOCAL_QUORUM, concurrency=3, ks_name=ks_name)

    new_servers: list[ServerInfo] = []
    if remove_dead_nodes_with == "remove":
        # We must mark dead nodes as permanently dead so that they are ignored in topology commands. Without this step,
        # ALTER KEYSPACE below would fail on the global token metadata barrier.
        # For now, we do not have a specific API to mark nodes as dead, so we use a workaround.
        # FIXME: use the specific API once scylladb/scylladb#21281 is fixed.
        logging.info(f'Marking {dead_servers} as permanently dead')
        await manager.remove_node(live_servers[0].server_id, dead_servers[0].server_id,
                                  [dead_srv.ip_addr for dead_srv in dead_servers[1:]],
                                  expected_error='Removenode failed')

        logging.info(f'Decreasing RF of {ks_name} to 0 in dc2')
        for i in range(1, rf + 1):
            # ALTER KEYSPACE with tablets can decrease RF only by one.
            # Send requests to the recovery leader (which is the topology coordinator) to avoid rejection on
            # "Another global topology request is ongoing, please retry.". Other nodes could receive a second request
            # before updating the topology state after the first request.
            await cql.run_async(f"""ALTER KEYSPACE {ks_name} WITH replication =
                                {{'class': 'NetworkTopologyStrategy', 'dc1': {rf}, 'dc2': {rf - i}}}""",
                                host=recovery_leader_host)

        logging.info(f'Removing {dead_servers}')
        for i, being_removed in enumerate(dead_servers):
            ignored = [dead_srv.ip_addr for dead_srv in dead_servers[i + 1:]]
            initiator = live_servers[i]
            await manager.remove_node(initiator.server_id, being_removed.server_id, ignored)
    else:
        logging.info(f'Replacing {dead_servers}')
        for i, being_replaced in enumerate(dead_servers):
            replace_cfg = ReplaceConfig(replaced_id=being_replaced.server_id, reuse_ip_addr=False, use_host_id=True,
                                        ignore_dead_nodes=[dead_srv.ip_addr for dead_srv in dead_servers[i + 1:]])
            new_servers.append(await manager.server_add(replace_cfg=replace_cfg, config=cfg, property_file=property_file_dc2))

    logging.info(f'Unsetting the recovery_leader config option on {live_servers}')
    for srv in live_servers:
        await manager.server_update_config(srv.server_id, 'recovery_leader', '')

    logging.info(f'Deleting persistent data of group 0 {first_group0_id} on {live_servers}')
    for h in hosts:
        await delete_raft_group_data(first_group0_id, cql, h)

    # Disable load balancer on the topology coordinator node so that an ongoing tablet migration doesn't fail one of the
    # check_system_topology_and_cdc_generations_v3_consistency calls below. A tablet migration can suddenly make
    # version or fence_version inconsistent among nodes.
    await manager.api.disable_tablet_balancing(live_servers[0].ip_addr)

    cql, hosts = await manager.get_ready_cql(live_servers + new_servers)

    logging.info('Performing consistency checks after the recovery procedure')
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)
    await check_token_ring_and_group0_consistency(manager)
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts, ignored_hosts=dead_hosts)

    new_servers_num = 3 if remove_dead_nodes_with == "remove" else 1
    logging.info(f'Adding {new_servers_num} new servers to dc2')
    new_servers += await manager.servers_add(new_servers_num, config=cfg, property_file=property_file_dc2)

    cql, hosts = await manager.get_ready_cql(live_servers + new_servers)

    if remove_dead_nodes_with == "remove":
        logging.info(f'Increasing RF of {ks_name} back to {rf} in dc2')
        for i in range(1, rf + 1):
            await cql.run_async(f"""ALTER KEYSPACE {ks_name} WITH replication =
                                {{'class': 'NetworkTopologyStrategy', 'dc1': {rf}, 'dc2': {i}}}""",
                                host=recovery_leader_host)

    # After increasing RF back to 3 in dc2 (if remove_dead_nodes_with == "remove"), we can start sending writes to dc2.
    dc2_cql = cluster_con(
            [srv.ip_addr for srv in new_servers], 9042, False,
            load_balancing_policy=WhiteListRoundRobinPolicy([srv.ip_addr for srv in new_servers])).connect()
    finish_writes_dc2 = await start_writes(dc2_cql, rf, ConsistencyLevel.LOCAL_QUORUM, concurrency=3, ks_name=ks_name)

    # Send some writes to dc2.
    logging.info('Sleeping for 1 s')
    await asyncio.sleep(1)

    logging.info(f'Performing consistency checks after adding {new_servers}')
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)
    await check_token_ring_and_group0_consistency(manager)
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts, ignored_hosts=dead_hosts)

    await finish_writes_dc1()
    await finish_writes_dc2()
