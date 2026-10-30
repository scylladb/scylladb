#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import time
import pytest

from cassandra.cluster import ConsistencyLevel
from cassandra.policies import WhiteListRoundRobinPolicy

from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import gather_safely, unique_name, wait_for_cql_and_get_hosts
from test.cluster.util import check_system_topology_and_cdc_generations_v3_consistency, \
        check_token_ring_and_group0_consistency, delete_discovery_state_and_group0_id, delete_raft_group_data, \
        reconnect_driver, start_writes, wait_for_cdc_generations_publishing


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("remove_dead_nodes_with", ["remove", "replace"])
async def test_raft_recovery_user_data(manager: ScyllaClusterManager, remove_dead_nodes_with: str):
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
    # Workaround for flakiness from https://github.com/scylladb/scylladb/issues/23565.
    cfg = {'hinted_handoff_enabled': False}

    # Add servers to dc2 first, so 3 out of 5 voters will be there.
    logging.info('Adding servers that will be killed to dc2')
    dead_servers = await manager.servers_add(3, config=cfg, auto_rack_dc='dc2')
    logging.info('Adding servers that will survive majority loss to dc1')
    live_servers = await manager.servers_add(3, config=cfg, auto_rack_dc='dc1')
    logging.info(f'Servers to survive majority loss: {live_servers}, servers to be killed: {dead_servers}')

    cql, _ = await manager.get_ready_cql(live_servers + dead_servers)
    hosts = await wait_for_cql_and_get_hosts(cql, live_servers, time.time() + 60)
    dead_hosts = await wait_for_cql_and_get_hosts(cql, dead_servers, time.time() + 60)
    dead_host_ids = await gather_safely(*(manager.get_host_id(srv.server_id) for srv in dead_servers))

    # When table audit is enabled, Scylla creates the "audit" keyspace with
    # NetworkTopologyStrategy. During remove_node, streaming fails for the audit keyspace
    # with "zero replica after the removal" when all nodes from dc2 are removed.
    # By setting RF=3 only in dc1, we ensure the audit data stays on the surviving nodes.
    # Only alter if the audit keyspace exists (it might not exist if audit is disabled).
    result = await cql.run_async("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'audit'")
    if result:
        await cql.run_async("ALTER KEYSPACE audit WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': 3}")

    first_group0_id = (await cql.run_async(
            "SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'"))[0].value

    rf: int = 3
    ks_name = unique_name()
    # Use a separate CQL connection for the write workload as `cql` must be reconnected below to prevent hitting
    # https://github.com/scylladb/python-driver/issues/295.
    ccluster_all_nodes = manager.con_gen([srv.ip_addr for srv in live_servers + dead_servers])
    cql_all_nodes = ccluster_all_nodes.connect()
    await wait_for_cql_and_get_hosts(cql_all_nodes, live_servers + dead_servers, time.time() + 60)
    finish_writes = await start_writes(cql_all_nodes, rf, ConsistencyLevel.LOCAL_QUORUM, concurrency=5,
                                       ks_name=ks_name, node_shutdowns=True)

    # Send some writes before we kill nodes.
    logging.info('Sleeping for 1 s')
    await asyncio.sleep(1)

    logging.info(f'Killing {dead_servers}')
    await gather_safely(*(manager.server_stop(server_id=srv.server_id, convict=False) for srv in dead_servers))

    logging.info('Checking that group 0 has no majority')
    with pytest.raises(Exception, match="raft operation \\[read_barrier\\] timed out"):
        await read_barrier(manager.api, live_servers[0].ip_addr, timeout=2)

    logging.info('Starting the recovery procedure')

    logging.info(f'Restarting {live_servers}')
    await manager.rolling_restart(live_servers)

    await reconnect_driver(manager)
    cql, _ = await manager.get_ready_cql(live_servers)

    logging.info(f'Deleting the persistent discovery state and group 0 ID on {live_servers}')
    await gather_safely(*(delete_discovery_state_and_group0_id(cql, h) for h in hosts))

    # FIXME: use the API to find the recovery leader here when it is implemented. A background operation like a tablet
    # migration could change the group 0 state just before losing the majority. Then, node 0 could be an incorrect
    # recovery leader because there could be another node in dc1 with a newer group 0 state.
    recovery_leader_id = await manager.get_host_id(live_servers[0].server_id)

    async def set_recovery_leader(srv: ServerInfo):
        await manager.server_update_config(srv.server_id, 'recovery_leader', recovery_leader_id)

    logging.info(f'Restarting {live_servers} with recovery leader {live_servers[0].server_id}')
    await manager.rolling_restart(live_servers, with_down=set_recovery_leader)

    await reconnect_driver(manager)
    cql, hosts = await manager.get_ready_cql(live_servers)

    # Ensure we keep sending writes only to dc1 for now. At the end of the test, if remove_dead_nodes_with == "remove",
    # we add 3 new nodes to dc2 and increase RF in dc2 from 0 to 3. We increase RF by 1 in each of the 3 steps. When
    # we increase RF from 0 to 1 in the first step, writes send to dc2 would fail (RF=1, only 1 pending replica in dc2,
    # but CL=LOCAL_QUORUM requires 1 normal replica). Writes would also fail after the second step (RF=2, 1 normal
    # replica, 1 pending replica, CL=LOCAL_QUORUM requires 2 normal replicas). So, we can start sending writes to dc2
    # only after increasing RF to 3, which we do - see finish_writes_dc2.
    await finish_writes()
    safe_driver_shutdown(ccluster_all_nodes)
    ccluster_dc1 = manager.con_gen(
            [srv.ip_addr for srv in live_servers],
            load_balancing_policy=WhiteListRoundRobinPolicy([srv.ip_addr for srv in live_servers]))
    dc1_cql = ccluster_dc1.connect()
    finish_writes_dc1 = await start_writes(dc1_cql, rf, ConsistencyLevel.LOCAL_QUORUM, concurrency=3, ks_name=ks_name)

    # Mark dead nodes as permanently dead so that they are ignored in topology commands. For remove, without this step,
    # ALTER KEYSPACE below would fail on the global token metadata barrier. For replace, it lets every replace request
    # ignore the dead nodes replaced by the other requests, so the requests can run concurrently.
    logging.info(f'Marking {dead_servers} as permanently dead')
    await manager.api.exclude_node(live_servers[0].ip_addr, dead_host_ids)

    new_servers: list[ServerInfo] = []
    if remove_dead_nodes_with == "remove":
        logging.info(f'Decreasing RF of {ks_name} to 0 in dc2')
        await cql.run_async(f"""ALTER KEYSPACE {ks_name} WITH replication =
                            {{'class': 'NetworkTopologyStrategy', 'dc1': {rf}, 'dc2': 0}}""")

        # Remove concurrently: distinct dead nodes can be removed in parallel (see
        # test_concurrent_removenode_two_initiators_two_dead_nodes in test_topology_remove_decom.py),
        # as long as each request ignores every other not-yet-removed dead node.
        logging.info(f'Removing {dead_servers} concurrently')
        await gather_safely(*(manager.remove_node(live_servers[i].server_id, dead_servers[i].server_id, dead_host_ids)
                              for i in range(len(dead_servers))))
    else:
        # Replace dead nodes concurrently. Not recommended for users, but it should work, and it significantly speeds up
        # the test in debug mode.
        async def replace_server(i, being_replaced):
            replace_cfg = ReplaceConfig(replaced_id=being_replaced.server_id, reuse_ip_addr=False, use_host_id=True)
            return await manager.server_add(replace_cfg=replace_cfg, config=cfg,
                                            property_file={'dc': 'dc2', 'rack': f'rack{i + 1}'})

        logging.info(f'Replacing {dead_servers} concurrently')
        new_servers.extend(await gather_safely(*(replace_server(i, srv) for i, srv in enumerate(dead_servers))))

    logging.info(f'Unsetting the recovery_leader config option on {live_servers}')
    await gather_safely(*(manager.server_remove_config_option(srv.server_id, 'recovery_leader') for srv in live_servers))

    logging.info(f'Deleting persistent data of group 0 {first_group0_id} on {live_servers}')
    await gather_safely(*(delete_raft_group_data(first_group0_id, cql, h) for h in hosts))

    # Disable load balancer on the topology coordinator node so that an ongoing tablet migration doesn't fail one of the
    # check_system_topology_and_cdc_generations_v3_consistency calls below. A tablet migration can suddenly make
    # version or fence_version inconsistent among nodes.
    await manager.disable_tablet_balancing()

    cql, hosts = await manager.get_ready_cql(live_servers + new_servers)

    logging.info('Performing consistency checks after the recovery procedure')
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)
    await check_token_ring_and_group0_consistency(manager)
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts, ignored_hosts=dead_hosts)

    new_servers_num = 3 if remove_dead_nodes_with == "remove" else 1
    logging.info(f'Adding {new_servers_num} new servers to dc2')
    new_servers += await manager.servers_add(new_servers_num, config=cfg, auto_rack_dc='dc2')

    # Reconnect the driver as a workaround for https://github.com/scylladb/scylladb/issues/27862.
    await reconnect_driver(manager)
    cql, hosts = await manager.get_ready_cql(live_servers + new_servers)

    if remove_dead_nodes_with == "remove":
        logging.info(f'Increasing RF of {ks_name} back to {rf} in dc2')
        await cql.run_async(f"""ALTER KEYSPACE {ks_name} WITH replication =
                            {{'class': 'NetworkTopologyStrategy', 'dc1': {rf}, 'dc2': {rf}}}""")

    # After increasing RF back to 3 in dc2 (if remove_dead_nodes_with == "remove"), we can start sending writes to dc2.
    ccluster_dc2 = manager.con_gen(
            [srv.ip_addr for srv in new_servers],
            load_balancing_policy=WhiteListRoundRobinPolicy([srv.ip_addr for srv in new_servers]))
    dc2_cql = ccluster_dc2.connect()
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
    safe_driver_shutdown(ccluster_dc1)
    safe_driver_shutdown(ccluster_dc2)



async def test_raft_recovery_user_data_sc(manager: ScyllaClusterManager):
    """
    Test that the Raft-based recovery procedure works correctly with a strongly consistent keyspace.

    1. Start a cluster with two dcs, dc1 and dc2, containing three nodes each. dc2 contains 3 out of 5 group 0 voters.
    2. Create a strongly consistent keyspace with RF=3 in dc1 and RF=0 in dc2, and write some data to it.
    3. Kill all nodes from dc2, causing a permanent group 0 majority loss. The tablets' raft groups lose no member.
    4. Run the recovery procedure to recreate group 0 with nodes from dc1, checking after each step that the keyspace
    holds all the data written so far and accepts writes.
    5. Replace the dead nodes from dc2 and check the keyspace again.

    FIXME: give the keyspace replicas in dc2 and write to it during the rolling restarts, as
    test_raft_recovery_user_data does, once raft snapshot transfer is implemented for strongly consistent tables. Both
    need a snapshot transfer: after the rolling restarts, the replicas in dc1 have no log entries a new replica in dc2
    could catch up from, and a replica restarted before the leader misses the entries the leader drops from its log on
    its own restart.
    test_minority_dc_loss in test_strong_consistency_multidc.py covers a raft group of a strongly consistent tablet
    losing its replicas in a dc.
    """
    cfg = {'experimental_features': ['strongly-consistent-tables']}

    # Add servers to dc2 first, so 3 out of 5 group 0 voters will be there.
    logging.info('Adding servers that will be killed to dc2')
    dead_servers = await manager.servers_add(3, config=cfg, auto_rack_dc='dc2')
    logging.info('Adding servers that will survive majority loss to dc1')
    live_servers = await manager.servers_add(3, config=cfg, auto_rack_dc='dc1')
    logging.info(f'Servers to survive majority loss: {live_servers}, servers to be killed: {dead_servers}')

    cql, _ = await manager.get_ready_cql(live_servers + dead_servers)
    hosts = await wait_for_cql_and_get_hosts(cql, live_servers, time.time() + 60)
    dead_host_ids = await gather_safely(*(manager.get_host_id(srv.server_id) for srv in dead_servers))

    # See test_raft_recovery_user_data for the explanation on why this is needed.
    result = await cql.run_async("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'audit'")
    if result:
        await cql.run_async("ALTER KEYSPACE audit WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': 3}")

    first_group0_id = (await cql.run_async(
            "SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'"))[0].value

    ks = unique_name()
    cf = 'test'
    # FIXME: remove the initial tablet count once tablets of strongly consistent tables can be split and merged.
    # Until then, the tablet count stays fixed.
    await cql.run_async(f"""CREATE KEYSPACE {ks} WITH replication =
                        {{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 0}}
                        AND tablets = {{'initial': 4}} AND consistency = 'global'""")
    await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk int PRIMARY KEY, v int)")
    table_id = await manager.get_table_id(ks, cf)

    async def catch_up():
        """
        Wait until the raft group of every tablet has a leader and the servers from dc1, which host all the tablets,
        have applied everything committed so far.
        """
        group_ids = [str(r.raft_group_id) for r in await manager.get_cql().run_async(
                f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")]
        # Passing a timeout matters: a tablet group defines no default one.
        await gather_safely(*(read_barrier(manager.api, srv.ip_addr, group_id=gid, timeout=60)
                              for srv in live_servers for gid in group_ids))

    # Each step of the test writes its own keys, and reads all keys written so far.
    keys_per_step = 50
    written: dict[int, int] = {}

    async def write_and_check(step: int):
        # Wait until every tablet's raft group has a leader: a restart moves leadership.
        await catch_up()
        # Prepare the statements on every call: the driver session changes after each reconnect_driver.
        cql = manager.get_cql()
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, v) VALUES (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.QUORUM
        select_stmt = cql.prepare(f"SELECT pk, v FROM {ks}.{cf} WHERE pk = ?")
        select_stmt.consistency_level = ConsistencyLevel.QUORUM

        keys = range(step * keys_per_step, (step + 1) * keys_per_step)
        logging.info(f'Writing keys {keys} to {ks}.{cf}')
        await gather_safely(*(cql.run_async(insert_stmt, (k, step)) for k in keys))
        written.update({k: step for k in keys})
        logging.info(f'Reading {ks}.{cf}')
        results = await gather_safely(*(cql.run_async(select_stmt, (k,)) for k in written))
        assert {row.pk: row.v for rows in results for row in rows} == written

    await write_and_check(0)

    logging.info(f'Killing {dead_servers}')
    await gather_safely(*(manager.server_stop(server_id=srv.server_id, convict=False) for srv in dead_servers))

    logging.info('Checking that group 0 has no majority')
    with pytest.raises(Exception, match="raft operation \\[read_barrier\\] timed out"):
        await read_barrier(manager.api, live_servers[0].ip_addr, timeout=2)

    await write_and_check(1)

    logging.info('Starting the recovery procedure')

    logging.info(f'Restarting {live_servers}')
    await manager.rolling_restart(live_servers)

    await reconnect_driver(manager)
    cql, _ = await manager.get_ready_cql(live_servers)
    await write_and_check(2)

    logging.info(f'Deleting the persistent discovery state and group 0 ID on {live_servers}')
    await gather_safely(*(delete_discovery_state_and_group0_id(cql, h) for h in hosts))

    # FIXME: use the API to find the recovery leader here when it is implemented. See test_raft_recovery_user_data.
    recovery_leader = live_servers[0]
    recovery_leader_id = await manager.get_host_id(recovery_leader.server_id)

    async def set_recovery_leader(srv: ServerInfo):
        await manager.server_update_config(srv.server_id, 'recovery_leader', recovery_leader_id)

    logging.info(f'Restarting {live_servers} with recovery leader {recovery_leader.server_id}')
    await manager.rolling_restart(live_servers, with_down=set_recovery_leader)

    await reconnect_driver(manager)
    cql, hosts = await manager.get_ready_cql(live_servers)
    await write_and_check(3)

    # Mark the dead nodes as permanently dead, so that every replace request ignores the dead nodes replaced by the
    # other requests, and the requests can run concurrently.
    logging.info(f'Marking {dead_servers} as permanently dead')
    await manager.api.exclude_node(recovery_leader.ip_addr, dead_host_ids)

    async def replace_server(i, being_replaced):
        replace_cfg = ReplaceConfig(replaced_id=being_replaced.server_id, reuse_ip_addr=False, use_host_id=True)
        return await manager.server_add(replace_cfg=replace_cfg, config=cfg,
                                        property_file={'dc': 'dc2', 'rack': f'rack{i + 1}'})

    logging.info(f'Replacing {dead_servers} concurrently')
    new_servers = list(await gather_safely(*(replace_server(i, srv) for i, srv in enumerate(dead_servers))))

    logging.info(f'Unsetting the recovery_leader config option on {live_servers}')
    await gather_safely(*(manager.server_remove_config_option(srv.server_id, 'recovery_leader') for srv in live_servers))

    logging.info(f'Deleting persistent data of group 0 {first_group0_id} on {live_servers}')
    await gather_safely(*(delete_raft_group_data(first_group0_id, cql, h) for h in hosts))

    await reconnect_driver(manager)
    await manager.get_ready_cql(live_servers + new_servers)
    await write_and_check(4)
