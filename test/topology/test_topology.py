#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import pytest
import logging
import asyncio
import random
import time

from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from cassandra.cluster import Session
from test.pylib.random_tables import RandomTables
from test.pylib.rest_client import inject_error_one_shot

logger = logging.getLogger(__name__)


async def get_token_ring_host_ids(manager: ManagerClient, srv: ServerInfo) -> set[str]:
    """Get the host IDs of normal token owners known by `srv`."""
    token_endpoint_map = await manager.api.client.get_json("/storage_service/tokens_endpoint", srv.ip_addr)
    normal_endpoints = {e["value"] for e in token_endpoint_map}
    logger.info(f"Normal endpoints' IPs by {srv}: {normal_endpoints}")
    host_id_map = await manager.api.client.get_json('/storage_service/host_id', srv.ip_addr)
    all_host_ids = {e["value"] for e in host_id_map}
    logger.info(f"All host IDs by {srv}: {all_host_ids}")
    normal_host_ids = {e["value"] for e in host_id_map if e["key"] in normal_endpoints}
    logger.info(f"Normal endpoints' host IDs by {srv}: {normal_host_ids}")
    return normal_host_ids


async def get_current_group0_config(manager: ManagerClient, srv: ServerInfo) -> set[tuple[str, bool]]:
    """Get the current Raft group 0 configuration known by `srv`.
       The first element of each tuple is the Raft ID of the node (which is equal to the Host ID),
       the second element indicates whether the node is a voter.
     """
    assert(manager.cql)
    host = (await wait_for_cql_and_get_hosts(manager.cql, [srv], time.time() + 60))[0]
    group0_id = (await manager.cql.run_async(
        "select value from system.scylla_local where key = 'raft_group0_id'",
        host=host))[0].value
    config = await manager.cql.run_async(
        f"select server_id, can_vote from system.raft_state where group_id = {group0_id} and disposition = 'CURRENT'",
        host=host)
    result = {(str(m.server_id), bool(m.can_vote)) for m in config}
    logger.info(f"Group 0 members by {srv}: {result}")
    return result


async def check_token_ring_and_group0_consistency(manager: ManagerClient) -> None:
    """Ensure that the normal token owners and group 0 members match
       according to each currently running server.
    """
    servers = await manager.running_servers()
    for srv in servers:
        group0_members = await get_current_group0_config(manager, srv)
        group0_ids = {m[0] for m in group0_members}
        token_ring_ids = await get_token_ring_host_ids(manager, srv)
        assert token_ring_ids == group0_ids


async def wait_for_token_ring_and_group0_consistency(manager: ManagerClient, deadline: float) -> None:
    """Weaker version of the above check; the token ring is not immediately updated after
    bootstrap/replace/decommission - the normal tokens of the new node propagate through gossip.
    Take this into account and wait for the equality condition to hold, with a timeout.
    """
    servers = await manager.running_servers()
    for srv in servers:
        group0_members = await get_current_group0_config(manager, srv)
        group0_ids = {m[0] for m in group0_members}
        async def token_ring_matches():
            token_ring_ids = await get_token_ring_host_ids(manager, srv)
            diff = token_ring_ids ^ group0_ids
            if diff:
                logger.warning(f"Group 0 members and token ring members don't yet match" \
                               f" according to {srv}, symmetric difference: {diff}")
                return None
            return True
        await wait_for(token_ring_matches, deadline, period=.5)


@pytest.mark.asyncio
async def test_add_server_add_column(manager, random_tables):
    """Add a node and then add a column to a table and verify"""
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_stop_server_add_column(manager, random_tables):
    """Add a node, stop an original node, add a column"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
    await manager.server_stop(servers[1].server_id)
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_restart_server_add_column(manager, random_tables):
    """Add a node, stop an original node, add a column"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    ret = await manager.server_restart(servers[1].server_id)
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_remove_node_add_column(manager, random_tables):
    """Add a node, remove an original node, add a column"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
    await manager.server_stop_gracefully(servers[1].server_id)              # stop     [1]
    await manager.remove_node(servers[0].server_id, servers[1].server_id)   # Remove   [1]
    await check_token_ring_and_group0_consistency(manager)
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_decommission_node_add_column(manager, random_tables):
    """Add a node, remove an original node, add a column"""
    table = await random_tables.add_table(ncolumns=5)
    servers = await manager.running_servers()
    decommission_target = servers[1]
    # The sleep injections significantly increase the probability of reproducing #11780:
    # 1. bootstrapped_server finishes bootstrapping and enters NORMAL state
    # 2. decommission_target starts storage_service::handle_state_normal(bootstrapped_server),
    #    enters sleep before calling storage_service::notify_joined
    # 3. we start decommission on decommission_target
    # 4. decommission_target sends node_ops_verb with decommission_prepare request to bootstrapped_server
    # 5. bootstrapped_server receives the RPC and enters sleep
    # 6. decommission_target handle_state_normal wakes up,
    #    calls storage_service::notify_joined which drops some RPC clients
    # 7. If #11780 is not fixed, this will fail the node_ops_verb RPC, causing decommission to fail
    await manager.api.enable_injection(
        decommission_target.ip_addr, 'storage_service_notify_joined_sleep', one_shot=True)
    bootstrapped_server = await manager.server_add()
    async def no_joining_nodes():
        joining_nodes = await manager.api.get_joining_nodes(decommission_target.ip_addr)
        return not joining_nodes
    # Wait until decommission_target thinks that bootstrapped_server is NORMAL
    # note: when this wait finishes, we're usually in the middle of storage_service::handle_state_normal
    await wait_for(no_joining_nodes, time.time() + 30, period=.1)
    await manager.api.enable_injection(
        bootstrapped_server.ip_addr, 'storage_service_decommission_prepare_handler_sleep', one_shot=True)
    await manager.decommission_node(decommission_target.server_id)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    await table.add_column()
    await random_tables.verify_schema()

@pytest.mark.asyncio
async def test_replace_different_ip(manager: ManagerClient, random_tables) -> None:
    servers = await manager.running_servers()
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = False)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

@pytest.mark.asyncio
async def test_replace_different_ip_using_host_id(manager: ManagerClient, random_tables) -> None:
    servers = await manager.running_servers()
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = True)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

@pytest.mark.asyncio
async def test_replace_reuse_ip(manager: ManagerClient, random_tables) -> None:
    servers = await manager.running_servers()
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True, use_host_id = False)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

@pytest.mark.asyncio
async def test_replace_reuse_ip_using_host_id(manager: ManagerClient, random_tables) -> None:
    servers = await manager.running_servers()
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True, use_host_id = True)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)


# Checks basic functionality on the cluster with different values of the --smp parameter on the nodes.
@pytest.mark.asyncio
async def test_nodes_with_different_smp(manager: ManagerClient, random_tables: RandomTables) -> None:
    # In this test it's more convenient to start with a fresh cluster.
    # We don't need the default nodes,
    # but there is currently no way to express this in the test infrastructure

    # When the node starts it tries to communicate with others
    # by sending group0_peer_exchange message to them.
    # This message can be handled on arbitrary shard of the target node.
    # The method manager.server_add waits for node to start, which can only happen
    # if this message has been handled correctly.
    #
    # Note: messaging_service is initialized with server_socket::load_balancing_algorithm::port
    # policy, this means that the shard for message will be chosen as client_port % smp::count.
    # The client port in turn is chosen as rand() * smp::count + current_shard
    # (posix_socket_impl::find_port_and_connect).
    # If this succeeds to occupy a free port in 5 tries and smp::count is the same
    # on both nodes, then it's guaranteed that the message will be
    # processed on the same shard as the calling code.
    # In the general case, we cannot assume that this same shard guarantee holds.
    logger.info(f'Adding --smp=3 server')
    await manager.server_add(cmdline=['--smp', '3'])

    # Remove the original 3 servers, the problem is easier to reproduce with --smp values
    # that we pick, not the (currently) default --smp=2 coming from the suite.
    logger.info(f'Decommissioning old servers')
    servers = await manager.running_servers()
    for s in servers[:-1]:
        await manager.decommission_node(s.server_id)
        await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    logger.info(f'Adding --smp=4 server')
    await manager.server_add(cmdline=['--smp', '4'])
    logger.info(f'Adding --smp=5 server')
    await manager.server_add(cmdline=['--smp', '5'])

    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    logger.info(f'Creating new tables')
    await random_tables.add_tables(ntables=4, ncolumns=5)
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_remove_garbage_group0_members(manager: ManagerClient, random_tables):
    """
    Verify that failing to leave group 0 or remove a node from group 0 in removenode/decommission
    can be handled by executing removenode (which should clear the 'garbage' group 0 member),
    even though the node is no longer a token ring member.
    """
    # 4 servers, one dead
    await manager.server_add()
    servers = await manager.running_servers()
    removed_host_id = await manager.get_host_id(servers[0].server_id)
    await manager.server_stop_gracefully(servers[0].server_id)

    logging.info(f'removenode {servers[0]} using {servers[1]}')
    # removenode will fail after removing the server from the token ring,
    # but before removing it from group 0
    await inject_error_one_shot(manager.api, servers[1].ip_addr,
                                'removenode_fail_before_remove_from_group0')
    try:
        await manager.remove_node(servers[1].server_id, servers[0].server_id)
    except Exception:
        # Note: the exception returned here is only '500 internal server error',
        # need to look in test.py log for the actual message coming from Scylla.
        logging.info(f'expected exception during injection')

    # Query the storage_service/host_id endpoint to calculate a list of known token ring members' Host IDs
    # (internally, this endpoint uses token_metadata)
    token_ring_ids = await get_token_ring_host_ids(manager, servers[1])
    logging.info(f'token ring members: {token_ring_ids}')

    group0_members = await get_current_group0_config(manager, servers[1])
    logging.info(f'group 0 members: {group0_members}')
    group0_ids = {m[0] for m in group0_members}

    # Token ring members should currently be a subset of group 0 members
    assert token_ring_ids <= group0_ids

    garbage_members = group0_ids - token_ring_ids
    logging.info(f'garbage members: {garbage_members}')
    assert len(garbage_members) == 1
    garbage_member = next(iter(garbage_members))

    # The garbage member is the one that we failed to remove
    assert garbage_member == removed_host_id

    # Verify that at least it's a non-voter.
    assert garbage_member in {m[0] for m in group0_members if not m[1]}

    logging.info(f'removenode {servers[0]} using {servers[1]} again')
    # Retry removenode. It should skip the token ring removal step and remove the server from group 0.
    await manager.remove_node(servers[1].server_id, servers[0].server_id)

    group0_members = await get_current_group0_config(manager, servers[1])
    logging.info(f'group 0 members: {group0_members}')
    group0_ids = {m[0] for m in group0_members}

    # Token ring members and group 0 members should now be the same.
    assert token_ring_ids == group0_ids

    # Verify that availability is not reduced.
    # Stop one of the 3 remaining servers and try to remove it. It should succeed with only 2 servers.

    logging.info(f'stop {servers[1]}')
    await manager.server_stop_gracefully(servers[1].server_id)

    logging.info(f'removenode {servers[1]} using {servers[2]}')
    await manager.remove_node(servers[2].server_id, servers[1].server_id)

    # Perform a similar scenario with decommission. One of the node fails to decommission fully,
    # but it manages to leave the token ring. We observe the leftovers using the same APIs as above
    # and remove the leftovers.
    # We can do this with only 2 nodes because during decommission we become a non-voter before
    # leaving the token ring, thus the remaining single node will become a voting majority
    # and will be able to perform removenode alone.

    decommissioned_host_id = await manager.get_host_id(servers[2].server_id)
    await manager.api.enable_injection(
        servers[2].ip_addr, 'decommission_fail_before_leave_group0', one_shot=True)
    logging.info(f'decommission {servers[2]}')
    try:
        await manager.decommission_node(servers[2].server_id)
    except Exception:
        logging.info(f'expected exception during injection')
    logging.info(f'stop {servers[2]}')
    await manager.server_stop_gracefully(servers[2].server_id)

    token_ring_ids = await get_token_ring_host_ids(manager, servers[3])
    logging.info(f'token ring members: {token_ring_ids}')

    group0_members = await get_current_group0_config(manager, servers[3])
    logging.info(f'group 0 members: {group0_members}')
    group0_ids = {m[0] for m in group0_members}

    assert token_ring_ids <= group0_ids

    garbage_members = group0_ids - token_ring_ids
    logging.info(f'garbage members: {garbage_members}')
    assert len(garbage_members) == 1
    garbage_member = next(iter(garbage_members))

    assert garbage_member == decommissioned_host_id
    assert garbage_member in {m[0] for m in group0_members if not m[1]}

    logging.info(f'removenode {servers[2]} using {servers[3]}')
    await manager.remove_node(servers[3].server_id, servers[2].server_id)

    await check_token_ring_and_group0_consistency(manager)


@pytest.mark.asyncio
@pytest.mark.skip(reason="Wait for @slow attribute, #11713")
async def test_remove_node_with_concurrent_ddl(manager, random_tables):
    stopped = False
    ddl_failed = False

    async def do_ddl():
        nonlocal ddl_failed
        iteration = 0
        while not stopped:
            logger.debug(f'ddl, iteration {iteration} started')
            try:
                # If the node was removed, the driver may retry "create table" on another node,
                # but the request might have already been completed.
                # The same applies to drop_table.

                await random_tables.add_tables(5, 5, if_not_exists=True)
                await random_tables.verify_schema()
                while len(random_tables.tables) > 0:
                    await random_tables.drop_table(random_tables.tables[-1], if_exists=True)
                logger.debug(f'ddl, iteration {iteration} finished')
            except:
                logger.exception(f'ddl, iteration {iteration} failed')
                ddl_failed = True
                raise
            iteration += 1

    async def do_remove_node():
        for i in range(10):
            logger.debug(f'do_remove_node [{i}], iteration started')
            if ddl_failed:
                logger.debug(f'do_remove_node [{i}], ddl failed, exiting')
                break
            server_ids = await manager.running_servers()
            host_ids = await asyncio.gather(*(manager.get_host_id(s) for s in server_ids))
            initiator_index, target_index = random.sample(range(len(server_ids)), 2)
            initiator_ip = server_ids[initiator_index]
            target_ip = server_ids[target_index]
            target_host_id = host_ids[target_index]
            logger.info(f'do_remove_node [{i}], running remove_node, '
                        f'initiator server [{initiator_ip}], target ip [{target_ip}], '
                        f'target host id [{target_host_id}]')
            await manager.wait_for_host_known(initiator_ip, target_host_id)
            logger.info(f'do_remove_node [{i}], stopping target server [{target_ip}], host_id [{target_host_id}]')
            await manager.server_stop_gracefully(target_ip)
            logger.info(f'do_remove_node [{i}], target server [{target_ip}] stopped, '
                        f'waiting for it to be down on [{initiator_ip}]')
            await manager.wait_for_host_down(initiator_ip, target_ip)
            logger.info(f'do_remove_node [{i}], invoking remove_node')
            await manager.remove_node(initiator_ip, target_ip, target_host_id)
            # TODO: check that group 0 no longer contains the removed node (#12153)
            logger.info(f'do_remove_node [{i}], remove_node done')
            new_server_ip = await manager.server_add()
            logger.info(f'do_remove_node [{i}], server_add [{new_server_ip}] done')
            logger.info(f'do_remove_node [{i}], iteration finished')

    ddl_task = asyncio.create_task(do_ddl())
    try:
        await do_remove_node()
    finally:
        logger.debug("do_remove_node finished, waiting for ddl fiber")
        stopped = True
        await ddl_task
        logger.debug("ddl fiber done, finished")


@pytest.mark.asyncio
async def test_start_after_sudden_stop(manager: ManagerClient, random_tables) -> None:
    """Tests a server can rejoin the cluster after being stopped suddenly"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_stop(servers[0].server_id)
    await table.add_column()
    await manager.server_start(servers[0].server_id)
    await random_tables.verify_schema()
