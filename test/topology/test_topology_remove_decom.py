#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import logging
import asyncio
import random
import time
from test.pylib.util import wait_for
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.topology.util import check_token_ring_and_group0_consistency,            \
                               wait_for_token_ring_and_group0_consistency
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_remove_node_add_column(manager: ManagerClient, random_tables: RandomTables):
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
async def test_decommission_node_add_column(manager: ManagerClient, random_tables: RandomTables):
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
@pytest.mark.skip(reason="Wait for @slow attribute, #11713")
async def test_remove_node_with_concurrent_ddl(manager: ManagerClient, random_tables: RandomTables):
    stopped = False
    ddl_failed = False

    async def do_ddl() -> None:
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

    async def do_remove_node() -> None:
        for i in range(10):
            logger.debug(f'do_remove_node [{i}], iteration started')
            if ddl_failed:
                logger.debug(f'do_remove_node [{i}], ddl failed, exiting')
                break
            server_infos = await manager.running_servers()
            host_ids = await asyncio.gather(*(manager.get_host_id(s.server_id) for s in server_infos))
            initiator_index, target_index = random.sample(range(len(server_infos)), 2)
            initiator_info = server_infos[initiator_index]
            target_info = server_infos[target_index]
            target_host_id = host_ids[target_index]
            logger.info(f'do_remove_node [{i}], running remove_node, '
                        f'initiator server [{initiator_info.ip_addr}], target ip [{target_info.ip_addr}], '
                        f'target host id [{target_host_id}]')
            await manager.wait_for_host_known(initiator_info.ip_addr, target_host_id)
            logger.info(f'do_remove_node [{i}], stopping target server [{target_info.ip_addr}], host_id [{target_host_id}]')
            await manager.server_stop_gracefully(target_info.server_id)
            logger.info(f'do_remove_node [{i}], invoking remove_node')
            await manager.remove_node(initiator_info.server_id, target_info.server_id, [target_info.ip_addr])
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
async def test_rebuild_node(manager: ManagerClient, random_tables: RandomTables):
    """rebuild a node"""
    servers = await manager.running_servers()
    await manager.rebuild_node(servers[0].server_id)
    await check_token_ring_and_group0_consistency(manager)
