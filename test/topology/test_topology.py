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

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_add_server_add_column(manager, random_tables):
    """Add a node and then add a column to a table and verify"""
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
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
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
@pytest.mark.skip(reason="Flaky due to #11780")
async def test_decommission_node_add_column(manager, random_tables):
    """Add a node, remove an original node, add a column"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
    await manager.decommission_node(servers[1].server_id)             # Decommission [1]
    await table.add_column()
    await random_tables.verify_schema()


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
