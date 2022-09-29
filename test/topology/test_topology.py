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
import aiohttp


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
    servers = await manager.servers()
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
    await manager.server_stop(servers[1])
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_restart_server_add_column(manager, random_tables):
    """Add a node, stop an original node, add a column"""
    servers = await manager.servers()
    table = await random_tables.add_table(ncolumns=5)
    ret = await manager.server_restart(servers[1])
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_remove_server_add_column(manager, random_tables):
    """Add a node, remove an original node, add a column"""
    servers = await manager.servers()
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
    await manager.server_remove(servers[1])
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_remove_node_add_table(manager, random_tables):
    http_session = aiohttp.ClientSession()

    stopped = False

    async def get_server_api_url(server_id):
        config = await manager.server_get_config(server_id)
        return f'http://{config["api_address"]}:10000/storage_service'

    async def get_host_id(url):
        async with http_session.get(url + '/hostid/local') as resp:
            assert resp.status == 200
            host_id = await resp.text()
        if len(host_id) >= 2 and host_id[0] == '"' and host_id[-1] == '"':
            host_id = host_id[1:-1]
        return host_id

    async def do_ddl():
        iteration = 0
        while not stopped:
            logger.info(f'ddl, iteration {iteration} started')
            await random_tables.add_tables(5, 5)
            await random_tables.verify_schema()
            while len(random_tables.tables) > 0:
                await random_tables.drop_table(random_tables.tables[-1])
            logger.info(f'ddl, iteration {iteration} finished')
            iteration += 1

    async def wait_for_host_id(url, host_id):
        logger.info(f'wait_for_host_id enter, url [{url}], host_id [{host_id}]')
        while True:
            logger.info(f'wait_for_host_id, running query')
            async with await http_session.get(url + '/host_id') as resp:
                resp_body = await resp.text()
                logger.info(f'wait_for_host_id request finished, status {resp.status}, body [{resp_body}]')
                if host_id in resp_body:
                    logger.info(f'wait_for_host_id host_id [{host_id}] found, SUCCESS')
                    break
            await asyncio.sleep(0.05)

    async def remove_node(url, host_id):
        logger.info(f'remove_node request started, url [{url}], host_id [{host_id}]')
        async with await http_session.post(url + '/remove_node', params={'host_id': host_id}) as resp:
            logger.info(f'remove_node request finished, status {resp.status}, text [{await resp.text()}]')
            assert resp.status == 200

    async def main():
        for i in range(10):
            logger.info(f'main [{i}], iteration started')
            server_ids = await manager.servers()
            server_api_urls = []
            host_ids = []
            for server_id in server_ids:
                api_url = await get_server_api_url(server_id)
                server_api_urls.append(api_url)
                host_id = await get_host_id(api_url)
                host_ids.append(host_id)
                logger.info(f'main [{i}], server [{server_id}], api_url [{api_url}], host_id [{host_id}]')
            initiator_index = random.randint(0, len(server_ids) - 1)
            while True:
                removenode_index = random.randint(0, len(server_ids) - 1)
                if removenode_index != initiator_index:
                    break
            logger.info(f'main [{i}], running remove_node, initiator server [{server_api_urls[initiator_index]}], '
                        f'remove_node server [{server_api_urls[removenode_index]}]')
            await wait_for_host_id(server_api_urls[initiator_index], host_ids[removenode_index])
            logger.info(f'stopping target server [{server_ids[removenode_index]}], host_id [{host_ids[removenode_index]}]')
            await manager.server_stop_gracefully(server_ids[removenode_index])
            logger.info(f'target server [{server_ids[removenode_index]}] stopped')
            await remove_node(server_api_urls[initiator_index], host_ids[removenode_index])

            new_server_id = await manager.server_add()
            logger.info(f'main [{i}], server_add [{new_server_id}] done')
            logger.info(f'main [{i}], iteration finished')

    ddl_task = asyncio.create_task(do_ddl())
    await main()
    logger.info("main, finished, waiting for ddl fiber")
    stopped = True
    await ddl_task
    logger.info("main, ddl fiber done, finished")
