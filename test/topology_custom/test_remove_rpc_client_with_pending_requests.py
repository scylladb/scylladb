#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import logging
import time

import pytest
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables, Column, TextType
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_remove_rpc_client_with_pending_requests(request, manager: ManagerClient) -> None:
    # Regression test for #17445

    logger.info("starting first two nodes")
    servers = await manager.servers_add(2)

    logger.info(f"wait_for_cql_and_get_hosts for the first node {servers[0]}")
    host0 = (await wait_for_cql_and_get_hosts(manager.get_cql(), [servers[0]], time.time() + 60))[0]

    logger.info(f"creating test table")
    random_tables = RandomTables(request.node.name, manager, "ks", 2)
    await random_tables.add_table(name='test_table', pks=1, columns=[
        Column(name="key", ctype=TextType),
        Column(name="value", ctype=TextType)
    ])

    logger.info(f"inserting test data")
    test_rows_count = 10
    futures = []
    expected_data = []
    for i in range(0, test_rows_count):
        k = f'key_{i}'
        v = f'value_{i}'
        expected_data.append((k, v))
        futures.append(manager.get_cql().run_async("insert into ks.test_table(key, value) values (%s, %s)",
                                                   parameters=[k, v], host=host0))
    await asyncio.gather(*futures)
    expected_data.sort()

    logger.info(f"adding the third node")
    servers += [await manager.server_add(start=False)]

    logger.info(f"starting the third node [{servers[2]}]")
    third_node_future = asyncio.create_task(manager.server_start(servers[2].server_id))

    logger.info(f"running read requests in a loop while the third node is joining")
    reads_count = 0
    start_time = time.time()
    while not third_node_future.done():
        result_set = await manager.get_cql().run_async(SimpleStatement("select * from ks.test_table",
                                                                       consistency_level=ConsistencyLevel.ALL),
                                                       host=host0)
        actual_data = []
        for row in result_set:
            actual_data.append((row.key, row.value))
        actual_data.sort()
        assert actual_data == expected_data
        reads_count += 1
    finish_time = time.time()
    logger.info(f"done, reads count {reads_count}, took {finish_time - start_time} seconds")

    await third_node_future
