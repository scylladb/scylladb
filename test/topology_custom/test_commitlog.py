#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest
import logging
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver
from test.topology.conftest import skip_mode
from test.pylib.random_tables import Column, TextType

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_reboot(request, manager: ManagerClient):
    # Check that commitlog provides durability in case of a node reboot.
    #
    # 1. start a node with commitlog_sync: batch
    # 2. create a test table t
    # 3. write some data to the table and truncate it, now a truncation record should be created
    # 4. cleanly restart a node with enabled injection decrease_schema_commitlog_base_segment_id,
    #    this allows to set new segment id to zero, which imitates machine reboot
    # 5. write some data into t
    # 6. save its current content in local variable
    # 7. kill -9 the node and restart it
    # 8. check the table content is preserved

    server_info = await manager.server_add(config={
        'commitlog_sync': 'batch'
    })
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)
    logger.info("Node started")

    random_tables = RandomTables(request.node.name, manager, "ks", 1)
    await random_tables.add_table(name='t', pks=1, columns=[
        Column(name="key", ctype=TextType),
        Column(name="value", ctype=TextType)
    ])
    logger.info("Test table created")

    async def load_table() -> list:
        result = await cql.run_async("select * from ks.t")
        result.sort()
        return result
    def save_table(k: str, v: str):
        return cql.run_async("insert into ks.t(key, value) values (%s, %s)",
                             parameters = [k, v])

    test_rows_count = 10
    futures = []
    for i in range(0, test_rows_count):
        futures.append(save_table(f'key_{i}', f'value_{i}'))
    await asyncio.gather(*futures)
    logger.info("Some data is written into test table")

    await cql.run_async("truncate table ks.t")
    logger.info("Test table is truncated")

    await manager.server_stop_gracefully(server_info.server_id)
    await manager.server_update_config(server_info.server_id,
                                       'error_injections_at_startup',
                                       ['decrease_commitlog_base_segment_id'])
    await manager.server_start(server_info.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)
    logging.info("Node is restarted with decrease_schema_commitlog_base_segment_id injection")

    futures = []
    for i in range(0, test_rows_count):
        futures.append(save_table(f'new_key_{i}', f'new_value_{i}'))
    await asyncio.gather(*futures)
    logger.info("Some new data is written into test table")

    table_content_before_crash = await load_table()
    await manager.server_stop(server_info.server_id)
    await manager.server_start(server_info.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)
    logging.info("Node is killed and restarted")

    table_content_after_crash = await load_table()
    logging.info(f"table content before crash [{table_content_before_crash}], "
                 f"after crash [{table_content_after_crash}]")
    assert table_content_before_crash == table_content_after_crash
