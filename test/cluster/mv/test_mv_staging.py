#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode
from test.pylib.util import wait_for_view
from test.pylib.internal_types import ServerInfo
from test.cluster.util import new_test_keyspace, wait_for_cql_and_get_hosts

import asyncio
import logging
import os
import time
import re
import pytest

logger = logging.getLogger(__name__)


async def assert_row_count_on_host(cql, host, ks, table, row_count):
    stmt = SimpleStatement(f"SELECT * FROM {ks}.{table}", consistency_level = ConsistencyLevel.LOCAL_ONE)
    rows = await cql.run_async(stmt, host=host)
    assert len(rows) == row_count

async def get_table_dir(manager: ManagerClient, server: ServerInfo, ks: str, table: str):
    workdir = await manager.server_get_workdir(server.server_id)
    ks_dir = os.path.join(workdir, "data", ks)

    table_pattern = re.compile(f"{table}-")
    for root, dirs, files in os.walk(ks_dir):
        for d in dirs:
            if table_pattern.match(d):
                return os.path.join(root, d)

async def delete_table_sstables(manager: ManagerClient, server: ServerInfo, ks: str, table: str):
    table_dir = await get_table_dir(manager, server, ks, table)
    for root, dirs, files in os.walk(table_dir):
        for file in files:
            path = os.path.join(root, file)
            os.remove(path)
        break # break unconditionally here to remove only files in `table_dir`

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_staging_backlog_processed_after_restart(manager: ManagerClient):
    """
    Verifies that staging sstables are processed after node restart.
    """

    node_count = 2
    servers = await manager.servers_add(node_count, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v int, PRIMARY KEY (key))")

        # Populate the base table
        rows = 1000
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.tab (key, c, v) VALUES ({i}, {i}, 1)") for i in range(rows)])

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.tab "
                            "WHERE key IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, key)")
        await wait_for_view(cql, 'mv', node_count)

        # Flush on node0
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "tab")
        await manager.api.keyspace_flush(servers[0].ip_addr, ks, "mv")

        # Delete sstables
        await delete_table_sstables(manager, servers[0], ks, "tab")
        await delete_table_sstables(manager, servers[0], ks, "mv")

        # Restart node0
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.server_start(servers[0].server_id)

        # Assert that node0 has no data for base table and MV
        hosts = await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)
        await manager.server_stop_gracefully(servers[1].server_id)
        await assert_row_count_on_host(cql, hosts[0], ks, "tab", 0)
        await assert_row_count_on_host(cql, hosts[0], ks, "mv", 0)
        await manager.server_start(servers[1].server_id)

        # Repair the base table
        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()
        await manager.api.enable_injection(servers[0].ip_addr, "view_update_generator_consume_staging_sstable", one_shot=False)
        await manager.api.repair(servers[0].ip_addr, ks, "tab")
        await s0_log.wait_for(f"Processing {ks} failed for table tab", from_mark=s0_mark, timeout=60)
        await s0_log.wait_for(f"Finished user-requested repair for vnode keyspace={ks}", from_mark=s0_mark, timeout=60)

        # Assert view backlog on server 0
        await manager.server_stop_gracefully(servers[1].server_id)
        await assert_row_count_on_host(cql, hosts[0], ks, "tab", 1000)
        await assert_row_count_on_host(cql, hosts[0], ks, "mv", 0)
        await manager.server_start(servers[1].server_id)

        # Restart node0 with staging backlog
        s0_mark = await s0_log.mark()
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.server_start(servers[0].server_id)

        await s0_log.wait_for(f"Processed {ks}.tab", from_mark=s0_mark, timeout=60)
        hosts = await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)
        await assert_row_count_on_host(cql, hosts[0], ks, "tab", 1000)
        await assert_row_count_on_host(cql, hosts[0], ks, "mv", 1000)
