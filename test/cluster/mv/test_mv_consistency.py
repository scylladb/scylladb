#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import logging
import asyncio
import pytest
from test.pylib.util import wait_for_view
from test.cluster.util import new_test_keyspace
from test.cluster.test_view_building_coordinator import delete_table_sstables
from test.cluster.util import reconnect_driver

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_rebuild_materialized_view(manager: ManagerClient) -> None:
    servers = [
        await manager.server_add(config={'tablets_mode_for_new_keyspaces': 'enabled'}, property_file={'dc': 'dc1', 'rack': 'rack1'}),
        await manager.server_add(config={'tablets_mode_for_new_keyspaces': 'enabled'}, property_file={'dc': 'dc1', 'rack': 'rack1'}),
        await manager.server_add(config={'tablets_mode_for_new_keyspaces': 'enabled'}, property_file={'dc': 'dc1', 'rack': 'rack1'}),
    ]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1} AND tablets = {'initial': 100}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")

        await wait_for_view(cql, 'mv_cf_view', len(servers))


        row_count = 100
        for i in range(row_count):
            await cql.run_async(f"INSERT INTO {ks}.tab (key, c, v) VALUES ({i}, {i}, '{'a' * 100}')")

        # Create missing rows in the view by deleting sstables
        for s in servers:            
            await manager.api.keyspace_flush(s.ip_addr, ks)
            await delete_table_sstables(manager, s, ks, "mv_cf_view")
            await manager.server_stop_gracefully(s.server_id)
            await manager.server_start(s.server_id)
            cql = await reconnect_driver(manager)

        count = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.mv_cf_view")
        assert count[0][0] == 0

        await cql.run_async(f"REBUILD MATERIALIZED VIEW {ks}.mv_cf_view WHERE c = 1")
        count = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.mv_cf_view WHERE c = 1")
        assert count[0][0] == 1 

        await cql.run_async(f"REBUILD MATERIALIZED VIEW {ks}.mv_cf_view WHERE key = 2")
        count = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.mv_cf_view WHERE c = 2")
        assert count[0][0] == 1 

        await cql.run_async(f"REBUILD MATERIALIZED VIEW {ks}.mv_cf_view WHERE c = 3 and key = 3")
        count = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.mv_cf_view WHERE c = 3 and key = 3")
        assert count[0][0] == 1 

        await cql.run_async(f"REBUILD MATERIALIZED VIEW {ks}.mv_cf_view WHERE c < {10}")
        count = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.mv_cf_view")
        assert count[0][0] == 10

        await cql.run_async(f"REBUILD MATERIALIZED VIEW {ks}.mv_cf_view")
        count = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.mv_cf_view")
        assert count[0][0] == row_count
