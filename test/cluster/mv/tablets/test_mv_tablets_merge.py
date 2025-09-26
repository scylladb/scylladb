#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import pytest
import time

from test.cluster.util import new_test_keyspace, new_test_table, new_materialized_view
from test.pylib.tablets import get_tablet_count
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_mv_merge_allowed(manager):
    """
    Test that tablet merge is allowed for materialized views and their base tables.
    """
    cfg = {'tablet_load_stats_refresh_interval_in_seconds': 1}
    server = await manager.server_add(config=cfg)
    cql = manager.cql
    _ = await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': true, 'initial': 1}}") as ks:
        async with new_test_table(manager, ks, "p int PRIMARY KEY, x int", "WITH tablets = {'min_tablet_count': 2}") as table:
            async with new_materialized_view(manager, table, "*", "x, p", "p is not null and x is not null", "WITH tablets = {'min_tablet_count': 2}") as mv:
                async def tablet_count_is(name, expected_tablet_count):
                    keyspace_name, table_name = name.split(".")
                    current_tablet_count = await get_tablet_count(manager, server, keyspace_name, table_name)
                    if current_tablet_count == expected_tablet_count:
                        return True
                
                assert tablet_count_is(table, 2)
                await cql.run_async(f"ALTER TABLE {table} WITH tablets = {{'min_tablet_count': 1}}")
                await wait_for(lambda: tablet_count_is(table, 1), time.time() + 60)

                assert tablet_count_is(mv, 2)
                await cql.run_async(f"ALTER MATERIALIZED VIEW {mv} WITH tablets = {{'min_tablet_count': 1}}")
                await wait_for(lambda: tablet_count_is(mv, 1), time.time() + 60)
