#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import asyncio

from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from cassandra.query import SimpleStatement, ConsistencyLevel

import pytest

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_truncation_on_drop(manager: ManagerClient):
    await manager.server_add()
    cql = manager.get_cql()

    # Create a keyspace
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')
        table_id = await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'")
        table_id = table_id[0].id

        keys = range(1024)
        await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});') for k in keys])
        await cql.run_async(f'TRUNCATE TABLE {ks}.test')

        # should have some truncation records now
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM system.truncated where table_uuid={table_id}'))
        assert row[0].count > 0

        await cql.run_async(f"DROP TABLE {ks}.test")

        # should have no truncation records now
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM system.truncated where table_uuid={table_id}'))
        assert row[0].count == 0

@pytest.mark.asyncio
async def test_truncation_records_pruned_on_dirty_restart(manager: ManagerClient):
    server = await manager.server_add()
    cql = manager.get_cql()

    async def restart():
        await manager.server_stop(server.server_id)
        await manager.server_start(server.server_id)
        manager.driver_close()
        await manager.driver_connect()
        return manager.cql
    
    # Create a keyspace
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')
        table_id = await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'")
        table_id = table_id[0].id

        keys = range(1024)
        await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});') for k in keys])
        await cql.run_async(f'TRUNCATE TABLE {ks}.test')

        # should have some truncation records now
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM system.truncated where table_uuid={table_id}'))
        assert row[0].count > 0

        logger.debug("Kill + restart the node")
        cql = await restart()

        # should still have same truncation records
        row2 = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM system.truncated where table_uuid={table_id}'))
        assert row2[0].count == row[0].count

        # should _not_ have any data.
        row2 = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM {ks}.test'))
        assert row2[0].count == 0

        logger.debug("Fake 'old' dropped table")
        # don't do this at home kids.

        await cql.run_async(f"DELETE FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'")
        await cql.run_async(f"DELETE FROM system.tablets WHERE table_id = {table_id}")

        logger.debug("Kill + restart the node again")
        cql = await restart()

        # should have no truncation records now
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM system.truncated where table_uuid={table_id}', consistency_level=ConsistencyLevel.ONE))
        assert row[0].count == 0
