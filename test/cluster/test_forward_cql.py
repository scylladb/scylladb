#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import asyncio
import logging
import time

from cassandra import RequestExecutionException, WriteTimeout

from test.cluster.util import new_test_keyspace, new_test_table
from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_forward_cql_basic(manager: ManagerClient):
    cmdline = [
        '--logger-log-level', 'forward_cql_service=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    await manager.servers_add(2, cmdline=cmdline, auto_rack_dc='dc1')
    cql = manager.get_cql()

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND consistency = 'local'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value text") as table:

            await cql.run_async(f"INSERT INTO {table} (pk, value) VALUES (1, 'a')")

            rows = await cql.run_async(f"SELECT pk, value FROM {table} WHERE pk = 1")
            assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
            assert rows[0].pk == 1
            assert rows[0].value == 'a'

            await cql.run_async(f"UPDATE {table} SET value = 'c' WHERE pk = 2")
            rows = await cql.run_async(f"SELECT pk, value FROM {table} WHERE pk = 2")
            assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
            assert rows[0].pk == 2
            assert rows[0].value == 'c'

            await cql.run_async(f"DELETE FROM {table} WHERE pk = 1")
            rows = await cql.run_async(f"SELECT pk, value FROM {table} WHERE pk = 1")
            assert len(rows) == 0, f"Expected 0 rows, got {len(rows)}"

@pytest.mark.asyncio
async def test_forward_cql_prepared_with_bound_values(manager: ManagerClient):
    """
    Test that prepared statements with bound values:
     * work with strongly consistent tables,
     * are getting forwarded
     * are properly cached on the raft leaders
    """
    cmdline = [
        '--logger-log-level', 'forward_cql_service=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    await manager.servers_add(2, cmdline=cmdline, auto_rack_dc='dc1')
    cql = manager.get_cql()

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND consistency = 'local'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value int") as table:
            await cql.run_async(f"INSERT INTO {table} (pk, value) VALUES (0, 0)")
            # Prepare statement
            stmt = cql.prepare(f"SELECT pk, value FROM {table} WHERE pk = ?")

            # Execute prepared INSERT once to populate the cache
            await cql.run_async(stmt, [0])

            res = cql.execute(stmt, (0,), trace=True)
            row = res.one()
            assert row.pk == 0
            assert row.value == 0
            trace = res.get_query_trace()
            forwarded_statement_found_in_cache = False
            executed_locally_as_leader = False
            for event in trace.events:
                logger.info(f"Trace event: {event.description}")
                if "Prepared statement found in cache" in event.description:
                    forwarded_statement_found_in_cache = True
                if "Statement executed locally as leader" in event.description:
                    executed_locally_as_leader = True
            # Either the statement was forwarded (and found in cache) or executed locally as leader
            assert forwarded_statement_found_in_cache or executed_locally_as_leader, \
                "Statement was neither forwarded to cache nor executed locally as leader"

@pytest.mark.asyncio
async def test_forward_cql_cache_invalidation(manager: ManagerClient):
    """
    Test that cql forwarding works after invalidation of prepared statement cache on schema changes.
    """
    cmdline = [
        '--logger-log-level', 'forward_cql_service=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    await manager.servers_add(2, cmdline=cmdline, auto_rack_dc='dc1')
    cql = manager.get_cql()

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND consistency = 'local'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value int") as table:
            insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, value) VALUES (?, ?)")
            select_stmt = cql.prepare(f"SELECT pk, value FROM {table} WHERE pk = ?")

            await cql.run_async(insert_stmt, [1, 100])
            rows = await cql.run_async(select_stmt, [1])
            assert len(rows) == 1
            assert rows[0].value == 100

            # Altering schema invalidates prepared statement cache
            await cql.run_async(f"ALTER TABLE {table} ADD extra_column text")
            await cql.run_async(insert_stmt, [2, 200])
            rows = await cql.run_async(select_stmt, [2])
            assert len(rows) == 1
            assert rows[0].value == 200


@pytest.mark.asyncio
async def test_forward_cql_eventual_consistency_keyspace_not_forwarded(manager: ManagerClient):
    """
    Test that regular (eventual consistency) keyspaces don't use forwarding.
    """
    cmdline = [
        '--logger-log-level', 'forward_cql_service=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    await manager.servers_add(2, cmdline=cmdline, auto_rack_dc='dc1')
    cql = manager.get_cql()

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    async with new_test_keyspace(manager, ks_opts) as ks:
        logger.info(f"Created regular (eventual consistency) keyspace {ks}")
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value text") as table:
            res = cql.execute(f"SELECT pk, value FROM {table} WHERE pk = 1", trace=True)
            trace = res.get_query_trace()
            forwarding_used = False
            for event in trace.events:
                logger.info(f"Trace event: {event.description}")
                if "Forward CQL request received" in event.description:
                    forwarding_used = True
            assert not forwarding_used, "Forwarding was used for eventual consistency keyspace"

@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_forward_cql_exception_passthrough(manager: ManagerClient):
    """
    Verify that exception returned on the target replica is correctly returned to the client.
    """
    cmdline = [
        '--logger-log-level', 'forward_cql_service=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    await manager.servers_add(2, cmdline=cmdline, auto_rack_dc='dc1')
    servers = await manager.running_servers()
    cql = manager.get_cql()

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND consistency = 'local'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value int") as table:
            await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, "database_apply_force_timeout", one_shot=False) for s in servers))

            with pytest.raises((WriteTimeout, RequestExecutionException)):
                cql.execute(f"INSERT INTO {table} (pk, value) VALUES (1, 1)")
            await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, "database_apply_force_timeout") for s in servers))
