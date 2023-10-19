#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver

import pytest
import asyncio
import logging
import time


logger = logging.getLogger(__name__)


async def inject_error_one_shot_on(manager, error_name, servers):
    errs = [inject_error_one_shot(manager.api, s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False) for s in servers]
    await asyncio.gather(*errs)


@pytest.mark.asyncio
async def test_tablet_metadata_propagates_with_schema_changes_in_snapshot_mode(manager: ManagerClient):
    """Test that you can create a table and insert and query data"""

    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_proxy=trace',
        '--logger-log-level', 'cql_server=trace',
        '--logger-log-level', 'query_processor=trace',
        '--logger-log-level', 'gossip=trace',
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'messaging_service=trace',
        '--logger-log-level', 'rpc=trace',
        ]
    servers = [await manager.server_add(cmdline=cmdline),
               await manager.server_add(cmdline=cmdline),
               await manager.server_add(cmdline=cmdline)]

    s0 = servers[0].server_id
    not_s0 = servers[1:]

    # s0 should miss schema and tablet changes
    await manager.server_stop_gracefully(s0)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', "
                        "'replication_factor': 3, 'initial_tablets': 100};")

    # force s0 to catch up later from the snapshot and not the raft log
    await inject_error_one_shot_on(manager, 'raft_server_force_snapshot', not_s0)
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    keys = range(10)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 1);") for k in keys])

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(list(rows)) == len(keys)
    for r in rows:
        assert r.c == 1

    manager.driver_close()
    await manager.server_start(s0, wait_others=2)
    await manager.driver_connect(server=servers[0])
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)

    # Trigger a schema change to invoke schema agreement waiting to make sure that s0 has the latest schema
    await cql.run_async("CREATE KEYSPACE test_dummy WITH replication = {'class': 'NetworkTopologyStrategy', "
                        "'replication_factor': 1, 'initial_tablets': 1};")

    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 2);", execution_profile='whitelist')
                           for k in keys])

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == 2

    conn_logger = logging.getLogger("conn_messages")
    conn_logger.setLevel(logging.DEBUG)
    try:
        # Check that after rolling restart the tablet metadata is still there
        for s in servers:
            await manager.server_restart(s.server_id, wait_others=2)

        cql = await reconnect_driver(manager)

        await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)

        await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 3);", execution_profile='whitelist')
                               for k in keys])

        rows = await cql.run_async("SELECT * FROM test.test;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == 3
    finally:
        conn_logger.setLevel(logging.INFO)

    await cql.run_async("DROP KEYSPACE test;")
    await cql.run_async("DROP KEYSPACE test_dummy;")


@pytest.mark.asyncio
async def test_scans(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = [await manager.server_add(), await manager.server_add(), await manager.server_add()]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': 1, 'initial_tablets': 8};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    keys = range(100)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    rows = await cql.run_async("SELECT count(*) FROM test.test;")
    assert rows[0].count == len(keys)

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == r.pk

    await cql.run_async("DROP KEYSPACE test;")


@pytest.mark.asyncio
async def test_table_drop_with_auto_snapshot(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = { 'auto_snapshot': True }
    servers = [await manager.server_add(config = cfg),
               await manager.server_add(config = cfg),
               await manager.server_add(config = cfg)]

    cql = manager.get_cql()

    # Increases the chance of tablet migration concurrent with schema change
    await inject_error_on(manager, "tablet_allocator_shuffle", servers)

    for i in range(3):
        await cql.run_async("DROP KEYSPACE IF EXISTS test;")
        await cql.run_async("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1, 'initial_tablets': 8 };")
        await cql.run_async("CREATE TABLE IF NOT EXISTS test.tbl_sample_kv (id int, value text, PRIMARY KEY (id));")
        await cql.run_async("INSERT INTO test.tbl_sample_kv (id, value) VALUES (1, 'ala');")

    await cql.run_async("DROP KEYSPACE test;")


@pytest.mark.asyncio
async def test_topology_changes(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = [await manager.server_add(), await manager.server_add(), await manager.server_add()]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': 1, 'initial_tablets': 32};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    async def check():
        logger.info("Checking table")
        rows = await cql.run_async("SELECT * FROM test.test;")
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await inject_error_on(manager, "tablet_allocator_shuffle", servers)

    logger.info("Adding new server")
    await manager.server_add()

    await check()

    logger.info("Adding new server")
    await manager.server_add()

    await check()
    time.sleep(5) # Give load balancer some time to do work
    await check()

    await manager.decommission_node(servers[0].server_id)

    await check()

    await cql.run_async("DROP KEYSPACE test;")
