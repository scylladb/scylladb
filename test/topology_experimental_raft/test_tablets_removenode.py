#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from cassandra.query import SimpleStatement, ConsistencyLevel # type: ignore

from test.pylib.manager_client import ManagerClient

import pytest
import asyncio
import logging

from test.pylib.scylla_cluster import ReplaceConfig

logger = logging.getLogger(__name__)


async def create_keyspace(cql, name, initial_tablets, rf):
    await cql.run_async(f"CREATE KEYSPACE {name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"
                        f" AND tablets = {{'initial': {initial_tablets}}};")


async def run_async_cl_all(cql, query: str):
    stmt = SimpleStatement(query, consistency_level = ConsistencyLevel.ALL)
    return await cql.run_async(stmt)


@pytest.mark.asyncio
async def test_replace(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = ['--logger-log-level', 'storage_service=trace']

    # 4 nodes so that we can find new tablet replica for the RF=3 table on removenode
    servers = await manager.servers_add(4, cmdline=cmdline)

    cql = manager.get_cql()

    await create_keyspace(cql, "test", 32, rf=1)
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    await create_keyspace(cql, "test2", 32, rf=2)
    await cql.run_async("CREATE TABLE test2.test (pk int PRIMARY KEY, c int);")

    # RF=3
    await create_keyspace(cql, "test3", 32, rf=3)
    await cql.run_async("CREATE TABLE test3.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    await asyncio.gather(*[run_async_cl_all(cql, f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])
    await asyncio.gather(*[run_async_cl_all(cql, f"INSERT INTO test2.test (pk, c) VALUES ({k}, {k});") for k in keys])
    await asyncio.gather(*[run_async_cl_all(cql, f"INSERT INTO test3.test (pk, c) VALUES ({k}, {k});") for k in keys])

    async def check():
        # RF=1 table "test" will experience data loss so don't check it.
        # We include it to check that the system doesn't crash.

        logger.info("Checking table test2")
        query = SimpleStatement("SELECT * FROM test2.test;", consistency_level=ConsistencyLevel.ONE)
        rows = await cql.run_async(query)
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

        logger.info("Checking table test3")
        query = SimpleStatement("SELECT * FROM test3.test;", consistency_level=ConsistencyLevel.ONE)
        rows = await cql.run_async(query)
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await check()

    # Disable migrations concurrent with replace since we don't handle nodes going down during migration yet.
    # See https://github.com/scylladb/scylladb/issues/16527
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info('Replacing a node')
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = True)
    await manager.server_add(replace_cfg)
    servers = servers[1:]

    await check()


@pytest.mark.asyncio
async def test_removenode(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = ['--logger-log-level', 'storage_service=trace']

    # 4 nodes so that we can find new tablet replica for the RF=3 table on removenode
    servers = await manager.servers_add(4, cmdline=cmdline)

    cql = manager.get_cql()

    # RF=1
    await create_keyspace(cql, "test", 32, rf=1)
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    # RF=2
    await create_keyspace(cql, "test2", 32, rf=2)
    await cql.run_async("CREATE TABLE test2.test (pk int PRIMARY KEY, c int);")

    # RF=3
    await create_keyspace(cql, "test3", 32, rf=3)
    await cql.run_async("CREATE TABLE test3.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    await asyncio.gather(*[run_async_cl_all(cql, f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])
    await asyncio.gather(*[run_async_cl_all(cql, f"INSERT INTO test2.test (pk, c) VALUES ({k}, {k});") for k in keys])
    await asyncio.gather(*[run_async_cl_all(cql, f"INSERT INTO test3.test (pk, c) VALUES ({k}, {k});") for k in keys])

    async def check():
        # RF=1 table "test" will experience data loss so don't check it.
        # We include it to check that the system doesn't crash.

        logger.info("Checking table test2")
        query = SimpleStatement("SELECT * FROM test2.test;", consistency_level=ConsistencyLevel.ONE)
        rows = await cql.run_async(query)
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

        logger.info("Checking table test3")
        query = SimpleStatement("SELECT * FROM test3.test;", consistency_level=ConsistencyLevel.ONE)
        rows = await cql.run_async(query)
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await check()

    # Disable migrations concurrent with removenode since we don't handle nodes going down during migration yet.
    # See https://github.com/scylladb/scylladb/issues/16527
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info('Removing a node')
    await manager.server_stop(servers[0].server_id)
    await manager.remove_node(servers[1].server_id, servers[0].server_id)
    servers = servers[1:]

    await check()


@pytest.mark.asyncio
async def test_removenode_with_ignored_node(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
    ]

    # 5 nodes because we need a quorum with 2 nodes down.
    # 4 nodes would be enough to not lose data with RF=3.
    servers = await manager.servers_add(5, cmdline=cmdline)

    cql = manager.get_cql()

    await create_keyspace(cql, "test", 32, rf=3)
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(512)
    await asyncio.gather(*[run_async_cl_all(cql, f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    async def check():
        logger.info("Checking")
        query = SimpleStatement("SELECT * FROM test.test;", consistency_level=ConsistencyLevel.ONE)
        rows = await cql.run_async(query)
        assert len(rows) == len(keys)
        for r in rows:
            assert r.c == r.pk

    await check()

    # Disable migrations concurrent with removenode since we don't handle nodes going down during migration yet.
    # See https://github.com/scylladb/scylladb/issues/16527
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    logger.info('Removing a node with another node down')
    await manager.server_stop(servers[0].server_id) # removed
    await manager.server_stop(servers[1].server_id) # ignored
    await manager.remove_node(servers[2].server_id, servers[0].server_id, [servers[1].ip_addr])
    servers = servers[1:]

    await check()

    logger.info('Removing a node')
    await manager.remove_node(servers[1].server_id, servers[0].server_id)

    await check()
