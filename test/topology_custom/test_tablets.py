#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_tablet_replication_factor_enough_nodes(manager: ManagerClient):
    cfg = {'enable_user_defined_functions': False,
           'experimental_features': ['tablets', 'consistent-topology-changes']}
    servers = await manager.servers_add(2, config=cfg)

    cql = manager.get_cql()
    res = await cql.run_async("SELECT data_center FROM system.local")
    this_dc = res[0].data_center

    await cql.run_async(f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 3}}")
    with pytest.raises(ConfigurationException, match=f"Datacenter {this_dc} doesn't have enough nodes"):
        await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    await cql.run_async(f"ALTER KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 2}}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")


@pytest.mark.asyncio
async def test_tablet_cannot_decommision_below_replication_factor(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'experimental_features': ['tablets', 'consistent-topology-changes']}
    servers = await manager.servers_add(4, config=cfg)

    logger.info("Creating table")
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")
    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    logger.info("Decommission some node")
    await manager.decommission_node(servers[0].server_id)

    with pytest.raises(HTTPError, match="Decommission failed"):
        logger.info("Decommission another node")
        await manager.decommission_node(servers[1].server_id)

    # Three nodes should still provide CL=3
    logger.info("Checking table")
    query = SimpleStatement("SELECT * FROM test.test;", consistency_level=ConsistencyLevel.THREE)
    rows = await cql.run_async(query)
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == r.pk


@pytest.mark.asyncio
async def test_tablet_degraded_scan_query(manager: ManagerClient):
    cfg = {'enable_user_defined_functions': False,
           'experimental_features': ['tablets', 'consistent-topology-changes']}
    servers = await manager.servers_add(4, config=cfg)

    cql = manager.get_cql()
    res = await cql.run_async("SELECT data_center FROM system.local")
    this_dc = res[0].data_center

    await cql.run_async(f"CREATE KEYSPACE test_keyspace WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 3}}")
    await cql.run_async("CREATE TABLE test_keyspace.test_table (pk int PRIMARY KEY, c int);")

    # Stop all but one node
    for srv in servers[1:]:
        await manager.server_stop_gracefully(srv.server_id)

    stmt = "SELECT * FROM test_keyspace.test_table"
    for cl in [ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE,
               ConsistencyLevel.TWO, ConsistencyLevel.THREE,
               ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_QUORUM,
               ConsistencyLevel.ALL]:
        query = SimpleStatement(stmt, consistency_level=cl)
        with pytest.raises(NoHostAvailable, match=r"Cannot achieve consistency level for cl \w+. Requires [1-3], alive [0-1]"):
            await cql.run_async(query)
