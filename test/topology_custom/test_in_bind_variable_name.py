#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

# Coverage for the name of the IN bind variable, SCYLLADB-3454.

import logging
import time
from contextlib import asynccontextmanager

from cassandra import ConsistencyLevel
from cassandra.cluster import ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy

from test.pylib.async_cql import _wrap_future
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.conftest import cluster_con
from test.topology.util import new_test_keyspace, new_test_table

logger = logging.getLogger(__name__)


@asynccontextmanager
async def exclusive_cql(manager: ManagerClient, server: ServerInfo):
    """A driver connection that talks to a single node only."""
    cluster = cluster_con([server.ip_addr], manager.port, manager.use_ssl,
                          load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr]))
    try:
        cql = cluster.connect()
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
        yield cql
    finally:
        cluster.shutdown()


async def assert_binding_by_name_works_across_nodes(manager: ManagerClient,
                                                    prepare_on: ServerInfo, execute_on: ServerInfo,
                                                    query: str, expected_name: str, key: int):
    """
    Prepare on one node and let the other node coordinate the request. The
    statement is bound by name, using the name the preparing node reported, while
    the coordinator is a node that would have named the variable the other way.
    """
    other = 'other_node'
    cluster = cluster_con([prepare_on.ip_addr], manager.port, manager.use_ssl,
                          load_balancing_policy=WhiteListRoundRobinPolicy([prepare_on.ip_addr]))
    try:
        cql = cluster.connect()
        cluster.add_execution_profile(other, ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy([execute_on.ip_addr]),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=200))

        prepared = cql.prepare(query)
        assert [col.name for col in prepared.column_metadata] == ['p', expected_name]

        future = cql.execute_async(prepared, {'p': key, expected_name: [key]}, execution_profile=other)
        assert [(key, key)] == await _wrap_future(future)
        assert str(future.coordinator_host.address) == execute_on.ip_addr
        logger.info(f"the statement prepared on {prepare_on.ip_addr} as {expected_name!r} "
                    f"was served by {execute_on.ip_addr}")
    finally:
        cluster.shutdown()


async def test_in_bind_variable_name_mixed_config(manager: ManagerClient):
    """
    cql_in_bind_variable_name_uses_uppercase_operator decides whether the
    synthetic bind variable of an IN restriction is reported as "IN(c)" or
    "in(c)". The name is picked when the statement is prepared, so in a cluster
    whose nodes disagree about the item an application gets whichever spelling
    the node it prepared against uses.

    The name never reaches the coordinator with this driver: it resolves the
    name to a position when the values are bound, and the values are sent
    positionally. A request therefore succeeds no matter which node coordinates
    it, even when that node would have named the variable the other way. Drivers
    that instead send the names along with the values leave the matching to the
    coordinator, which rejects a name its own copy of the statement does not
    have; that case is not covered here.
    """
    lowercase_server = await manager.server_add(config={'cql_in_bind_variable_name_uses_uppercase_operator': False})
    uppercase_server = await manager.server_add(config={'cql_in_bind_variable_name_uses_uppercase_operator': True})

    async with new_test_keyspace(manager,
            "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as keyspace:
        async with new_test_table(manager, keyspace, "p int, c int, PRIMARY KEY (p, c)") as table:
            query = f"SELECT * FROM {table} WHERE p = ? AND c IN ?"

            async with exclusive_cql(manager, lowercase_server) as cql_lowercase, \
                       exclusive_cql(manager, uppercase_server) as cql_uppercase:
                assert [col.name for col in cql_lowercase.prepare(query).column_metadata] == ['p', 'in(c)']
                assert [col.name for col in cql_uppercase.prepare(query).column_metadata] == ['p', 'IN(c)']
                logger.info("the two nodes report different names for the same query")

            for p in range(2):
                await manager.get_cql().run_async(f"INSERT INTO {table} (p,c) VALUES ({p},{p})")

            await assert_binding_by_name_works_across_nodes(manager, lowercase_server, uppercase_server,
                                                            query, 'in(c)', 0)
            await assert_binding_by_name_works_across_nodes(manager, uppercase_server, lowercase_server,
                                                            query, 'IN(c)', 1)


async def test_in_bind_variable_name_of_a_cached_statement(manager: ManagerClient):
    """
    The name is picked when the statement is prepared, and the item is part of
    the prepared statement cache key. A statement that is already cached keeps
    the name it was prepared under, but does not shadow the changed item:
    preparing the same query again gets a fresh cache entry and reports the
    new name.

    The cache is per shard, hence the single-shard node: it guarantees the
    second prepare runs on a shard that still holds the statement cached under
    the previous setting, so the test would catch that entry being returned.
    """
    server = await manager.server_add(cmdline=['--smp', '1'],
                                      config={'cql_in_bind_variable_name_uses_uppercase_operator': True})

    async with new_test_keyspace(manager,
            "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as keyspace:
        async with new_test_table(manager, keyspace, "p int, c int, PRIMARY KEY (p, c)") as table:
            async with exclusive_cql(manager, server) as cql:
                query = f"SELECT * FROM {table} WHERE p = ? AND c IN ?"
                not_in_query = f"SELECT * FROM {table} WHERE p = ? AND c NOT IN ?"
                assert [col.name for col in cql.prepare(query).column_metadata] == ['p', 'IN(c)']
                assert [col.name for col in cql.prepare(not_in_query).column_metadata] == ['p', 'NOT IN(c)']
                logger.info("the statements are cached under the uppercase names")

                cql.execute("UPDATE system.config SET value='false' "
                            "WHERE name='cql_in_bind_variable_name_uses_uppercase_operator'")
                logger.info("the item is false now")

                assert [col.name for col in cql.prepare(query).column_metadata] == ['p', 'in(c)']
                assert [col.name for col in cql.prepare(not_in_query).column_metadata] == ['p', 'not in(c)']
                logger.info("preparing the same query strings again reports the new names")
