#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.pylib.util import gather_safely, wait_for
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerInfo
from test.pylib.tablets import get_tablet_replicas
from cassandra import WriteTimeout
from cassandra.cluster import ConsistencyLevel
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement, BoundStatement

import pytest
import logging
import time
import uuid
import asyncio


logger = logging.getLogger(__name__)


async def wait_for_leader(manager: ManagerClient, s: ServerInfo, group_id: str):
    async def get_leader_host_id():
        result = await manager.api.get_raft_leader(s.ip_addr, group_id)
        return None if uuid.UUID(result).int == 0 else result
    return await wait_for(get_leader_host_id, time.time() + 60)


@pytest.mark.asyncio
async def test_basic_write_read(manager: ManagerClient):

    logger.info("Bootstrapping cluster")
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)

    logger.info("Load host_id-s for servers")
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    logger.info("Creating a strongly-consistent keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        logger.info("Creating a table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Select raft group id for the tablet")
        table_id = await manager.get_table_id(ks, 'test')
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets where table_id = {table_id}")
        group_id = str(rows[0].raft_group_id)

        logger.info(f"Get current leader for the group {group_id}")
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        leader_host = host_by_host_id(leader_host_id)
        non_leader_host = next((host_by_host_id(hid) for hid in host_ids if hid != leader_host_id), None)
        assert non_leader_host is not None

        logger.info(f"Run INSERT statement on leader {leader_host}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 20)", host=leader_host)

        logger.info(f"Run SELECT statement on leader {leader_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=leader_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20

        logger.info(f"Run INSERT statement on non-leader {non_leader_host}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 30)", host=non_leader_host)

        logger.info(f"Run SELECT statement on non-leader {non_leader_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=non_leader_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 30

        # Check that we can restart a server with an active tablets raft group
        await manager.server_restart(servers[2].server_id)

    # To check that the servers can be stopped gracefully. By default the test runner just kills them.
    await gather_safely(*[manager.server_stop_gracefully(s.server_id) for s in servers])

@pytest.mark.asyncio
async def test_forward_cql_basic(manager: ManagerClient):
    cmdline = [
        '--logger-log-level', 'cql_server=trace',
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
    When we prepare an statement not on the leader, we should
    still be able to forward it to the leader with bound values.
    In this test we prepare a statement that should be forwarded to
    the leader (so an INSERT statement) and then execute it.
    It should correctly insert the value.
    Then, we execute it again and check that we used the cache
    on the leader side and that it updated the value correctly again.
    """
    cmdline = [
        '--logger-log-level', 'cql_server=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    await manager.servers_add(2, cmdline=cmdline, auto_rack_dc='dc1')
    cql = manager.get_cql()

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND consistency = 'local'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value int") as table:
            # Prepare statement
            stmt = cql.prepare(f"INSERT INTO {table} (pk, value) VALUES (0, ?)")

            # Execute prepared INSERT once to verify results and populate the cache on the leader
            await cql.run_async(stmt, [0])
            res = await cql.run_async(f"SELECT value FROM {table} WHERE pk = 0")
            assert len(res) == 1 and res[0].value == 0

            # Execute prepared INSERT again to verify that we use the cache on the leader
            traced_execute = cql.execute(stmt, (0,), trace=True)
            res = await cql.run_async(f"SELECT value FROM {table} WHERE pk = 0")
            assert len(res) == 1 and res[0].value == 0
            trace = traced_execute.get_query_trace()
            for event in trace.events:
                logger.info(f"Trace event: {event.description}")
                assert "Prepared statement not found on target" not in event.description

@pytest.mark.asyncio
async def test_forward_cql_cache_invalidation(manager: ManagerClient):
    """
    Test that cql forwarding works after invalidation of prepared statement cache on schema changes.
    """
    cmdline = [
        '--logger-log-level', 'cql_server=trace',
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
        '--logger-log-level', 'cql_server=trace',
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

@pytest.mark.skip_mode('release', "error injections aren't enabled in release mode")
async def test_forward_cql_exception_passthrough(manager: ManagerClient):
    """
    Verify that coordinator exception returned on the target replica is correctly returned to the client.
    """
    cmdline = [
        '--logger-log-level', 'cql_server=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    await manager.servers_add(2, cmdline=cmdline, auto_rack_dc='dc1')
    servers = await manager.running_servers()
    cql = manager.get_cql()

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND consistency = 'local'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value int") as table:
            await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, "sc_modification_statement_timeout", one_shot=False) for s in servers))

            with pytest.raises(WriteTimeout):
                await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, value) VALUES (1, 1)", retry_policy=FallthroughRetryPolicy()))
            await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, "sc_modification_statement_timeout") for s in servers))


@pytest.mark.asyncio
async def test_forward_cql_non_replica_insert_and_select(manager: ManagerClient):
    """
    Test that both INSERT and SELECT statements work correctly when executed
    on a node that is not a replica for the targeted tablet.

    For modifications, the request should be forwarded to the Raft leader.
    For selects, the request can be forwarded to any replica (the closest one is preferred).
    """
    cmdline = [
        '--logger-log-level', 'cql_server=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]

    servers = await manager.servers_add(3, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
    ])
    for server in servers:
        await manager.api.disable_tablet_balancing(server.ip_addr)
    cql, hosts = await manager.get_ready_cql(servers)


    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND consistency = 'local' AND tablets = {'initial': 1}")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value text") as table:
            just_table = table.split('.')[-1]
            # Identify the replicas for the only tablet in the table
            tablet_replicas = await get_tablet_replicas(manager, servers[0], ks, just_table, 0)
            replica_hosts = [str(replica[0]) for replica in tablet_replicas]
            non_replica_host = [host for host in hosts if str(host.host_id) not in replica_hosts][0]
            logger.info(f"time:{time.time()} Tablet replicas for {table}: {replica_hosts}, using non-replica host {non_replica_host} for testing")

            # Insert data using the non-replica server. Should be forwarded to the leader, asking the replica in the same rack first.
            await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, value) VALUES (1, 'test_value')", consistency_level=ConsistencyLevel.ONE), host=non_replica_host)

            # Select data using the non-replica server. Should be forwarded to the replica from the same rack.
            rows = await cql.run_async(SimpleStatement(f"SELECT pk, value FROM {table} WHERE pk = 1", consistency_level=ConsistencyLevel.ONE), host=non_replica_host)
            assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
            assert rows[0].pk == 1
            assert rows[0].value == 'test_value'

            # Test with prepared statements as well
            insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, value) VALUES (?, ?)")
            bound_insert_stmt = BoundStatement(insert_stmt, consistency_level=ConsistencyLevel.ONE)
            bound_insert_stmt.bind([2, 'prepared_value'])

            select_stmt = cql.prepare(f"SELECT pk, value FROM {table} WHERE pk = ?")
            bound_select_stmt = BoundStatement(select_stmt, consistency_level=ConsistencyLevel.ONE)
            bound_select_stmt.bind([2])

            await cql.run_async(bound_insert_stmt, host=non_replica_host)
            rows = await cql.run_async(bound_select_stmt, host=non_replica_host)
            assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
            assert rows[0].pk == 2
            assert rows[0].value == 'prepared_value'
