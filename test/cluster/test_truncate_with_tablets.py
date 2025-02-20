#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.protocol import InvalidRequest
from cassandra.cluster import TruncateError
from cassandra.policies import FallthroughRetryPolicy
from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode
from test.cluster.util import get_topology_coordinator, new_test_keyspace
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for_cql_and_get_hosts
import time
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_truncate_while_migration(manager: ManagerClient):

    logger.info('Bootstrapping cluster')
    cfg = { 'enable_tablets': True,
            'error_injections_at_startup': ['migration_streaming_wait']
            }

    servers = []
    servers.append(await manager.server_add(config=cfg))

    cql = manager.get_cql()

    # Create a keyspace with tablets and initial_tablets == 2, then insert data
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')

        keys = range(1024)
        await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});') for k in keys])

        # Add a node to the cluster. This will cause the tablet load balancer to migrate one tablet to the new node
        servers.append(await manager.server_add(config=cfg))

        # Wait for tablet streaming to start
        pending_node = servers[1]
        pending_log = await manager.server_open_log(pending_node.server_id)

        await pending_log.wait_for('migration_streaming_wait: start')
        await manager.api.message_injection(pending_node.ip_addr, 'migration_streaming_wait')

        # Do a TRUNCATE TABLE while the tablet is being streamed
        await cql.run_async(f'TRUNCATE TABLE {ks}.test')

        # Wait for streaming to complete
        await pending_log.wait_for('raft_topology - Streaming for tablet migration of.*successful')

        # Check if we have any data
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM {ks}.test', consistency_level=ConsistencyLevel.ALL))
        assert row[0].count == 0


async def get_raft_leader_and_log(manager: ManagerClient, servers):
    raft_leader_host_id = await get_topology_coordinator(manager)
    for s in servers:
        if raft_leader_host_id == await manager.get_host_id(s.server_id):
            raft_leader = s
            break
    raft_leader_log = await manager.server_open_log(raft_leader.server_id)
    return (raft_leader, raft_leader_log)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_truncate_with_concurrent_drop(manager: ManagerClient):

    logger.info('Bootstrapping cluster')
    cfg = { 'enable_tablets': True,
            'error_injections_at_startup': ['truncate_table_wait']
            }

    servers = []
    servers.append(await manager.server_add(config=cfg))
    servers.append(await manager.server_add(config=cfg))
    servers.append(await manager.server_add(config=cfg))

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # Create a keyspace with tablets and initial_tablets == 2, then insert data
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')

        keys = range(1024)
        await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});') for k in keys])

        (raft_leader, raft_leader_log) = await get_raft_leader_and_log(manager, servers)

        if raft_leader == servers[0]:
            trunc_host = hosts[1]
            drop_host = hosts[2]
        elif raft_leader == servers[1]:
            trunc_host = hosts[0]
            drop_host = hosts[2]
        elif raft_leader == servers[2]:
            trunc_host = hosts[0]
            drop_host = hosts[1]
        else:
            assert False, 'Unable to determine raft leader'

        # Start a TRUNCATE in the background
        trunc_future = cql.run_async(f'TRUNCATE TABLE {ks}.test', host=trunc_host)
        # Wait for the topology coordinator to reach a point wher it is about to start sending the truncate RPCs
        await raft_leader_log.wait_for('truncate_table_wait: start')
        # Execute DROP TABLE
        await cql.run_async(f'DROP TABLE {ks}.test', host=drop_host)
        # Release TRUNCATE table in topology coordinator
        await manager.api.message_injection(raft_leader.ip_addr, 'truncate_table_wait')
        # Check we received an error
        with pytest.raises(InvalidRequest, match='unconfigured table test'):
            await trunc_future


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_truncate_while_node_restart(manager: ManagerClient):

    logger.info('Bootstrapping cluster')
    cfg = { 'enable_tablets': True }

    servers = []
    servers.append(await manager.server_add(config=cfg))
    servers.append(await manager.server_add(config=cfg))
    servers.append(await manager.server_add(config=cfg))

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # Create a keyspace with tablets and initial_tablets == 2, then insert data
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')

        keys = range(1024)
        await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});') for k in keys])

        (raft_leader, raft_leader_log) = await get_raft_leader_and_log(manager, servers)

        # Decide which node to restart; select a node with a replica but not the raft leader
        tablet_replicas = await get_all_tablet_replicas(manager, raft_leader, ks, 'test')
        replica_hosts = [tr.replicas[0][0] for tr in tablet_replicas]
        for s in servers:
            if s != raft_leader:
                host_id = await manager.get_host_id(s.server_id)
                if host_id in replica_hosts:
                    restart_node = s
                    break

        # Shutdown the node containing a replica
        await manager.server_stop_gracefully(restart_node.server_id)
        # Start truncating in the background
        trunc_future = cql.run_async(f'TRUNCATE TABLE {ks}.test', host=hosts[0])
        # Restart the node
        await manager.server_start(restart_node.server_id)
        # Wait for truncate to complete
        await trunc_future

        # Check if truncate was successful
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM {ks}.test', consistency_level=ConsistencyLevel.ALL))
        assert row[0].count == 0


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_truncate_with_coordinator_crash(manager: ManagerClient):

    logger.info('Bootstrapping cluster')
    cfg = { 'enable_tablets': True }

    servers = []
    servers.append(await manager.server_add(config=cfg))
    servers.append(await manager.server_add(config=cfg))

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # Create a keyspace with tablets and initial_tablets == 2, then insert data
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')

        keys = range(1024)
        await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});') for k in keys])

        (raft_leader, raft_leader_log) = await get_raft_leader_and_log(manager, servers)

        if raft_leader == servers[0]:
            trunc_host = hosts[1]
        else:
            trunc_host = hosts[0]

        # Enable injection to crash the raft leader after truncate cleared the session ID
        await manager.api.enable_injection(raft_leader.ip_addr, 'truncate_crash_after_session_clear', one_shot=False)

        # Start a TRUNCATE in the background
        trunc_future = cql.run_async(f'TRUNCATE TABLE {ks}.test', host=trunc_host)
        # Wait for the topology coordinator to crash
        await raft_leader_log.wait_for('truncate_crash_after_session_clear hit, killing the node')
        await manager.server_stop(raft_leader.server_id)
        # Restart the crashed node
        await manager.server_start(raft_leader.server_id)
        # Wait for truncate to complete
        await trunc_future

        # Check if we have any data
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM {ks}.test', consistency_level=ConsistencyLevel.ALL))
        assert row[0].count == 0


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_truncate_while_truncate_already_waiting(manager: ManagerClient):

    logger.info('Bootstrapping cluster')
    cfg = { 'enable_tablets': True,
            'error_injections_at_startup': ['migration_streaming_wait']
            }

    servers = []
    servers.append(await manager.server_add(config=cfg))

    cql = manager.get_cql()

    # Create a keyspace with tablets and initial_tablets == 2, then insert data
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')

        keys = range(1024)
        await asyncio.gather(*[cql.run_async(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});') for k in keys])

        # Add a node to the cluster. This will cause the load balancer to migrate one tablet to the new node
        servers.append(await manager.server_add(config=cfg))

        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        s1_log = await manager.server_open_log(servers[1].server_id)

        # Wait for tablet streaming to start
        await s1_log.wait_for('migration_streaming_wait: start')

        # Run a truncate which will quickly time out, but the truncate fiber remains alive
        # Do not attempt to retry automatically (hense the FallthroughRetryPolicy)
        with pytest.raises((TruncateError), match=f'Timeout during TRUNCATE TABLE of {ks}.test'):
            await cql.run_async(SimpleStatement(f'TRUNCATE TABLE {ks}.test USING TIMEOUT 100ms', retry_policy=FallthroughRetryPolicy()))

        # Run another truncate on the same table while the timedout one is still waiting
        truncate_future = cql.run_async(f'TRUNCATE TABLE {ks}.test', host=hosts[1])

        # Make sure the second truncate re-used the existing global topology request
        await s1_log.wait_for(f'Ongoing TRUNCATE for table {ks}.test')

        # Release streaming
        await manager.api.message_injection(servers[1].ip_addr, 'migration_streaming_wait')

        # Wait for the joined truncate to complete
        await truncate_future

        # Check if we have any data
        row = await cql.run_async(SimpleStatement(f'SELECT COUNT(*) FROM {ks}.test', consistency_level=ConsistencyLevel.ALL))
        assert row[0].count == 0
