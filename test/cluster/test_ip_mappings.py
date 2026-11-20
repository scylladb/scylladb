# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import time
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

import pytest
import logging
from uuid import UUID

from test.pylib.internal_types import ServerUpState
from test.pylib.rest_client import inject_error_one_shot, read_barrier
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import gather_safely, wait_for
from test.cluster.util import disable_schema_agreement_wait, new_test_keyspace, reconnect_driver

from cassandra.cluster import ConsistencyLevel, SimpleStatement

logger = logging.getLogger(__name__)

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_broken_bootstrap(manager: ScyllaClusterManager):
    server_a = await manager.server_add()
    server_b = await manager.server_add(start=False)

    async with new_test_keyspace(manager, "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.test"
        await manager.cql.run_async(f"CREATE TABLE {table} (a int PRIMARY KEY, b int)")
        for i in range(100):
            await manager.cql.run_async(f"INSERT INTO {table} (a, b) VALUES ({i}, {i})")
        await inject_error_one_shot(manager.api, server_a.ip_addr, "crash-before-bootstrapping-node-added")
        try:
            # Timeout fast since we do not expect the operation to complete
            # because the coordinator is dead by now due to the error injection
            # above
            await manager.server_start(server_b.server_id, timeout=5)
            pytest.fail("Expected server_add to fail")
        except Exception:
            pass

        await gather_safely(*(manager.server_stop(srv.server_id, convict=False) for srv in [server_a, server_b]))

        await manager.server_start(server_a.server_id)
        await manager.driver_connect()

        for i in range(100):
            await manager.cql.run_async(f"INSERT INTO {table} (a, b) VALUES ({i}, {i})")
            response = await manager.cql.run_async(f"SELECT * FROM {table} WHERE a = {i}")
            assert response[0].b == i


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize('reuse_ip', [False, True])
async def test_full_shutdown_during_replace(manager: ScyllaClusterManager, reuse_ip: bool):
    """
    Test that shutting down all live nodes during replace doesn't cause the IP address of the replacing node to be
    missing on restart. After restarts, the test performs requests with the replacing node as a pending replica, which
    would crash the request coordinators if the IP address was missing.

    Regression test for scylla-enterprise#5686. Note that in the reported issue `nodetool status` performed during
    stuck replace caused a crash, but the problem turned out to be more general.
    """
    logger.info('Adding servers')
    leader = await manager.server_add(property_file={'dc': 'dc1', 'rack': 'rack0'})
    follower, dead_server = await manager.servers_add(2, auto_rack_dc='dc1')
    live_servers = [leader, follower]

    logger.info('Waiting for driver')
    cql, [host] = await manager.get_ready_cql([follower])

    with disable_schema_agreement_wait(cql):  # This makes the test a bit faster.
        async with new_test_keyspace(manager, """WITH REPLICATION = {'class': 'NetworkTopologyStrategy',
                                     'replication_factor': 3} AND tablets = {'enabled': false}""", host) as ks:
            table = f'{ks}.test'
            await cql.run_async(f'CREATE TABLE {table} (a int PRIMARY KEY, b int)', host=host)

            logger.info(f'Stopping {dead_server}')
            await manager.server_stop_gracefully(dead_server.server_id)

            await manager.api.enable_injection(
                leader.ip_addr, 'topology_coordinator/write_both_read_old/before_global_token_metadata_barrier', True)

            replace_cfg = ReplaceConfig(replaced_id=dead_server.server_id, reuse_ip_addr=reuse_ip, use_host_id=False)
            logger.info(f'Adding the server that will replace {dead_server}')
            replacing_server = await manager.server_add(
                replace_cfg, property_file=dead_server.property_file(), start=False)

            logger.info(f'Trying to replace {dead_server} with {replacing_server}')
            replacing_task = asyncio.create_task(manager.server_start(replacing_server.server_id))

            logger.info('Waiting for the topology coordinator to pause in write_both_read_old')
            leader_log = await manager.server_open_log(leader.server_id)
            await leader_log.wait_for(
                'topology_coordinator/write_both_read_old/before_global_token_metadata_barrier: waiting for message')

            replacing_host_id = await manager.get_host_id(replacing_server.server_id)

            logger.info(f'Stopping {live_servers + [replacing_server]}')
            await gather_safely(*(manager.server_stop(srv.server_id, convict=False) for srv in live_servers + [replacing_server]))
            replacing_task.cancel()

            for srv in live_servers:
                await manager.server_update_config(srv.server_id, 'error_injections_at_startup',
                    ['topology_coordinator/write_both_read_old/before_global_token_metadata_barrier'])

            logger.info(f'Starting {live_servers}')
            await gather_safely(*(manager.server_start(srv.server_id) for srv in live_servers))

            # This is needed to ensure that the replacing node will be a pending replica for the requests below.
            logger.info(f'Waiting for {live_servers} to apply the latest topology state')
            await gather_safely(*[read_barrier(manager.api, srv.ip_addr) for srv in live_servers])

            await reconnect_driver(manager)
            logger.info('Waiting for driver')
            cql, hosts = await manager.get_ready_cql(live_servers)

            logger.info(f'Sending requests to {table} with {replacing_server} as a pending replica')
            for i in range(100):
                await cql.run_async(SimpleStatement(f'INSERT INTO {table} (a, b) VALUES ({i}, {i})',
                                                    consistency_level=ConsistencyLevel.ONE))
                response = await manager.cql.run_async(f"SELECT * FROM {table} WHERE a = {i}")
                assert response[0].b == i

            # Verify that `/storage_service/tokens/{endpoint}` returns no tokens for IP of the node being replaced iff
            # reuse_ip is True. The goal here is to check that the API endpoint resolves the provided IP address to the
            # host ID of the replacing node when reuse_ip is True. There is nothing special about this endpoint; we
            # can use any endpoint that calls `gossiper::get_host_id(inet_address endpoint)`. When reuse_ip is False, we
            # expect non-empty tokens as a sanity check.
            logger.info(f'Checking tokens of {dead_server.ip_addr}')
            for srv in live_servers:
                tokens = await manager.api.get_tokens(srv.ip_addr, dead_server.ip_addr)
                assert (len(tokens) == 0) == reuse_ip

            for srv in live_servers:
                await manager.api.message_injection(
                    srv.ip_addr, 'topology_coordinator/write_both_read_old/before_global_token_metadata_barrier')

            logs = [await manager.server_open_log(srv.server_id) for srv in live_servers]
            logger.info(f'Waiting for {replacing_server} to be removed from gossip after replace rollback')
            await gather_safely(*[
                log.wait_for(f'gossip - Finished to force remove node {replacing_host_id}')
                for log in logs
            ])

            logger.info(f'Retrying replace of {dead_server}')
            new_server = await manager.server_add(replace_cfg, property_file=dead_server.property_file())
            live_servers.append(new_server)

            logger.info(f'Checking peers on {live_servers}')
            host_ids = [await manager.get_host_id(srv.server_id) for srv in live_servers]
            for srv, host in zip(live_servers, hosts):
                result = await cql.run_async('SELECT peer, host_id FROM system.peers', host=host)
                peers = {(row.peer, row.host_id) for row in result}
                expected = {(other.ip_addr, UUID(id)) for other, id in zip(live_servers, host_ids) if other != srv}
                assert peers == expected


async def peers_rows_for_host_id(cql, host, host_id: UUID) -> list[str]:
    rows = await cql.run_async("SELECT peer, host_id FROM system.peers", host=host)
    return sorted(str(r.peer) for r in rows if r.host_id == host_id)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_bootstrapping_node_restarts_with_new_ip(manager: ScyllaClusterManager):
    """
    A node being bootstrapped crashes and comes back with a different IP address
    while it is still in the `bootstrapping` state in system.topology (this is
    what happens when the Kubernetes host of a joining pod is rebooted and the
    pod is recreated elsewhere).

    raft_topology_update_ip() used to insert the new IP into system.peers
    without removing the row with the old IP. The next reload of the peers
    cache then failed with the "duplicate IP for host_id" internal error and
    every node that had learned the new IP through gossip aborted.

    The test checks that the nodes survive and that system.peers ends up with
    a single row for the joining node.

    Regression test for https://github.com/scylladb/scylladb/issues/31762
    """
    servers = await manager.servers_add(2)
    coordinator = servers[0]
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]

    logger.info(f"Blocking the topology coordinator {coordinator} in the bootstrapping state")
    await manager.api.enable_injection(coordinator.ip_addr, 'delay_node_bootstrap', one_shot=False)

    new_server = await manager.server_add(start=False)
    await manager.server_start(new_server.server_id, connect_driver=False,
                               expected_server_up_state=ServerUpState.PROCESS_STARTED)
    await logs[0].wait_for('delay_node_bootstrap: waiting for message', from_mark=marks[0])
    new_host_id = UUID(await manager.get_host_id(new_server.server_id))
    old_ip = new_server.ip_addr

    cql, hosts = await manager.get_ready_cql(servers)
    for host in hosts:
        assert await peers_rows_for_host_id(cql, host, new_host_id) == [old_ip]

    logger.info(f"Restarting the bootstrapping node {new_server} with a different IP")
    await manager.server_stop(new_server.server_id, convict=False)
    new_ip = await manager.server_change_ip(new_server.server_id)
    logger.info(f"{new_server}: {old_ip} -> {new_ip}")
    # A restarted joining node cannot resume its bootstrap: it has already
    # joined group 0, so join_topology() mints a fresh request id and waits for
    # it with require_entry=true, and the lookup fails with the "no entry for
    # request id" internal error shortly after gossip starts. Park the node
    # right before that wait so that its new IP reliably reaches the other
    # nodes through gossip.
    await manager.server_update_config(new_server.server_id, 'error_injections_at_startup',
                                       ['crash_before_topology_request_completion'])
    await manager.server_start(new_server.server_id, connect_driver=False,
                               expected_server_up_state=ServerUpState.PROCESS_STARTED)
    await manager.api.wait_for_injection_enter(new_ip, 'crash_before_topology_request_completion')

    async def peers_updated():
        for host in hosts:
            rows = await peers_rows_for_host_id(cql, host, new_host_id)
            if rows != [new_ip]:
                logger.info(f"{host}: system.peers rows for {new_host_id} are {rows}, waiting for [{new_ip}]")
                return None
        return True
    await wait_for(peers_updated, time.time() + 60)

    for server, log, mark in zip(servers, logs, marks):
        assert not await log.grep('duplicate IP for host_id', from_mark=mark), \
            f"{server} hit the duplicate IP internal error"

    # Stop the parked node, let the coordinator finish the operation and remove
    # the dead node from the topology.
    await manager.server_stop(new_server.server_id, convict=True)
    logger.info(f"Unblocking the topology coordinator {coordinator}")
    await manager.api.message_injection(coordinator.ip_addr, 'delay_node_bootstrap')
    await manager.remove_node(coordinator.server_id, new_server.server_id)

    cql, hosts = await manager.get_ready_cql(servers)
    for host in hosts:
        assert await peers_rows_for_host_id(cql, host, new_host_id) == []


async def test_peers_table_read_fixup_does_not_deadlock(manager: ScyllaClusterManager):
    """
    peers_table_read_fixup() removes stale system.peers rows (two IPs for the
    same host_id) on the first read of the table after startup. With Raft
    topology the first read happens while starting group 0, from
    get_or_load_peers_cache() which holds _peers_cache_lock; the removal used to
    go through remove_endpoint() which takes the same non-reentrant lock, so
    the node hung forever right after "starting group 0" on every restart.

    Regression test for https://github.com/scylladb/scylladb/issues/31762
    """
    servers = await manager.servers_add(2)
    cql, hosts = await manager.get_ready_cql(servers)
    victim, other = servers
    other_host_id = UUID(await manager.get_host_id(other.server_id))

    # Plant a stale row for `other` with an older timestamp so that the fixup
    # keeps the genuine row and removes this one.
    stale_ip = '127.255.255.254'
    await cql.run_async(f"INSERT INTO system.peers (peer, host_id) VALUES ('{stale_ip}', {other_host_id}) USING TIMESTAMP 1",
                        host=hosts[0])
    assert await peers_rows_for_host_id(cql, hosts[0], other_host_id) == sorted([stale_ip, other.ip_addr])

    log = await manager.server_open_log(victim.server_id)
    mark = await log.mark()
    await manager.server_restart(victim.server_id, wait_others=1)
    await log.wait_for('the record is stale, removing it', from_mark=mark)

    cql, hosts = await manager.get_ready_cql(servers)
    assert await peers_rows_for_host_id(cql, hosts[0], other_host_id) == [other.ip_addr]
