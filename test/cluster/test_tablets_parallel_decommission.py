#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import logging
import time

import pytest

from test.cluster.util import get_coordinator_host, new_test_keyspace, ensure_group0_leader_on
from test.pylib.internal_types import ServerInfo, IPAddress
from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import gather_safely
from test.pylib.tablets import get_replica_count_by_host
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


async def count_requests_queued(manager: ManagerClient, coord_srv: ServerInfo, request_type: str) -> int:
    tasks = await manager.api.get_tasks(coord_srv.ip_addr, 'node_ops')
    logger.info(f'tasks: {tasks}')
    return len(list(filter(lambda t: t['type'] == request_type, tasks)))


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_tablets_are_drained_in_parallel(manager: ManagerClient):
    """
    Verifies that when we start decommissioning two nodes at the same time,
    migrations draining tablets from both nodes are interleaved, indicating that
    the decommissions are indeed progressing in parallel and not sequentially.
    """

    cmdline = [
        '--logger-log-level', 'load_balancer=debug',
    ]

    servers = await manager.servers_add(4, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc1", "rack": "rack2"},
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc1", "rack": "rack2"},
    ])
    await ensure_group0_leader_on(manager, servers[0])

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                                          " 'dc1': ['rack1', 'rack2']} AND tablets = {'initial': 32};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (pk int PRIMARY KEY);")

        coord_srv = await get_coordinator_host(manager)
        log = await manager.server_open_log(coord_srv.server_id) # group0 leader
        mark = await log.mark()
        await manager.api.enable_injection(coord_srv.ip_addr, "topology_coordinator_pause_before_processing_backlog", one_shot=True)

        decomm_task1 = asyncio.create_task(manager.decommission_node(servers[2].server_id)) # rack1
        decomm_task2 = asyncio.create_task(manager.decommission_node(servers[3].server_id)) # rack2

        mark, _ = await log.wait_for('topology_coordinator_pause_before_processing_backlog: waiting', from_mark=mark)

        # Pause topology until all requests are queued to workaround for group0 concurrent modification
        # preventing second request from being queued due to fast migrations on behalf of the first requests.
        # This problem is more pronounced in test environment where migrations are instant.
        logger.info("Waiting for requests to be queued")
        async def requests_queued():
            count = await count_requests_queued(manager, coord_srv, 'decommission')
            return count if count == 2 else None
        await wait_for(requests_queued, time.time() + 60)
        await manager.api.message_injection(coord_srv.ip_addr, "topology_coordinator_pause_before_processing_backlog")

        decomm_hostid_1 = await manager.get_host_id(servers[2].server_id)
        decomm_hostid_2 = await manager.get_host_id(servers[3].server_id)

        # Verify that migrations are interleaved, which indicates parallel decommission.
        mark, _ = await log.wait_for(f"Initiating tablet cleanup of (.*) on {decomm_hostid_1}", from_mark=mark)
        mark, _ = await log.wait_for(f"Initiating tablet cleanup of (.*) on {decomm_hostid_2}", from_mark=mark)
        mark, _ = await log.wait_for(f"Initiating tablet cleanup of (.*) on {decomm_hostid_1}", from_mark=mark)
        mark, _ = await log.wait_for(f"Initiating tablet cleanup of (.*) on {decomm_hostid_2}", from_mark=mark)

        await decomm_task1
        await decomm_task2


@pytest.mark.asyncio
@pytest.mark.parametrize("same_rack", [False, True])
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_tablets_are_rebuilt_in_parallel(manager: ManagerClient, same_rack):
    """
    Verifies that when we start removing two nodes at the same time,
    tablet replica rebuilds for both nodes are interleaved, indicating that
    the removals are indeed progressing in parallel and not sequentially.
    """

    cmdline = [
        '--logger-log-level', 'load_balancer=debug',
    ]

    # same_rack == True
    # rack1: 1 server
    # rack2: 3 servers (will have 2 removed)
    #
    # same_rack == False
    # rack1: 2 servers (will have 1 removed)
    # rack2: 2 servers (will have 1 removed)

    servers = await manager.servers_add(4, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc1", "rack": "rack2"},
        {"dc": "dc1", "rack": "rack2" if same_rack else "rack1"}, # will be removed
        {"dc": "dc1", "rack": "rack2"}, # will be removed
    ])
    await ensure_group0_leader_on(manager, servers[0])
    coord_srv = servers[0]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                                          " 'dc1': ['rack1', 'rack2']} AND tablets = {'initial': 32};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (pk int PRIMARY KEY);")

        servers_to_remove = [servers[2], servers[3]]
        host_ids_to_remove = await gather_safely(*(manager.get_host_id(s.server_id) for s in servers_to_remove))

        logger.info("Stopping servers to be removed")

        await gather_safely(*(manager.server_stop_gracefully(srv.server_id) for srv in servers_to_remove))

        for srv in servers_to_remove:
            await manager.server_not_sees_other_server(coord_srv.ip_addr, srv.ip_addr)

        log = await manager.server_open_log(coord_srv.server_id)
        mark = await log.mark()
        await manager.api.enable_injection(coord_srv.ip_addr, "topology_coordinator_pause_before_processing_backlog", one_shot=True)

        logger.info("Removing servers")
        await manager.api.exclude_node(coord_srv.ip_addr, host_ids_to_remove)
        tasks = [asyncio.create_task(manager.remove_node(coord_srv.server_id, srv.server_id)) for srv in servers_to_remove]

        mark, _ = await log.wait_for('topology_coordinator_pause_before_processing_backlog: waiting', from_mark=mark)

        # Pause topology until all requests are queued to workaround for group0 concurrent modification
        # preventing second request from being queued due to fast migrations on behalf of the first requests.
        # This problem is more pronounced in test environment where migrations are instant.
        logger.info("Waiting for requests to be queued")
        async def requests_queued():
            count = await count_requests_queued(manager, coord_srv, 'remove node')
            return count if count == 2 else None
        await wait_for(requests_queued, time.time() + 60)
        await manager.api.message_injection(coord_srv.ip_addr, "topology_coordinator_pause_before_processing_backlog")

        # Verify that migrations are interleaved.
        mark, _ = await log.wait_for(f"Tablet cleanup of (.*) on {host_ids_to_remove[0]}", from_mark=mark)
        mark, _ = await log.wait_for(f"Tablet cleanup of (.*) on {host_ids_to_remove[1]}", from_mark=mark)
        mark, _ = await log.wait_for(f"Tablet cleanup of (.*) on {host_ids_to_remove[0]}", from_mark=mark)
        mark, _ = await log.wait_for(f"Tablet cleanup of (.*) on {host_ids_to_remove[1]}", from_mark=mark)

        await gather_safely(*tasks)


async def get_task_for_node(manager: ManagerClient, api_node: IPAddress, node_id: str):
    """
    Returns task id of task which works on a given node_id, based on task's `entity` field.
    """

    tasks = await manager.api.get_tasks(api_node, 'node_ops')
    logger.info(f'tasks: {tasks}')
    task = next(t for t in tasks if t['entity'] == node_id)
    task_id = task['task_id']
    logger.info(f'decommission task: {task_id}')
    return task_id


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_decommission_can_be_canceled(manager: ManagerClient):
    """
    Verifies that decommission can be canceled when it's still in the phase of migrating tablets.
    """

    cmdline = [
        '--logger-log-level', 'load_balancer=debug',
    ]

    servers = await manager.servers_add(2, cmdline=cmdline, property_file={"dc": "dc1", "rack": "rack1"})
    await ensure_group0_leader_on(manager, servers[0])
    coord_serv = servers[0]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                        " 'dc1': ['rack1']} AND tablets = {'initial': 32};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (pk int PRIMARY KEY);")

        coord_log = await manager.server_open_log(coord_serv.server_id) # group0 leader
        mark = await coord_log.mark()
        await manager.api.enable_injection(coord_serv.ip_addr, "topology_coordinator_pause_before_processing_backlog", one_shot=True)

        decomm_task = asyncio.create_task(manager.decommission_node(servers[1].server_id))
        decomm_hostid = await manager.get_host_id(servers[1].server_id)

        await coord_log.wait_for('topology_coordinator_pause_before_processing_backlog: waiting', from_mark=mark)

        tasks = await manager.api.get_tasks(servers[0].ip_addr, 'node_ops')
        logger.info(f'tasks: {tasks}')
        task = next(t for t in tasks if t['type'] == 'decommission')
        task_id = task['task_id']
        logger.info(f'decommission task: {task_id}')

        # Aborting in a pending or paused state should be immediate even if migrations are ongoing.
        await manager.api.abort_task(servers[0].ip_addr, task_id)
        await manager.api.wait_task(servers[0].ip_addr, task_id)

        tasks = await manager.api.get_tasks(servers[0].ip_addr, 'node_ops')
        logger.info(f'tasks: {tasks}')

        await manager.api.message_injection(coord_serv.ip_addr, "topology_coordinator_pause_before_processing_backlog")

        with pytest.raises(Exception, match="aborted on user request"):
            await decomm_task

        # Verify that decommissioned node is still serving tablets and not drained.
        await manager.api.quiesce_topology(servers[0].ip_addr)
        load = await get_replica_count_by_host(manager, servers[0], ks, "tab")
        assert load[decomm_hostid] > 0

        logger.info('Verify start_time is preserved by abort')
        task2 = await manager.api.get_task_status(servers[0].ip_addr, task_id)
        assert task2['start_time'] == task['start_time']

        logger.info('Verify aborting during paused state')

        mark = await coord_log.mark()
        await manager.api.enable_injection(coord_serv.ip_addr, "wait_after_tablet_cleanup", one_shot=True)
        decomm_task = asyncio.create_task(manager.decommission_node(servers[1].server_id))
        await coord_log.wait_for('Waiting after tablet cleanup', from_mark=mark)
        task_id = await get_task_for_node(manager, servers[0].ip_addr, decomm_hostid)
        await manager.api.abort_task(servers[0].ip_addr, task_id)
        await manager.api.wait_task(servers[0].ip_addr, task_id)
        await manager.api.message_injection(coord_serv.ip_addr, "wait_after_tablet_cleanup")

        with pytest.raises(Exception, match="aborted on user request"):
            await decomm_task

        # Verify decommission can be retried and completes successfully.
        await manager.decommission_node(servers[1].server_id)
        load = await get_replica_count_by_host(manager, servers[0], ks, "tab")
        assert load[decomm_hostid] == 0


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_decommission_is_rejected_when_another_one_is_still_pending(manager: ManagerClient):
    """
    Verify that when there is pending decommission, the next one is already validated
    taking into account that the node will be removed by the first one.
    """

    cmdline = [
        '--logger-log-level', 'load_balancer=debug',
    ]
    servers = await manager.servers_add(3, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc1", "rack": "rack2"},
        {"dc": "dc1", "rack": "rack2"},
    ])

    await ensure_group0_leader_on(manager, servers[0])
    coord_serv = servers[0]

    await manager.disable_tablet_balancing()

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                        " 'dc1': ['rack1', 'rack2']} AND tablets = {'initial': 32};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (pk int PRIMARY KEY);")

        coord_log = await manager.server_open_log(coord_serv.server_id) # group0 leader
        mark = await coord_log.mark()
        await manager.api.enable_injection(coord_serv.ip_addr, "wait_after_tablet_cleanup", one_shot=True)

        decomm_task = asyncio.create_task(manager.decommission_node(servers[1].server_id))

        # Trap the first decommission in the tablet draining stage
        await coord_log.wait_for('Waiting after tablet cleanup', from_mark=mark)

        # Second decommission should fail, because if both decommissions succeeded, the rack would be empty.
        with pytest.raises(Exception, match="node decommission rejected: "):
            await manager.decommission_node(servers[2].server_id)

        await manager.api.message_injection(coord_serv.ip_addr, "wait_after_tablet_cleanup")
        # The first one should succeed.
        await decomm_task


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_remove_is_canceled_if_there_is_node_down(manager: ManagerClient):
    """
    Verifies that request is canceled if the vnode part would fail due to node being down.
    Preserves the old behavior of serial decommission, where topology coordinator
    decides to cancel all pending requests in get_next_task() if none of them can proceed.
    """

    cmdline = [
        '--logger-log-level', 'load_balancer=debug',
    ]
    servers = await manager.servers_add(4, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc1", "rack": "rack2"},
        {"dc": "dc1", "rack": "rack2"},
        {"dc": "dc1", "rack": "rack2"},
    ])

    await ensure_group0_leader_on(manager, servers[0])
    coord_serv = servers[0]

    await manager.disable_tablet_balancing()

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                        " 'dc1': ['rack1', 'rack2']} AND tablets = {'initial': 32};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (pk int PRIMARY KEY);")

        await manager.server_stop_gracefully(servers[3].server_id)
        await manager.server_stop_gracefully(servers[2].server_id)

        with pytest.raises(Exception, match="Canceled. Dead nodes: "):
            await manager.remove_node(coord_serv.server_id, servers[2].server_id)


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_decommission_start_time_is_stable(manager: ManagerClient):
    """
    Verifies that task's start time is constant across the whole operation.
    """

    servers = await manager.servers_add(2, property_file={"dc": "dc1", "rack": "rack1"})
    await ensure_group0_leader_on(manager, servers[0])

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                                          " 'dc1': ['rack1']} AND tablets = {'initial': 32};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (pk int PRIMARY KEY);")

        coord_serv = servers[0]
        coord_log = await manager.server_open_log(coord_serv.server_id) # group0 leader
        mark = await coord_log.mark()
        await manager.api.enable_injection(coord_serv.ip_addr, "topology_coordinator_pause_after_node_transition", one_shot=True)

        decomm_task = asyncio.create_task(manager.decommission_node(servers[1].server_id))
        decomm_hostid = await manager.get_host_id(servers[1].server_id)

        await coord_log.wait_for('topology_coordinator_pause_after_node_transition: waiting for message', from_mark=mark)

        tasks = await manager.api.get_tasks(servers[0].ip_addr, 'node_ops')
        logger.info(f'tasks: {tasks}')
        task = next(t for t in tasks if t['type'] == 'decommission')
        logger.info(f'decommission task: {task}')
        assert task['entity'] == decomm_hostid

        await manager.api.message_injection(coord_serv.ip_addr, "topology_coordinator_pause_after_node_transition")
        await decomm_task

        tasks = await manager.api.get_tasks(servers[0].ip_addr, 'node_ops')
        logger.info(f'tasks: {tasks}')
        task2 = next(t for t in tasks if t['type'] == 'decommission')
        assert task2['start_time'] == task['start_time']

        await manager.api.quiesce_topology(servers[0].ip_addr)

        tasks = await manager.api.get_tasks(servers[0].ip_addr, 'node_ops')
        logger.info(f'tasks: {tasks}')
        task2 = next(t for t in tasks if t['type'] == 'decommission')
        assert task2['start_time'] == task['start_time']


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_decommission_can_not_be_canceled_once_running(manager: ManagerClient):
    """
    Verifies that attempt to abort decommission is rejected if it's already in the vnode transition phase.
    """

    servers = await manager.servers_add(2, property_file={"dc": "dc1", "rack": "rack1"})
    await ensure_group0_leader_on(manager, servers[0])

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                                          " 'dc1': ['rack1']} AND tablets = {'initial': 32};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (pk int PRIMARY KEY);")

        coord_serv = servers[0]
        coord_log = await manager.server_open_log(coord_serv.server_id) # group0 leader
        mark = await coord_log.mark()

        # Could be any injection in the middle of decommission without group0 guard held.
        await manager.api.enable_injection(coord_serv.ip_addr, "in_left_token_ring_transition", one_shot=True)

        decomm_task = asyncio.create_task(manager.decommission_node(servers[1].server_id))
        decomm_hostid = await manager.get_host_id(servers[1].server_id)

        await coord_log.wait_for('in_left_token_ring_transition: waiting', from_mark=mark)

        tasks = await manager.api.get_tasks(servers[0].ip_addr, 'node_ops')
        logger.info(f'tasks: {tasks}')
        task_to_cancel = next(t['task_id'] for t in tasks if t['type'] == 'decommission')
        logger.info(f'decommission task: {task_to_cancel}')

        with pytest.raises(Exception, match="cannot be aborted"):
            await manager.api.abort_task(servers[0].ip_addr, task_to_cancel)

        status = await manager.api.get_task_status(servers[0].ip_addr, task_to_cancel)
        logger.info(f'status: {status}')
        assert status['state'] == 'running'

        await manager.api.message_injection(coord_serv.ip_addr, "in_left_token_ring_transition")
        await decomm_task

        load = await get_replica_count_by_host(manager, servers[0], ks, "tab")
        assert load[decomm_hostid] == 0


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_decommission_fails_if_capacity_is_gone_during_draining(manager: ManagerClient):
    """
    Verifies the scenario of drain not being able to progress because there is no viable
    replica to take over the tablets being drained. Decommission should fail in this case
    and not hang forever.
    """

    cmdline = [
        '--logger-log-level', 'load_balancer=debug',
    ]

    servers = await manager.servers_add(3, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc1", "rack": "rack2"},
        {"dc": "dc1", "rack": "rack2"},
    ])
    await ensure_group0_leader_on(manager, servers[0])

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                                          " 'dc1': ['rack2']} AND tablets = {'initial': 32};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (pk int PRIMARY KEY);")

        coord_serv = servers[0]
        coord_log = await manager.server_open_log(coord_serv.server_id) # group0 leader

        mark = await coord_log.mark()
        await manager.api.enable_injection(coord_serv.ip_addr, "wait_after_tablet_cleanup", one_shot=True)

        decomm_task = asyncio.create_task(manager.decommission_node(servers[2].server_id))

        await coord_log.wait_for('Waiting after tablet cleanup', from_mark=mark)

        srv_to_remove = servers[1]
        await manager.server_stop_gracefully(srv_to_remove.server_id)
        await manager.server_not_sees_other_server(coord_serv.ip_addr, srv_to_remove.ip_addr)
        await manager.api.exclude_node(coord_serv.ip_addr, [await manager.get_host_id(srv_to_remove.server_id)])

        await manager.api.message_injection(coord_serv.ip_addr, "wait_after_tablet_cleanup")

        with pytest.raises(Exception, match="Decommission failed"):
            await decomm_task


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_node_lost_during_decommission_drain(manager: ManagerClient):
    """
    Verifies the scenario when decommissioned node is lost and marked as excluded during draining.
    In such case, drain will complete and do the rebuilds. But decommission should fail
    since we cannot tell the user that we decommissioned the node without data loss.
    The user will have to do removenode on it.
    """

    cmdline = [
        '--logger-log-level', 'load_balancer=debug',
    ]

    servers = await manager.servers_add(2, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc1", "rack": "rack1"},
    ])
    await ensure_group0_leader_on(manager, servers[0])

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                                          " 'dc1': ['rack1']} AND tablets = {'initial': 32};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (pk int PRIMARY KEY);")

        coord_serv = servers[0]
        coord_log = await manager.server_open_log(coord_serv.server_id) # group0 leader

        mark = await coord_log.mark()

        await manager.api.quiesce_topology(coord_serv.ip_addr)

        await manager.api.enable_injection(coord_serv.ip_addr, "migration_streaming_wait", one_shot=True)

        srv_to_remove = servers[1]
        nodetool_task = asyncio.create_task(manager.decommission_node(srv_to_remove.server_id))

        mark, _ = await coord_log.wait_for('migration_streaming_wait: start', from_mark=mark)
        await manager.api.enable_injection(coord_serv.ip_addr, "topology_coordinator_pause_before_processing_backlog", one_shot=True)
        await manager.api.message_injection(coord_serv.ip_addr, "migration_streaming_wait")

        tasks = await manager.api.get_tasks(servers[0].ip_addr, 'node_ops')
        logger.info(f'tasks: {tasks}')
        decomm_task = next(t['task_id'] for t in tasks if t['type'] == 'decommission')
        logger.info(f'decommission task: {decomm_task}')

        await coord_log.wait_for('topology_coordinator_pause_before_processing_backlog: waiting', from_mark=mark)

        await manager.server_stop(srv_to_remove.server_id)
        await manager.server_not_sees_other_server(coord_serv.ip_addr, srv_to_remove.ip_addr)
        await manager.api.exclude_node(coord_serv.ip_addr, [await manager.get_host_id(srv_to_remove.server_id)])

        await manager.api.message_injection(coord_serv.ip_addr, "topology_coordinator_pause_before_processing_backlog")

        # Node is down, connection was dropped
        with pytest.raises(Exception):
            await nodetool_task

        await manager.api.wait_task(servers[0].ip_addr, decomm_task)

        # Verify that decommission failed.
        status = await manager.api.get_task_status(servers[0].ip_addr, decomm_task)
        logger.info(f"status: {status}")
        assert status['state'] == 'failed'

        # No tablet rebuilds must be started.
        assert not await coord_log.grep("Initiating tablet.*rebuild")

        await manager.remove_node(coord_serv.server_id, srv_to_remove.server_id)
