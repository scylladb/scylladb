#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import time
import logging
import requests
import re

from cassandra.cluster import ConnectionException, NoHostAvailable  # type: ignore
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for

from test.topology.conftest import skip_mode
from test.topology.util import get_topology_coordinator, find_server_by_host_id


logger = logging.getLogger(__name__)

def get_hint_manager_metric(server: ServerInfo, metric_name: str) -> int:
    result = 0
    metrics = requests.get(f"http://{server.ip_addr}:9180/metrics").text
    pattern = re.compile(f"^scylla_hints_manager_{metric_name}")
    for metric in metrics.split('\n'):
        if pattern.match(metric) is not None:
            result += int(float(metric.split()[1]))
    return result

# Creates a sync point for ALL hosts.
def create_sync_point(node: ServerInfo) -> str:
    return requests.post(f"http://{node.ip_addr}:10000/hinted_handoff/sync_point/").json()

def await_sync_point(node: ServerInfo, sync_point: str, timeout: int) -> bool:
    params = {
        "id": sync_point,
        "timeout": str(timeout)
    }

    response = requests.get(f"http://{node.ip_addr}:10000/hinted_handoff/sync_point", params=params).json()
    match response:
        case "IN_PROGRESS":
            return False
        case "DONE":
            return True
        case _:
            pytest.fail(f"Unexpected response from the server: {response}")

# Write with RF=1 and CL=ANY to a dead node should write hints and succeed
@pytest.mark.asyncio
async def test_write_cl_any_to_dead_node_generates_hints(manager: ManagerClient):
    node_count = 2
    servers = await manager.servers_add(node_count)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")

    await manager.server_stop_gracefully(servers[1].server_id)

    def get_hints_written_count(server):
        return get_hint_manager_metric(server, "written")

    hints_before = get_hints_written_count(servers[0])

    # Some of the inserts will be targeted to the dead node.
    # The coordinator doesn't have live targets to send the write to, but it should write a hint.
    for i in range(100):
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i+1})", consistency_level=ConsistencyLevel.ANY))

    # Verify hints are written
    hints_after = get_hints_written_count(servers[0])
    assert hints_after > hints_before

@pytest.mark.asyncio
async def test_limited_concurrency_of_writes(manager: ManagerClient):
    """
    We want to verify that Scylla correctly limits the concurrency of writing hints to disk.
    To do that, we leverage error injections decreasing the threshold when hints should start
    being rejected, and we expect to receive an exception indicating that a node is overloaded.
    """
    node1 = await manager.server_add(config={
        "error_injections_at_startup": ["decrease_max_size_of_hints_in_progress"]
    })
    node2 = await manager.server_add()

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")

    await manager.server_stop_gracefully(node2.server_id)

    async with inject_error(manager.api, node1.ip_addr, "slow_down_writing_hints"):
        try:
            for i in range(100):
                await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i})", consistency_level=ConsistencyLevel.ONE))
            pytest.fail("The coordinator node has not been overloaded, which indiciates that the concurrency of writing hints is NOT limited")
        except NoHostAvailable as e:
            for _, err in e.errors.items():
                assert err.summary == "Coordinator node overloaded" and re.match(r"Too many in flight hints: \d+", err.message)

@pytest.mark.asyncio
async def test_sync_point(manager: ManagerClient):
    """
    We want to verify that the sync point API is compliant with its design.
    This test concerns one particular aspect of it: Scylla should create a sync point
    for ALL nodes if the parameter `target_hosts` of a request is empty, not just
    live nodes.
    """
    node_count = 3
    [node1, node2, node3] = await manager.servers_add(node_count)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")

    await manager.server_stop_gracefully(node2.server_id)
    await manager.server_stop_gracefully(node3.server_id)

    await manager.server_not_sees_other_server(node1.ip_addr, node2.ip_addr)
    await manager.server_not_sees_other_server(node1.ip_addr, node3.ip_addr)

    mutation_count = 5
    for primary_key in range(mutation_count):
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({primary_key}, {primary_key})", consistency_level=ConsistencyLevel.ONE))

    # Mutations need to be applied to hinted handoff's commitlog before we create the sync point.
    # Otherwise, the sync point will correspond to no hints at all.

    # We need to wrap the function in an async function to make `wait_for` be able to use it below.
    async def check_no_hints_in_progress_node1() -> bool:
        return get_hint_manager_metric(node1, "size_of_hints_in_progress") == 0

    deadline = time.time() + 30
    await wait_for(check_no_hints_in_progress_node1, deadline)

    sync_point1 = create_sync_point(node1)

    await manager.server_start(node2.server_id)
    await manager.server_sees_other_server(node1.ip_addr, node2.ip_addr)

    assert not await_sync_point(node1, sync_point1, 30)

    await manager.server_start(node3.server_id)
    await manager.server_sees_other_server(node1.ip_addr, node3.ip_addr)

    assert await_sync_point(node1, sync_point1, 30)


@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_hints_consistency_during_decommission(manager: ManagerClient):
    """
    This test reproduces the failure observed in scylladb/scylla-dtest#4582
    in a more reliable way than the test_hintedhandoff_decom dtest.

    We want to make sure that data stored in hints will not get lost if hints replay
    happens in parallel to streaming during decommission.

    The test is vnodes-specific.
    """
    (server1, server2, server3) = await manager.servers_add(3, config={
        "error_injections_at_startup": ["decrease_hints_flush_period"]
    })
    cql = manager.cql

    logger.info("Creatting a keyspace with RF=1 and a table")
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = { 'enabled': false }")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")

    logger.info("Stopping node 3")
    await manager.server_stop_gracefully(server3.server_id)
    await manager.others_not_see_server(server3.ip_addr)

    # Write 100 rows with CL=ANY. Some of the rows will only be stored as hints because of RF=1
    logger.info("Writing 100 rows with CL=ANY")
    for i in range(100):
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ANY))

    # Temporarily pause hints replay, we will unpause it after decommission starts and streaming is done,
    # but before switching to writing to new nodes
    logger.info("Pause hints replay on nodes 1 and 2")
    for srv in (server1, server2):
        await manager.api.enable_injection(srv.ip_addr, "hinted_handoff_pause_hint_replay", one_shot=False)

    # Start the node
    logger.info("Start node 3")
    await manager.server_start(server3.server_id)
    await manager.servers_see_each_other([server1, server2, server3])

    # Record the current position of hints so that we can wait for them later
    sync_points = [create_sync_point(srv) for srv in (server1, server2)]

    async with asyncio.TaskGroup() as tg:
        coord = await get_topology_coordinator(manager)
        coord_srv = await find_server_by_host_id(manager, [server1, server2, server3], coord)

        # Make sure topology coordinator will pause right after streaming
        logger.info("Enabling injection on the topology coordinator that will tell it to pause streaming")
        await manager.api.enable_injection(coord_srv.ip_addr, "topology_coordinator_pause_after_streaming", one_shot=False)
        coord_log = await manager.server_open_log(coord_srv.server_id)
        coord_mark = await coord_log.mark()

        # Start decommission - it will get stuck on error injection so do it in the background
        logger.info("Starting decommission in the background")
        decommission_result = tg.create_task(manager.decommission_node(server3.server_id))

        # Wait until streaming ends
        logger.info("Wait until decomission finishes streaming")
        await coord_log.wait_for(f'decommissioning: streaming completed for node', from_mark=coord_mark)

        # Now, unpause hints and let them be replayed
        logger.info("Unpause hints replay on nodes 1 and 2")
        for srv in (server1, server2):
            await manager.api.disable_injection(srv.ip_addr, "hinted_handoff_pause_hint_replay")

        logger.info("Wait until hints are replayed from nodes 1 and 2")
        await asyncio.gather(*(asyncio.to_thread(await_sync_point, srv, pt, timeout=30) for srv, pt in zip((server1, server2), sync_points)))

        # Unpause streaming and let decommission finish
        logger.info("Unpause streaming")
        await manager.api.disable_injection(coord_srv.ip_addr, "topology_coordinator_pause_after_streaming")

        logger.info("Wait until decomission finishes")
        await decommission_result

    # Verify that no data has been lost - if the hints replay only sent the hints to the original destination (server3),
    # then they will be only present on server3 which already left the cluster
    logger.info("Verify that no data stored in hints have been lost")
    for i in range(100):
        assert list(await cql.run_async(f"SELECT v FROM ks.t WHERE pk = {i}")) == [(i + 1,)]
