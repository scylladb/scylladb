#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import time
import logging
import re

from cassandra.cluster import NoHostAvailable  # type: ignore
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.internal_types import IPAddress, ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import ScyllaMetricsClient, TCPRESTClient, inject_error
from test.pylib.tablets import get_tablet_replicas
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import gather_safely, wait_for

from test.cluster.conftest import skip_mode
from test.cluster.util import get_topology_coordinator, find_server_by_host_id, keyspace_has_tablets, new_test_keyspace, new_test_table


logger = logging.getLogger(__name__)

async def get_hint_metrics(client: ScyllaMetricsClient, server_ip: IPAddress, metric_name: str):
    metrics = await client.query(server_ip)
    return metrics.get(f"scylla_hints_manager_{metric_name}")


async def wait_until_hint_writing_settled(manager: ManagerClient, servers: list[ServerInfo],
                                          timeout: float = 180) -> None:
    async def check():
        all_in_progress = await gather_safely(*[get_hint_metrics(manager.metrics, srv.ip_addr, "size_of_hints_in_progress") for srv in servers])
        logger.debug(f"Waiting for hint writing to settle. All in progress: {all_in_progress}")
        in_progress = sum(all_in_progress)
        return True if in_progress == 0 else None
    await wait_for(check, time.time() + timeout)


async def wait_until_hints_are_sent_from(manager: ManagerClient, servers: list[ServerInfo],
                                         expected_count: float, timeout: float = 180) -> None:
    """
    Wait until `expected_count` hints have been sent on `servers`.
    """
    async def check():
        all_sent_total = await gather_safely(*[get_hint_metrics(manager.metrics, srv.ip_addr, "sent_total") for srv in servers])
        logger.debug(f"Waiting for {expected_count} hints in total to be sent. All sent total: {all_sent_total}")
        sent_total = sum(all_sent_total)
        return True if sent_total >= expected_count else None
    await wait_for(check, time.time() + timeout)


async def create_sync_point(client: TCPRESTClient, server_ip: IPAddress) -> str:
    response = await client.post_json("/hinted_handoff/sync_point", host=server_ip, port=10_000)
    return response

async def await_sync_point(client: TCPRESTClient, server_ip: IPAddress, sync_point: str, timeout: int) -> bool:
    params = {
        "id": sync_point,
        "timeout": str(timeout)
    }

    response = await client.get_json("/hinted_handoff/sync_point", host=server_ip, port=10_000, params=params)
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
    cmdline = ["--logger-log-level", "hints_manager=trace"]
    servers = await manager.servers_add(node_count, cmdline=cmdline)

    async def wait_for_hints_written(min_hint_count: int, timeout: int):
        async def aux():
            hints_written = await get_hint_metrics(manager.metrics, servers[0].ip_addr, "written")
            if hints_written >= min_hint_count:
                return True
            return None
        assert await wait_for(aux, time.time() + timeout)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        uses_tablets = await keyspace_has_tablets(manager, ks)
        # If the keyspace uses tablets, let's explicitly require the table to use multiple tablets.
        # Otherwise, it could happen that all mutations would target servers[0] only, which would
        # ultimately lead to a test failure here. We rely on the assumption that mutations will be
        # distributed more or less uniformly!
        extra_opts = "WITH tablets = {'min_tablet_count': 16}" if uses_tablets else ""
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int", extra_opts) as table:
            await manager.server_stop_gracefully(servers[1].server_id)

            hints_before = await get_hint_metrics(manager.metrics, servers[0].ip_addr, "written")

            stmt = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
            stmt.consistency_level = ConsistencyLevel.ANY

            # Some of the inserts will be targeted to the dead node.
            # The coordinator doesn't have live targets to send the write to, but it should write a hint.
            await gather_safely(*[cql.run_async(stmt, (i, i + 1)) for i in range(100)])

            # Verify hints are written
            await wait_for_hints_written(hints_before + 1, timeout=60)

            # For dropping the keyspace
            await manager.server_start(servers[1].server_id)

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

    cql = await manager.get_cql_exclusive(node1)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int primary key, v int)")

        await manager.server_stop_gracefully(node2.server_id)

        async with inject_error(manager.api, node1.ip_addr, "slow_down_writing_hints"):
            try:
                for i in range(100):
                    await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES ({i}, {i})", consistency_level=ConsistencyLevel.ONE))
                pytest.fail("The coordinator node has not been overloaded, which indiciates that the concurrency of writing hints is NOT limited")
            except NoHostAvailable as e:
                for _, err in e.errors.items():
                    assert err.summary == "Coordinator node overloaded" and re.match(r"Too many in flight hints: \d+", err.message)

        # For dropping the keyspace
        await manager.server_start(node2.server_id)

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
    async with new_test_keyspace(manager, "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int primary key, v int)")

        await manager.server_stop_gracefully(node2.server_id)
        await manager.server_stop_gracefully(node3.server_id)

        await manager.server_not_sees_other_server(node1.ip_addr, node2.ip_addr)
        await manager.server_not_sees_other_server(node1.ip_addr, node3.ip_addr)

        mutation_count = 5
        for primary_key in range(mutation_count):
            await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES ({primary_key}, {primary_key})", consistency_level=ConsistencyLevel.ONE))

        # Mutations need to be applied to hinted handoff's commitlog before we create the sync point.
        # Otherwise, the sync point will correspond to no hints at all.

        async def check_written_hints(min_count: int) -> bool:
            errors = await get_hint_metrics(manager.metrics, node1.ip_addr, "errors")
            assert errors == 0, "Writing hints to disk failed"

            hints = await get_hint_metrics(manager.metrics, node1.ip_addr, "written")
            if hints >= min_count:
                return True
            return None

        deadline = time.time() + 30
        await wait_for(lambda: check_written_hints(2 * mutation_count), deadline)

        sync_point1 = await create_sync_point(manager.api.client, node1.ip_addr)

        await manager.server_start(node2.server_id)
        await manager.server_sees_other_server(node1.ip_addr, node2.ip_addr)

        assert not (await await_sync_point(manager.api.client, node1.ip_addr, sync_point1, 3))

        await manager.server_start(node3.server_id)
        await manager.server_sees_other_server(node1.ip_addr, node3.ip_addr)

        assert await await_sync_point(manager.api.client, node1.ip_addr, sync_point1, 30)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason="error injections aren't enabled in release mode")
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
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = { 'enabled': false }") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int primary key, v int)")

        logger.info("Stopping node 3")
        await manager.server_stop_gracefully(server3.server_id)
        await manager.others_not_see_server(server3.ip_addr)

        # Write 100 rows with CL=ANY. Some of the rows will only be stored as hints because of RF=1
        logger.info("Writing 100 rows with CL=ANY")
        for i in range(100):
            await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ANY))

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
        sync_points = await asyncio.gather(*[create_sync_point(manager.api.client, srv.ip_addr) for srv in (server1, server2)])
        sync_points = list(sync_points)

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
            await asyncio.gather(*(await_sync_point(manager.api.client, srv.ip_addr, pt, timeout=30)
                                   for srv, pt in zip((server1, server2), sync_points)))

            # Unpause streaming and let decommission finish
            logger.info("Unpause streaming")
            await manager.api.disable_injection(coord_srv.ip_addr, "topology_coordinator_pause_after_streaming")

            logger.info("Wait until decomission finishes")
            await decommission_result

        # Verify that no data has been lost - if the hints replay only sent the hints to the original destination (server3),
        # then they will be only present on server3 which already left the cluster
        logger.info("Verify that no data stored in hints have been lost")
        for i in range(100):
            assert list(await cql.run_async(f"SELECT v FROM {table} WHERE pk = {i}")) == [(i + 1,)]

@pytest.mark.asyncio
async def test_hints_consistency_during_replace(manager: ManagerClient):
    """
    Reproducer for https://github.com/scylladb/scylladb/issues/24980
    In this test, we stop a node, then write some data with CL=ANY and RF=1
    to generate hints, and then replace the stopped node with a new one.
    After completing hint replay, all rows should be present on the cluster.
    """
    servers = await manager.servers_add(3, config={
        "error_injections_at_startup": ["decrease_hints_flush_period"]
    })
    cql = await manager.get_cql_exclusive(servers[0])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int primary key, v int)")

        await manager.server_stop_gracefully(servers[2].server_id)
        await manager.others_not_see_server(servers[2].ip_addr)

        # Write 100 rows with CL=ANY. Some of the rows will only be stored as hints because of RF=1
        for i in range(100):
            await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ANY))

        # Hint writes are fire-and-forget (store_hint() submits do_store_hint()
        # asynchronously via a gate). Wait for all pending hint writes to complete
        # before creating the sync point, otherwise it may capture a stale
        # replay position and miss some hints.
        async def no_pending_hint_writes():
            size = await get_hint_metrics(manager.metrics, servers[0].ip_addr, "size_of_hints_in_progress")
            if size == 0:
                return True
            return None
        await wait_for(no_pending_hint_writes, time.time() + 30)

        sync_point = await create_sync_point(manager.api.client, servers[0].ip_addr)

        await manager.server_add(replace_cfg=ReplaceConfig(replaced_id = servers[2].server_id, reuse_ip_addr = False, use_host_id = True))

        assert await await_sync_point(manager.api.client, servers[0].ip_addr, sync_point, 30)
        # Verify that all rows were recovered by the hint replay
        for i in range(100):
            assert list(await cql.run_async(f"SELECT v FROM {table} WHERE pk = {i}")) == [(i + 1,)]

@pytest.mark.asyncio
async def test_draining_hints(manager: ManagerClient):
    """
    This test verifies that all hints are drained when a node is being decommissioned.
    """

    s1, s2, _ = await manager.servers_add(3)
    cql = manager.get_cql()

    await manager.api.set_logger_level(s1.ip_addr, "hints_manager", "trace")

    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s2.server_id)

    # Generate hints towards s2 on s1 with probability 1 - ((#nodes - 1) / #nodes)^1000 ~= 1.
    for i in range(1000):
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ANY))

    sync_point = await create_sync_point(manager.api.client, s1.ip_addr)
    await manager.server_start(s2.server_id)

    async with asyncio.TaskGroup() as tg:
        _ = tg.create_task(manager.decommission_node(s1.server_id, timeout=60))
        _ = tg.create_task(await_sync_point(manager.api.client, s1.ip_addr, sync_point, 60))

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_canceling_hint_draining(manager: ManagerClient):
    """
    This test verifies that draining hints is canceled as soon as we issue a shutdown,
    but it's resumed after starting the node again.
    """

    s1, s2, _ = await manager.servers_add(3)
    cql = manager.get_cql()

    host_id2 = await manager.get_host_id(s2.server_id)

    await manager.api.set_logger_level(s1.ip_addr, "hints_manager", "trace")

    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s2.server_id)

    # Generate hints towards s2 on s1 with probability 1 - ((#nodes - 1) / #nodes)^1000 ~= 1.
    for i in range(1000):
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ANY))

    sync_point = await create_sync_point(manager.api.client, s1.ip_addr)

    await manager.api.enable_injection(s1.ip_addr, "hinted_handoff_pause_hint_replay", False, {})
    await manager.remove_node(s1.server_id, s2.server_id)
    await manager.server_stop_gracefully(s1.server_id)

    s1_log = await manager.server_open_log(s1.server_id)
    s1_mark = await s1_log.mark()

    await manager.server_update_cmdline(s1.server_id, ["--logger-log-level", "hints_manager=trace"])
    await manager.server_start(s1.server_id)

    s1_log = await manager.server_open_log(s1.server_id)

    # Make sure the node still knows about the decommissioned node and does start draining for it.
    await s1_log.wait_for(f"Draining starts for {host_id2}", from_mark=s1_mark)

    # Make sure draining finishes successfully.
    assert await await_sync_point(manager.api.client, s1.ip_addr, sync_point, 60)
    await s1_log.wait_for(f"Removed hint directory for {host_id2}")

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_hint_to_pending(manager: ManagerClient):
    """
    This test reproduces the scenario where sending a hint to a pending replica is needed
    for consistency as in https://github.com/scylladb/scylladb/issues/19835.
    In the test, we have 2 servers and a table with RF=1. One server is stopped, and we
    perform a write generating a hint to it. Then, we start the stopped server again and
    immediately request a tablet migration from that server. The hint is sent after the
    tablet migration performs streaming but before it completes. The order of operations
    is induced using error injections.
    At the end, we verify that the hint was successfully applied.
    """
    servers = await manager.servers_add(2, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r1"},
    ])
    cql = await manager.get_cql_exclusive(servers[0])
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int primary key, v int)")
        replica = (await get_tablet_replicas(manager, servers[0], ks, "t", 0))[0]
        host_ids = [await manager.get_host_id(server.server_id) for server in servers]
        if replica[0] != host_ids[1]:
            # We'll use server 0 as the source of the hint, so the tablet replica needs to be on server 1
            await manager.api.move_tablet(servers[0].ip_addr, ks, "t", replica[0], replica[1], host_ids[1], 0, 0)

        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.others_not_see_server(servers[1].ip_addr)

        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (0, 0)", consistency_level=ConsistencyLevel.ANY))

        await manager.api.enable_injection(servers[0].ip_addr, "hinted_handoff_pause_hint_replay", False)
        await manager.server_start(servers[1].server_id)
        sync_point = await create_sync_point(manager.api.client, servers[0].ip_addr)

        await manager.api.enable_injection(servers[0].ip_addr, "pause_after_streaming_tablet", False)
        tablet_migration = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, "t", host_ids[1], 0, host_ids[0], 0, 0))

        async def migration_reached_streaming():
            stages = await cql.run_async(f"SELECT stage FROM system.tablets WHERE keyspace_name='{ks}' ALLOW FILTERING")
            logger.info(f"Current stages: {[row.stage for row in stages]}")
            return set(["streaming"]) == set([row.stage for row in stages]) or None
        await wait_for(migration_reached_streaming, time.time() + 60)

        await manager.api.disable_injection(servers[0].ip_addr, "hinted_handoff_pause_hint_replay")
        assert await await_sync_point(manager.api.client, servers[0].ip_addr, sync_point, 30)

        await manager.api.message_injection(servers[0].ip_addr, "pause_after_streaming_tablet")
        done, pending = await asyncio.wait([tablet_migration])
        for task in pending:
            task.cancel()
        for task in done:
            task.result()

        assert list(await cql.run_async(f"SELECT v FROM {table} WHERE pk = 0")) == [(0,)]
<<<<<<< HEAD
||||||| parent of e26fec5e79 (test/cluster/test_hints.py: Migrate dtest test_hintedhandoff_retransmit)

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_hint_to_leaving_when_reducing_rf(manager: ManagerClient):
    '''
    This test checks if hint_sender sends a mutation to a leaving replica if the mutation
    belongs to a tablet which is being removed due to RF--. This is needed to improve
    consistency. https://scylladb.atlassian.net/browse/SCYLLADB-287
    '''
    # We have only one shard to force the two sets of hints to the same shard and avoid
    # the problem with waiting for hint sync point timing out when the only hint on a shard has been dropped
    # https://scylladb.atlassian.net/browse/SCYLLADB-1192
    cmdline = ['--smp=1', "--logger-log-level", "hints_manager=trace"]
    servers = await manager.servers_add(3, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ], cmdline=cmdline)
    cql = await manager.get_cql_exclusive(servers[0])
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ['r2', 'r3']}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int primary key, v int) WITH tablets = {{'min_tablet_count': 1}};")
        host_ids = [await manager.get_host_id(server.server_id) for server in servers]

        # Stop the servers with replicas
        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.others_not_see_server(servers[1].ip_addr)
        await manager.server_stop_gracefully(servers[2].server_id)
        await manager.others_not_see_server(servers[2].ip_addr)

        # This will cause the hint for host_ids[1] to be dropped
        await manager.api.enable_injection(servers[0].ip_addr, 'drop_hint_for_host', one_shot=False, parameters={'hint_host_dst': host_ids[1]})

        # This will attempt to write the hints for both replicas, but only the write for the hint for host_ids[2] will succeed
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (0, 0)", consistency_level=ConsistencyLevel.ANY))

        # Write another record, but this time disable dropping the hint. This is needed to get around the problem
        # where waiting for the hint sync point times out when we only have a single hint which was dropped
        # https://scylladb.atlassian.net/browse/SCYLLADB-1192
        await manager.api.disable_injection(servers[0].ip_addr, 'drop_hint_for_host')
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ANY))

        await manager.api.enable_injection(servers[0].ip_addr, "hinted_handoff_pause_hint_replay", one_shot=False)
        await manager.server_start(servers[1].server_id)
        await manager.server_start(servers[2].server_id)

        coord = await get_topology_coordinator(manager)
        coord_serv = await find_server_by_host_id(manager, servers, coord)
        await manager.api.enable_injection(coord_serv.ip_addr, "stream_tablet_wait", one_shot=False)

        alter_rf_fut = cql.run_async(f"ALTER KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'dc1': ['r2']}}")

        async def migration_reached_streaming():
            stages = await cql.run_async(f"SELECT stage FROM system.tablets WHERE keyspace_name='{ks}' ALLOW FILTERING")
            logger.info(f"Current stages: {[row.stage for row in stages]}")
            return set(["streaming"]) == set([row.stage for row in stages]) or None
        await wait_for(migration_reached_streaming, time.time() + 60)

        sync_point = await create_sync_point(manager.api.client, servers[0].ip_addr)

        # Complete hints handoff
        await manager.api.disable_injection(servers[0].ip_addr, "hinted_handoff_pause_hint_replay")
        assert await await_sync_point(manager.api.client, servers[0].ip_addr, sync_point, 30)

        await manager.api.disable_injection(coord_serv.ip_addr, "stream_tablet_wait")

        await alter_rf_fut

        assert list(await cql.run_async(f"SELECT v FROM {table} WHERE pk = 0")) == [(0,)]
=======

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_hint_to_leaving_when_reducing_rf(manager: ManagerClient):
    '''
    This test checks if hint_sender sends a mutation to a leaving replica if the mutation
    belongs to a tablet which is being removed due to RF--. This is needed to improve
    consistency. https://scylladb.atlassian.net/browse/SCYLLADB-287
    '''
    # We have only one shard to force the two sets of hints to the same shard and avoid
    # the problem with waiting for hint sync point timing out when the only hint on a shard has been dropped
    # https://scylladb.atlassian.net/browse/SCYLLADB-1192
    cmdline = ['--smp=1', "--logger-log-level", "hints_manager=trace"]
    servers = await manager.servers_add(3, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ], cmdline=cmdline)
    cql = await manager.get_cql_exclusive(servers[0])
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ['r2', 'r3']}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int primary key, v int) WITH tablets = {{'min_tablet_count': 1}};")
        host_ids = [await manager.get_host_id(server.server_id) for server in servers]

        # Stop the servers with replicas
        await manager.server_stop_gracefully(servers[1].server_id)
        await manager.others_not_see_server(servers[1].ip_addr)
        await manager.server_stop_gracefully(servers[2].server_id)
        await manager.others_not_see_server(servers[2].ip_addr)

        # This will cause the hint for host_ids[1] to be dropped
        await manager.api.enable_injection(servers[0].ip_addr, 'drop_hint_for_host', one_shot=False, parameters={'hint_host_dst': host_ids[1]})

        # This will attempt to write the hints for both replicas, but only the write for the hint for host_ids[2] will succeed
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (0, 0)", consistency_level=ConsistencyLevel.ANY))

        # Write another record, but this time disable dropping the hint. This is needed to get around the problem
        # where waiting for the hint sync point times out when we only have a single hint which was dropped
        # https://scylladb.atlassian.net/browse/SCYLLADB-1192
        await manager.api.disable_injection(servers[0].ip_addr, 'drop_hint_for_host')
        await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ANY))

        await manager.api.enable_injection(servers[0].ip_addr, "hinted_handoff_pause_hint_replay", one_shot=False)
        await manager.server_start(servers[1].server_id)
        await manager.server_start(servers[2].server_id)

        coord = await get_topology_coordinator(manager)
        coord_serv = await find_server_by_host_id(manager, servers, coord)
        await manager.api.enable_injection(coord_serv.ip_addr, "stream_tablet_wait", one_shot=False)

        alter_rf_fut = cql.run_async(f"ALTER KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'dc1': ['r2']}}")

        async def migration_reached_streaming():
            stages = await cql.run_async(f"SELECT stage FROM system.tablets WHERE keyspace_name='{ks}' ALLOW FILTERING")
            logger.info(f"Current stages: {[row.stage for row in stages]}")
            return set(["streaming"]) == set([row.stage for row in stages]) or None
        await wait_for(migration_reached_streaming, time.time() + 60)

        sync_point = await create_sync_point(manager.api.client, servers[0].ip_addr)

        # Complete hints handoff
        await manager.api.disable_injection(servers[0].ip_addr, "hinted_handoff_pause_hint_replay")
        assert await await_sync_point(manager.api.client, servers[0].ip_addr, sync_point, 30)

        await manager.api.disable_injection(coord_serv.ip_addr, "stream_tablet_wait")

        await alter_rf_fut

        assert list(await cql.run_async(f"SELECT v FROM {table} WHERE pk = 0")) == [(0,)]


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_hint_retransmission_keeps_column_mappings(manager: ManagerClient):
    """
    This test reproduces: https://github.com/scylladb/scylladb/issues/4122.

    A commitlog segment encodes the schema - and with it the column mapping - only in the
    *first* entry written for a given schema version, see `_known_schema_versions` in
    db/commitlog/commitlog.cc. When sending a segment fails partway through, `hint_sender`
    remembers the position of the first hint that failed and retries the segment from there
    (`_last_not_complete_rp`). The entries it re-reads carry no column mapping of their own,
    so the mapping learnt during the previous attempt must survive the retry in
    `_last_schema_ver_to_column_mapping`. Before the fix that cache was dropped between
    attempts, `get_column_mapping()` threw `no_column_mapping`, and the affected hints were
    silently dropped and only accounted for in `scylla_hints_manager_discarded`.

    Reproducing that requires the segment to be retransmitted from a position *past* the
    entry holding the column mapping, i.e. some hints of the segment must be applied before
    the rest fail. The test arranges exactly that:
      1. node2 is stopped and hints for it accumulate on node1 while replay is paused,
      2. node2 is restarted with hint application blocked on an error injection, so that we
         decide which hints get applied,
      3. the first hint of the segment - the one whose entry carries the column mapping - is
         let through,
      4. applying a hint is then made to fail on node2, which aborts the segment and makes
         node1 record a resume position in the middle of it,
      5. hints are let through again and the segment is retransmitted from that position.

    Hint replay is paused while node2's injection is reconfigured. That does not stop a hint
    that is already past the pause check, so the prefix let through in step 3 may be one hint
    longer than intended, but the send concurrency of 1 keeps it at that.
    """
    # Enough hints for the segment to still hold unsent ones once the first ones are applied.
    row_count = 200

    # A single shard means a single hint_sender, so the segment we interrupt is the one that
    # gets retransmitted.
    cmdline = ["--smp=1", "--logger-log-level", "hints_manager=trace"]
    config = {
        # This test relies on the fact that hinted handoff limits the number
        # of hints that can be sent at a time. Set it explicitly.
        #
        # We set it to 1 to avoid problems with mixed results: if we set
        # concurrency to, say, 10, some of the sent hints could succeed,
        # while others fail. If the first hint in the segment failed,
        # then when we started retrying to send the hints, we'd read the
        # segment from the very beginning. That would defeat the point.
        "max_hinted_handoff_concurrency": 1,
        # Make hints replayable quickly.
        "error_injections_at_startup": ["decrease_hints_flush_period"]
    }
    node1, node2 = await manager.servers_add(2, cmdline=cmdline, config=config, auto_rack_dc="dc")

    table = "ks.tbl"
    ks_name, cf_name = table.split(".")

    cql = await manager.get_cql_exclusive(node1)
    await cql.run_async(f"CREATE KEYSPACE {ks_name} WITH replication = "
                        f"{{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}}")
    await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")

    logger.info("Stopping node 2 so that every write stores a hint on node 1")
    await manager.server_stop_gracefully(node2.server_id)
    await manager.others_not_see_server(node2.ip_addr)

    # Replay must not start before we are able to control which hints get applied.
    await manager.api.enable_injection(node1.ip_addr, "hinted_handoff_pause_hint_replay", one_shot=False)

    logger.info(f"Writing {row_count} rows with CL=ONE")
    stmt = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
    stmt.consistency_level = ConsistencyLevel.ONE
    await gather_safely(*(cql.run_async(stmt, [i, i + 1]) for i in range(row_count)))

    await wait_until_hint_writing_settled(manager, [node1])
    written = await get_hint_metrics(manager.metrics, node1.ip_addr, "written")
    assert written > 0

    logger.info("Restarting node 2 with hint application blocked on an injection")
    await manager.server_start(node2.server_id)
    await manager.servers_see_each_other([node1, node2])
    await manager.api.enable_injection(node2.ip_addr, "database_apply", one_shot=False,
                                       parameters={"ks_name": ks_name, "cf_name": cf_name, "what": "wait"})

    node1_log, node2_log = await gather_safely(*[
        asyncio.create_task(manager.server_open_log(node1.server_id)),
        asyncio.create_task(manager.server_open_log(node2.server_id))])
    node2_mark = await node2_log.mark()

    logger.info("Resuming hint replay")
    await manager.api.disable_injection(node1.ip_addr, "hinted_handoff_pause_hint_replay")

    # The first hint is the one that carries the column mapping.
    logger.info("Waiting for the first hint to reach node 2")
    await node2_log.wait_for("database_apply: wait", from_mark=node2_mark)

    # Pause the sender again. The injection is checked once per hint, right before it's sent,
    # so this does not stop the hints that are already past that check: the first one is
    # blocked on node 2, and the second one may already be waiting for the send budget the
    # first one holds. It does stop the third one: the budget is not released until the
    # second hint has been sent, and by then this injection is enabled again.
    await manager.api.enable_injection(node1.ip_addr, "hinted_handoff_pause_hint_replay", one_shot=False)

    node2_mark = await node2_log.mark()
    logger.info("Letting the hint that carries the column mapping through")
    await manager.api.disable_injection(node2.ip_addr, "database_apply")
    await node2_log.wait_for("database_apply: done", from_mark=node2_mark)

    # The first hint - the one whose entry carries the column mapping - has been sent by now.
    # The second one may have been sent too, as described above, but nothing beyond it, so
    # the segment still holds hints that have to be retransmitted.
    await wait_until_hints_are_sent_from(manager, [node1], expected_count=1)
    sent = await get_hint_metrics(manager.metrics, node1.ip_addr, "sent_total")
    assert sent <= 2, f"More hints were sent than expected: {sent}"

    # From now on applying a hint fails on node2, so the rest of the segment cannot be sent.
    logger.info("Making the remaining hints fail on node 2")
    await manager.api.enable_injection(node2.ip_addr, "database_apply", one_shot=False,
                                       parameters={"ks_name": ks_name, "cf_name": cf_name, "what": "throw"})

    node1_mark = await node1_log.mark()
    await manager.api.disable_injection(node1.ip_addr, "hinted_handoff_pause_hint_replay")

    # hint_sender logs this when it gives up on a segment and records where to resume it.
    logger.info("Waiting for the segment to be aborted mid-way")
    await node1_log.wait_for("Error while sending hints from", from_mark=node1_mark)

    logger.info("Letting hints through again - the segment is retransmitted from the recorded position")
    await manager.api.disable_injection(node2.ip_addr, "database_apply")

    await wait_until_hints_are_sent_from(manager, [node1], expected_count=written)

    discarded = await get_hint_metrics(manager.metrics, node1.ip_addr, "discarded")
    assert discarded == 0, f"{discarded} hints were discarded while retransmitting the segment"

    # The metric above is the direct regression check. Verify the data as well so that the
    # test keeps its meaning if hints ever get lost without being accounted as discarded.
    logger.info("Verifying that node 2 received all the rows")
    await manager.server_stop_gracefully(node1.server_id)
    cql = await manager.get_cql_exclusive(node2)
    rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {table}",
                                                consistency_level=ConsistencyLevel.ONE))
    assert sorted((row.pk, row.v) for row in rows) == [(i, i + 1) for i in range(row_count)]
>>>>>>> e26fec5e79 (test/cluster/test_hints.py: Migrate dtest test_hintedhandoff_retransmit)
