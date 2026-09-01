#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import glob
import os
import pytest
import time
import logging
import re

from cassandra.cluster import NoHostAvailable  # type: ignore
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.internal_types import IPAddress, ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import HTTPError, ScyllaMetricsClient, TCPRESTClient, inject_error
from test.pylib.tablets import get_tablet_replicas
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import gather_safely, wait_for

from test.pylib import nodetool
from test.cluster.util import get_topology_coordinator, keyspace_has_tablets, new_test_keyspace, new_test_table


logger = logging.getLogger(__name__)


async def get_metric(client: ScyllaMetricsClient, server_ip: IPAddress, metric_name: str) -> float | None:
    metrics = await client.query(server_ip)
    return metrics.get(metric_name)


async def get_hint_metrics(client: ScyllaMetricsClient, server_ip: IPAddress, metric_name: str):
    return await get_metric(client, server_ip, f"scylla_hints_manager_{metric_name}")


async def sum_hint_metric(client: ScyllaMetricsClient, servers: list[ServerInfo], metric_name: str) -> float:
    total = 0.0
    results = await gather_safely(
        *[get_hint_metrics(client, server.ip_addr, metric_name) for server in servers])
    for result in results:
        total += result or 0.0
    return total


async def get_all_hint_metrics(client: ScyllaMetricsClient, server_ip: IPAddress) -> dict:
    metrics = await client.query(server_ip)

    def resolve_metric(name: str):
        return metrics.get(f"scylla_hints_manager_{name}") or 0.0

    metric_names = [
        "size_of_hints_in_progress",
        "written",
        "errors",
        "dropped",
        "sent_total",
        "sent_bytes_total",
        "discarded",
        "send_errors",
        "corrupted_files",
        "pending_drains",
        "pending_sends"]

    return {name: resolve_metric(name) for name in metric_names}


# Hint segments live in <workdir>/hints/<shard>/<target host id>/HintsLog-*.log.
async def get_hints_dir(manager: ScyllaClusterManager, server: ServerInfo) -> str:
    return os.path.join(await manager.server_get_workdir(server.server_id), "hints")


def hint_dir_exists(hints_dir: str, target_host_id: str, shard: int | None = None) -> bool:
    shard_glob = "*" if shard is None else str(shard)
    return len(glob.glob(os.path.join(hints_dir, shard_glob, str(target_host_id)))) > 0


def list_hint_target_dirs(hints_dir: str) -> set[str]:
    return {os.path.basename(path) for path in glob.glob(os.path.join(hints_dir, "*", "*"))}


def count_hint_segments(hints_dir: str, target_host_id: str, shard: int | None = None) -> int:
    shard_glob = "*" if shard is None else str(shard)
    return len(glob.glob(os.path.join(hints_dir, shard_glob, str(target_host_id), "HintsLog-*.log")))


async def wait_until_hint_writing_settled(manager: ScyllaClusterManager, servers: list[ServerInfo],
                                          timeout: float = 180) -> None:
    async def check():
        all_metrics = await gather_safely(*[get_all_hint_metrics(manager.metrics, srv.ip_addr) for srv in servers])
        logger.debug(f"Waiting for hint writing to settle. Metric snapshot: {all_metrics}")
        in_progress = sum([metrics["size_of_hints_in_progress"] for metrics in all_metrics])
        return True if in_progress == 0 else None
    await wait_for(check, time.time() + timeout)


async def wait_until_hints_are_sent_from(manager: ScyllaClusterManager, servers: list[ServerInfo],
                                         expected_count: float, timeout: float = 180) -> None:
    """
    Wait until `expected_count` hints have been sent on `servers`.
    """
    async def check():
        all_metrics = await gather_safely(*[get_all_hint_metrics(manager.metrics, srv.ip_addr) for srv in servers])
        logger.debug(f"Waiting for {expected_count} hints in total to be sent. Metric snapshot: {all_metrics}")
        sent = sum([metrics["sent_total"] for metrics in all_metrics])
        return True if sent >= expected_count else None
    await wait_for(check, time.time() + timeout)


async def capture_stable_hint_count(manager: ScyllaClusterManager, servers: list[ServerInfo],
                                    stable_checks: int = 3, interval: float = 2, timeout: float = 60) -> float:
    """
    Poll the cumulative `written` hint counter until it stops changing, then return it.

    Writing a hint for a down replica is a fire-and-forget background operation relative to
    the client-visible ack of a write that didn't use CL=ANY, so the counter can still be
    climbing for a while after the last write returned.
    """
    stable = 0
    last = await sum_hint_metric(manager.metrics, servers, "written")
    deadline = time.time() + timeout
    while stable < stable_checks - 1:
        assert time.time() < deadline, f"the number of written hints did not stabilize (last value: {last})"
        await asyncio.sleep(interval)
        current = await sum_hint_metric(manager.metrics, servers, "written")
        stable = stable + 1 if current == last else 0
        last = current
    return last


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


async def update_hh_enabled_via_http_api(manager: ScyllaClusterManager, server: ServerInfo, new_value: str) -> None:
    """Change hint generation options at runtime and verify the change is visible."""
    if new_value in ("true", "false"):
        endpoint = "/storage_proxy/hinted_handoff_enabled"
        params = {"enable": new_value}
        expected = new_value == "true"
    else:
        endpoint = "/storage_proxy/hinted_handoff_enabled_by_dc"
        params = {"dcs": new_value}
        expected = sorted(new_value.split(","))

    logger.info(f"Setting hint generation options on {server.ip_addr} to {new_value} via {endpoint}")
    await manager.api.client.post(endpoint, host=server.ip_addr, params=params)

    response = await manager.api.client.get_json(endpoint, host=server.ip_addr)
    # The list of DCs may come back in a different order than the one we requested.
    if isinstance(response, list):
        response = sorted(response)
    assert response == expected


async def assert_rows_present(cql, table: str, pk: str, keys, present: bool) -> None:
    stmt = SimpleStatement(f"SELECT {pk} FROM {table}", consistency_level=ConsistencyLevel.ONE)
    rows = await cql.run_async(stmt, all_pages=True)
    results = set(getattr(row, pk) for row in rows)
    keys = set(keys)

    if present:
        assert keys.issubset(results), f"{keys} vs. {results}"
    else:
        assert len(keys.intersection(results)) == 0, f"{keys} vs. {results}"


# Write with RF=1 and CL=ANY to a dead node should write hints and succeed
async def test_write_cl_any_to_dead_node_generates_hints(manager: ScyllaClusterManager):
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

async def test_limited_concurrency_of_writes(manager: ScyllaClusterManager):
    """
    We want to verify that Scylla correctly limits the concurrency of writing hints to disk.
    To do that, we leverage error injections decreasing the threshold when hints should start
    being rejected, and we expect to receive an exception indicating that a node is overloaded.
    """
    node1 = await manager.server_add(config={
        "error_injections_at_startup": ["decrease_max_size_of_hints_in_progress"]
    }, property_file = {"dc":"dc1", "rack":"rack1"})
    node2 = await manager.server_add(property_file = {"dc":"dc1", "rack":"rack2"})

    cql = await manager.get_cql_exclusive(node1)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
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

async def test_sync_point(manager: ScyllaClusterManager):
    """
    We want to verify that the sync point API is compliant with its design.
    This test concerns one particular aspect of it: Scylla should create a sync point
    for ALL nodes if the parameter `target_hosts` of a request is empty, not just
    live nodes.
    """
    node_count = 3
    [node1, node2, node3] = await manager.servers_add(node_count, auto_rack_dc="dc1")

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
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


@pytest.mark.skip_mode(mode='release', reason="error injections aren't enabled in release mode")
async def test_hints_consistency_during_decommission(manager: ScyllaClusterManager):
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
            coord_srv = await manager.find_server_by_host_id([server1, server2, server3], coord)

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

async def test_hints_consistency_during_replace(manager: ScyllaClusterManager):
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

async def test_draining_hints(manager: ScyllaClusterManager):
    """
    This test verifies that all hints are drained when a node is being decommissioned.
    """

    s1, s2, s3 = await manager.servers_add(3, auto_rack_dc="dc")

    cql = manager.get_cql()

    await manager.api.set_logger_level(s1.ip_addr, "hints_manager", "trace")

    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s2.server_id)

    # Generate hints towards s2 on s1 with probability 1 - ((#nodes - 1) / #nodes)^1000 ~= 1.
    for i in range(1000):
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ANY))

    sync_point = await create_sync_point(manager.api.client, s1.ip_addr)
    await manager.server_start(s2.server_id)

    await cql.run_async(f"ALTER KEYSPACE ks WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'dc': {[s2.rack, s3.rack]}}}")
    async with asyncio.TaskGroup() as tg:
        _ = tg.create_task(manager.decommission_node(s1.server_id, timeout=60))
        _ = tg.create_task(await_sync_point(manager.api.client, s1.ip_addr, sync_point, 60))

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_canceling_hint_draining(manager: ScyllaClusterManager):
    """
    This test verifies that draining hints is canceled as soon as we issue a shutdown,
    but it's resumed after starting the node again.
    """
    s1, s2, s3 = await manager.servers_add(3, auto_rack_dc="dc")

    cql = manager.get_cql()
    host_id2 = await manager.get_host_id(s2.server_id)

    await manager.api.set_logger_level(s1.ip_addr, "hints_manager", "trace")

    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s2.server_id)

    # Generate hints towards s2 on s1 with probability 1 - ((#nodes - 1) / #nodes)^1000 ~= 1.
    for i in range(1000):
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ANY))

    sync_point = await create_sync_point(manager.api.client, s1.ip_addr)

    await manager.api.enable_injection(s1.ip_addr, "hinted_handoff_pause_hint_replay", False, {})
    await nodetool.excludenode(cql, host_id2)
    await cql.run_async(f"ALTER KEYSPACE ks WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'dc': {[s1.rack, s3.rack]}}}")

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

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_hint_to_pending(manager: ScyllaClusterManager):
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

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_hint_to_leaving_when_reducing_rf(manager: ScyllaClusterManager):
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
        coord_serv = await manager.find_server_by_host_id(servers, coord)
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


@pytest.mark.skip_mode(mode="release", reason="error injections aren't enabled in release mode")
async def test_hints_rebalance(manager: ScyllaClusterManager):
    """
    Verify that hint segments are rebalanced evenly across shards when the shard count changes,
    that the hint directories of shards which no longer exist are removed, and that the hints
    are still delivered afterwards.
    """
    config = {"error_injections_at_startup": ["decrease_hint_segment_size", "decrease_hints_flush_period"]}
    servers = await manager.servers_add(2, cmdline=["--smp", "1"], config=config, auto_rack_dc="dc1")
    s1, s2 = servers

    # Disable tablet load balancing so that tablets don't migrate out of shard 0. The cluster
    # starts with a single shard, so with balancing disabled every tablet replica stays on
    # shard 0 and reducing the shard count later in the test remains legal
    # (https://github.com/scylladb/scylladb/issues/16739).
    await manager.disable_tablet_balancing()

    s1_hints_dir = await get_hints_dir(manager, s1)
    s2_host_id = await manager.get_host_id(s2.server_id)

    cql = await manager.get_cql_exclusive(s1)
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s2.server_id)
    await manager.others_not_see_server(s2.ip_addr)

    # We cannot rely on glob listing the segments as some of them
    # may get removed by commitlog if they're redundant.
    async def populate_segments(min_segment_count: int) -> None:
        # This relies on the segment storing no more than 1 MiB of data.
        inserts_per_segment = 4500
        # Let's give ourselves some slack to make sure that we really
        # create those segments.
        insert_count = (inserts_per_segment + 1500) * min_segment_count
        batch_size = 1000
        batch_count = insert_count // batch_size

        insert_stmt = cql.prepare("INSERT INTO ks.t (pk, v) VALUES (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ONE

        for batch_idx in range(0, batch_count):
            begin, end = batch_idx * batch_size, (batch_idx + 1) * batch_size
            await gather_safely(*[cql.run_async(insert_stmt, (pk, pk)) for pk in range(begin, end)])
            # Avoid overloading the server.
            await wait_until_hint_writing_settled(manager, [s1])

        dropped = await get_hint_metrics(manager.metrics, s1.ip_addr, "dropped")
        errors = await get_hint_metrics(manager.metrics, s1.ip_addr, "errors")
        assert dropped == 0, f"{s1.ip_addr} dropped hints count: {dropped}"
        assert errors == 0, f"{s1.ip_addr} error count: {errors}"

        return insert_count

    # Three segments should be enough to get confident rebalancing works
    # after changing SMP: 1 -> 3 -> 2.
    expected_rows = await populate_segments(3)

    async def restart_with_smp(server_id, smp: int, hinted_handoff_enabled: bool):
        await manager.server_stop_gracefully(server_id)
        await manager.server_update_cmdline(server_id, [f"--smp={smp}"])
        hh_enabled = "true" if hinted_handoff_enabled else "fake_dc"
        await manager.server_update_config(server_id, "hinted_handoff_enabled", hh_enabled)
        await manager.server_start(server_id)

    async def check_balanced(hints_dir, num_shards: int) -> None:
        async def check():
            counts = [count_hint_segments(hints_dir, s2_host_id, shard) for shard in range(num_shards)]
            logger.info(f"Hint segments per shard: {counts}")
            # If there are no directories, the test verifies nothing.
            # Let's not silently pass.
            if max(counts) == 0:
                return None
            if max(counts) - min(counts) <= 1:
                return True
            logger.info(f"hint segments are not evenly balanced across shards: {counts}")
            return None
        await wait_for(check, time.time() + 120)

    logger.info("Restarting s1 with 3 shards and hinted handoff disabled")
    await restart_with_smp(s1.server_id, smp=3, hinted_handoff_enabled=False)
    await check_balanced(s1_hints_dir, num_shards=3)

    logger.info("Restarting s1 with 2 shards and hinted handoff disabled")
    await restart_with_smp(s1.server_id, smp=2, hinted_handoff_enabled=False)
    await check_balanced(s1_hints_dir, num_shards=2)

    # Check that shard 2 directories are gone.
    async def check_directory_gone():
        return True if (not hint_dir_exists(s1_hints_dir, s2_host_id, shard=2)) else None
    await wait_for(check_directory_gone, time.time() + 120)

    await manager.server_start(s2.server_id)
    # Re-enable hinted handoff.
    await update_hh_enabled_via_http_api(manager, s1, "true")
    await wait_until_hints_are_sent_from(manager, [s1], expected_rows)

    await manager.server_stop_gracefully(s1.server_id)

    cql = await manager.get_cql_exclusive(s2)
    await assert_rows_present(cql, "ks.t", "pk", list(range(expected_rows)), present=True)


async def test_hints_removenode(manager: ScyllaClusterManager):
    """
    Hints addressed to a node that is removed from the cluster with removenode must be drained
    to the new owners of the data instead of being dropped.
    """
    cmdline = ["--logger-log-level", "hints_manager=trace"]
    s1, _, s3 = await manager.servers_add(3, cmdline=cmdline, property_file={"dc": "dc1", "rack": "rack1"})

    s3_host_id = await manager.get_host_id(s3.server_id)
    s1_hints_dir = await get_hints_dir(manager, s1)

    cql = await manager.get_cql_exclusive(s1)
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s3.server_id)
    await manager.others_not_see_server(s3.ip_addr)

    row_count = 100
    stmt = cql.prepare("INSERT INTO ks.t (pk, v) VALUES (?, ?)")
    stmt.consistency_level = ConsistencyLevel.ANY
    await gather_safely(*[cql.run_async(stmt, (pk, pk + 1)) for pk in range(row_count)])

    await wait_until_hint_writing_settled(manager, [s1])
    written = await get_hint_metrics(manager.metrics, s1.ip_addr, "written")
    assert written > 0

    await manager.remove_node(s1.server_id, s3.server_id)
    await wait_until_hints_are_sent_from(manager, [s1], written)

    async def hint_dir_removed():
        return True if not hint_dir_exists(s1_hints_dir, s3_host_id) else None
    await wait_for(hint_dir_removed, time.time() + 60)

    rows = await cql.run_async("SELECT * FROM ks.t")
    results = set((row.pk, row.v) for row in rows)
    expected = set((pk, pk + 1) for pk in range(row_count))
    assert results == expected, f"Mismatch: {results} vs. {expected}"


async def test_hints_basic_check(manager: ScyllaClusterManager):
    """
    The most basic scenario: a write performed while a replica is down must reach that replica
    once it comes back, and it must reach it through a hint - which is verified by reading the
    row from that replica alone.
    """
    s1, s2, s3 = await manager.servers_add(3, auto_rack_dc="dc1")

    s1_host_id = await manager.get_host_id(s1.server_id)
    s2_hints_dir = await get_hints_dir(manager, s2)
    s3_hints_dir = await get_hints_dir(manager, s3)

    cql = await manager.get_cql_exclusive(s2)
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s1.server_id)
    await manager.others_not_see_server(s1.ip_addr)

    await cql.run_async(SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (0, 1)",
                                        consistency_level=ConsistencyLevel.ONE))

    await asyncio.sleep(5)
    assert hint_dir_exists(s2_hints_dir, s1_host_id) or hint_dir_exists(s3_hints_dir, s1_host_id)

    await manager.server_start(s1.server_id)
    await manager.servers_see_each_other([s1, s2, s3])

    hint_flush_threshold = 15
    await asyncio.sleep(hint_flush_threshold)

    await manager.server_stop_gracefully(s2.server_id)
    await manager.server_stop_gracefully(s3.server_id)

    cql = await manager.get_cql_exclusive(s1)
    rows = list(await cql.run_async(SimpleStatement("SELECT v FROM ks.t WHERE pk = 0",
                                                    consistency_level=ConsistencyLevel.ONE)))
    assert rows == [(1,)], "the hint was not replayed to the node that was down"


async def test_hints_counter(manager: ScyllaClusterManager):
    """
    Counter updates must be replayed correctly from hints, both when the hint's target is still
    a replica for the mutation ("regular" hints) and when a topology change has made it stop
    being one ("orphaned" hints) - the two use different code paths when being sent. Counters
    are not idempotent, so an incorrectly replayed hint is immediately visible in the values.
    """
    s1, s2 = await manager.servers_add(2, auto_rack_dc="dc1")

    cql = await manager.get_cql_exclusive(s1)
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', "
                        "'replication_factor': 2}")
    await cql.run_async("CREATE TABLE ks.tbl (pk int PRIMARY KEY, c counter) WITH speculative_retry = 'NONE' AND read_repair_chance = 0 AND dclocal_read_repair_chance = 0")

    await manager.server_stop_gracefully(s2.server_id)
    await manager.others_not_see_server(s2.ip_addr)

    # Counter updates need at least CL=ONE, they can't use CL=ANY.
    stmt = cql.prepare("UPDATE ks.tbl SET c = c + ? WHERE pk = ?")
    stmt.consistency_level = ConsistencyLevel.ONE
    expected = {pk: 100 * pk for pk in range(100)}
    for pk, value in expected.items():
        await cql.run_async(stmt, (value, pk))

    # Restart s1 with hinted handoff disabled, so that it doesn't start sending the hints
    # just written as soon as s2 comes back, before the topology change below has a chance
    # to make some of them orphaned.
    logger.info("Restarting s1 with hinted handoff disabled...")
    await manager.server_stop_gracefully(s1.server_id)
    await manager.server_update_cmdline(s1.server_id, ["--hinted-handoff-enabled", "false"])
    await manager.server_start(s1.server_id)
    cql = await manager.get_cql_exclusive(s1)

    logger.info("Starting s2...")
    await manager.server_start(s2.server_id)
    await manager.servers_see_each_other([s1, s2])

    # The new node makes some of the stored hints orphaned.
    logger.info("Adding s3...")
    s3 = await manager.server_add(property_file=s2.property_file())

    logger.info("Restarting s1 with hinted handoff enabled...")
    await manager.server_stop_gracefully(s1.server_id)
    await manager.server_update_cmdline(s1.server_id, ["--hinted-handoff-enabled", "true"])
    await manager.server_start(s1.server_id)
    await manager.servers_see_each_other([s1, s2])
    await manager.servers_see_each_other([s1, s3])

    async def hints_sent() -> bool | None:
        sent = await get_hint_metrics(manager.metrics, s1.ip_addr, "sent_total")
        return True if sent and sent >= len(expected) else None
    await wait_for(hints_sent, time.time() + 60)

    # We want to check that hints for counters are sent correctly.
    # At this point, there should be 100 hints sent from node1 and node2.
    # There was a topology change between hints storing and sending, so
    # node2 might no longer be a replica for some of them (those hints are
    # orphaned in a sense). Non-orphaned and orphaned hints use a slightly
    # different code path for sending and we want to test them both.
    #
    # We can check if values written by both paths have sensible values
    # by stopping node1 and reading with CL=ONE.
    # - Rows with nodes 1 & 2 as replicas were written to node2
    #   by non-orphaned hints. We will read those rows from node2 only,
    #   because node1 is down.
    # - Rows with nodes 1 & 3 as replicas were written to node3
    #   by orphaned hints. If they were sent incorrectly, they
    #   will overwrite what node3 previously had. We will read
    #   those rows from node3 only, because node1 is down.
    # - Rows with nodes 2 & 3 as replicas will be read from either node2
    #   or node3. They should have correct value, but it won't be obvious
    #   from which node the value came if it is incorrect.
    await manager.server_stop_gracefully(s1.server_id)

    cql = await manager.get_cql_exclusive(s3)
    read_stmt = cql.prepare("SELECT c FROM ks.tbl WHERE pk = ?")
    read_stmt.consistency_level = ConsistencyLevel.ONE
    actual = {pk: list(await cql.run_async(read_stmt, (pk,)))[0].c for pk in expected}
    assert actual == expected


async def test_hints_decom(manager: ScyllaClusterManager):
    """
    Verify that hints are drained when a target for hints is decommissioned.
    """
    cmdline = ["--logger-log-level", "hints_manager=trace:storage_proxy=trace"]
    s1, s2, s3 = await manager.servers_add(3, cmdline=cmdline, property_file={"dc": "dc1", "rack": "r1"})

    s1_hints_dir = await get_hints_dir(manager, s1)
    s2_hints_dir = await get_hints_dir(manager, s2)
    s3_host_id = await manager.get_host_id(s3.server_id)

    cql = await manager.get_cql_exclusive(s1)
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', "
                        "'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.tbl (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s3.server_id)
    await manager.others_not_see_server(s3.ip_addr)

    row_count = 100
    await gather_safely(*[cql.run_async(SimpleStatement(
        f"INSERT INTO ks.tbl (pk, v) VALUES ({i}, {i + 1})",
        consistency_level=ConsistencyLevel.ANY
    )) for i in range(row_count)])

    await manager.server_start(s3.server_id)
    await manager.servers_see_each_other([s1, s2, s3])

    await manager.decommission_node(s3.server_id)

    hint_flush_threshold = 15
    await asyncio.sleep(hint_flush_threshold)

    assert not hint_dir_exists(s1_hints_dir, s3_host_id)
    assert not hint_dir_exists(s2_hints_dir, s3_host_id)


async def test_hints_dont_revive(manager: ScyllaClusterManager):
    """
    A hint carries the timestamp of the original write, so replaying it after the row has been
    deleted must not resurrect the row.
    """
    s1, s2, s3 = await manager.servers_add(3, auto_rack_dc="dc1")

    s1_host_id = await manager.get_host_id(s1.server_id)
    s2_hint_dir = await get_hints_dir(manager, s2)
    s3_hint_dir = await get_hints_dir(manager, s3)

    cql = await manager.get_cql_exclusive(s2)
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s1.server_id)
    await manager.others_not_see_server(s1.ip_addr)

    await cql.run_async(SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (0, 1)",
                                        consistency_level=ConsistencyLevel.TWO))

    await asyncio.sleep(5)

    assert hint_dir_exists(s2_hint_dir, s1_host_id) or hint_dir_exists(s3_hint_dir, s1_host_id)

    await manager.server_start(s1.server_id)
    await manager.servers_see_each_other([s1, s2, s3])

    hinting_node = None
    non_hinting_node = None
    if hint_dir_exists(s2_hint_dir, s1_host_id):
        await manager.server_stop_gracefully(s2.server_id)
        await manager.others_not_see_server(s2.ip_addr)
        hinting_node = s2
        non_hinting_node = s3
    else:
        await manager.server_stop_gracefully(s3.server_id)
        await manager.others_not_see_server(s3.ip_addr)
        hinting_node = s3
        non_hinting_node = s2

    await manager.server_start(s1.server_id)
    await manager.servers_see_each_other([s1, non_hinting_node])

    cql = await manager.get_cql_exclusive(s1)
    await cql.run_async(SimpleStatement("DELETE FROM ks.t WHERE pk = 0",
                                        consistency_level=ConsistencyLevel.TWO))

    await manager.server_start(hinting_node.server_id)

    hint_flush_threshold = 15
    logger.info(f"Waiting {hint_flush_threshold}s for hints to be sent...")
    await asyncio.sleep(hint_flush_threshold)

    await manager.server_stop_gracefully(s2.server_id)
    await manager.server_stop_gracefully(s3.server_id)
    await manager.server_not_sees_other_server(s1.ip_addr, s2.ip_addr)
    await manager.server_not_sees_other_server(s1.ip_addr, s3.ip_addr)

    results = await cql.run_async(SimpleStatement(
        "SELECT * FROM ks.t WHERE pk = 0",
        consistency_level=ConsistencyLevel.ONE))
    assert len(results) == 0

    logger.info("Checking on node2...")
    await manager.server_start(s2.server_id)
    await manager.servers_see_each_other([s1, s2])
    await manager.server_stop_gracefully(s1.server_id)
    await manager.server_not_sees_other_server(s2.ip_addr, s1.ip_addr)

    cql = await manager.get_cql_exclusive(s2)
    results = await cql.run_async(SimpleStatement(
        "SELECT * FROM ks.t WHERE pk = 0",
        consistency_level=ConsistencyLevel.ONE))
    assert len(results) == 0

    logger.info("Checking on node3...")
    await manager.server_start(s3.server_id)
    await manager.servers_see_each_other([s2, s3])
    await manager.server_stop_gracefully(s2.server_id)
    await manager.server_not_sees_other_server(s3.ip_addr, s2.ip_addr)

    cql = await manager.get_cql_exclusive(s3)
    results = await cql.run_async(SimpleStatement(
        "SELECT * FROM ks.t WHERE pk = 0",
        consistency_level=ConsistencyLevel.ONE))
    assert len(results) == 0


async def validate_max_hinted_handoff_concurrency(manager: ScyllaClusterManager, server: ServerInfo,
                                                  expected_value: int) -> None:
    value = await manager.api.client.get_json("/v2/config/max_hinted_handoff_concurrency",
                                              host=server.ip_addr, port=10_000)
    assert int(value) == expected_value, \
        f"expected max_hinted_handoff_concurrency to be {expected_value} on {server.ip_addr}, got {value}"


@pytest.mark.parametrize("max_hinted_handoff_concurrency,cmdline", [
    (0, None),
    (64, None),
    (128, ["--max-hinted-handoff-concurrency=128"]),
])
async def test_support_max_hh_concurrency_param(manager: ScyllaClusterManager,
                                                max_hinted_handoff_concurrency: int,
                                                cmdline: list[str] | None):
    """
    The max_hinted_handoff_concurrency option must be readable through the config REST API
    whether it's left at its default, set in scylla.yaml, or passed on the command line, and
    hints must still be delivered when the concurrency is capped.
    """
    # 0 is the default and means "8 * shard_count", so it's deliberately left unset.
    config = None
    if cmdline is None and max_hinted_handoff_concurrency != 0:
        config = {"max_hinted_handoff_concurrency": max_hinted_handoff_concurrency}

    servers = await manager.servers_add(3, cmdline=cmdline, config=config, auto_rack_dc="dc1")
    s1, s2, s3 = servers

    for server in servers:
        await validate_max_hinted_handoff_concurrency(manager, server, max_hinted_handoff_concurrency)

    cql = await manager.get_cql_exclusive(s2)
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s1.server_id)
    await manager.others_not_see_server(s1.ip_addr)

    row_count = 100
    stmt = cql.prepare("INSERT INTO ks.t (pk, v) VALUES (?, ?)")
    stmt.consistency_level = ConsistencyLevel.ONE
    await gather_safely(*[cql.run_async(stmt, (pk, pk + 1)) for pk in range(row_count)])

    await manager.server_start(s1.server_id)
    await manager.servers_see_each_other([s1, s2, s3])

    await wait_until_hints_are_sent_from(manager, [s2, s3], expected_count=row_count)

    await manager.server_stop_gracefully(s2.server_id)
    await manager.others_not_see_server(s2.ip_addr)

    cql = await manager.get_cql_exclusive(s1)
    results = await cql.run_async("SELECT count(*) FROM ks.t")
    assert results[0].count == row_count


async def test_ignore_invalid_hint_directories(manager: ScyllaClusterManager):
    """
    A directory whose name is invalid must be ignored by the hint manager - both
    by the periodic scan and when the host filter changes - and it must be left
    untouched on disk.
    """
    hint_dir_name = "gibberish_name"

    server = await manager.server_add(cmdline=["--logger-log-level", "hints_resource_manager=trace"])
    hints_dir = await get_hints_dir(manager, server)

    artificial_dir = os.path.join(hints_dir, "0", hint_dir_name)
    os.makedirs(artificial_dir, exist_ok=True)
    with open(os.path.join(artificial_dir, "gibberish_hint_file"), "w") as f:
        f.write("Some random content for the file")

    log = await manager.server_open_log(server.server_id)

    await log.wait_for(f"Encountered a hint directory of invalid name while scanning: {hint_dir_name}", timeout=60)
    assert list_hint_target_dirs(hints_dir) == {hint_dir_name}

    # Changing the DCs hints may be sent to forces the shard hint managers to redo their scan.
    mark = await log.mark()
    await update_hh_enabled_via_http_api(manager, server, "some_dc")
    await log.wait_for(f"Encountered a hint directory of invalid name while changing the host filter: "
                       f"{hint_dir_name}", from_mark=mark, timeout=60)
    assert list_hint_target_dirs(hints_dir) == {hint_dir_name}


async def test_hints_switch_config_in_runtime_via_http_api(manager: ScyllaClusterManager):
    """
    Enabling, disabling and DC-filtering hinted handoff through the HTTP API must take effect
    at runtime, on every shard.
    Ref: https://github.com/scylladb/scylla/issues/5634
    """
    # Hinted handoff must not be set on the command line: a command line option always overrides
    # the configuration file and would prevent the option from being reloaded at runtime.
    # Multiple shards are used so that we also check that the filtering is updated on all of them.
    cmdline = ["--smp", "3", "--logger-log-level", "hints_manager=trace"]
    config = {"hinted_handoff_enabled": "false"}
    s1, s2, s3 = await manager.servers_add(3, cmdline=cmdline, config=config, property_file=[
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc2", "rack": "rack1"},
        {"dc": "dc3", "rack": "rack1"}])

    keys_enabled = list(range(100))
    keys_disabled = list(range(100, 200))
    keys_dc3_only = list(range(200, 300))
    expected_hints = 0

    cql = await manager.get_cql_exclusive(s1)
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = "
                        "{'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1, 'dc3': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s2.server_id)
    await manager.server_stop_gracefully(s3.server_id)
    await manager.server_not_sees_other_server(s1.ip_addr, s2.ip_addr)
    await manager.server_not_sees_other_server(s1.ip_addr, s3.ip_addr)

    stmt = cql.prepare("INSERT INTO ks.t (pk, v) VALUES (?, ?)")
    stmt.consistency_level = ConsistencyLevel.ONE

    async def insert(keys) -> None:
        for key in keys:
            await cql.run_async(stmt, (key, key + 1))

    await update_hh_enabled_via_http_api(manager, s1, "true")
    await insert(keys_enabled)
    # Each write generates a hint for the dc2 replica and one for the dc3 replica.
    expected_hints += 2 * len(keys_enabled)

    await update_hh_enabled_via_http_api(manager, s1, "false")
    await insert(keys_disabled)
    # No hints should be generated at all.

    # The extra dummy DCs also exercise the parsing of the comma-separated list.
    await update_hh_enabled_via_http_api(manager, s1, ",".join([s3.datacenter, "some-dc", "some-other-dc"]))
    await insert(keys_dc3_only)
    expected_hints += len(keys_dc3_only)

    await manager.server_start(s2.server_id)
    await manager.server_start(s3.server_id)
    await manager.servers_see_each_other([s1, s2, s3])

    await update_hh_enabled_via_http_api(manager, s1, "true")
    await wait_until_hints_are_sent_from(manager, [s1], expected_hints)

    await manager.server_stop_gracefully(s1.server_id)
    await manager.server_stop_gracefully(s3.server_id)

    cql = await manager.get_cql_exclusive(s2)
    await assert_rows_present(cql, "ks.t", "pk", keys_enabled, present=True)
    await assert_rows_present(cql, "ks.t", "pk", keys_disabled, present=False)
    await assert_rows_present(cql, "ks.t", "pk", keys_dc3_only, present=False)

    await manager.server_start(s3.server_id)
    await manager.server_stop_gracefully(s2.server_id)

    cql = await manager.get_cql_exclusive(s3)
    await assert_rows_present(cql, "ks.t", "pk", keys_enabled, present=True)
    await assert_rows_present(cql, "ks.t", "pk", keys_disabled, present=False)
    await assert_rows_present(cql, "ks.t", "pk", keys_dc3_only, present=True)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_hintedhandoff_sync_point_api(manager: ScyllaClusterManager):
    """
    Tests the HTTP API for hint sync points.
    Hint sync points allow to wait until all current hints are sent
    between two specified sets of nodes: sources and destinations.

    There are five subtests:
    - Check that waiting for a point finishes after all waited on hints are replayed
    - Check that waiting for a point aborts when the waiting node shuts down
    - Check that waiting for a point works after the node is restarted with a different number of shards
    - Check that waiting for a point created on another node is forbidden
    - Check that waiting for a point works when the waiting node is being decommissioned
    """

    async def start_node(server: ServerInfo, smp: int = 3) -> None:
        await manager.server_update_cmdline(server.server_id,
                                            ["--smp", str(smp), "--logger-log-level", "hints_manager=trace"])
        await manager.server_start(server.server_id)
        await manager.servers_see_each_other([s1, s2])

    async def create_sync_point(node: ServerInfo = None) -> str:
        node = node or s1
        sync_point_id = await manager.api.client.post_json(
            "/hinted_handoff/sync_point", host=node.ip_addr, port=10_000,
            params={"target_hosts": s2.ip_addr})
        assert isinstance(sync_point_id, str)
        logger.info(f"Created a sync point on {node.server_id} / {node.ip_addr}: {sync_point_id}")
        return sync_point_id

    async def wait_for_sync_point(sync_point_id: str, timeout: int, expect: str, node: ServerInfo = None) -> None:
        node = node or s1
        logger.info(f"Waiting for a sync point on {node.server_id} / {node.ip_addr}: {sync_point_id}")
        params = {"id": sync_point_id, "timeout": str(timeout)}
        if expect == "FAILED":
            try:
                status = await manager.api.client.get_json("/hinted_handoff/sync_point", host=node.ip_addr,
                                                            port=10_000, params=params)
                pytest.fail(f"Expected waiting for the sync point to fail, but got: {status}")
            except HTTPError as e:
                logger.debug(f"Got the expected failure: {e}")
        else:
            status = await manager.api.client.get_json("/hinted_handoff/sync_point", host=node.ip_addr,
                                                        port=10_000, params=params)
            assert status == expect
            logger.debug(f"Got status {status}, which was expected")

    s1, s2 = await manager.servers_add(2, cmdline=["--smp", "3", "--logger-log-level", "hints_manager=trace"],
                                       property_file={"dc": "dc1", "rack": "r1"})

    # We are using RF=1, so roughly half of the writes will be written as hints
    keys1 = list(range(100))
    keys2 = list(range(100, 200))
    keys3 = list(range(200, 300))

    # Nothing is written for subtest 4
    keys5 = list(range(400, 500))

    cql = await manager.get_cql_exclusive(s1)
    logger.info("Creating a keyspace...")
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")

    logger.info("Creating a table...")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    stmt = cql.prepare("INSERT INTO ks.t (pk, v) VALUES (?, ?)")
    stmt.consistency_level = ConsistencyLevel.ANY

    async def insert(keys) -> None:
        await gather_safely(*[cql.run_async(stmt, (key, key + 1)) for key in keys])

    logger.info("SUBTEST 1: Create a hint sync point, unpause hint replay and wait until they are replayed to the end")

    logger.info("Stopping s2...")
    await manager.server_stop_gracefully(s2.server_id)
    await manager.others_not_see_server(s2.ip_addr)

    logger.info("Pause hint replay on s1")
    await manager.api.enable_injection(s1.ip_addr, "hinted_handoff_pause_hint_replay", one_shot=False)

    logger.info(f"Inserting {len(keys1)} keys...")
    await insert(keys1)

    logger.info("Starting s2...")
    await start_node(s2)

    logger.info("Create hint sync point")
    sync_point_id = await create_sync_point()

    logger.info("Check that the sync point is not immediately resolved (because hint replay is paused)")
    await wait_for_sync_point(sync_point_id, 0, expect="IN_PROGRESS")

    logger.info("Start waiting for the point, asynchronously, with infinite timeout")
    fut = asyncio.create_task(wait_for_sync_point(sync_point_id, -1, expect="DONE"))

    logger.info("Unpause hint replay on s1")
    await manager.api.disable_injection(s1.ip_addr, "hinted_handoff_pause_hint_replay")

    # Waiting should resolve soon - successfully
    logger.info("Join with the future waiting for the point")
    await asyncio.wait_for(fut, timeout=60)

    logger.info("Check that the sync point still returns success")
    await wait_for_sync_point(sync_point_id, 0, expect="DONE")

    logger.info("Verifying that hints replayed all of the data...")
    await assert_rows_present(cql, "ks.t", "pk", keys1, present=True)

    logger.info("SUBTEST 2: Create a hint sync point, shutdown the waiting node and observe the failure")

    logger.info("Stopping s2...")
    await manager.server_stop_gracefully(s2.server_id)
    await manager.others_not_see_server(s2.ip_addr)

    logger.info("Pause hint replay on s1")
    await manager.api.enable_injection(s1.ip_addr, "hinted_handoff_pause_hint_replay", one_shot=False)

    logger.info(f"Inserting {len(keys2)} keys...")
    await insert(keys2)

    logger.info("Starting s2...")
    await start_node(s2)

    logger.info("Create hint sync point...")
    sync_point_id = await create_sync_point()

    # Asynchronously wait, indefinitely
    logger.info("Start waiting for the point, asynchronously, with infinite timeout")
    fut = asyncio.create_task(wait_for_sync_point(sync_point_id, -1, expect="FAILED"))

    logger.info("Stopping s1...")
    await manager.server_stop_gracefully(s1.server_id)

    logger.info("Join with the future waiting for the point")
    await asyncio.wait_for(fut, timeout=60)

    logger.info("Starting s1...")
    await start_node(s1)
    cql = await manager.get_cql_exclusive(s1)
    # We need to re-prepare the statement after restarting the node.
    stmt = cql.prepare("INSERT INTO ks.t (pk, v) VALUES (?, ?)")
    stmt.consistency_level = ConsistencyLevel.ANY

    # Error injections are reset on restart, so hint replay will be unpaused at this point

    logger.info("SUBTEST 3: Create a hint sync point, restart with different shard count and wait until hints are replayed")

    logger.info("Stopping s2...")
    await manager.server_stop_gracefully(s2.server_id)
    await manager.others_not_see_server(s2.ip_addr)

    logger.info("Pause hint replay on s1")
    await manager.api.enable_injection(s1.ip_addr, "hinted_handoff_pause_hint_replay", one_shot=False)

    logger.info(f"Inserting {len(keys3)} keys...")
    await insert(keys3)

    logger.info("Starting s2...")
    await start_node(s2)

    logger.info("Create hint sync point")
    sync_point_id = await create_sync_point()

    logger.info("Stopping s1...")
    await manager.server_stop_gracefully(s1.server_id)

    # manager.get_cql() (used internally by keyspace_has_tablets) is a shared driver session
    # that may still be pinned to s1's now-dead control connection; reconnect it onto s2, the
    # only server left standing, before using it.
    await manager.driver_connect(server=s2)

    if await keyspace_has_tablets(manager, "ks"):
        # reducing shard count with tablet-enabled tables is not yet supported (#16739).
        await start_node(s1)
    else:
        logger.info("Starting s1 with SMP=2...")
        await start_node(s1, smp=2)
    cql = await manager.get_cql_exclusive(s1)
    # We need to re-prepare the statement after restarting the node.
    stmt = cql.prepare("INSERT INTO ks.t (pk, v) VALUES (?, ?)")
    stmt.consistency_level = ConsistencyLevel.ANY

    # Hint replay is unpaused because of the restart

    logger.info("Wait until all hints are successfully replayed...")
    await wait_for_sync_point(sync_point_id, 60, expect="DONE")

    logger.info("Verifying that hints replayed all of the data...")
    await assert_rows_present(cql, "ks.t", "pk", keys3, present=True)

    logger.info("SUBTEST 4: Create a hint sync point and try to use it on another node - should fail")

    logger.info("Create hint sync point on s1")
    sync_point_id = await create_sync_point(node=s1)

    logger.info("Try waiting for the point on s2 - should fail")
    await wait_for_sync_point(sync_point_id, 0, expect="FAILED", node=s2)

    logger.info("SUBTEST 5: Create a hint sync point and decommission the target node - waiting should succeed")

    logger.info("Stopping s2...")
    await manager.server_stop_gracefully(s2.server_id)
    await manager.others_not_see_server(s2.ip_addr)

    logger.info("Pause hint replay on s1")
    await manager.api.enable_injection(s1.ip_addr, "hinted_handoff_pause_hint_replay", one_shot=False)

    logger.info(f"Inserting {len(keys5)} keys...")
    await insert(keys5)

    logger.info("Starting s2...")
    await start_node(s2)

    logger.info("Create hint sync point on s1")
    sync_point_id = await create_sync_point(node=s1)

    logger.info("Decommissioning s2...")
    await manager.decommission_node(s2.server_id)

    logger.info("Check that the sync point is not yet resolved (because hint replay is paused)")
    await wait_for_sync_point(sync_point_id, 0, expect="IN_PROGRESS")

    logger.info("Start waiting for the point, asynchronously, with infinite timeout")
    fut = asyncio.create_task(wait_for_sync_point(sync_point_id, -1, expect="DONE"))

    logger.info("Unpause hint replay on s1")
    await manager.api.disable_injection(s1.ip_addr, "hinted_handoff_pause_hint_replay")

    # Waiting should resolve soon - successfully
    logger.info("Join with the future waiting for the point")
    await asyncio.wait_for(fut, timeout=60)

    logger.info("Verifying that hints replayed all of the data...")
    await assert_rows_present(cql, "ks.t", "pk", keys5, present=True)


async def test_hint_storage_proxy_metrics(manager: ScyllaClusterManager):
    """
    The hint metrics of the sender and of the receiver must agree: what the sender counts as
    sent must be what the receiver counts as received, both in hints and in bytes.
    """
    sent_total_metric = "scylla_hints_manager_sent_total"
    sent_bytes_total_metric = "scylla_hints_manager_sent_bytes_total"
    received_total_metric = "scylla_storage_proxy_replica_received_hints_total"
    received_bytes_total_metric = "scylla_storage_proxy_replica_received_hints_bytes_total"

    s1, s2 = await manager.servers_add(2, auto_rack_dc="dc1")

    s2_host_id = await manager.get_host_id(s2.server_id)
    s1_hints_dir = await get_hints_dir(manager, s1)

    cql = await manager.get_cql_exclusive(s1)
    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(s2.server_id)
    await manager.others_not_see_server(s2.ip_addr)

    # Exactly one mutation is performed: the `sent` metric is only reliable for a single hint.
    await cql.run_async(SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (0, 1)",
                                        consistency_level=ConsistencyLevel.ANY))

    async def hint_segment_written():
        return True if count_hint_segments(s1_hints_dir, s2_host_id) > 0 else None
    await wait_for(hint_segment_written, time.time() + 60)

    await manager.server_start(s2.server_id)
    await manager.servers_see_each_other([s1, s2])

    async def hint_sent():
        sent = await get_metric(manager.metrics, s1.ip_addr, sent_total_metric)
        return True if (sent and sent > 0) else None
    await wait_for(hint_sent, time.time() + 15)

    # The receiver's counters may be updated slightly after the sender's, so give them
    # a chance to catch up before comparing.
    async def metrics_agree():
        sent = await get_metric(manager.metrics, s1.ip_addr, sent_total_metric)
        received = await get_metric(manager.metrics, s2.ip_addr, received_total_metric)
        return True if sent == received else None
    await wait_for(metrics_agree, time.time() + 60)

    sent_total = await get_metric(manager.metrics, s1.ip_addr, sent_total_metric)
    sent_bytes_total = await get_metric(manager.metrics, s1.ip_addr, sent_bytes_total_metric)
    received_total = await get_metric(manager.metrics, s2.ip_addr, received_total_metric)
    received_bytes_total = await get_metric(manager.metrics, s2.ip_addr, received_bytes_total_metric)

    logger.info(f"Sent hint count     : {sent_total}")
    logger.info(f"Sent hint size      : {sent_bytes_total}")
    logger.info(f"Received hint count : {received_total}")
    logger.info(f"Received hint size  : {received_bytes_total}")

    assert sent_total is not None and sent_total > 0
    assert sent_bytes_total is not None and sent_bytes_total > 0

    assert sent_total == received_total
    assert sent_bytes_total == received_bytes_total
