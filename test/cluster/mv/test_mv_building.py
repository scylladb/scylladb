#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import pytest
import logging
import random
import time
from test.pylib.manager_client import ManagerClient, wait_for_cql_and_get_hosts
from test.pylib.tablets import get_tablet_replica
from test.pylib.util import wait_for, wait_for_view
from test.cluster.util import get_topology_coordinator, new_test_keyspace, reconnect_driver

from cassandra.cluster import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

logger = logging.getLogger(__name__)

PAUSE_VIEW_BUILDER_INJECTION = "view_builder_pause_before_mark_success"

async def mark_server_logs(manager: ManagerClient, servers) -> dict:
    marks = {}
    for s in servers:
        log = await manager.server_open_log(s.server_id)
        marks[s.server_id] = await log.mark()
    return marks

async def wait_for_view_building_in_progress(manager: ManagerClient, servers, marks: dict,
                                             view_name: str, timeout: float = 120):
    # Either outcome means the node is no longer in the "not started building yet" state.
    pattern = (f"{PAUSE_VIEW_BUILDER_INJECTION}: waiting for message"
               f"|Finished building view [^ ]*\\.{view_name}\\b")

    async def wait_one(server) -> None:
        log = await manager.server_open_log(server.server_id)
        await log.wait_for(pattern, from_mark=marks[server.server_id], timeout=timeout)
    await asyncio.gather(*(wait_one(s) for s in servers))

async def resume_view_builder(manager: ManagerClient, servers):
    for s in servers:
        await manager.api.message_injection(s.ip_addr, PAUSE_VIEW_BUILDER_INJECTION)
        await manager.api.disable_injection(s.ip_addr, PAUSE_VIEW_BUILDER_INJECTION)

# This test makes sure that view building is done mainly in the streaming
# scheduling group. We check that by grepping all relevant logs in TRACE mode
# and verifying that they come from the streaming scheduling group.
#
# For more context, see: https://github.com/scylladb/scylladb/issues/21232.
# This test reproduces the issue in non-tablet mode.
@pytest.mark.skip_mode(mode='debug', reason='the test needs to do some work which takes too much time in debug mode')
async def test_view_building_scheduling_group(manager: ManagerClient):
    # Note: The view building coordinator works in the gossiping scheduling group,
    #       and we intentionally omit it here.
    # Note: We include "view" for keyspaces that don't use the view building coordinator
    #       and will follow the legacy path instead.
    loggers = ["view_building_worker", "view_consumer", "view_update_generator", "view"]
    # Flatten the list of lists.
    cmdline = sum([["--logger-log-level", f"{logger}=trace"] for logger in loggers], [])

    server = await manager.server_add(cmdline=cmdline)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (p int, c int, PRIMARY KEY (p, c))")

        # Insert 50000 rows to the table. Use unlogged batches to speed up the process.
        for i in range(1000):
            inserts = [f"INSERT INTO {ks}.tab(p, c) VALUES ({i+1000*x}, {i+1000*x})" for x in range(50)]
            batch = "BEGIN UNLOGGED BATCH\n" + "\n".join(inserts) + "\nAPPLY BATCH\n"
            await manager.cql.run_async(batch)

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT p, c FROM {ks}.tab WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)")
        await wait_for_view(cql, 'mv', 1)

        logger_alternative = "|".join(loggers)
        pattern = rf"\[shard [0-9]+:(.+)\] ({logger_alternative}) - "

        results = await log.grep(pattern, from_mark=mark)
        # Sanity check. If there are no logs, something's wrong.
        assert len(results) > 0

        # In case of non-tablet keyspaces, we won't use the view building coordinator.
        # Instead, view updates will follow the legacy path. Along the way, we'll observe
        # this message, which will be printed using another scheduling group, so let's
        # filter it out.
        predicate = lambda result: f"Building view {ks}.mv, starting at token" not in result[0]
        results = list(filter(predicate, results))

        # Take the first parenthesized match for each result, i.e. the scheduling group.
        sched_groups = [matches[1] for _, matches in results]

        assert all(sched_group == "strm" for sched_group in sched_groups)

# A sanity check test ensures that starting and shutting down Scylla when view building is
# disabled is conducted properly and we don't run into any issues.
@pytest.mark.check_nodes_for_errors
async def test_start_scylla_with_view_building_disabled(manager: ManagerClient):
    server = await manager.server_add(config={"view_building": "false"})
    # The test framework will make sure no errors have been reported.
    # We could simply grep the logs of the node, searching for "ERROR".
    # However, some errors are expected and they could make the test flaky.
    # That already happened in SCYLLA-2317.
    await manager.server_stop_gracefully(server_id=server.server_id)

# While view building is in progress, drop the index (which changes the schema
# of the base table). The state of the view table corresponding to the index
# may become inconsistent with the base table because they got detached.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_view_building_during_drop_index(manager: ManagerClient):
    server = await manager.server_add()
    cql = manager.get_cql()
    await manager.api.enable_injection(server.ip_addr, "view_builder_consume_end_of_partition_delay", one_shot=True)

    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE ks.tab (p int, c int, PRIMARY KEY (p, c))")
    await cql.run_async("INSERT INTO ks.tab (p,c) VALUES (123, 1000)")

    await cql.run_async("CREATE INDEX idx1 ON ks.tab (c)")
    # wait for view building to start
    async def view_build_started():
        started = await cql.run_async(f"SELECT COUNT(*) FROM system.view_build_status_v2 WHERE status = 'STARTED' AND view_name = 'idx1_index' ALLOW FILTERING")
        all = await cql.run_async(f"SELECT * FROM system.view_build_status_v2")
        logger.info(f"View build status: {all}")
        return started[0][0] == 1 or None
    await wait_for(view_build_started, time.time() + 60, 0.1)

    # while view building is delayed, we drop the view and change the schema of the base table
    await cql.run_async("DROP INDEX ks.idx1")
    await manager.api.message_injection(server.ip_addr, "view_builder_consume_end_of_partition_delay")

    await cql.run_async("DROP TABLE ks.tab")

# Start view building and interrupt it while some shards started and registered their
# view building status and some shards didn't. Specifically, the last shard is paused.
# We restart the node in this state and verify that when it comes up the view building
# is completed eventually and is correct.
# Reproduces #22989
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_interrupt_view_build_shard_registration(manager: ManagerClient):
    cmdline = ['--smp=4']
    servers = await manager.servers_add(1, cmdline=cmdline)
    server = servers[0]

    logger.info("Populate table")
    cql = manager.get_cql()
    n_partitions = 1000
    ks = 'ks'
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets={{'enabled':false}}")
    await cql.run_async(f"CREATE TABLE {ks}.test (p int, c int, PRIMARY KEY(p,c));")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (p, c) VALUES ({k}, {k+1});") for k in range(n_partitions)])

    # pause the last shard so it won't be registered
    await manager.api.enable_injection(server.ip_addr, "add_new_view_fail_last_shard", one_shot=False)

    await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT p, c FROM {ks}.test WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)")

    # wait for some shards to register
    async def some_registered():
        rows = await cql.run_async(f"SELECT * FROM system.scylla_views_builds_in_progress WHERE keyspace_name = '{ks}' AND view_name = 'mv'")
        if len(rows) > 0:
            return True
    await wait_for(some_registered, time.time() + 60)

    # restart while some shards registered but the last shard didn't
    await manager.server_stop_gracefully(server.server_id)

    await manager.server_start(server.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60)

    await wait_for_view(cql, 'mv', 1, timeout = 60)

    res = await cql.run_async(f"SELECT * FROM {ks}.test")
    assert len(res) == n_partitions
    res = await cql.run_async(f"SELECT * FROM {ks}.mv")
    assert len(res) == n_partitions

# The test verifies that when a reshard happens when building multiple views,
# which have different progress, we won't mistakenly decide that a view is built
# even if a build step is empty due to resharding.
# Reproduces https://github.com/scylladb/scylladb/issues/26523
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_empty_build_step_after_reshard(manager: ManagerClient):
    server = await manager.server_add(cmdline=['--smp', '1', '--logger-log-level', 'view=debug'])
    partitions = random.sample(range(1000), 129) # need more than 128 to allow the first build step to finish and save the progress
    logger.info(f"Using partitions: {partitions}")
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets={{'enabled':false}}")
    await cql.run_async(f"CREATE TABLE ks.test (p int, c int, PRIMARY KEY(p,c));")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO ks.test (p, c) VALUES ({k}, {k+1});") for k in partitions])

    # Create first materialized view and wait until building starts. The base table has enough partitions for 2 build steps.
    # Allow the first build step to finish and save progress. In the second step there's only one partition left to build, which will land only on one
    # of the shards after resharding.
    await manager.api.enable_injection(server.ip_addr, "delay_finishing_build_step", one_shot=False)
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv AS SELECT p, c FROM ks.test WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)")
    async def progress_saved():
        rows = await cql.run_async(f"SELECT * FROM system.scylla_views_builds_in_progress WHERE keyspace_name = 'ks' AND view_name = 'mv'")
        return len(rows) > 0 or None
    await wait_for(progress_saved, time.time() + 60)
    await manager.api.enable_injection(server.ip_addr, "dont_start_build_step", one_shot=False)
    await manager.api.message_injection(server.ip_addr, "delay_finishing_build_step")

    # Create second materialized view and immediately restart the server to cause resharding. The new view will effectively start building after the restart.
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv2 AS SELECT p, c FROM ks.test WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)")
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_start(server.server_id, cmdline_options_override=['--smp', '2', '--logger-log-level', 'view=debug'])
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
    await wait_for_view(cql, 'mv', 1)
    await wait_for_view(cql, 'mv2', 1)

    # Verify that no rows are missing
    base_rows = await cql.run_async(f"SELECT * FROM ks.test")
    mv_rows = await cql.run_async(f"SELECT * FROM ks.mv")
    mv2_rows = await cql.run_async(f"SELECT * FROM ks.mv2")
    assert len(base_rows) == len(mv_rows) == len(mv2_rows) == 129

# Tests that an interrupted MV build resumes correctly after resharding.
# Complements test_empty_build_step_after_reshard which tests a specific bug (#26523) where
# an empty build step after reshard doesn't crash. This test verifies that a build paused
# mid-progress (with saved progress token) resumes correctly on a differently-sharded node.
# Migrated from dtest materialized_views_test.py::TestInterruptBuildProcess::test_interrupt_build_process_with_resharding_*_test
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("smp_before,smp_after", [
    (1, 2),   # increase shards
    (2, 1),   # decrease shards
])
@pytest.mark.parametrize("interrupt_resharding", [False, True])
async def test_interrupt_build_with_resharding(manager: ManagerClient, smp_before: int, smp_after: int, interrupt_resharding: bool):
    """Test that an interrupted MV build process resumes correctly after resharding.

    Scenarios:
    - interrupt_resharding=False: interrupt build, restart with different shard count, verify build resumes
    - interrupt_resharding=True: interrupt build, start resharding, interrupt resharding too, restart, verify
    """
    cmdline_before = ['--smp', str(smp_before), '--logger-log-level', 'view=debug']
    cmdline_after = ['--smp', str(smp_after), '--logger-log-level', 'view=debug']

    server = await manager.server_add(cmdline=cmdline_before)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")

        n_partitions = 10
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.tab (key, c, v) VALUES ({i}, {i}, '{i}')") for i in range(n_partitions)])

        # Pause the view builder using injection to control build progress
        await manager.api.enable_injection(server.ip_addr, "delay_finishing_build_step", one_shot=False)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.tab "
                        "WHERE key IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v)")

        # Wait for build to start and save progress
        async def progress_saved():
            rows = await cql.run_async(
                f"SELECT * FROM system.scylla_views_builds_in_progress WHERE keyspace_name = '{ks}' AND view_name = 'mv' ALLOW FILTERING")
            return len(rows) > 0 or None
        await wait_for(progress_saved, time.time() + 60)

        # Block further build steps and release the current one
        await manager.api.enable_injection(server.ip_addr, "dont_start_build_step", one_shot=False)
        await manager.api.message_injection(server.ip_addr, "delay_finishing_build_step")

        # Stop the node
        logger.info(f"Stopping node. Resharding {smp_before} -> {smp_after}, interrupt_resharding={interrupt_resharding}")
        await manager.server_stop_gracefully(server.server_id)

        # Restart with new shard count
        if interrupt_resharding:
            # Start, wait for resharding to begin, then kill and restart.
            # Prevent view building worker from resuming work on view building.
            await manager.server_update_config(server.server_id, "error_injections_at_startup", ["dont_start_build_step"])
            log = await manager.server_open_log(server.server_id)
            mark = await log.mark()
            await manager.server_start(server.server_id, cmdline_options_override=cmdline_after)
            try:
                await log.wait_for("Reshard", from_mark=mark, timeout=30)
            except:
                pass  # resharding might not always appear in logs for small data
            await manager.server_stop(server.server_id, convict=False)
            await manager.server_remove_config_option(server.server_id, "error_injections_at_startup")
            await manager.server_start(server.server_id, cmdline_options_override=cmdline_after)
        else:
            await manager.server_start(server.server_id, cmdline_options_override=cmdline_after)

        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

        # Wait for view building to complete
        await wait_for_view(cql, 'mv', 1)

        # Verify all data is correct
        base_rows = set(await cql.run_async(f"SELECT key, c, v FROM {ks}.tab"))
        view_rows = set(await cql.run_async(f"SELECT key, c, v FROM {ks}.mv"))
        assert view_rows == base_rows, f"View has {len(view_rows)} rows, base has {len(base_rows)} rows"

# It may happen that a node fails to process an RPC request from the coordinator.
# In that case, we would like to prevent sending more requests to it because
# they're most likely going to fail as well. Verify that that's the case.
#
# Reproduces scylladb/scylladb#26686.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_backoff_when_node_fails_task_rpc(manager: ManagerClient):
    """
    Scenario:
    1. Set up a cluster. Two racks are sufficient.
    2. Create the schema. Fill in the table with enough data so that the view
       building won't finish immediately.
    3. Create a view.
    4. Enable the error injection that will simulate gossip taking time with marking
       nodes as DOWN.
    5. Stop the target node.
    6. Wait for the view building coordinator to report that sending an RPC
       to the target node has failed.
    7. Disable the injection and revive the target node.
    8. The total number of warnings should be small.
    """

    # Not needed, but it will be of tremendous help if we end up debugging a failure.
    cmdline = ["--logger-log-level", "view_building_coordinator=trace",
               "--logger-log-level", "view_building_worker=trace",
               "--logger-log-level", "load_balancer=debug",
               "--logger-log-level", "storage_service=debug",
               "--logger-log-level", "raft_topology=debug"]

    s1, s2 = await manager.servers_add(2, cmdline=cmdline, auto_rack_dc="dc1")

    host_id1 = await manager.get_host_id(s1.server_id)
    host_id2 = await manager.get_host_id(s2.server_id)

    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2}")
    await cql.run_async("CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))")

    row_count = 50_000
    await asyncio.gather(*[cql.run_async(f"INSERT INTO ks.t (pk, ck, v) VALUES ({i}, {i + 1}, {i + 2})") for i in range(row_count)])

    # Topology coordinator = view building coordinator.
    topology_coordinator = await get_topology_coordinator(manager)
    if topology_coordinator != host_id1:
        s1, s2 = s2, s1
        host_id1, host_id2 = host_id2, host_id1
    logger.info(f"Coordinator: server ID={s1.server_id}, IP={s1.ip_addr}, host ID={host_id1}")
    logger.info(f"Target: server ID={s2.server_id}, IP={s2.ip_addr}, host ID={host_id2}")

    # Prevent the target node from processing everything too early.
    pause_vb_err = "view_building_worker_pause_build_range_task"
    await manager.api.enable_injection(s2.ip_addr, pause_vb_err, one_shot=False)

    log = await manager.server_open_log(s1.server_id)
    mark = await log.mark()

    # Make the view building coordinator ignore the gossiper. The purpose of this
    # is to simulate a situation when the gossiper doesn't mark a dead node as
    # such immediately. In a scenario like that, the view building coordinator
    # will be retrying to send a request to it.
    ignore_gossiper_err = "view_building_coordinator_ignore_gossiper"
    await manager.api.enable_injection(s1.ip_addr, ignore_gossiper_err, one_shot=False)
    await manager.server_stop(s2.server_id, convict=False)

    start = time.time()

    # We want to have at least 2 tablets per node to force node 1
    # to have multiple tablet replica targets on node 2. Why?
    #
    # Because we also want to test that Scylla won't undergo a storm
    # of warning messages from the view building coordinator. They should
    # be rate limited. If we have multiple tablet replica targets, we will
    # verify that the number of those messages is controlled.
    #
    # Hence: min_tablet_count = 2 * (node count) = 4.
    #
    # We rely on two assumptions
    # 1. Each node will have at least two shards.
    # 2. The load balancer will distribute the tablets equally between the nodes.
    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv AS SELECT * FROM ks.t "
                        "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v IS NOT NULL "
                        "PRIMARY KEY ((ck, pk), v) "
                        "WITH tablets = {'min_tablet_count': 4}")

    error = rf"Work on tasks .* on replica {host_id2}:\d+, failed with error"

    await log.wait_for(error, from_mark=mark)
    await manager.api.disable_injection(s1.ip_addr, ignore_gossiper_err)
    await manager.server_start(s2.server_id)

    # Not needed anymore.
    await manager.api.disable_injection(s2.ip_addr, pause_vb_err)
    await wait_for_view(cql, "mv", 2)

    end = time.time()
    # The duration of the view building process (in seconds, rounded up).
    duration = int(end - start) + 1

    matches = await log.grep(error, from_mark=mark)
    match_count = len(matches)

    logger.info(f"Got {match_count} matches")

    # Technically, we shouldn't get more than one failure per second,
    # but let's cut it some slack to avoid flakiness.
    slack = 5

    assert match_count <= duration + slack

# Test that the view builder does not finish when some replica nodes are down,
# and resumes correctly once they come back.
# Migrated from dtest materialized_views_test.py::TestMaterializedViews::test_do_not_finish_view_building_with_hints
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_do_not_finish_view_builder_with_nodes_down(manager: ManagerClient):
    """Test that the view builder does not complete while replica nodes are down,
    and finishes successfully after they are restarted."""
    node_count = 3
    servers = await manager.servers_add(node_count, config={
        "hinted_handoff_enabled": False,
        "shadow_round_ms": 1000,
    }, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ], cmdline=[
        '--logger-log-level', 'storage_proxy=debug',
        '--logger-log-level', 'cql_server=debug',
    ])
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")

        n_partitions = 1000
        for i in range(n_partitions):
            await cql.run_async(f"INSERT INTO {ks}.tab (key, c, v) VALUES ({i}, {i}, '{i}')")

        # Pause the view builder using the vnode-specific injection point.
        # view_builder_consume_end_of_partition_delay pauses processing at the end of each partition.
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, "view_builder_consume_end_of_partition_delay", one_shot=False)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL AND key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v)")

        # Wait for the view builder to start on at least one node
        async def view_build_started():
            rows = await cql.run_async(f"SELECT * FROM system.view_build_status_v2 WHERE keyspace_name = '{ks}' AND view_name = 'mv_cf_view' ALLOW FILTERING")
            if len(rows) > 0:
                return True
        await wait_for(view_build_started, time.time() + 60, period=0.1)

        # Stop two nodes. Use non-graceful stop to avoid waiting for the
        # view_builder_consume_end_of_partition_delay injection to time out.
        logger.info("Stopping nodes 2 and 3")
        await manager.server_stop(servers[1].server_id, convict=True)
        await manager.server_stop(servers[2].server_id, convict=True)

        # Unpause the view builder on the remaining node - it should not finish
        # because it cannot replicate view updates to the down nodes.
        await manager.api.message_injection(servers[0].ip_addr, "view_builder_consume_end_of_partition_delay")
        await manager.api.disable_injection(servers[0].ip_addr, "view_builder_consume_end_of_partition_delay")

        # Verify the view builder does not finish while nodes are down.
        logger.info("Verifying the view builder does not finish while nodes are down")
        build_status = await cql.run_async(
            f"SELECT * FROM system.view_build_status_v2 WHERE keyspace_name = '{ks}' AND view_name = 'mv_cf_view'",
            host=cql.cluster.metadata.get_host(servers[0].ip_addr))
        assert len(build_status) > 0 and any(bs.status != 'SUCCESS' for bs in build_status), \
            "View builder should still be in progress while nodes are down"

        # Restart the stopped nodes
        logger.info("Restarting nodes 2 and 3")
        await manager.server_start(servers[1].server_id)
        await manager.server_start(servers[2].server_id)
        await manager.servers_see_each_other(servers)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        logger.info("Waiting for the view builder to complete")
        await wait_for_view(cql, 'mv_cf_view', node_count)

        # Verify all data
        base_rows = set(await cql.run_async(f"SELECT key, c, v FROM {ks}.tab"))
        view_rows = set(await cql.run_async(f"SELECT key, c, v FROM {ks}.mv_cf_view"))
        assert view_rows == base_rows

# Migrated from dtest secondary_indexes_test.py node action tests.
# Verifies that node operations during view building complete correctly (vnodes).
@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("operation", ["stop", "remove", "decommission", "add"])
async def test_node_operation_during_view_building(manager: ManagerClient, operation: str):
    """Test that node operations during view building don't break the build (vnodes)."""
    if operation in ["remove", "decommission"]:
        node_count = 4
        rack_layout = ["r1", "r2", "r3", "r4"]
    else:
        node_count = 3
        rack_layout = ["r1", "r2", "r3"]

    property_file = [{"dc": "dc1", "rack": rack} for rack in rack_layout]
    servers = await manager.servers_add(node_count, config={
        "hinted_handoff_enabled": False,
    }, property_file=property_file)
    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'enabled': false}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (key int, c int, v text, PRIMARY KEY (key, c))")

        n_partitions = 100
        for i in range(n_partitions):
            await cql.run_async(f"INSERT INTO {ks}.tab (key, c, v) VALUES ({i}, {i}, '{i}')")

        # Pause view building to ensure build is in progress when we perform node action
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, PAUSE_VIEW_BUILDER_INJECTION, one_shot=False)
        marks = await mark_server_logs(manager, servers)

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_cf_view AS SELECT * FROM {ks}.tab "
                        "WHERE c IS NOT NULL AND key IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, key, v)")

        await wait_for_view_building_in_progress(manager, servers, marks, 'mv_cf_view')

        target = servers[-1]
        expected_node_count = node_count

        if operation == "stop":
            await manager.server_stop(target.server_id, convict=False)
            await resume_view_builder(manager, servers[:-1])
            await manager.server_start(target.server_id)
            await manager.servers_see_each_other(servers)
            cql, _ = await manager.get_ready_cql(servers)
        elif operation == "remove":
            await manager.server_stop(target.server_id, convict=True)
            await manager.remove_node(servers[0].server_id, target.server_id)
            await resume_view_builder(manager, servers[:-1])
            expected_node_count -= 1
        elif operation == "decommission":
            await manager.decommission_node(target.server_id)
            expected_node_count -= 1
            await resume_view_builder(manager, servers[:-1])
        elif operation == "add":
            await resume_view_builder(manager, servers)
            await manager.server_add(property_file=property_file[-1])
            expected_node_count += 1

        await wait_for_view(cql, 'mv_cf_view', expected_node_count)

        # Wait for all staging sstables to be processed and view updates to be applied.
        async def view_updates_drained():
            for server in await manager.running_servers():
                metrics = await manager.metrics.query(server.ip_addr)
                for name in ["scylla_database_view_update_backlog",
                             "scylla_view_update_generator_queued_batches_count",
                             "scylla_view_update_generator_sstables_to_move_count",
                             "scylla_view_update_generator_sstables_pending_work"]:
                    val = metrics.get(name)
                    if val is not None and val > 0:
                        return None
            return True
        await wait_for(view_updates_drained, deadline=time.time() + 30)

        if operation in ["remove", "decommission"]:
            # View updates from staging SSTables can be skipped during topology changes because the
            # receiving node may process them before the replication map reflects the new owner.
            # Repair the view before checking local contents so this test focuses on node operations
            # completing during view build, not on that known staging-update race.
            # This is known issue with vnode views and topology operations, we're not going to fix it.
            await manager.api.repair(servers[0].ip_addr, ks, "mv_cf_view")

        # Verify data correctness
        base_rows = set(await cql.run_async(f"SELECT key, c, v FROM {ks}.tab"))
        view_rows = set(await cql.run_async(f"SELECT key, c, v FROM {ks}.mv_cf_view"))
        assert view_rows == base_rows
