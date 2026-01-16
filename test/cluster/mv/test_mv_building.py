#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import logging
import random
import time
from test.pylib.manager_client import ManagerClient, wait_for_cql_and_get_hosts
from test.pylib.tablets import get_tablet_replica
from test.pylib.util import wait_for, wait_for_view
from test.cluster.conftest import skip_mode
from test.cluster.util import get_topology_coordinator, new_test_keyspace, reconnect_driver

logger = logging.getLogger(__name__)

# This test makes sure that view building is done mainly in the streaming
# scheduling group. We check that by grepping all relevant logs in TRACE mode
# and verifying that they come from the streaming scheduling group.
#
# For more context, see: https://github.com/scylladb/scylladb/issues/21232.
# This test reproduces the issue in non-tablet mode.
@pytest.mark.asyncio
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
@pytest.mark.asyncio
async def test_start_scylla_with_view_building_disabled(manager: ManagerClient):
    server = await manager.server_add(config={"view_building": "false"})
    await manager.server_stop_gracefully(server_id=server.server_id)

    # Make sure there have been no errors.
    log = await manager.server_open_log(server.server_id)
    res = await log.grep(r"ERROR.*\[shard [0-9]+:[a-z]+\]")
    assert len(res) == 0

# Build multiple views of one base table, and while view building is running move
# some of the base tablets to another node. Verify the view build is completed.
# More specifically, we move all tablets except the first one to reproduce issue #21829.
# The issue happens when we start building a view at a token F and then all partitions
# with tokens >=F are moved, and it causes the view builder to enter an infinite loop
# building the same token ranges repeatedly because it doesn't reach F.
@pytest.mark.asyncio
async def test_view_building_with_tablet_move(manager: ManagerClient, build_mode: str):
    servers = [await manager.server_add()]

    await manager.disable_tablet_balancing()

    table = 'test'

    view_count = 4
    views = [f"{table}_view_{i}" for i in range(view_count)]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, c int)")

        # prefill the base table with enough rows so that view building takes some time
        # and runs during the tablet move
        keys = 200000 if build_mode != 'debug' else 10000
        batch_size = 50
        for k in range(0, keys, batch_size):
            inserts = [f"INSERT INTO {ks}.{table}(pk, c) VALUES ({i}, {i})" for i in range(k, k+batch_size)]
            batch = "BEGIN UNLOGGED BATCH\n" + "\n".join(inserts) + "\nAPPLY BATCH\n"
            await manager.cql.run_async(batch)

        logger.info("Adding new server")
        servers.append(await manager.server_add())

        # create some views so they are built together but starting at different tokens
        for view in views:
            await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.{view} AS SELECT * FROM {ks}.{table} WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk)")
            await asyncio.sleep(1)

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)
        dst_shard = 0

        # move all tablets except the first one (with lowest token range) to the other node.
        table_id = await manager.get_table_id(ks, table)
        rows = await manager.cql.run_async(f"SELECT last_token FROM system.tablets where table_id = {table_id}")
        move_tablets_tasks = []
        for r in rows[1:]:
            tablet_token = r.last_token
            replica = await get_tablet_replica(manager, servers[0], ks, table, tablet_token)
            move_tablets_tasks.append(asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, table, replica[0], replica[1], s1_host_id, dst_shard, tablet_token)))
        await asyncio.gather(*move_tablets_tasks)

        for view in views:
            await wait_for_view(cql, view, len(servers))

# While view building is in progress, drop the index (which changes the schema
# of the base table). The state of the view table corresponding to the index
# may become inconsistent with the base table because they got detached.
@pytest.mark.asyncio
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
@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_interrupt_view_build_shard_registration(manager: ManagerClient):
    cmdline = ['--smp=4']
    cfg = {"commitlog_sync_period_in_ms": 1000}
    servers = await manager.servers_add(1, cmdline=cmdline, config=cfg)
    server = servers[0]

    logger.info("Populate table")
    cql = manager.get_cql()
    n_partitions = 1000
    ks = 'ks'
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets={{'enabled':false}}")
    await cql.run_async(f"CREATE TABLE {ks}.test (p int, c int, PRIMARY KEY(p,c));")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (p, c) VALUES ({k}, {k+1});") for k in range(n_partitions)])

    # pause the last shard so it won't be registered
    await manager.api.enable_injection(server.ip_addr, "add_new_view_pause_last_shard", one_shot=True)

    await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT p, c FROM {ks}.test WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)")

    # wait for some shards to register
    async def some_registered():
        rows = await cql.run_async(f"SELECT * FROM system.scylla_views_builds_in_progress WHERE keyspace_name = '{ks}' AND view_name = 'mv'")
        if len(rows) > 0:
            return True
    await wait_for(some_registered, time.time() + 60)
    await asyncio.sleep(2) # ensure commitlog sync

    # restart while some shards registered but the last shard didn't
    await manager.server_stop(server.server_id)

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
@pytest.mark.asyncio
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

# It may happen that a node fails to process an RPC request from the coordinator.
# In that case, we would like to prevent sending more requests to it because
# they're most likely going to fail as well. Verify that that's the case.
#
# Reproduces scylladb/scylladb#26686.
@pytest.mark.asyncio
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
    await manager.server_stop(s2.server_id)

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
