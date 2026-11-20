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
from test.pylib.util import unique_name, wait_for, wait_for_view
from test.topology.conftest import skip_mode
from test.topology.util import new_test_keyspace, reconnect_driver

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

# This test makes sure that view building is done mainly in the streaming scheduling group
# and not the gossip scheduling group. We do that by measuring the time each group was
# busy during the view building process and confirming that the gossip group was busy
# much less than the streaming group.
# Reproduces https://github.com/scylladb/scylladb/issues/21232
@pytest.mark.asyncio
@skip_mode('debug', 'the test needs to do some work which takes too much time in debug mode')
async def test_view_building_scheduling_group(manager: ManagerClient):
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tab (p int, c int, PRIMARY KEY (p, c))")

        # Insert 50000 rows to the table. Use unlogged batches to speed up the process.
        for i in range(1000):
            inserts = [f"INSERT INTO {ks}.tab(p, c) VALUES ({i+1000*x}, {i+1000*x})" for x in range(50)]
            batch = "BEGIN UNLOGGED BATCH\n" + "\n".join(inserts) + "\nAPPLY BATCH\n"
            await manager.cql.run_async(batch)

        metrics_before = await manager.metrics.query(server.ip_addr)
        ms_gossip_before = metrics_before.get('scylla_scheduler_runtime_ms', {'group': 'gossip'})
        ms_streaming_before = metrics_before.get('scylla_scheduler_runtime_ms', {'group': 'streaming'})

        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT p, c FROM {ks}.tab WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)")
        await wait_for_view(cql, 'mv', 1)

        metrics_after = await manager.metrics.query(server.ip_addr)
        ms_gossip_after = metrics_after.get('scylla_scheduler_runtime_ms', {'group': 'gossip'})
        ms_streaming_after = metrics_after.get('scylla_scheduler_runtime_ms', {'group': 'streaming'})
        ms_streaming = ms_streaming_after - ms_streaming_before
        ms_statement = ms_gossip_after - ms_gossip_before
        ratio = ms_statement / ms_streaming
        print(f"ms_streaming: {ms_streaming}, ms_statement: {ms_statement}, ratio: {ratio}")
        assert ratio < 0.1

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

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

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
@skip_mode('release', 'error injections are not supported in release mode')
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

# The test verifies that when a reshard happens when building multiple views,
# which have different progress, we won't mistakenly decide that a view is built
# even if a build step is empty due to resharding.
# Reproduces https://github.com/scylladb/scylladb/issues/26523
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
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
    await manager.server_update_cmdline(server.server_id, ['--smp', '2', '--logger-log-level', 'view=debug'])
    await manager.server_start(server.server_id)
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
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
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
        await manager.server_update_cmdline(server.server_id, cmdline_after)
        if interrupt_resharding:
            # Start, wait for resharding to begin, then kill and restart.
            # Prevent view building worker from resuming work on view building.
            await manager.server_update_config(server.server_id, "error_injections_at_startup", ["dont_start_build_step"])
            log = await manager.server_open_log(server.server_id)
            mark = await log.mark()
            await manager.server_start(server.server_id)
            try:
                await log.wait_for("Reshard", from_mark=mark, timeout=30)
            except:
                pass  # resharding might not always appear in logs for small data
            await manager.server_stop(server.server_id)
            await manager.server_update_config(server.server_id, "error_injections_at_startup", [])
            await manager.server_start(server.server_id)
        else:
            await manager.server_start(server.server_id)

        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

        # Wait for view building to complete
        await wait_for_view(cql, 'mv', 1)

        # Verify all data is correct
        base_rows = set(await cql.run_async(f"SELECT key, c, v FROM {ks}.tab"))
        view_rows = set(await cql.run_async(f"SELECT key, c, v FROM {ks}.mv"))
        assert view_rows == base_rows, f"View has {len(view_rows)} rows, base has {len(base_rows)} rows"

# Migrated from dtest secondary_indexes_test.py node action tests.
# Verifies that node operations during view building complete correctly (vnodes).
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
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
            await manager.server_stop(target.server_id)
            await resume_view_builder(manager, servers[:-1])
            await manager.server_start(target.server_id)
            await manager.servers_see_each_other(servers)
            cql, _ = await manager.get_ready_cql(servers)
        elif operation == "remove":
            await manager.server_stop(target.server_id)
            await manager.remove_node(servers[0].server_id, target.server_id)
            await resume_view_builder(manager, servers[:-1])
            expected_node_count -= 1
        elif operation == "decommission":
            await manager.decommission_node(target.server_id)
            expected_node_count -= 1
            await resume_view_builder(manager, servers[:-1])
        elif operation == "add":
            await manager.server_add(property_file=property_file[-1])
            expected_node_count += 1
            await resume_view_builder(manager, servers)

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
