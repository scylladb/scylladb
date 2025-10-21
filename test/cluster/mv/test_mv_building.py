#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import logging
import time
from test.pylib.manager_client import ManagerClient, wait_for_cql_and_get_hosts
from test.pylib.tablets import get_tablet_replica
from test.pylib.util import wait_for, wait_for_view
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace, reconnect_driver

logger = logging.getLogger(__name__)

# This test makes sure that view building is done mainly in the streaming
# scheduling group. We check that by grepping all relevant logs in TRACE mode
# and verifying that they come from the streaming scheduling group.
#
# For more context, see: https://github.com/scylladb/scylladb/issues/21232.
# This test reproduces the issue in non-tablet mode.
@pytest.mark.asyncio
@skip_mode('debug', 'the test needs to do some work which takes too much time in debug mode')
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

# Start view building and interrupt it while some shards started and registered their
# view building status and some shards didn't. Specifically, the last shard is paused.
# We restart the node in this state and verify that when it comes up the view building
# is completed eventually and is correct.
# Reproduces #22989
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
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
