#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import logging
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_replica
from test.pylib.util import wait_for, wait_for_view
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)

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
