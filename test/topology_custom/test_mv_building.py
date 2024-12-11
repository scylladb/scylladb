#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import pytest
from test.pylib.manager_client import ManagerClient

from test.pylib.util import wait_for_view

# This test makes sure that view building is done mainly in the streaming scheduling group
# and not the gossip scheduling group. We do that by measuring the time each group was
# busy during the view building process and confirming that the gossip group was busy
# much less than the streaming group.
# Reproduces https://github.com/scylladb/scylladb/issues/21232
@pytest.mark.asyncio
async def test_view_building_scheduling_group(manager: ManagerClient):
    server = await manager.server_add()
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE ks.tab (p int, c int, PRIMARY KEY (p, c))")

    # Insert 50000 rows to the table. Use unlogged batches to speed up the process.
    for i in range(1000):
        inserts = [f"INSERT INTO ks.tab(p, c) VALUES ({i+1000*x}, {i+1000*x})" for x in range(50)]
        batch = "BEGIN UNLOGGED BATCH\n" + "\n".join(inserts) + "\nAPPLY BATCH\n"
        await manager.cql.run_async(batch)

    metrics_before = await manager.metrics.query(server.ip_addr)
    ms_gossip_before = metrics_before.get('scylla_scheduler_runtime_ms', {'group': 'gossip'})
    ms_streaming_before = metrics_before.get('scylla_scheduler_runtime_ms', {'group': 'streaming'})

    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv AS SELECT p, c FROM ks.tab WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)")
    await wait_for_view(cql, 'mv', 1)

    metrics_after = await manager.metrics.query(server.ip_addr)
    ms_gossip_after = metrics_after.get('scylla_scheduler_runtime_ms', {'group': 'gossip'})
    ms_streaming_after = metrics_after.get('scylla_scheduler_runtime_ms', {'group': 'streaming'})
    ms_streaming = ms_streaming_after - ms_streaming_before
    ms_statement = ms_gossip_after - ms_gossip_before
    ratio = ms_statement / ms_streaming
    print(f"ms_streaming: {ms_streaming}, ms_statement: {ms_statement}, ratio: {ratio}")
    assert ratio < 0.1

# While view building is in progress, drop the view and change the schema
# of the base table. The view table's state may become inconsistent with
# the base table because they are detached.
@pytest.mark.asyncio
async def test_view_building_during_drop_view(manager: ManagerClient):
    server = await manager.server_add()
    cql = manager.get_cql()
    await manager.api.enable_injection(server.ip_addr, "view_builder_consume_end_of_partition_delay_3s", one_shot=True)

    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE ks.tab (p int, c int, PRIMARY KEY (p, c))")
    await cql.run_async("INSERT INTO ks.tab (p,c) VALUES (123, 1000)")

    await cql.run_async("CREATE INDEX idx1 ON ks.tab (c)")
    # wait for view building to start
    await asyncio.sleep(1)

    # while view building is delayed, we drop the view and change the schema of the base table
    await cql.run_async("DROP INDEX ks.idx1")
    await asyncio.sleep(5)

    await cql.run_async("DROP TABLE ks.tab")
