#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
