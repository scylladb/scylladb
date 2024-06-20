#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient

import asyncio
import pytest
from test.topology.conftest import skip_mode
from test.pylib.util import wait_for_view
from test.topology_experimental_raft.test_mv_tablets import pin_the_only_tablet
from test.pylib.tablets import get_tablet_replica

# This test reproduces issue #18542
# In the test, we create a table and perform a write to it a couple of times
# Each time, we check that a view update backlog on some shard increased
# due to the write.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_view_backlog_increased_after_write(manager: ManagerClient) -> None:
    node_count = 2
    # Use a higher smp to make it more likely that the writes go to a different shard than the coordinator.
    servers = await manager.servers_add(node_count, cmdline=['--smp', '5'], config={'error_injections_at_startup': ['never_finish_remote_view_updates'], 'enable_tablets': True})
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
                         "AND tablets = {'initial': 1}")
    await cql.run_async(f"CREATE TABLE ks.tab (base_key int, view_key int, v text, PRIMARY KEY (base_key, view_key))")
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab "
                    "WHERE view_key IS NOT NULL and base_key IS NOT NULL PRIMARY KEY (view_key, base_key) ")

    await wait_for_view(cql, 'mv_cf_view', node_count)
    # Only remote updates hold on to memory, so make the update remote
    await pin_the_only_tablet(manager, "ks", "tab", servers[0])
    (_, shard) = await get_tablet_replica(manager, servers[0], "ks", "tab", 0)
    await pin_the_only_tablet(manager, "ks", "mv_cf_view", servers[1])

    for v in [1000, 4000, 16000, 64000, 256000]:
        # Don't use a prepared statement, so that writes are likely sent to a different shard
        # than the one containing the key.
        await cql.run_async(f"INSERT INTO ks.tab (base_key, view_key, v) VALUES ({v}, {v}, '{v*'a'}')")
        # The view update backlog should increase on the node generating view updates
        local_metrics = await manager.metrics.query(servers[0].ip_addr)
        view_backlog = local_metrics.get('scylla_storage_proxy_replica_view_update_backlog', shard=str(shard))
        # The read view_backlog might still contain backlogs from the previous iterations, so we only assert that it is large enough
        assert view_backlog > v

    await cql.run_async(f"DROP KEYSPACE ks")

# This test reproduces issues #18461 and #18783
# In the test, we create a table and perform a write to it that fills the view update backlog.
# After a gossip round is performed, the following write should succeed.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_gossip_same_backlog(manager: ManagerClient) -> None:
    node_count = 2
    servers = await manager.servers_add(node_count, config={'error_injections_at_startup': ['view_update_limit', 'update_backlog_immediately'], 'enable_tablets': True})
    cql, hosts = await manager.get_ready_cql(servers)
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
                         "AND tablets = {'initial': 1}")
    await cql.run_async(f"CREATE TABLE ks.tab (key int, c int, v text, PRIMARY KEY (key, c))")
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab "
                    "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")
    await wait_for_view(cql, 'mv_cf_view', node_count)

    # Only remote updates hold on to memory, so make the update remote
    await pin_the_only_tablet(manager, "ks", "tab", servers[0])
    await pin_the_only_tablet(manager, "ks", "mv_cf_view", servers[1])

    stmt = cql.prepare(f"INSERT INTO ks.tab (key, c, v) VALUES (?, ?, ?)")

    await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, "never_finish_remote_view_updates", one_shot=False) for s in servers))
    await cql.run_async(stmt, [0, 0, 240000*'a'], host=hosts[0])
    await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, "never_finish_remote_view_updates") for s in servers))
    # The next write should be admitted eventually, after a gossip round (1s) is performed
    await cql.run_async(stmt, [0, 0, 'a'], host=hosts[0])

    await cql.run_async(f"DROP KEYSPACE ks")
