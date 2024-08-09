#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient

import asyncio
import pytest
import time
import logging

from test.topology.conftest import skip_mode
from test.pylib.util import wait_for_view
from test.topology_experimental_raft.test_mv_tablets import pin_the_only_tablet, get_tablet_replicas

from cassandra.cluster import ConsistencyLevel, EXEC_PROFILE_DEFAULT # type: ignore
from cassandra.cqltypes import Int32Type # type: ignore
from cassandra.policies import FallthroughRetryPolicy # type: ignore
from cassandra.query import SimpleStatement, BoundStatement # type: ignore

logger = logging.getLogger(__name__)

# In the test, we create a table and perform a pair of writes to it. The
# second write should fail due to admission control. We check that this
# is indeed the error thrown.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_mv_admission_control_exception(manager: ManagerClient) -> None:
    node_count = 2
    config = {'error_injections_at_startup': ['view_update_limit', 'delay_before_remote_view_update', 'update_backlog_immediately'], 'enable_tablets': True}
    servers = await manager.servers_add(node_count, config=config)
    cql, hosts = await manager.get_ready_cql(servers)
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
                        "AND tablets = {'initial': 1}")
    await cql.run_async(f"CREATE TABLE ks.tab (key int, c int, v text, PRIMARY KEY (key, c))")
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab "
                    "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")
    await wait_for_view(cql, 'mv_cf_view', node_count)

    # Only remote updates hold on to memory, so make the update remote by pinning base and view tablets to different nodes.
    await pin_the_only_tablet(manager, "ks", "tab", servers[0])
    await pin_the_only_tablet(manager, "ks", "mv_cf_view", servers[1])

    # Prepare the statement so that the write goes to the same shard both
    # times (the first write will cause only the shard on which it was
    # performed to have the updated view update backlog).
    stmt = cql.prepare(f"INSERT INTO ks.tab (key, c, v) VALUES (?, ?, ?)")
    # To inspect the error message, we need to disable retries, which can't
    # be done in `prepare()` or `run_async()`. Instead, we use `BoundStatement`.
    bnd_stmt = BoundStatement(stmt, retry_policy=FallthroughRetryPolicy())
    await cql.run_async(bnd_stmt.bind([0, 0, 240000*'a']), host=hosts[0])
    with pytest.raises(Exception, match="View update backlog is too high"):
        await cql.run_async(bnd_stmt.bind([0, 0, 'a']), host=hosts[0])

    await cql.run_async(f"DROP KEYSPACE ks")

# In this test we have a table with a materialized view and a replication factor of 3
# and 4 nodes so that not all views get paired with replicas on the same nodes.
# One of the nodes behaves as if it was slower than the rest - its view update backlog
# quickly reaches its limit.
# In the test we check that when the backlog increases on the node, following requests
# are rejected by admission control and retried until they reach all replicas, instead
# of succeeding just on the remaining replicas, reaching a quorum, but failing the
# write on the slow node.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_mv_retried_writes_reach_all_replicas(manager: ManagerClient) -> None:
    node_count = 4
    servers = await manager.servers_add(node_count - 1, config={'error_injections_at_startup': ['update_backlog_immediately'], 'enable_tablets': True})
    server = await manager.server_add(config={'error_injections_at_startup': ['view_update_limit', 'delay_before_remote_view_update', 'update_backlog_immediately'], 'enable_tablets': True})

    cql, hosts = await manager.get_ready_cql(servers)
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}}"
                        "AND tablets = {'initial': 1}")
    await cql.run_async(f"CREATE TABLE ks.tab (key int, c int, v text, PRIMARY KEY (key, c))")
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab "
                    "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")
    await wait_for_view(cql, 'mv_cf_view', node_count)

    # Disable tablet balancing so that the slow node doesn't get tablets moved away from it.
    for s in servers:
        await manager.api.disable_tablet_balancing(s.ip_addr)
    await manager.api.disable_tablet_balancing(server.ip_addr)

    # Make sure that the slow node has a base table tablet and no view tablets, so that the
    # view updates from it are remote. (using shard 0 and token 0 when moving tablets as they don't make a difference here)
    base_tablet_replicas = await get_tablet_replicas(manager, servers[0], "ks", "tab", 0)
    base_tablet_hosts = [str(replica[0]) for replica in base_tablet_replicas]
    slow_host_id = await manager.get_host_id(server.server_id)
    if str(slow_host_id) not in base_tablet_hosts:
        base_tablet_host_id, base_tablet_shard = base_tablet_replicas[0]
        await manager.api.move_tablet(servers[0].ip_addr, "ks", "tab", base_tablet_host_id, base_tablet_shard, slow_host_id, 0, 0)
    view_tablet_replicas = await get_tablet_replicas(manager, servers[0], "ks", "mv_cf_view", 0)
    view_tablet_hosts = [str(replica[0]) for replica in view_tablet_replicas]
    for replica_host, replica_shard in view_tablet_replicas:
        if str(replica_host) != str(slow_host_id):
            continue
        slow_host_shard = replica_shard
        # Move the view tablet to the node that doesn't have one
        for s in servers:
            fast_host_id = await manager.get_host_id(s.server_id)
            if str(fast_host_id) not in view_tablet_hosts:
                await manager.api.move_tablet(servers[0].ip_addr, "ks", "mv_cf_view", slow_host_id, slow_host_shard, fast_host_id, 0, 0)
        break

    # Prepare the statement so that the write goes to the same shard
    # for all requests (the backlog increase caused by a write is only
    # immediately noted on the shard that the write was performed on).
    stmt = cql.prepare(f"INSERT INTO ks.tab (key, c, v) VALUES (?, ?, ?)")
    for i in range(10):
        # Perform a write that will increase the view update backlog on the slow node
        # to a level causing admission control to reject the following writes.
        await cql.run_async(stmt, [0, i, 240000*'a'], host=hosts[0])
        # Based on whether the response from the slow node is received before the next write,
        # the following small write can serve two purposes:
        # 1. If the response is received before the next write, the write will be rejected by
        #   admission control and retried until it reaches all replicas.
        # 2. If the response is not received before the next write, the write will be sent to
        #   the slow node without causing the view update backlog limit to be exceeded. Then,
        #   due to cl=ALL, the coordinator will wait for the response from the slow node, which
        #   will carry an up-to-date view update backlog for the next large write.
        cl_all_execution_profile = cql.execution_profile_clone_update(EXEC_PROFILE_DEFAULT, consistency_level = ConsistencyLevel.ALL)
        await cql.run_async(stmt, [0, 10 + i, 'a'], host=hosts[0], execution_profile=cl_all_execution_profile)

    # Verify that all writes reached the slow node
    await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
    print(f"Connecting to {server.ip_addr}")
    await manager.driver_connect(server=server)
    cql = manager.get_cql()

    assert len(await cql.run_async(SimpleStatement(f"SELECT * FROM ks.tab", consistency_level=ConsistencyLevel.ONE))) == 20
