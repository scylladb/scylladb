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
from cassandra.cqltypes import Int32Type
from test.topology.util import reconnect_driver

from cassandra.query import SimpleStatement, BoundStatement # type: ignore
from cassandra.policies import FallthroughRetryPolicy
from cassandra.cluster import ConsistencyLevel

logger = logging.getLogger(__name__)

async def wait_for_view(cql, name, node_count):
    deadline = time.time() + 120
    while time.time() < deadline:
        done = await cql.run_async(f"SELECT COUNT(*) FROM system_distributed.view_build_status WHERE status = 'SUCCESS' AND view_name = '{name}' ALLOW FILTERING")
        if done[0][0] == node_count:
            return
        else:
            time.sleep(0.2)
    raise Exception("Timeout waiting for views to build")

def get_replicas(cql, key):
    return cql.cluster.metadata.get_replicas("ks", Int32Type.serialize(key, cql.cluster.protocol_version))

def get_remote_key(cql, local_node):
    i = 0
    while local_node == get_replicas(cql, i)[0]:
        i = i + 1
    return i

# In the test, we create a table and perform a pair of writes to it. The
# second write should fail due to admission control. We check that this
# is indeed the error thrown.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_mv_admission_control_exception(manager: ManagerClient) -> None:
    node_count = 2
    servers = await manager.servers_add(node_count, config={'error_injections_at_startup': ['view_update_limit', 'delay_before_remote_view_update', 'update_backlog_immediately']})
    cql, hosts = await manager.get_ready_cql(servers)
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE ks.tab (key int, c int, v text, PRIMARY KEY (key, c))")
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab "
                    "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")
    await wait_for_view(cql, 'mv_cf_view', node_count)
    key = int(time.time())
    logger.info(f"Base table key: {key}")
    # Only remote updates hold on to memory, so make the update remote
    remote_key = get_remote_key(cql, local_node = get_replicas(cql, key)[0])

    # Prepare the statement so that the write goes to the same shard both
    # times (the first write will cause only the shard on which it was
    # performed to have the updated view update backlog).
    stmt = cql.prepare(f"INSERT INTO ks.tab (key, c, v) VALUES (?, ?, ?)")
    # To inspect the error message, we need to disable retries, which can't
    # be done in `prepare()` or `run_async()`. Instead, we use `BoundStatement`.
    bnd_stmt = BoundStatement(stmt, retry_policy=FallthroughRetryPolicy())
    await cql.run_async(bnd_stmt.bind([key, remote_key, 240000*'a']), host=hosts[0])
    with pytest.raises(Exception, match="View update backlog is too high"):
        await cql.run_async(bnd_stmt.bind([key, remote_key, 240000*'a']), host=hosts[0])

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
    servers = await manager.servers_add(node_count - 1, config={'error_injections_at_startup': ['update_backlog_immediately']})
    server = await manager.server_add(config={'error_injections_at_startup': ['view_update_limit', 'delay_before_remote_view_update', 'update_backlog_immediately']})
    cql, hosts = await manager.get_ready_cql(servers)
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}")
    await cql.run_async(f"CREATE TABLE ks.tab (key int, c int, v text, PRIMARY KEY (key, c))")
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab "
                    "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")
    await wait_for_view(cql, 'mv_cf_view', node_count)

    key = int(time.time())
    while server.ip_addr not in get_replicas(cql, key):
        key = key + 1
    logger.info(f"Base table key: {key}")

    # Prepare the statement so that the write goes to the same shard 
    # for all requests (the backlog increase caused by a write is only
    # immediately noted on the shard that the write was performed on).
    stmt = cql.prepare(f"INSERT INTO ks.tab (key, c, v) VALUES (?, ?, ?)")
    for i in range(10):
        await cql.run_async(stmt, [key, i, 240000*'a'], host=hosts[0])
        # Sleep a bit to prevent multiple requests from being sent at the same time,
        # without updating the view backlog in between.
        await asyncio.sleep(0.2)

    await manager.server_stop(servers[0].server_id)
    await manager.server_stop(servers[1].server_id)
    await manager.server_stop(servers[2].server_id)
    cql = await reconnect_driver(manager)
    assert len(await cql.run_async(SimpleStatement(f"SELECT * FROM ks.tab WHERE key={key}", consistency_level=ConsistencyLevel.ONE))) == 10
