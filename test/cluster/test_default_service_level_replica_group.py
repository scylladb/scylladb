# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio
import time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for
from test.cluster.util import new_test_keyspace, new_test_table, new_materialized_view


async def test_default_service_level_replica_group(manager: ScyllaClusterManager):
    """The replica work of the default service level runs in the sl:$default_replica
       group, so that the coordinator work does not delay it (SCYLLADB-6090).
       - The two groups have the shares 900 and 100 that main.cc gives them.
       - The coordinator node reads its local replica in sl:$default_replica,
         and the coordinator step stays in sl:default.
       - The other node runs the read that it gets over RPC in sl:$default_replica.
       - The writes to a table with a view check that the replica work uses the
         semaphores of the default service level: without them, each view update
         on the other node logs an error.
    """
    servers = await manager.servers_add(2, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    coordinator = hosts[0]

    for server in servers:
        metrics = await manager.metrics.query(server.ip_addr)
        for group, shares in [('sl:default', 100), ('sl:$default_replica', 900)]:
            assert metrics.get('scylla_scheduler_shares', {'group': group, 'shard': '0'}) == shares

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"
    async with new_test_keyspace(manager, ks_opts) as ks, \
               new_test_table(manager, ks, "p int PRIMARY KEY, v int") as table, \
               new_materialized_view(manager, table, "*", "v, p", "v IS NOT NULL AND p IS NOT NULL"):
        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await log.mark() for log in logs]
        async def local_view_updates():
            return [(await manager.metrics.query(s.ip_addr)).get('scylla_database_total_view_updates_pushed_local') or 0 for s in servers]
        local_view_updates_before = await local_view_updates()
        insert = SimpleStatement(f"INSERT INTO {table} (p, v) VALUES (%s, %s)", consistency_level=ConsistencyLevel.ALL)
        await asyncio.gather(*[cql.run_async(insert, (p, p), host=coordinator) for p in range(100)])
        # Only the view updates that a node applies locally use the semaphore of the log check.
        assert all(a > b for a, b in zip(await local_view_updates(), local_view_updates_before))
        for log, mark in zip(logs, marks):
            assert not await log.grep("View update concurrency semaphore for scheduling group", from_mark=mark)

        select = SimpleStatement(f"SELECT v FROM {table} WHERE p = 1", consistency_level=ConsistencyLevel.ALL)
        trace_id = cql.execute(select, host=coordinator, trace=True).get_query_trace().trace_id
        events_query = SimpleStatement(f"SELECT activity, source, thread FROM system_traces.events WHERE session_id = {trace_id}",
                                       consistency_level=ConsistencyLevel.ALL)

        # Each shard of each node writes its trace events later, with its own timer.
        # Wait until the events of both replicas and of the coordinator are there.
        async def read_events():
            events = list(await cql.run_async(events_query))
            replica_reads = [e for e in events if e.activity.startswith("Start querying singular range")]
            rpc_reads = [e for e in events if "message received from" in e.activity]
            local_sends = [e for e in events if "querying locally" in e.activity]
            if rpc_reads and local_sends and {str(e.source) for e in replica_reads} == {str(s.ip_addr) for s in servers}:
                return replica_reads + rpc_reads, local_sends
            return None
        replica_steps, coordinator_steps = await wait_for(read_events, time.time() + 60)
        # The thread of a trace event is "shard N/<scheduling group>".
        for e in replica_steps:
            assert e.thread.endswith("/sl:$default_replica"), f"{e.activity} ran in {e.thread}"
        for e in coordinator_steps:
            assert e.thread.endswith("/sl:default"), f"{e.activity} ran in {e.thread}"
