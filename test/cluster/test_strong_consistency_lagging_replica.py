#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-2565",
    reason="a replica behind the leader's snapshot needs a snapshot transfer, which is not implemented",
)
async def test_replica_behind_leader_snapshot_catches_up(manager: ScyllaClusterManager):
    """Check that a replica that missed entries the leader has since dropped from its log
    catches up once it is back.

    Idea: a leader keeps only the log above its last snapshot (plus a trailing part), so a
    follower that is down while the leader snapshots can only be brought up to date by a
    snapshot transfer. Nothing here overloads the cluster: one follower is stopped, a few
    rows are written, and the leader is asked for a snapshot, which it takes with no
    trailing entries.

    Setup: 3 nodes, RF=3, one tablet, the tablet's raft group led by the first node.

    The third node is stopped once a read barrier shows it is up to date, ten rows are
    written, the leader takes a snapshot through the REST API, and the third node is
    restarted. A read barrier on it must then complete, i.e. it applies everything the
    leader has committed, and a CL=ONE read on it must return all ten rows.
    """
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, _ = await manager.get_ready_cql(servers)
    leader, _, lagging = servers

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            await ensure_raft_group_leader_on(manager, leader, group_id)
            await read_barrier(manager.api, lagging.ip_addr, group_id, timeout=60)

            # The lagging replica misses these writes, and the snapshot drops them from the leader's log.
            await manager.server_stop_gracefully(lagging.server_id)
            for pk in range(10):
                await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk})")
            await manager.api.client.post(f"/raft/trigger_snapshot/{group_id}", host=leader.ip_addr, params={"timeout": "60"})

            # Back and caught up.
            await manager.server_start(lagging.server_id)
            lagging_host = (await wait_for_cql_and_get_hosts(cql, [lagging], time.time() + 60))[0]
            await read_barrier(manager.api, lagging.ip_addr, group_id, timeout=60)
            local_read = SimpleStatement(f"SELECT pk FROM {table}", consistency_level=ConsistencyLevel.ONE)
            rows = await cql.run_async(local_read, host=lagging_host)
            assert sorted(r.pk for r in rows) == list(range(10))
