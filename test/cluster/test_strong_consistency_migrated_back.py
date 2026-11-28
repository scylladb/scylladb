#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

import pytest

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-5657",
                      reason="replay mixes the old incarnation's raft batches with the new one and refuses the boot")
async def test_restart_after_tablet_migrated_away_and_back(manager: ScyllaClusterManager):
    """
    Idea: a tablet migrated off a shard and back to it leaves the old incarnation's raft
    batches in that shard's commitlog. After a crash, replay must not mix them with the
    new incarnation's log, and the node must start with all the data.

    Setup: four nodes, two of them (A and B) in one rack, and an RF=3 table with one
    tablet and the balancer off. A is killed and restarted while it hosts the tablet, so
    its replay drops the log up to the persisted floor and the old incarnation's batches
    written after that start above index 1. The tablet then moves from A to B and back
    to the same shard of A, led by a replica that was not restarted. Rows are written
    between all the steps. A is then killed and started, and every row is read back.
    """
    servers = await manager.servers_add(4, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, property_file=[
        {'dc': 'dc1', 'rack': 'rack1'},
        {'dc': 'dc1', 'rack': 'rack1'},
        {'dc': 'dc1', 'rack': 'rack2'},
        {'dc': 'dc1', 'rack': 'rack3'},
    ])
    cql, _ = await manager.get_ready_cql(servers)
    await manager.disable_tablet_balancing()
    host_ids = [await manager.get_host_id(s.server_id) for s in servers]

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks, \
            new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
        table_name = table.split('.')[-1]
        group_id = await get_table_raft_group_id(manager, ks, table_name)
        tablet, = await get_all_tablet_replicas(manager, servers[0], ks, table_name)
        # The rack1 replica is A; the other rack1 node is B.
        a_host, a_shard = next(r for r in tablet.replicas if r[0] in host_ids[:2])
        a = servers[host_ids.index(a_host)]
        b_host = next(h for h in host_ids[:2] if h != a_host)
        # rack2's only node, so a replica.
        leader = servers[2]

        pk = 0

        async def write(n: int):
            nonlocal pk
            for _ in range(n):
                await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk})")
                pk += 1

        await write(10)
        await manager.server_stop(a.server_id, convict=False)
        await manager.server_start(a.server_id)
        cql, _ = await manager.get_ready_cql(servers)
        # A's log now starts above index 1, and a joining replica can only catch up from
        # index 1: snapshot transfer is not implemented for these groups.
        await ensure_raft_group_leader_on(manager, leader, group_id)
        await write(10)

        logger.info("Moving the tablet from %s:%s to %s:0", a_host, a_shard, b_host)
        await manager.api.move_tablet(servers[0].ip_addr, ks, table_name, a_host, a_shard, b_host, 0, tablet.last_token)
        await write(10)
        logger.info("Moving the tablet back to %s:%s", a_host, a_shard)
        await manager.api.move_tablet(servers[0].ip_addr, ks, table_name, b_host, 0, a_host, a_shard, tablet.last_token)
        await write(10)

        await manager.server_stop(a.server_id, convict=False)
        await manager.server_start(a.server_id)
        cql, _ = await manager.get_ready_cql(servers)

        for i in range(pk):
            rows = await cql.run_async(f"SELECT v FROM {table} WHERE pk = {i}")
            assert [r.v for r in rows] == [i], f"pk={i}"
