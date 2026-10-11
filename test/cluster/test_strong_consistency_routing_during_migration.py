#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.pool import Host
from cassandra.query import PreparedStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import get_coordinator_host, new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

REDIRECTED = 'scylla_transport_requests_forwarded_redirected'
FORWARDED_FAILED = 'scylla_transport_requests_forwarded_failed'
WRITE_NODE_BOUNCES = 'scylla_strong_consistency_coordinator_write_node_bounces'
READ_NODE_BOUNCES = 'scylla_strong_consistency_coordinator_read_node_bounces'
NODE_BOUNCES = (READ_NODE_BOUNCES, WRITE_NODE_BOUNCES)
WAIT_AFTER_CLEANUP = 'wait_after_tablet_cleanup'
KS_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
           " AND tablets = {'initial': 1} AND consistency = 'global'")


async def counters(manager: ScyllaClusterManager, server: ServerInfo, names: tuple[str, ...] = (REDIRECTED, FORWARDED_FAILED)) -> dict[str, float]:
    """The counters `names` on `server`, summed over its shards."""
    metrics = await manager.metrics.query(server.ip_addr)
    return {name: metrics.get(name) or 0 for name in names}


async def read(cql: Session, select: PreparedStatement, host: Host, cl: ConsistencyLevel = ConsistencyLevel.QUORUM) -> list[int]:
    """The values of `c` for `pk = 0`, read through `host` at `cl`."""
    bound = select.bind([0])
    bound.consistency_level = cl
    return [r.c for r in await cql.run_async(bound, host=host)]


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_stale_leader_cache_costs_one_hop_after_migration(manager: ScyllaClusterManager):
    """Move the leader's replica to another node and check how a non-replica routes to the tablet.

    While the move is held at the cleanup, a CL=ONE read must not reach the old leader.
    After the move: a write may be redirected once but must not fail or reach the old
    leader, the next write costs nothing, and a quorum read through every replica returns
    a write sent through the new one.
    --smp=1 keeps all writes on one shard, where the leader cache lives.
    """
    servers = await manager.servers_add(5, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE + ['--smp=1'], auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]
    by_host_id = dict(zip(host_ids, zip(servers, hosts)))
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            tablet, = await get_all_tablet_replicas(manager, servers[0], ks, table_name)
            replica_shards = {str(host): shard for host, shard in tablet.replicas}
            replicas = set(replica_shards)
            non_replica_id, new_replica_id = [hid for hid in host_ids if hid not in replicas]
            non_replica, non_replica_host = by_host_id[non_replica_id]
            _, new_replica_host = by_host_id[new_replica_id]
            old_leader_id = await wait_for_leader(manager, by_host_id[next(iter(replicas))][0], group_id)
            old_leader, _ = by_host_id[old_leader_id]
            follower, _ = by_host_id[next(hid for hid in replicas if hid != old_leader_id)]
            logger.info(f"group {group_id}: replicas {replicas}, leader {old_leader_id}, non-replica {non_replica_id}, future replica {new_replica_id}")

            insert = cql.prepare(f"INSERT INTO {table} (pk, c) VALUES (?, ?)")
            select = cql.prepare(f"SELECT c FROM {table} WHERE pk = ?")

            async def write(c: int, host: Host = non_replica_host) -> None:
                await cql.run_async(insert.bind([0, c]), host=host)

            # Warm the non-replica's leader cache: from then on its writes are not redirected.
            await write(1)
            warm = await counters(manager, non_replica)
            await write(2)
            assert await counters(manager, non_replica) == warm, "the leader cache is not warm"

            # Move the leader's replica. The follower stays a replica, so it keeps knowing who leads.
            coordinator = await get_coordinator_host(manager)
            coordinator_log = await manager.server_open_log(coordinator.server_id)
            mark = await coordinator_log.mark()
            await manager.api.enable_injection(coordinator.ip_addr, WAIT_AFTER_CLEANUP, one_shot=True)
            move = asyncio.create_task(manager.api.move_tablet(follower.ip_addr, ks, table_name,
                                                               old_leader_id, replica_shards[old_leader_id], new_replica_id, 0, tablet.last_token))
            await coordinator_log.wait_for(f"{WAIT_AFTER_CLEANUP}: waiting for message", from_mark=mark, timeout=60)

            # Held at the cleanup: a CL=ONE read must not reach the old leader.
            old_leader_bounces = await counters(manager, old_leader, NODE_BOUNCES)
            assert await read(cql, select, non_replica_host, ConsistencyLevel.ONE) == [2]
            assert await counters(manager, old_leader, NODE_BOUNCES) == old_leader_bounces, "the read went through the old leader"
            assert await counters(manager, non_replica) == warm

            await manager.api.message_injection(coordinator.ip_addr, WAIT_AFTER_CLEANUP)
            await move
            await manager.api.quiesce_topology(follower.ip_addr)
            tablet, = await get_all_tablet_replicas(manager, follower, ks, table_name)
            replicas = {str(host) for host, _ in tablet.replicas}
            assert replicas == (set(replica_shards) - {old_leader_id}) | {new_replica_id}
            # A follower keeps naming the leader that just left until it hears from the new one.
            async def new_leader() -> str | None:
                leader = await wait_for_leader(manager, follower, group_id)
                return leader if leader in replicas else None
            leader = await wait_for(new_leader, time.time() + 60, label='a current replica to lead')
            logger.info(f"group {group_id}: replicas {replicas}, leader {leader}")

            # Stale cache: the cached leader is no longer a replica. The entry must be dropped
            # without asking it, and the write forwarded to a current replica, redirected at most once.
            await write(3)
            after_stale = await counters(manager, non_replica)
            assert after_stale[FORWARDED_FAILED] == warm[FORWARDED_FAILED]
            assert after_stale[REDIRECTED] - warm[REDIRECTED] <= 1
            assert await counters(manager, old_leader, NODE_BOUNCES) == old_leader_bounces, "the write went through the old leader"

            # Refreshed cache: no redirect.
            await write(4)
            assert await counters(manager, non_replica) == after_stale

            # A write through the new replica is returned by a quorum read through every replica.
            await write(5, new_replica_host)
            for hid in replicas:
                assert await read(cql, select, by_host_id[hid][1]) == [5], f"read via {hid}"
