#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import re

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.pool import Host
from cassandra.query import BoundStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id
from test.cluster.util import get_coordinator_host, new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas

logger = logging.getLogger(__name__)

REDIRECTED = 'scylla_transport_requests_forwarded_redirected'
FORWARDED_FAILED = 'scylla_transport_requests_forwarded_failed'
NODE_BOUNCES = ('scylla_strong_consistency_coordinator_read_node_bounces',
                'scylla_strong_consistency_coordinator_write_node_bounces')
WAIT_AFTER_CLEANUP = 'wait_after_tablet_cleanup'
# How the coordinator of a non-replica routed a request, from its trace
# (service/strong_consistency/coordinator.cc, create_operation_ctx).
CACHED_LEADER = re.compile(r'redirecting to the cached leader ([0-9a-f-]{36}):')
EVICTED = re.compile(r'The cached leader ([0-9a-f-]{36}) of raft group .* evicting it')
CLOSEST_REPLICA = 'redirecting to the closest serving replica'
KS_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
           " AND tablets = {'initial': 1} AND consistency = 'global'")


async def counters(manager: ScyllaClusterManager, server: ServerInfo, names: tuple[str, ...] = (REDIRECTED, FORWARDED_FAILED)) -> dict[str, float]:
    """The counters `names` on `server`, summed over its shards."""
    metrics = await manager.metrics.query(server.ip_addr)
    return {name: metrics.get(name) or 0 for name in names}


def traced(cql: Session, bound: BoundStatement, host: Host) -> tuple[list, list[str]]:
    """`bound` run through `host`: its rows and the trace of the request."""
    result = cql.execute(bound, host=host, trace=True)
    rows = list(result)
    return rows, [event.description for event in result.get_query_trace(max_wait_sec=60).events]


def named_host(trace: list[str], pattern: re.Pattern) -> str:
    """The host id named by the one event of `trace` that `pattern` matches."""
    hosts = [match.group(1) for event in trace if (match := pattern.search(event))]
    assert len(hosts) == 1, f"{pattern.pattern!r} in {trace}"
    return hosts[0]


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_stale_leader_cache_costs_one_hop_during_migration(manager: ScyllaClusterManager):
    """Move the leader's replica to another node and check how a non-replica routes to the tablet.

    Idea: a non-replica caches the leader of a strongly consistent tablet and forwards
    requests that need the leader to it. A migration moving the leader's replica leaves
    that cache naming a node which is not a serving replica. At the cleanup stage of the
    move, the old leader is still listed among the tablet's replicas but is outside the
    serving set, so a lookup against the replica list instead of the serving set would
    keep forwarding to it. The coordinator must drop the entry without asking the old
    leader and forward to a serving replica, redirected at most once; nothing may fail.

    Setup: 5 nodes with --smp=1 (the leader cache is per shard), RF=3, one tablet. A
    non-replica warms its cache with two writes; the trace of the second names the cached
    leader, whose replica is then moved to the fifth node, with the move held at the
    cleanup on the topology coordinator.

    In the hold, the traces of the requests through the non-replica show the routing: a
    write must evict the cached leader and go to the closest serving replica, costing at
    most one redirect and no failure, and the old leader's bounce counters must stay flat;
    the next write and a QUORUM read must go to the refreshed cached leader, which is a
    serving replica, at no cost. After the move, a write through the new replica is
    returned by a QUORUM read through every replica.
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
            non_replica_id, new_replica_id = [hid for hid in host_ids if hid not in replica_shards]
            non_replica, non_replica_host = by_host_id[non_replica_id]
            _, new_replica_host = by_host_id[new_replica_id]

            insert = cql.prepare(f"INSERT INTO {table} (pk, c) VALUES (?, ?)")
            select = cql.prepare(f"SELECT c FROM {table} WHERE pk = ?")
            select.consistency_level = ConsistencyLevel.QUORUM

            def write(c: int, host: Host = non_replica_host) -> list[str]:
                """Write `c` through `host`; the trace of the request."""
                return traced(cql, insert.bind([0, c]), host)[1]

            def read(host: Host) -> tuple[list[int], list[str]]:
                """The values of `c` for `pk = 0` read through `host` at QUORUM, with the trace."""
                rows, trace = traced(cql, select.bind([0]), host)
                return [r.c for r in rows], trace

            # Warm the non-replica's leader cache: the second write goes to the cached leader.
            write(1)
            old_leader_id = named_host(write(2), CACHED_LEADER)
            old_leader, _ = by_host_id[old_leader_id]
            follower, _ = by_host_id[next(hid for hid in replica_shards if hid != old_leader_id)]
            serving = (set(replica_shards) - {old_leader_id}) | {new_replica_id}
            logger.info(f"group {group_id}: replicas {set(replica_shards)}, leader {old_leader_id}, "
                        f"non-replica {non_replica_id}, future replica {new_replica_id}")
            warm = await counters(manager, non_replica)
            old_leader_bounces = await counters(manager, old_leader, NODE_BOUNCES)

            # Move the leader's replica and hold the move at the cleanup: the old leader is
            # still listed among the tablet's replicas but no longer serves it.
            coordinator = await get_coordinator_host(manager)
            coordinator_log = await manager.server_open_log(coordinator.server_id)
            mark = await coordinator_log.mark()
            await manager.api.enable_injection(coordinator.ip_addr, WAIT_AFTER_CLEANUP, one_shot=True)
            move = asyncio.create_task(manager.api.move_tablet(follower.ip_addr, ks, table_name,
                                                               old_leader_id, replica_shards[old_leader_id], new_replica_id, 0, tablet.last_token))
            await coordinator_log.wait_for(f"{WAIT_AFTER_CLEANUP}: waiting for message", from_mark=mark, timeout=60)

            # Stale cache: the entry is dropped without asking the old leader, and the write
            # forwarded to the closest serving replica, redirected at most once.
            trace = write(3)
            assert named_host(trace, EVICTED) == old_leader_id
            assert any(CLOSEST_REPLICA in event for event in trace), trace
            after_stale = await counters(manager, non_replica)
            assert after_stale[FORWARDED_FAILED] == warm[FORWARDED_FAILED]
            assert after_stale[REDIRECTED] - warm[REDIRECTED] <= 1

            # Refreshed cache: the next write and a linearizable read go to the new leader at no cost.
            assert named_host(write(4), CACHED_LEADER) in serving
            values, trace = read(non_replica_host)
            assert values == [4]
            assert named_host(trace, CACHED_LEADER) in serving
            assert await counters(manager, non_replica) == after_stale
            assert await counters(manager, old_leader, NODE_BOUNCES) == old_leader_bounces, "a request went through the old leader"

            await manager.api.message_injection(coordinator.ip_addr, WAIT_AFTER_CLEANUP)
            await move
            await manager.api.quiesce_topology(follower.ip_addr)
            tablet, = await get_all_tablet_replicas(manager, follower, ks, table_name)
            assert {str(host) for host, _ in tablet.replicas} == serving

            # A write through the new replica is returned by a quorum read through every replica.
            write(5, new_replica_host)
            for hid in serving:
                assert read(by_host_id[hid][1])[0] == [5], f"read via {hid}"
