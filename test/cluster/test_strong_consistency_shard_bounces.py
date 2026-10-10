#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import struct

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.metadata import Murmur3Token
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, wait_for_leader
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas

logger = logging.getLogger(__name__)

WRITE_SHARD_BOUNCES = 'scylla_strong_consistency_coordinator_write_shard_bounces'
WRITE_NODE_BOUNCES = 'scylla_strong_consistency_coordinator_write_node_bounces'
READ_SHARD_BOUNCES = 'scylla_strong_consistency_coordinator_read_shard_bounces'
READ_NODE_BOUNCES = 'scylla_strong_consistency_coordinator_read_node_bounces'
BOUNCES = (WRITE_SHARD_BOUNCES, WRITE_NODE_BOUNCES, READ_SHARD_BOUNCES, READ_NODE_BOUNCES)
SMP = 2


def ks_opts(rf: int) -> str:
    return (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"
            f" AND tablets = {{'initial': {SMP}}} AND consistency = 'global'")


def token_of(pk: int) -> int:
    return Murmur3Token.from_key(struct.pack('>i', pk)).value


def pinned_cluster(ip: str) -> Cluster:
    """A driver cluster that is not shard-aware. It opens one connection to `ip`, which lands
    on the shard the server picks, and sends every request through it. The manager's session
    cannot do that: it sends each request to the shard that owns its key."""
    profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([ip]), consistency_level=ConsistencyLevel.QUORUM, request_timeout=60)
    return Cluster(contact_points=[ip], protocol_version=4, execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                   shard_aware_options={'disable': True})


async def one_key_per_tablet(manager: ScyllaClusterManager, server: ServerInfo, ks: str, table_name: str) -> tuple[list[int], list[str]]:
    """One partition key for each of the two tablets, and the raft group id of each tablet, in tablet order.
    Checks that on every node the two tablets are on different shards. This is what makes one
    of the two keys land on the wrong shard of a connection pinned to one shard."""
    tablets = await get_all_tablet_replicas(manager, server, ks, table_name)
    assert len(tablets) == SMP
    boundary = tablets[0].last_token
    keys = [next(pk for pk in range(1000) if token_of(pk) <= boundary), next(pk for pk in range(1000) if token_of(pk) > boundary)]
    for host, shard in tablets[0].replicas:
        assert (host, shard ^ 1) in tablets[1].replicas, f"both tablets on shard {shard} of {host}: {tablets}"
    table_id = await manager.get_table_id(ks, table_name)
    rows = await manager.get_cql().run_async(f"SELECT last_token, raft_group_id FROM system.tablets WHERE table_id = {table_id}")
    group_ids = [str(next(r.raft_group_id for r in rows if r.last_token == t.last_token)) for t in tablets]
    logger.info(f"tablets {tablets}, keys {keys}, groups {group_ids}")
    return keys, group_ids


async def bounces(manager: ScyllaClusterManager, server: ServerInfo) -> dict[str, float]:
    m = await manager.metrics.query(server.ip_addr)
    return {name: m.get(name) or 0 for name in BOUNCES}


async def run_round(manager: ScyllaClusterManager, server: ServerInfo, group_ids: list[str], session: Session,
                    table: str, keys: list[int], cl: ConsistencyLevel | None, expected: dict[str, int]) -> None:
    """Write 1 into both keys, or read both keys at `cl`, through the pinned session, and check
    that the bounce counters of `server` grew by exactly `expected` (missing names: by 0).
    A local read does not wait for the replica to apply the last write, so the replica is
    made to catch up first."""
    if cl == ConsistencyLevel.ONE:
        for group_id in group_ids:
            await read_barrier(manager.api, server.ip_addr, group_id, timeout=60)
    before = await bounces(manager, server)
    for pk in keys:
        if cl is None:
            await session.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, 1)")
        else:
            rows = await session.run_async(SimpleStatement(f"SELECT c FROM {table} WHERE pk = {pk}", consistency_level=cl))
            assert [r.c for r in rows] == [1], f"pk {pk} at {cl}"
    after = await bounces(manager, server)
    assert {name: after[name] - before[name] for name in BOUNCES} == {name: expected.get(name, 0) for name in BOUNCES}, f"round {cl}"


async def test_shard_bounce_counters(manager: ScyllaClusterManager):
    """Check that a strongly consistent request that reaches the wrong shard of a replica
    is bounced to the right shard exactly once, and that the bounce is counted.

    One node with two shards, two tablets (one per shard) and a connection pinned to one
    shard. Of every pair of requests, one per tablet, exactly one is on the wrong shard.
    Writes, quorum reads and local (CL=ONE) reads each cost one shard bounce per pair and
    no node bounce: with RF=1 there is no other node to bounce to.
    """
    server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE + [f'--smp={SMP}'])
    await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, ks_opts(1)) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            keys, group_ids = await one_key_per_tablet(manager, server, ks, table.split('.')[-1])
            cluster = pinned_cluster(server.ip_addr)
            try:
                session = cluster.connect()
                for cl, expected in ((None, {WRITE_SHARD_BOUNCES: 1}),
                                     (ConsistencyLevel.QUORUM, {READ_SHARD_BOUNCES: 1}),
                                     (ConsistencyLevel.ONE, {READ_SHARD_BOUNCES: 1})):
                    await run_round(manager, server, group_ids, session, table, keys, cl, expected)
            finally:
                cluster.shutdown()


async def test_shard_then_node_bounce(manager: ScyllaClusterManager):
    """Check the bounces of strongly consistent requests sent to the wrong shard of a
    replica that is not the leader: the request is first bounced to the local shard that
    owns the tablet, then to the leader, and both hops are counted.

    Three nodes, two tablets (one per shard of each node) and a connection pinned to one
    shard of a node that leads neither group. With two groups on three nodes such a node
    always exists. A pair of writes costs one shard bounce and two node bounces. A node
    forward stores the leader in the leader cache of the shard that got the request, so a
    later linearizable request for that tablet on that shard goes to the leader directly.
    Local reads then cost one shard bounce and no node bounce, and quorum reads cost two
    node bounces and no shard bounce.
    """
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE + [f'--smp={SMP}'], auto_rack_dc='dc1')
    await manager.get_ready_cql(servers)
    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]

    async with new_test_keyspace(manager, ks_opts(3)) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            keys, group_ids = await one_key_per_tablet(manager, servers[0], ks, table.split('.')[-1])
            leaders = {await wait_for_leader(manager, servers[0], gid) for gid in group_ids}
            follower = next(s for s, hid in zip(servers, host_ids) if hid not in leaders)
            logger.info(f"leaders {leaders}, requests go to {follower.ip_addr}")
            cluster = pinned_cluster(follower.ip_addr)
            try:
                session = cluster.connect()
                for cl, expected in ((None, {WRITE_SHARD_BOUNCES: 1, WRITE_NODE_BOUNCES: 2}),
                                     (ConsistencyLevel.ONE, {READ_SHARD_BOUNCES: 1}),
                                     (ConsistencyLevel.QUORUM, {READ_NODE_BOUNCES: 2})):
                    await run_round(manager, follower, group_ids, session, table, keys, cl, expected)
            finally:
                cluster.shutdown()
