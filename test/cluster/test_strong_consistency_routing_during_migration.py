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
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

REDIRECTED = 'scylla_transport_requests_forwarded_redirected'
FORWARDED_FAILED = 'scylla_transport_requests_forwarded_failed'
WRITE_NODE_BOUNCES = 'scylla_strong_consistency_coordinator_write_node_bounces'
WAIT_FOR_SNAPSHOT_TRANSFER = 'sc_wait_for_snapshot_transfer'
KS_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
           " AND tablets = {'initial': 1} AND consistency = 'global'")


async def forwarding_counters(manager: ScyllaClusterManager, server: ServerInfo, names: tuple[str, ...] = (REDIRECTED, FORWARDED_FAILED)) -> dict[str, float]:
    """The counters `names` on `server`, summed over its shards. By default: how many
    requests `server` forwarded got redirected, and how many failed."""
    metrics = await manager.metrics.query(server.ip_addr)
    return {name: metrics.get(name) or 0 for name in names}


async def read(cql: Session, select: PreparedStatement, host: Host) -> list[int]:
    """The values of `c` for `pk = 0`, read through `host`."""
    return [r.c for r in await cql.run_async(select.bind([0]), host=host)]


async def replica_ids(manager: ScyllaClusterManager, server: ServerInfo, ks: str, table_name: str) -> set[str]:
    """Host ids of the replicas of the only tablet of `table_name`."""
    tablet, = await get_all_tablet_replicas(manager, server, ks, table_name)
    return {str(host) for host, _ in tablet.replicas}


async def move_leader_replica(manager: ScyllaClusterManager, server: ServerInfo, ks: str, table_name: str, group_id: str, to_host_id: str) -> str:
    """Move the leader's replica of the only tablet of `table_name` to `to_host_id`.

    Returns the host id of the leader the group has after the move. `server` must be a
    replica other than the leader: only a replica knows who leads, and it stays a replica.
    """
    tablet, = await get_all_tablet_replicas(manager, server, ks, table_name)
    replica_shards = {str(host): shard for host, shard in tablet.replicas}
    leader_id = await wait_for_leader(manager, server, group_id)
    logger.info(f"moving the replica of group {group_id} from leader {leader_id} to {to_host_id}")
    await manager.api.move_tablet(server.ip_addr, ks, table_name, leader_id, replica_shards[leader_id], to_host_id, 0, tablet.last_token)
    await manager.api.quiesce_topology(server.ip_addr)
    replicas_after = await replica_ids(manager, server, ks, table_name)
    assert replicas_after == (set(replica_shards) - {leader_id}) | {to_host_id}
    # A follower keeps naming the leader that just left until it hears from the new one.
    async def new_leader() -> str | None:
        leader = await wait_for_leader(manager, server, group_id)
        return leader if leader in replicas_after else None
    return await wait_for(new_leader, time.time() + 60, label=f"group {group_id} to be led by a current replica")


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_stale_leader_cache_costs_one_hop_after_migration(manager: ScyllaClusterManager):
    """Check how a non-replica routes strongly consistent requests when a migration takes
    the leader's replica to another node.

    While the migration is paused in the middle of the snapshot transfer, a write and a
    quorum read through the non-replica must still work: the old replica set serves them.
    After the migration the non-replica's leader cache names a node that is not a replica
    any more. The coordinator must drop that entry, without asking the old leader, and
    forward to a current replica. That costs at most one redirect (when the replica it
    picks is not the leader) and never fails; the next write goes straight to the leader
    again. A quorum read through every current replica returns the last write.
    """
    servers = await manager.servers_add(5, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]
    by_host_id = dict(zip(host_ids, zip(servers, hosts)))
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            replicas = await replica_ids(manager, servers[0], ks, table_name)
            non_replica_id, new_replica_id = [hid for hid in host_ids if hid not in replicas]
            non_replica, non_replica_host = by_host_id[non_replica_id]
            new_replica, _ = by_host_id[new_replica_id]
            old_leader_id = await wait_for_leader(manager, by_host_id[next(iter(replicas))][0], group_id)
            old_leader, _ = by_host_id[old_leader_id]
            follower, _ = by_host_id[next(hid for hid in replicas if hid != old_leader_id)]
            logger.info(f"group {group_id}: replicas {replicas}, leader {old_leader_id}, non-replica {non_replica_id}, future replica {new_replica_id}")

            insert = cql.prepare(f"INSERT INTO {table} (pk, c) VALUES (?, ?)")
            select = cql.prepare(f"SELECT c FROM {table} WHERE pk = ?")
            select.consistency_level = ConsistencyLevel.QUORUM

            async def write(c: int) -> None:
                await cql.run_async(insert.bind([0, c]), host=non_replica_host)

            # Warm the non-replica's leader cache: from then on its writes are not redirected.
            await write(1)
            warm = await forwarding_counters(manager, non_replica)
            await write(2)
            assert await forwarding_counters(manager, non_replica) == warm, "the leader cache is not warm"

            new_replica_log = await manager.server_open_log(new_replica.server_id)
            mark = await new_replica_log.mark()
            await manager.api.enable_injection(new_replica.ip_addr, WAIT_FOR_SNAPSHOT_TRANSFER, one_shot=True)
            move = asyncio.create_task(move_leader_replica(manager, follower, ks, table_name, group_id, new_replica_id))
            await new_replica_log.wait_for(f"{WAIT_FOR_SNAPSHOT_TRANSFER}: waiting for message", from_mark=mark, timeout=60)

            # Mid-migration: the old replica set still serves what the non-replica forwards.
            await write(3)
            assert await read(cql, select, non_replica_host) == [3]
            assert await forwarding_counters(manager, non_replica) == warm

            await manager.api.message_injection(new_replica.ip_addr, WAIT_FOR_SNAPSHOT_TRANSFER)
            new_leader_id = await move
            replicas = await replica_ids(manager, follower, ks, table_name)
            logger.info(f"group {group_id}: replicas {replicas}, leader {new_leader_id}")

            # Stale cache: the cached leader is not a replica any more. The entry must be
            # dropped without contacting the old leader (it would bounce the write), and the
            # write goes to the closest current replica, which may redirect it once.
            old_leader_bounces = await forwarding_counters(manager, old_leader, (WRITE_NODE_BOUNCES,))
            await write(4)
            after_stale = await forwarding_counters(manager, non_replica)
            assert after_stale[FORWARDED_FAILED] == warm[FORWARDED_FAILED]
            assert after_stale[REDIRECTED] - warm[REDIRECTED] <= 1
            assert await forwarding_counters(manager, old_leader, (WRITE_NODE_BOUNCES,)) == old_leader_bounces, "the write went through the old leader"

            # Refreshed cache: no redirect.
            await write(5)
            assert await forwarding_counters(manager, non_replica) == after_stale
            for hid in replicas:
                assert await read(cql, select, by_host_id[hid][1]) == [5], f"read via {hid}"


async def test_prepared_statement_survives_tablet_migration(manager: ScyllaClusterManager):
    """Check that prepared statements keep working when a migration takes the leader's
    replica to a node that was not a replica before.

    A bound statement sent to a follower is forwarded to the leader by its prepared id.
    After the migration that route is gone: the same statements are run through the same
    follower, which must forward them to the new leader, and through the new replica,
    which must serve them itself or forward them. No forwarded request may fail, and every
    current replica must return the last write. Splitting the tablet is not covered:
    strongly consistent tablets do not split yet.
    """
    servers = await manager.servers_add(4, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]
    by_host_id = dict(zip(host_ids, zip(servers, hosts)))
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            replicas = await replica_ids(manager, servers[0], ks, table_name)
            leader_id = await wait_for_leader(manager, by_host_id[next(iter(replicas))][0], group_id)
            follower, follower_host = by_host_id[next(hid for hid in replicas if hid != leader_id)]
            new_replica_id = next(hid for hid in host_ids if hid not in replicas)
            new_replica, new_replica_host = by_host_id[new_replica_id]

            insert = cql.prepare(f"INSERT INTO {table} (pk, c) VALUES (?, ?)")
            select = cql.prepare(f"SELECT c FROM {table} WHERE pk = ?")
            select.consistency_level = ConsistencyLevel.QUORUM

            await cql.run_async(insert.bind([0, 1]), host=follower_host)
            assert await read(cql, select, follower_host) == [1]
            before_move = {s: await forwarding_counters(manager, s) for s in (follower, new_replica)}

            await move_leader_replica(manager, follower, ks, table_name, group_id, new_replica_id)

            await cql.run_async(insert.bind([0, 2]), host=follower_host)
            assert await read(cql, select, follower_host) == [2]
            await cql.run_async(insert.bind([0, 3]), host=new_replica_host)
            assert await read(cql, select, new_replica_host) == [3]

            for s in (follower, new_replica):
                after_move = await forwarding_counters(manager, s)
                assert after_move[FORWARDED_FAILED] == before_move[s][FORWARDED_FAILED], f"a forwarded statement failed on {s.ip_addr}"
            for hid in await replica_ids(manager, servers[0], ks, table_name):
                assert await read(cql, select, by_host_id[hid][1]) == [3], f"read via {hid}"
