#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import time
import uuid

import pytest
from cassandra.cluster import ConsistencyLevel

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import new_test_keyspace, new_test_table, trigger_stepdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replicas
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

REDIRECTED = 'scylla_transport_requests_forwarded_redirected'
FORWARDED_OK = 'scylla_transport_requests_forwarded_successfully'
FORWARDED_FAILED = 'scylla_transport_requests_forwarded_failed'
WRITE_NODE_BOUNCES = 'scylla_strong_consistency_coordinator_write_node_bounces'
OLD_LEADER_BOUNCES = 'old_leader:' + WRITE_NODE_BOUNCES
PARK_WRITE = 'sc_coordinator_wait_before_begin_mutate'
DROP_LEADER_TRAFFIC = ('raft_drop_incoming_append_entries_for_specified_group',
                       'raft_drop_incoming_read_quorum_for_specified_group')


@pytest.mark.asyncio
@pytest.mark.parametrize("new_leader_known", [
    True,
    pytest.param(False, marks=pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')),
], ids=["new_leader_known", "election_in_flight"])
async def test_stale_leader_cache_costs_one_hop_after_stepdown(manager: ScyllaClusterManager, new_leader_known: bool):
    """Verify that a stale leader cache costs a strongly consistent write exactly one extra
    hop - never an error, never a wrong answer, never a redirect loop.

    new_leader_known: the write is sent once every replica agrees on the new leader.
    election_in_flight: the write reaches the old leader while it still leads and is parked there
    across the stepdown; the old leader is kept from hearing the new one, so the replicas disagree
    about who leads when the write resumes: it waits for a leader and still costs one hop.
    """
    # A slow raft tick puts the election timeout (10-20 ticks) far beyond the moment for which
    # election_in_flight keeps the old leader leaderless, so it never starts a vote of its own.
    # The first leader of a group is picked at bootstrap, not elected, so this costs no startup time.
    config = DEFAULT_CONFIG | {'error_injections_at_startup': [
        {'name': 'strongly-consistent-raft-group-tick-interval-in-ms', 'value': '1000'}]}
    servers = await manager.servers_add(4, config=config, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]
    by_host_id = dict(zip(host_ids, zip(servers, hosts)))

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            replica_host_ids = [str(r[0]) for r in await get_tablet_replicas(manager, servers[0], ks, table_name, 0)]
            assert len(replica_host_ids) == 3
            old_leader_id = await wait_for_leader(manager, by_host_id[replica_host_ids[0]][0], group_id)
            non_replica_id = next(hid for hid in host_ids if hid not in replica_host_ids)
            old_leader_server, _ = by_host_id[old_leader_id]
            non_replica_server, non_replica_host = by_host_id[non_replica_id]
            logger.info(f"group {group_id}: leader {old_leader_id}, replicas {replica_host_ids}, non-replica {non_replica_id}")

            # The leader cache is per shard of the coordinator. A bound statement carries a routing
            # key, so the driver pins every pk=0 request to the same shard of the non-replica; a
            # plain string statement would land on a random shard and see a random cache.
            insert = cql.prepare(f"INSERT INTO {table} (pk, c) VALUES (?, ?)")
            select = cql.prepare(f"SELECT c FROM {table} WHERE pk = ?")
            select.consistency_level = ConsistencyLevel.QUORUM

            async def write(c: int) -> None:
                await cql.run_async(insert.bind([0, c]), host=non_replica_host)

            async def snapshot() -> dict[str, int]:
                m = await manager.metrics.query(non_replica_server.ip_addr)
                old = await manager.metrics.query(old_leader_server.ip_addr)
                return {n: m.get(n) or 0 for n in (REDIRECTED, FORWARDED_OK, FORWARDED_FAILED, WRITE_NODE_BOUNCES)} | {
                    OLD_LEADER_BOUNCES: old.get(WRITE_NODE_BOUNCES) or 0}

            def deltas(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
                return {name: after[name] - before[name] for name in before}

            # Warm the non-replica's cache with the current leader. This write may itself be
            # redirected, which is why the counters are snapshotted only afterwards.
            await write(1)
            before = await snapshot()

            async def new_leader(views_of: list[str]) -> str | None:
                seen = {str(await manager.api.get_raft_leader(by_host_id[hid][0].ip_addr, group_id)) for hid in views_of}
                if len(seen) != 1:
                    return None
                leader = seen.pop()
                return leader if leader != old_leader_id and uuid.UUID(leader).int != 0 else None

            if new_leader_known:
                await trigger_stepdown(manager, old_leader_server, group_id)
                new_leader_id = await wait_for(lambda: new_leader(replica_host_ids), time.time() + 60,
                                               label=f"every replica of group {group_id} to know a new leader")
                await write(2)
            else:
                # Park the write on the old leader while it still leads, before it asks raft who leads.
                ip = old_leader_server.ip_addr
                await manager.api.enable_injection(ip, PARK_WRITE, one_shot=True)
                parked = asyncio.create_task(write(2))
                await manager.api.wait_for_injection_enter(ip, PARK_WRITE)

                # The stepdown completes on the successor's vote request, a separate verb, but the old
                # leader drops everything the new leader sends it and so never learns who won.
                for injection in DROP_LEADER_TRAFFIC:
                    await manager.api.enable_injection(ip, injection, one_shot=False, parameters={'value': group_id})
                await trigger_stepdown(manager, old_leader_server, group_id)
                new_leader_id = await wait_for(lambda: new_leader([h for h in replica_host_ids if h != old_leader_id]), time.time() + 60,
                                               label=f"the followers of group {group_id} to know a new leader")
                assert uuid.UUID(await manager.api.get_raft_leader(ip, group_id)).int == 0

                # Resume the write while the old leader knows no leader; wait until raft says it is
                # waiting for one, and only then let the new leader's messages through.
                log = await manager.server_open_log(old_leader_server.server_id)
                mark = await log.mark()
                await manager.api.set_logger_level(ip, "raft", "trace")
                await manager.api.message_injection(ip, PARK_WRITE)
                await log.wait_for(f"sc-{group_id}\\] the leader is unknown, waiting through uncertainty", from_mark=mark, timeout=60)
                for injection in DROP_LEADER_TRAFFIC:
                    await manager.api.disable_injection(ip, injection)
                await manager.api.set_logger_level(ip, "raft", "info")
                await parked
            logger.info(f"group {group_id}: new leader {new_leader_id}")

            # Stale cache: forwarded to the old leader, redirected once, then successful.
            after_stale = await snapshot()
            assert deltas(before, after_stale) == {
                REDIRECTED: 1, FORWARDED_OK: 1, FORWARDED_FAILED: 0, WRITE_NODE_BOUNCES: 1, OLD_LEADER_BOUNCES: 1}

            # Refreshed cache: forwarded straight to the new leader, no extra hop.
            await write(3)
            assert deltas(after_stale, await snapshot()) == {
                REDIRECTED: 0, FORWARDED_OK: 1, FORWARDED_FAILED: 0, WRITE_NODE_BOUNCES: 1, OLD_LEADER_BOUNCES: 0}

            follower_id = next(hid for hid in replica_host_ids if hid != new_leader_id)
            for hid in (follower_id, new_leader_id):
                rows = await cql.run_async(select.bind([0]), host=by_host_id[hid][1])
                assert [row.c for row in rows] == [3], f"read via {hid}"
