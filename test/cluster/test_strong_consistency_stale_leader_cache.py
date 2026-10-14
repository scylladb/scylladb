#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
from contextlib import suppress

import pytest
from cassandra.cluster import ConsistencyLevel

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table, trigger_stepdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replicas
from test.pylib.util import gather_safely

logger = logging.getLogger(__name__)

REDIRECTED = 'scylla_transport_requests_forwarded_redirected'
FORWARDED_OK = 'scylla_transport_requests_forwarded_successfully'
FORWARDED_FAILED = 'scylla_transport_requests_forwarded_failed'
WRITE_NODE_BOUNCES = 'scylla_strong_consistency_coordinator_write_node_bounces'
OLD_LEADER_BOUNCES = 'old_leader:' + WRITE_NODE_BOUNCES
PARK_WRITE = 'sc_coordinator_wait_before_begin_mutate'
DROP_LEADER_TRAFFIC = ('raft_drop_incoming_append_entries_for_specified_group',
                       'raft_drop_incoming_read_quorum_for_specified_group')
WAIT_TIMEOUT_S = 60


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", [
    "new_leader_known",
    pytest.param("election_in_flight", marks=pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')),
])
async def test_stale_leader_cache_costs_one_hop_after_stepdown(manager: ScyllaClusterManager, scenario: str):
    """Verify that in the scenarios below a stale leader cache costs a strongly consistent write
    exactly one extra hop - no error, no lost or duplicated write, no redirect loop.

    new_leader_known: the write is sent once every replica agrees on the new leader.
    election_in_flight: the write reaches the old leader while it still leads and is parked there
    across the stepdown; the old leader is kept from hearing the new one, so the replicas disagree
    about who leads when the write resumes: it waits for a leader and still costs one hop.

    The exact hop counts are not a documented guarantee of SC routing; they follow from the
    current implementation under the conditions this test sets up, so check these first if the
    counts change:
    - a single leader change with a stable successor, while the cached leader is still alive and
      still a replica. The forwarding loop (transport/server.cc) follows redirects without a
      limit, so a second leader change during the request would legitimately cost more hops;
    - a replica that knows no leader waits for one (need_wait_for_leader in begin_mutate())
      instead of redirecting. The one hop in election_in_flight relies on this.

    The writes append to a list, so every write leaves its own cell: a lost write(2) reads as
    [1, 3] and a re-executed one as [1, 2, 2, 3]. The cell keys are timeuuids built from the
    leader-assigned timestamp, so the order also checks that the new leader's timestamps are
    above the old leader's.
    """
    config = DEFAULT_CONFIG
    if scenario == "election_in_flight":
        # A slow raft tick puts the election timeout (10-20 ticks) far beyond the moment for which
        # the old leader is kept leaderless, so it never starts a vote of its own. It costs no time:
        # the bootstrap pick and the targeted stepdowns below transfer leadership without a tick.
        config = config | {'error_injections_at_startup': [
            {'name': 'strongly-consistent-raft-group-tick-interval-in-ms', 'value': '1000'}]}
    servers = await manager.servers_add(4, config=config, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*(manager.get_host_id(s.server_id) for s in servers))
    by_host_id = dict(zip(host_ids, zip(servers, hosts)))

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, l list<int>") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            replica_host_ids = [str(r[0]) for r in await get_tablet_replicas(manager, servers[0], ks, table_name, 0)]
            assert len(replica_host_ids) == 3
            old_leader_id, new_leader_id = replica_host_ids[:2]
            non_replica_id = next(hid for hid in host_ids if hid not in replica_host_ids)
            old_leader_server, _ = by_host_id[old_leader_id]
            non_replica_server, non_replica_host = by_host_id[non_replica_id]
            await ensure_raft_group_leader_on(manager, old_leader_server, group_id, timeout_seconds=WAIT_TIMEOUT_S)
            logger.info(f"group {group_id}: leader {old_leader_id}, next leader {new_leader_id}, non-replica {non_replica_id}")

            # The leader cache is per shard of the coordinator. A bound statement carries a routing
            # key, so the driver pins every pk=0 request to the same shard of the non-replica; a
            # plain string statement would land on a random shard and see a random cache.
            # Appending to a list only adds cells, so unlike l[i] = ? or l = l - ? it needs no
            # read-before-write, which SC tables reject ("don't support data prefetch").
            append = cql.prepare(f"UPDATE {table} SET l = l + ? WHERE pk = ?")
            select = cql.prepare(f"SELECT l FROM {table} WHERE pk = ?")
            select.consistency_level = ConsistencyLevel.QUORUM

            async def write(v: int) -> None:
                await cql.run_async(append.bind([[v], 0]), host=non_replica_host)

            async def snapshot() -> dict[str, int]:
                m, old = await gather_safely(manager.metrics.query(non_replica_server.ip_addr),
                                             manager.metrics.query(old_leader_server.ip_addr))
                return {n: m.get(n) or 0 for n in (REDIRECTED, FORWARDED_OK, FORWARDED_FAILED, WRITE_NODE_BOUNCES)} | {
                    OLD_LEADER_BOUNCES: old.get(WRITE_NODE_BOUNCES) or 0}

            def deltas(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
                return {name: after[name] - before[name] for name in before}

            async def wait_for_new_leader(views_of: list[str]) -> None:
                await gather_safely(*(wait_for_leader(manager, by_host_id[hid][0], group_id,
                                                      expected_host_id=new_leader_id, timeout=WAIT_TIMEOUT_S)
                                      for hid in views_of))

            async def stepdown_then_write() -> None:
                await trigger_stepdown(manager, old_leader_server, group_id, target_host_id=new_leader_id)
                await wait_for_new_leader(replica_host_ids)
                await write(2)

            async def write_parked_across_stepdown() -> None:
                # Park the write on the old leader while it still leads, before it asks raft who leads.
                ip = old_leader_server.ip_addr
                log = await manager.server_open_log(old_leader_server.server_id)
                await manager.api.set_logger_level(ip, "raft", "trace")
                mark = await log.mark()
                await manager.api.enable_injection(ip, PARK_WRITE, one_shot=True)
                parked = asyncio.create_task(write(2))

                async def cleanup() -> None:
                    # Disabling PARK_WRITE releases a write still parked on the server (a no-op once
                    # the one-shot has fired), so a failed run does not hang the node's shutdown.
                    for injection in (PARK_WRITE, *DROP_LEADER_TRAFFIC):
                        await manager.api.disable_injection(ip, injection)
                    await manager.api.set_logger_level(ip, "raft", "info")

                try:
                    await manager.api.wait_for_injection_enter(ip, PARK_WRITE)

                    # The stepdown completes on the successor's vote request, a separate verb, but the
                    # old leader drops everything the new leader sends it and so never learns who won.
                    for injection in DROP_LEADER_TRAFFIC:
                        await manager.api.enable_injection(ip, injection, one_shot=False, parameters={'value': group_id})
                    await trigger_stepdown(manager, old_leader_server, group_id, target_host_id=new_leader_id)
                    await wait_for_new_leader([h for h in replica_host_ids if h != old_leader_id])
                    assert await manager.api.get_raft_leader(ip, group_id) is None

                    # Resume the write while the old leader knows no leader; wait until raft says it is
                    # waiting for one, and only then let the new leader's messages through.
                    await manager.api.message_injection(ip, PARK_WRITE)
                    await log.wait_for(f"sc-{group_id}\\] the leader is unknown, waiting through uncertainty",
                                       from_mark=mark, timeout=WAIT_TIMEOUT_S)

                    # The isolated old leader must not have run out its election timeout meanwhile:
                    # a vote of its own would hand it leadership back and void the counts below.
                    candidate = [line for line, _ in await log.grep(f"tick\\[sc-{group_id}\\]: becoming a candidate", from_mark=mark)]
                    assert not candidate, f"old leader started an election while isolated: {candidate}"
                except BaseException:
                    # A write parked forever would hide the real error behind a pending task, and a
                    # cleanup failure (the node may be gone) must not replace the error either.
                    parked.cancel()
                    with suppress(asyncio.CancelledError):
                        await parked
                    with suppress(Exception):
                        await cleanup()
                    raise
                await cleanup()
                await parked

            # Warm the non-replica's cache with the current leader. This write may itself be
            # redirected, which is why the stale-cache deltas are taken only afterwards; it must
            # have been forwarded, or the deltas below would blame the stepdown for a cold cache.
            cold = await snapshot()
            await write(1)
            before = await snapshot()
            warmup = deltas(cold, before)
            assert warmup[FORWARDED_OK] == 1 and warmup[FORWARDED_FAILED] == 0, warmup

            await {"new_leader_known": stepdown_then_write,
                   "election_in_flight": write_parked_across_stepdown}[scenario]()

            # Stale cache: forwarded to the old leader, redirected once, then successful.
            after_stale = await snapshot()
            assert deltas(before, after_stale) == {
                REDIRECTED: 1, FORWARDED_OK: 1, FORWARDED_FAILED: 0, WRITE_NODE_BOUNCES: 1, OLD_LEADER_BOUNCES: 1}

            # Refreshed cache: forwarded straight to the new leader, no extra hop.
            await write(3)
            assert deltas(after_stale, await snapshot()) == {
                REDIRECTED: 0, FORWARDED_OK: 1, FORWARDED_FAILED: 0, WRITE_NODE_BOUNCES: 1, OLD_LEADER_BOUNCES: 0}

            for hid in (old_leader_id, new_leader_id):
                rows = await cql.run_async(select.bind([0]), host=by_host_id[hid][1])
                assert [row.l for row in rows] == [[1, 2, 3]], f"read via {hid}"
