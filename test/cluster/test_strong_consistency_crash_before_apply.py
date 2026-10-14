#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A strongly consistent write is acknowledged once the entry is committed; applying it to
the table happens later, in the state machine's apply fiber, on every replica. Between the
two the entry lives only in the replica's Raft log, i.e. in its commitlog. These tests
pause apply() right there, SIGKILL the replica and check that the entry comes back.

On restart the commitlog replay applies every entry up to the persisted commit_idx straight
into the memtable, without Raft, and advances the group's snapshot to commit_idx. The
commit_idx itself is stored with a plain write to system.raft_groups, so whether that path
is taken depends on whether the write reached the disk before the kill. The tests decide
it instead of leaving it to timing: either flush system.raft_groups while apply() is
paused (the replay recovers the entry alone, rewritten=0), or skip the commit_idx write
with sc_skip_store_commit_idx (the entry returns to Raft as an uncommitted tail,
rewritten=1, and the leader has to confirm it before it becomes visible).
"""

import asyncio
import itertools
import re
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager, ServerInfo
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

WAIT_BEFORE_APPLY = "strong_consistency_state_machine_wait_before_apply"
DROP_APPEND_ENTRIES = "raft_drop_incoming_append_entries_for_specified_group"
SKIP_STORE_COMMIT_IDX = "sc_skip_store_commit_idx"
CMDLINE = DEFAULT_CMDLINE + ['--logger-log-level', 'raft_commitlog_replay=debug', '--logger-log-level', 'raft_group_registry=debug']
SC_KS_OPTS = "WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} AND tablets = {{'initial': 1}} AND consistency = 'global'"

pytestmark = pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')


async def wait_for_read_barrier(manager: ScyllaClusterManager, ip: str, group_id: str) -> None:
    """Wait until a read barrier for `group_id` succeeds on `ip`. It raises while the
    group's Raft server is not registered on the node yet (e.g. right after a restart)."""
    async def barrier() -> bool:
        await read_barrier(manager.api, ip, group_id, timeout=60)
        return True
    await wait_for(barrier, time.time() + 120, label=f"read barrier for group {group_id} on {ip}")


async def pause_apply_of_a_write(manager: ScyllaClusterManager, cql, table: str, target: ServerInfo, target_host, coordinator_host,
                                 commit_idx: str = "persisted") -> SimpleStatement:
    """Write pk=0 through `coordinator_host` with apply() paused on `target` and wait for the
    acknowledgement. commit_idx is written to system.raft_groups before the entry is handed to
    the applier, but through the periodic commitlog, so whether it survives a SIGKILL is up to
    timing. The test decides instead: "persisted" flushes it, "lost" skips the write altogether.
    Returns a CL=ONE read of the row for later checks."""
    if commit_idx == "lost":
        # The warm-up's commit_idx is made durable first, so that only our entry is affected.
        await manager.api.keyspace_flush(target.ip_addr, "system", "raft_groups")
        await manager.api.enable_injection(target.ip_addr, SKIP_STORE_COMMIT_IDX, one_shot=False)
    await manager.api.enable_injection(target.ip_addr, WAIT_BEFORE_APPLY, one_shot=True)
    write = cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 1)", host=coordinator_host)
    # Entered apply() means the entry is committed on the target and its apply is paused.
    await manager.api.wait_for_injection_enter(target.ip_addr, WAIT_BEFORE_APPLY)
    local_read = SimpleStatement(f"SELECT c FROM {table} WHERE pk = 0", consistency_level=ConsistencyLevel.ONE)
    assert await cql.run_async(local_read, host=target_host) == [], "the row is already visible: apply() was not paused before the store"
    # The coordinator waits for commit only, so the client is acknowledged while apply is still paused.
    await asyncio.wait_for(write, 5)
    if commit_idx == "persisted":
        await manager.api.keyspace_flush(target.ip_addr, "system", "raft_groups")
    return local_read


async def crash_and_restart(manager: ScyllaClusterManager, target: ServerInfo, group_id: str, startup_injections: list | None = None) -> tuple[int, int]:
    """SIGKILL `target`, restart it and return the replay's (applied, rewritten) counters for
    the group: entries applied straight into the memtable, and entries handed back to Raft."""
    await manager.server_stop(target.server_id, convict=False)
    if startup_injections is not None:
        await manager.server_update_config(target.server_id, 'error_injections_at_startup', startup_injections)
    log = await manager.server_open_log(target.server_id)
    mark = await log.mark()
    await manager.server_start(target.server_id)
    replay_line = rf"group {group_id}: discarded_leader_change=\d+, applied=(\d+), rewritten=(\d+)"
    _, [(_, match)] = await log.wait_for(replay_line, from_mark=mark, timeout=120)
    return int(match.group(1)), int(match.group(2))


@pytest.mark.parametrize("commit_idx", ["persisted", "lost"])
async def test_follower_recovers_committed_entry_from_own_commitlog(manager: ScyllaClusterManager, commit_idx: str):
    """A follower is SIGKILLed between commit and apply, and restarted while every
    AppendEntries for the group is dropped on it. Its peers hold the entry too, but they
    cannot deliver it, so whatever the isolated follower has came from its own commitlog.

    commit_idx="persisted": the replay applies the entry itself (rewritten=0), and a CL=ONE
    read on the isolated follower already returns the row.
    commit_idx="lost": the replay hands the entry back to Raft as an uncommitted tail
    (rewritten=1); it is kept but not visible until the leader tells the follower it is
    committed, so the CL=ONE read is empty while the drop is active. The leader has the
    row, so this also shows the CL=ONE read is served locally and not forwarded.

    Then the drop is lifted and the follower has to rejoin the group's history without a
    snapshot (which SC does not implement): a read barrier on it completes, the row is
    visible and it receives a new write from the leader."""
    leader = await manager.server_add(config=DEFAULT_CONFIG, cmdline=CMDLINE, property_file={'dc': 'dc1', 'rack': 'r1'})
    followers = await manager.servers_add(2, config=DEFAULT_CONFIG | {'error_injections_at_startup': ['avoid_being_raft_leader']},
                                          cmdline=CMDLINE, property_file=[{'dc': 'dc1', 'rack': 'r2'}, {'dc': 'dc1', 'rack': 'r3'}])
    servers = [leader, *followers]
    target = followers[0]
    cql, hosts = await manager.get_ready_cql(servers)
    leader_host, target_host = hosts[0], hosts[1]

    async with new_test_keyspace(manager, SC_KS_OPTS.format(rf=3)) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            await wait_for_leader(manager, leader, group_id, expected_host_id=await manager.get_host_id(leader.server_id))

            # Warm-up: a committed and applied write, so that the next apply() on the target is ours.
            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (-1, -1)", host=leader_host)
            for s in servers:
                await read_barrier(manager.api, s.ip_addr, group_id, timeout=60)

            local_read = await pause_apply_of_a_write(manager, cql, table, target, target_host, leader_host, commit_idx)

            target_log = await manager.server_open_log(target.server_id)
            isolated = ['avoid_being_raft_leader', {'name': DROP_APPEND_ENTRIES, 'value': group_id}]
            applied, rewritten = await crash_and_restart(manager, target, group_id, isolated)
            assert rewritten == (0 if commit_idx == "persisted" else 1), f"unexpected recovery path: applied={applied}, rewritten={rewritten}"
            mark = await target_log.mark()
            hosts = await wait_for_cql_and_get_hosts(cql, [target], time.time() + 120)
            target_host = hosts[0]
            # The isolation premise: the target hears from the leader and throws it away.
            await target_log.wait_for(rf"Dropping append request .* for group {group_id}", from_mark=mark, timeout=60)
            isolated_read = [r.c for r in await cql.run_async(local_read, host=target_host)]
            if commit_idx == "persisted":
                assert isolated_read == [1], "the committed write is gone: nothing but the target's own commitlog could have brought it back"
            else:
                assert isolated_read == [], "an entry nobody has confirmed as committed must not be visible"

            # Back in the group without a snapshot transfer: the leader's next append matches the target's
            # log tail (persisted: its snapshot index; lost: the uncommitted entry itself).
            await manager.api.disable_injection(target.ip_addr, DROP_APPEND_ENTRIES)
            await wait_for_read_barrier(manager, target.ip_addr, group_id)
            assert [r.c for r in await cql.run_async(local_read, host=target_host)] == [1]
            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (1, 2)", host=leader_host)
            await read_barrier(manager.api, target.ip_addr, group_id, timeout=60)
            follow_up = SimpleStatement(f"SELECT c FROM {table} WHERE pk = 1", consistency_level=ConsistencyLevel.ONE)
            assert [r.c for r in await cql.run_async(follow_up, host=target_host)] == [2]


# Segment ids carry the shard in their top bits (db::replay_position: 10 cpu bits out of 64).
SEGMENT_ID = re.compile(r"CommitLog-\d+-(\d+)(?:\.[a-zA-Z]+)?\.log$")  # e.g. CommitLog-4-4071603.variant.log
SEGMENT_SHARD_SHIFT = 64 - 10


async def newest_segment_per_shard(manager: ScyllaClusterManager, ip: str) -> dict[int, int]:
    """The id of the segment each shard is currently writing to. The API lists every segment
    that still holds unflushed data; the one being written to is always among them."""
    newest: dict[int, int] = {}
    for name in await manager.api.client.get_json("/commitlog/segments/active", host=ip):
        if m := SEGMENT_ID.search(name):
            segment_id = int(m.group(1))
            shard = segment_id >> SEGMENT_SHARD_SHIFT
            newest[shard] = max(segment_id, newest.get(shard, 0))
    assert newest, "no commitlog segment names recognized"
    return newest


async def rotate_commitlog_segments(manager: ScyllaClusterManager, server: ServerInfo, cql, table: str) -> None:
    """Write to `table` (spread over all shards) until every shard writes to a new segment."""
    before = await newest_segment_per_shard(manager, server.ip_addr)
    padding = 'x' * 65536
    pks = itertools.count()

    async def rotated() -> bool | None:
        for _ in range(8):
            await cql.run_async(f"INSERT INTO {table} (pk, padding) VALUES ({next(pks)}, '{padding}')")
        after = await newest_segment_per_shard(manager, server.ip_addr)
        return all(after.get(shard, 0) > newest for shard, newest in before.items()) or None
    await wait_for(rotated, time.time() + 120, label="commitlog segment rotation")


async def test_paused_entry_keeps_its_commitlog_segment(manager: ScyllaClusterManager):
    """While apply() is paused the entry's replay-position handle is held by the group's
    raft_commitlog alone, not by any memtable. A flush must not let the commitlog reclaim
    that segment. The test puts the warm-up write and the paused entry into a segment of
    their own (the group's configuration and dummy entries keep their handles forever and
    would pin a shared segment), rotates past it, flushes everything, SIGKILLs the node and
    checks that the entry is replayed."""
    server = await manager.server_add(config=DEFAULT_CONFIG | {'commitlog_segment_size_in_mb': 1, 'commitlog_total_space_in_mb': 10000}, cmdline=CMDLINE)
    cql, hosts = await manager.get_ready_cql([server])
    host = hosts[0]

    filler_ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 16}"
    async with new_test_keyspace(manager, filler_ks_opts) as filler_ks:
        async with new_test_table(manager, filler_ks, "pk int PRIMARY KEY, padding text") as filler:
            async with new_test_keyspace(manager, SC_KS_OPTS.format(rf=1)) as ks:
                async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
                    group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
                    await wait_for_leader(manager, server, group_id)
                    # The leader's dummy entry keeps its handle forever, like the configuration, so it must be in the
                    # commitlog before the rotation. Leadership is reported before the entry is stored; a committed
                    # entry is a stored one.
                    await read_barrier(manager.api, server.ip_addr, group_id, timeout=60)

                    # A fresh segment for the warm-up and the paused entry, after the group's non-command entries.
                    await rotate_commitlog_segments(manager, server, cql, filler)
                    await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (-1, -1)", host=host)
                    await read_barrier(manager.api, server.ip_addr, group_id, timeout=60)

                    local_read = await pause_apply_of_a_write(manager, cql, table, server, host, host)

                    # Now the paused entry's handle is the only thing keeping its segment alive: the warm-up's
                    # handle is released by the flush of the SC table, and nothing else is written to it anymore.
                    await rotate_commitlog_segments(manager, server, cql, filler)
                    await manager.api.flush_all_keyspaces(server.ip_addr)

                    applied, rewritten = await crash_and_restart(manager, server, group_id)
                    assert rewritten == 0, f"unexpected recovery path: applied={applied}, rewritten={rewritten}"
                    host = (await wait_for_cql_and_get_hosts(cql, [server], time.time() + 120))[0]
                    await wait_for_read_barrier(manager, server.ip_addr, group_id)
                    assert [r.c for r in await cql.run_async(local_read, host=host)] == [1], \
                        "the committed write is gone: its commitlog segment did not survive the flush"
                    assert [r.c for r in await cql.run_async(f"SELECT c FROM {table} WHERE pk = -1", host=host)] == [-1]


@pytest.mark.parametrize("commit_idx", [
    pytest.param("persisted", marks=pytest.mark.skip_bug(
        link="https://scylladb.atlassian.net/browse/SCYLLADB-2565",
        reason="SC has no snapshot transfer: a restarted replica cannot catch up a follower "
               "that lacks an entry the replay folded into the snapshot")),
    "lost",
])
async def test_restarted_replica_catches_up_a_lagging_follower(manager: ScyllaClusterManager, commit_idx: str):
    """The crashed replica is the only one that can bring the entry back and the group
    has a follower that never received it. Once restarted, the replica has to become the
    leader and hand the entry to the lagging follower.

    commit_idx="lost": the replay hands the entry back to Raft as an uncommitted tail, so
    the replica's log ends with it and its snapshot stays below it. It wins the election
    with the longer log, the lagging follower's log tail matches its snapshot index, so
    AppendEntries carries the entry, and the new term's first entry commits it: the only
    surviving copy of an acknowledged write is committed again.
    commit_idx="persisted" (skip_bug SCYLLADB-2565): the replay has folded the entry into
    the replica's snapshot, so it cannot send it by AppendEntries - it needs a snapshot
    transfer, which SC does not implement. The group has no quorum: the lagging follower
    cannot catch up and the old leader is down, so the read barrier never completes."""
    leader = await manager.server_add(config=DEFAULT_CONFIG, cmdline=CMDLINE, property_file={'dc': 'dc1', 'rack': 'r1'})
    followers = await manager.servers_add(2, config=DEFAULT_CONFIG | {'error_injections_at_startup': ['avoid_being_raft_leader']},
                                          cmdline=CMDLINE, property_file=[{'dc': 'dc1', 'rack': 'r2'}, {'dc': 'dc1', 'rack': 'r3'}])
    servers = [leader, *followers]
    target, other = followers
    cql, hosts = await manager.get_ready_cql(servers)
    leader_host, target_host = hosts[0], hosts[1]

    async with new_test_keyspace(manager, SC_KS_OPTS.format(rf=3)) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split('.')[-1])
            await wait_for_leader(manager, leader, group_id, expected_host_id=await manager.get_host_id(leader.server_id))

            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (-1, -1)", host=leader_host)
            for s in servers:
                await read_barrier(manager.api, s.ip_addr, group_id, timeout=60)

            # The second follower never gets the entry, so it commits with the leader and the target alone.
            other_log = await manager.server_open_log(other.server_id)
            other_mark = await other_log.mark()
            await manager.api.enable_injection(other.ip_addr, DROP_APPEND_ENTRIES, one_shot=False, parameters={'value': group_id})
            local_read = await pause_apply_of_a_write(manager, cql, table, target, target_host, leader_host, commit_idx)
            await other_log.wait_for(rf"Dropping append request \(size: [1-9]\d*\) .* for group {group_id}", from_mark=other_mark, timeout=60)

            await manager.server_stop(leader.server_id, convict=False)
            await manager.api.disable_injection(other.ip_addr, DROP_APPEND_ENTRIES)
            # Only the target can bring the entry back, so it must be allowed to lead.
            applied, rewritten = await crash_and_restart(manager, target, group_id, startup_injections=[])
            assert rewritten == (0 if commit_idx == "persisted" else 1), f"unexpected recovery path: applied={applied}, rewritten={rewritten}"
            hosts = await wait_for_cql_and_get_hosts(cql, [target, other], time.time() + 120)
            # The election succeeds either way; with commit_idx persisted it is the barrier that never completes.
            await wait_for_leader(manager, target, group_id, expected_host_id=await manager.get_host_id(target.server_id))
            await wait_for_read_barrier(manager, target.ip_addr, group_id)
            # lost: the entry reaches the lagging follower by AppendEntries. persisted: only through the (missing) snapshot transfer.
            await wait_for_read_barrier(manager, other.ip_addr, group_id)
            for h in hosts:
                assert [r.c for r in await cql.run_async(local_read, host=h)] == [1]

            await manager.server_start(leader.server_id)
            await wait_for_cql_and_get_hosts(cql, servers, time.time() + 120)
            await wait_for_read_barrier(manager, leader.ip_addr, group_id)
