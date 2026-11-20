#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A three-node strongly consistent cluster under the register workload, and the failures
the node-failure tests inflict on it.

Rig owns the cluster, the workload over its register table and the actions that break
it: nodes crashed and restarted, a leader cut off with a tail that the next leader
truncates, a group's stores held before a crash, a group's descriptor writes failing.
The scenario tests in test_strong_consistency_node_failures.py and the random sequences
of test_strong_consistency_persistence_nemesis.py take the same actions, so both exercise
one implementation.  A test picks the action and when to take it; Rig keeps
the cluster whole between two actions and tells the clients' errors apart: the errors of
a lost node are accepted only in an operation that overlaps a REBOOT window
(``Rig.disruption``), so anything failing outside one fails the test.

The checker says nothing about availability: a cluster that refused every request after
a kill would pass it.  What a test claims about that it reads from the history with
``RegisterWorkload.stats(within_ns=...)``, over a window of its own name, which excuses
nothing.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import struct
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Mapping, Optional, Sequence

from cassandra import ConsistencyLevel, OperationTimedOut, Unavailable
from cassandra.murmur3 import murmur3
from cassandra.pool import Host

from test.cluster.strong_consistency.config import boot_sc_cluster, sc_keyspace_opts
from test.cluster.strong_consistency.outcomes import FailureContext, Outcome, only_during, tolerate_reboot
from test.cluster.strong_consistency.register_workload import (
    DisruptionWindow, RegisterWorkload, Stats, check_linearizable, run_workload)
from test.cluster.test_strong_consistency import wait_for_leader
from test.cluster.util import commitlog_segment_payload, commitlog_segments, new_test_keyspace
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

NUM_NODES = 3

# Pause between two operations of one client.  Slower than the harness default on purpose:
# while a tablet has no leader every write fails at once, and each of those is an operation
# the checker keeps open against its per-key budget (see RegisterWorkload.record_failure).
DML_PAUSE_S = (0.05, 0.1)

# Every duration below is multiplied by the `slow` fixture.
WARMUP_S = 5  # the whole cluster, before the disruption
GAP_S = 5  # after a kill before the restart, and between two actions
HEALED_S = 5  # at least this long on the whole cluster again, after the disruption
# Allowed for starting nodes and waiting until they caught up.  Only an upper bound: the
# run stops HEALED_S after the disruption, so a faster restart ends it sooner, and one
# slower than allowed leaves too little of the healed part and fails the test saying so.
RESTART_S = 15
# A node waits for its strongly consistent raft groups to start for 30s before it goes on
# booting (main.cc, wait_for_groups_to_start).  When every node restarts at once, none of
# them gets its groups started before that timeout: each logs "Not all Raft groups started:
# timedout" after exactly 30s, and the groups start right after it.  So the restart of the
# whole cluster takes those 30s on top of RESTART_S.
GROUPS_START_TIMEOUT_S = 30
MIN_FLUSHES = 3

# The window in which the clients may see the errors of a lost node: from before the first
# node is stopped until every node is back and caught up.
REBOOT = "reboot"


def tolerate_reboot_here(exc: BaseException, ctx: FailureContext) -> Optional[Outcome]:
    """tolerate_reboot without the two errors that can only be bugs in these tests.

    Unavailable: every node is a replica, so no coordinator can lack one.
    OperationTimedOut: the driver gave up before the server answered, although the
    server's own request timeouts are shorter than the driver's.
    """
    if isinstance(exc, (Unavailable, OperationTimedOut)):
        return None
    return tolerate_reboot(exc, ctx)


@dataclass(frozen=True)
class CommitlogSetup:
    """How the commitlog that holds the raft log is set up, for the tests whose crashes replay it."""

    config: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    cmdline: tuple[str, ...] = ()
    # Flush a random live node this often, (min, max) seconds; None: never.
    flush_every_s: Optional[tuple[float, float]] = None


SMALL_SEGMENTS = {
    'commitlog_segment_size_in_mb': 1,
    # Over this the commitlog flushes memtables on its own, and the segments it frees are
    # recycled rather than deleted.
    'commitlog_total_space_in_mb': 8,
}

COMMITLOG_SETUPS = {
    # 64 MB segments: in a run of these tests only a restart closes a segment, and no group
    # releases one, so every restart replays every raft batch since the cluster started.
    "default": CommitlogSetup(),
    # 1 MB segments, and a node flushed every 1-2s.  A flush closes the active segment and
    # flushes every memtable, so the groups release their segments in flush rounds and the
    # commitlog deletes them while the nodes crash.  The replay after a crash then starts
    # from a descriptor the run moved.
    "small-segments": CommitlogSetup(config=SMALL_SEGMENTS, flush_every_s=(1, 2)),
    # The same with O_DSYNC, the production default that every test tier turns off.
    "o-dsync": CommitlogSetup(config=SMALL_SEGMENTS, cmdline=('--commitlog-use-o-dsync', '1'),
                              flush_every_s=(1, 2)),
}

# Over the small-segments setups, for a test that keeps segments from being released: at
# their 8 MB, 4 MB per shard, a shard whose group cannot release runs out of commitlog
# space within seconds and stalls every write on it -- what held segments do by design,
# not what is tested -- and the commitlog flushes memtables on its own, which releases
# segments a test wants kept.
ROOM_FOR_HELD_SEGMENTS = {'commitlog_total_space_in_mb': 64}

# --- a leader cut off with an uncommitted tail ------------------------------------------

# Allowed for raft to notice a dead leader, elect a successor and commit on it; about 4s
# on an idle machine.  Only an allowance, like RESTART_S.
ELECT_S = 10
# Writes the successor must commit before the old leader comes back.  Its first entry,
# the dummy of its term, already lands on the first index of the old leader's tail, so
# the tail conflicts; the writes make the conflict longer than one entry.
MIN_WRITES_ON_NEW_LEADER = 20
# Sent straight to the cut-off leader, at keys outside the workload's, so the checker
# never sees them.  Only that leader ever holds them, so nobody may ever read them back,
# on any replica, at any consistency.
DOOMED_WRITES = 3
DOOMED_PK_BASE = 1_000_000
DROP_APPENDS = "raft_drop_incoming_append_entries_for_specified_group"
# Attempts at cutting a group's leader off while its leadership keeps moving.
CUT_ATTEMPTS = 5
UNCOMMITTED_TAIL_CMDLINE = (
    '--logger-log-level', 'raft_group_registry=debug',  # "Dropping append request"
    '--logger-log-level', 'raft_commitlog=debug',  # "truncate_log: group_id="
    '--logger-log-level', 'raft_commitlog_replay=debug',  # what each replay dropped
    '--logger-log-level', 'raft_groups_storage=debug',  # "released record for group_id="
)
# A release that wrote the group's truncation history into its descriptor; a release that
# found it unchanged says "(unchanged, not rewritten)".
RELEASE_WITH_TRUNCATIONS = r"released record for group_id={}: .*truncations=[1-9]\d*$"
# The summary line raft_commitlog_replay_buffer::finish_replay() logs per group.
REPLAY_SUMMARY = r"group {}: floor=.*dropped_truncated=(\d+), dropped_by_truncate=\d+, superseded=(\d+)"

# --- error injections in the raft log's persistence -----------------------------------

PAUSE_AFTER_STORE = "raft_commitlog_pause_after_store"
FAIL_DESCRIPTOR_WRITE = "raft_groups_storage_fail_descriptor_write"
HELD_S = 3  # a victim's stores held before the kill
FAILING_S = 5  # every descriptor write of a group fails on the victim for this long


def total(stats: Stats) -> int:
    """Every operation the stats count, whatever became of it."""
    return sum(dataclasses.astuple(stats))


@dataclass
class TailRound:
    """What one truncate_uncommitted_tail() did."""

    leader: ServerInfo  # the leader cut off, whose tail was truncated
    successor: str  # the host id of the leader that overwrote the tail
    # What the replay after the old leader's second crash dropped: copies by a persisted
    # truncation, and copies superseded by newer ones.
    dropped_truncated: int
    superseded: int


@dataclass
class Rig:
    """A three-node SC cluster with a register table on it, and the workload over the table."""

    manager: ScyllaClusterManager
    cql: Any
    servers: list[ServerInfo]
    workload: RegisterWorkload
    group_ids: list[str]
    by_host_id: dict[str, ServerInfo]
    slow: int
    # SELECT of one key at CL=ONE: what a node holds locally.
    local_read: Any
    # The node logs, by server id.
    logs: dict[int, Any]
    # (last token, raft group id) of every tablet, by token: which group a key belongs to.
    tablets: list[tuple[int, str]]
    flush_every_s: Optional[tuple[float, float]] = None
    # The server ids of the nodes killed and not yet caught up again; the flusher leaves them alone.
    down: set[int] = dataclasses.field(default_factory=set)
    # The server ids a test keeps the flusher away from for a while.
    no_flush: set[int] = dataclasses.field(default_factory=set)
    # Every key a doomed write was sent to (truncate_uncommitted_tail).
    doomed_pks: list[int] = dataclasses.field(default_factory=list)
    kills: int = 0
    flushes: int = 0

    def disruption(self):
        """Mark the body as a time the cluster is broken: a REBOOT window.

        Open it before breaking anything and leave it only once the cluster is whole
        again -- not when the command that broke it returns: a node that was started is
        not back until it answers and has caught up.
        """
        return self.workload.disruption_window(REBOOT)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds * self.slow)

    async def leader(self, group_id: Optional[str] = None) -> ServerInfo:
        """The leader of `group_id`, or of the first tablet.  Every node must be up."""
        return self.by_host_id[await wait_for_leader(self.manager, self.servers[0], group_id or self.group_ids[0])]

    async def kill(self, *servers: ServerInfo) -> None:
        self.down.update(s.server_id for s in servers)
        self.kills += 1
        await asyncio.gather(*(self.manager.server_stop(s.server_id, convict=False) for s in servers))

    async def restart(self, *servers: ServerInfo) -> list[Host]:
        """Start `servers` again and wait until the cluster is whole; returns the Host of every server."""
        await asyncio.gather(*(self.manager.server_start(s.server_id) for s in servers))
        hosts = await self.wait_caught_up()
        self.down.difference_update(s.server_id for s in servers)
        return hosts

    async def crash(self, *servers: ServerInfo, down_s: float = 0) -> DisruptionWindow:
        """Kill `servers` at once, keep them down for `down_s`, restart them and wait until
        the cluster is whole; returns the REBOOT window of all that."""
        async with self.disruption() as window:
            await self.kill(*servers)
            await self.sleep(down_s)
            await self.restart(*servers)
        return window

    async def flusher(self) -> None:
        """Flush a random live node every `flush_every_s` until the run ends.

        Not a disruption: a flush must not fail a request, so it opens no window.  A flush
        that fails because its node was killed meanwhile is skipped; any other failure fails
        the test.
        """
        rng = self.workload.rng_for("flusher")
        while not self.workload.stop_event.is_set():
            await self.sleep(rng.uniform(*self.flush_every_s))
            live = [s for s in self.servers if s.server_id not in self.down | self.no_flush]
            if not live:
                continue
            server = rng.choice(live)
            kills = self.kills
            try:
                await self.manager.api.flush_all_keyspaces(server.ip_addr)
            except Exception as exc:
                if self.kills == kills and server.server_id not in self.down:
                    raise
                logger.info("Flush of %s cut short by a kill: %s", server.ip_addr, exc)
                continue
            self.flushes += 1

    async def wait_caught_up(self) -> list[Host]:
        """Wait until every node is fully back.  Returns the driver Host of each server.

        Three steps, in this order: the nodes see each other through gossip, they answer
        CQL, and each passes a read barrier of every group (its raft server is registered,
        the group has a leader, and it has applied everything committed so far).

        The gossip step is a workaround for SCYLLADB-4722: when a node learns through
        gossip that a peer restarted, it drops its connections to that peer, and a request
        in flight on them fails -- a forwarded write fails with WriteFailure.  A read
        barrier can pass before that, since raft already talks to the peer.
        """
        await self.manager.servers_see_each_other(self.servers)
        hosts = await wait_for_cql_and_get_hosts(self.cql, self.servers, time.time() + 60)
        for server in self.servers:
            for group_id in self.group_ids:
                async def barrier() -> bool:  # raises until the group's raft server is registered
                    await read_barrier(self.manager.api, server.ip_addr, group_id, timeout=60)
                    return True
                await wait_for(barrier, time.time() + 120, label=f"{server.ip_addr} to pass a read barrier of group {group_id}")
        return hosts

    async def run(self, disruption_s: float, disrupt: Callable[[], Awaitable[None]]) -> None:
        """Run the workload with `disrupt` started after WARMUP_S, and stop it HEALED_S after `disrupt` returns.

        WARMUP_S + `disruption_s` + HEALED_S is the longest the clients may run:
        `disruption_s` is the allowance of `disrupt`, and past it the clients stop
        whatever `disrupt` is doing, so the healed part comes up short.

        `disrupt` must return with the cluster whole again, and wrap every moment it is not in a
        disruption.  It may check the workload's stop event between its steps, never in the
        middle of one: run_workload waits for it to return, so a disruptor that brings back what
        it broke leaves the cluster whole for the rest of the test even if the timer ran out.

        Asserts that no task failed and that the healed part lasted at least HEALED_S: one that
        was cut short means `disruption_s` is too small for this machine, not that the cluster
        recovered.
        """
        healed_since = 0

        async def scenario() -> None:
            nonlocal healed_since
            await self.sleep(WARMUP_S)
            await disrupt()
            healed_since = time.monotonic_ns()
            if not self.workload.stop_event.is_set():
                await self.sleep(HEALED_S)
                self.workload.stop_event.set()

        duration_s = (WARMUP_S + disruption_s + HEALED_S) * self.slow
        disruptors = [("disruptor", scenario)]
        if self.flush_every_s:
            disruptors.append(("flusher", self.flusher))
        errors = await run_workload(self.workload, self.cql, duration_s, disruptors)
        ended = time.monotonic_ns()
        logger.info("Run done: %s | flushes=%d", self.workload.stats_line(), self.flushes)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={self.workload.seed}): {errors}"
        healed_s = (ended - healed_since) / 1e9 if healed_since else 0
        assert healed_s >= HEALED_S * self.slow, (
            f"the cluster was whole for only {healed_s:.1f}s at the end of the {duration_s}s run, "
            f"expected >= {HEALED_S * self.slow}s: the disruption took longer than the {disruption_s * self.slow}s allowed")
        if self.flush_every_s:
            # About one per 1.5s while the nodes are up; a few mean the flusher ran, not
            # that it ran often.  A run is short: the warmup, the disruption and HEALED_S.
            assert self.flushes >= MIN_FLUSHES, f"only {self.flushes} flushes in the {duration_s}s run"

    async def read_every_key(self) -> None:
        """Read every key once into the history, so that the checker sees the final value
        of every register and a lost last write cannot go unnoticed.  Outside every
        disruption: a failure here fails the test.
        """
        client_id = self.workload.reset_client_id + 1
        for pk in range(self.workload.num_keys):
            op_id, t_call_ns = self.workload.history.record_call(client_id, "read", pk)
            try:
                rows = await self.cql.run_async(self.workload.read_stmt.bind([pk]))
            except Exception as exc:
                self.workload.record_failure(exc, op_id=op_id, client_id=client_id, op="read", key=pk, value=0,
                                             t_call_ns=t_call_ns)
                continue
            value = 0 if not rows or rows[0].c is None else rows[0].c
            self.workload.history.record_return(op_id, client_id, "read", pk, value, Outcome.OK)

    async def check(self, tmp_path) -> None:
        """Read every key once more and run the checker over the whole history."""
        await self.read_every_key()
        await check_linearizable(self.workload, output_dir=tmp_path / "porcupine-checker-output")

    # --- a leader cut off with an uncommitted tail --------------------------------------

    def group_of(self, pk: int) -> str:
        """The raft group of the tablet that holds key `pk`."""
        token = murmur3(struct.pack('>i', pk))
        return next(group_id for last_token, group_id in self.tablets if token <= last_token)

    def doomed_keys(self, group_id: str) -> list[int]:
        """DOOMED_WRITES keys of `group_id`'s tablet that no doomed write took yet."""
        pks: list[int] = []
        pk = DOOMED_PK_BASE + len(self.doomed_pks) * 1000  # past every key an earlier round could take
        while len(pks) < DOOMED_WRITES:
            if self.group_of(pk) == group_id:
                pks.append(pk)
            pk += 1
        return pks

    async def cut_off(self, group_id: str) -> tuple[str, ServerInfo, list[ServerInfo], list[int]]:
        """Make the followers of `group_id`'s leader drop its appends of the group.

        Returns the leader's host id, the leader, its followers and a mark of each
        follower's log taken before its cut.  Every node must be up.  Leadership can
        move between reading it and the cut, more likely right after a restart, and
        the doomed writes would then commit through the new leader: such a cut is
        undone and made again around the leader the followers name.
        """
        for _ in range(CUT_ATTEMPTS):
            leader_id = await wait_for_leader(self.manager, self.servers[0], group_id)
            leader = self.by_host_id[leader_id]
            followers = [s for s in self.servers if s != leader]
            marks = [await self.logs[f.server_id].mark() for f in followers]
            try:
                for f in followers:
                    await self.manager.api.enable_injection(f.ip_addr, DROP_APPENDS, one_shot=False,
                                                            parameters={"value": group_id})
                seen = {str(await self.manager.api.get_raft_leader(f.ip_addr, group_id)) for f in followers}
            except BaseException:
                await self.uncut(followers)
                raise
            if seen == {leader_id}:
                return leader_id, leader, followers, marks
            logger.info("The leader of group %s moved from %s to %s before the cut; cutting again", group_id,
                        leader_id, seen)
            await self.uncut(followers)
        raise AssertionError(f"the leader of group {group_id} moved before each of {CUT_ATTEMPTS} cuts "
                             f"(seed={self.workload.seed})")

    async def uncut(self, followers: list[ServerInfo]) -> None:
        for f in followers:
            await self.manager.api.disable_injection(f.ip_addr, DROP_APPENDS)

    async def truncate_uncommitted_tail(self, group_id: str, persist: bool) -> TailRound:
        """Kill the leader of `group_id` holding entries nobody else has, under load, let the
        next leader overwrite them, and crash the old leader again.

        The leader is cut off: both followers drop its appends of the group, so whatever
        it appends from then on never commits.  The workload's writes and DOOMED_WRITES
        doomed writes sent straight to it go into its log and its commitlog; it is killed
        only once both followers dropped a non-empty append and every doomed key is in its
        commitlog's files.  Seeing the followers drop appends alone is not enough: a doomed
        write can wait for log memory while nothing commits, and never reach the log.  The
        followers cannot elect a successor while the leader lives, whatever they drop: a
        follower does not campaign while the failure detector sees its leader alive, and a
        leader counts a follower as active by the same detector.  So the leader is still
        the leader when the doomed writes reach it; a successor would commit them instead.

        The followers elect a successor, which commits MIN_WRITES_ON_NEW_LEADER writes at
        the same indexes in a newer term, and the old leader is restarted: its replay
        recovers the tail as uncommitted, and raft truncates it when the successor's
        entries arrive.  The old leader is then crashed again, so a replay meets both
        copies of those indexes, the superseded one still in its segment.

        With `persist` the old leader persists the truncation in the group's descriptor
        before that crash, and keeps the segment with the truncated copies on disk: the
        replay must drop copies by the persisted truncation.
        """
        pks = self.doomed_keys(group_id)
        value = -1 - len(self.doomed_pks) // DOOMED_WRITES  # negative: no workload write uses it
        self.doomed_pks.extend(pks)
        hosts = await wait_for_cql_and_get_hosts(self.cql, self.servers, time.time() + 60)
        async with self.disruption():
            leader_id, leader, followers, follower_marks = await self.cut_off(group_id)
            # A flush of the whole node would clean the segment that holds the tail.
            self.no_flush.add(leader.server_id)
            leader_log = self.logs[leader.server_id]
            before = await leader_log.mark()
            logger.info("Cut off the leader %s of group %s", leader.ip_addr, group_id)
            doomed: list[Awaitable] = []
            try:
                sent_at = time.time()
                doomed = [self.cql.run_async(self.workload.write_stmt.bind([value, pk]),
                                             host=hosts[self.servers.index(leader)])
                          for pk in pks]
                for f, mark in zip(followers, follower_marks):
                    await self.logs[f.server_id].wait_for(
                        rf"Dropping append request \(size: [1-9]\d*\) from .* for group {group_id}",
                        from_mark=mark, timeout=60 * self.slow)
                await wait_for_keys_in_commitlog(self.manager, leader, pks, since=sent_at - 1,
                                                 timeout=60 * self.slow)
                await self.kill(leader)
            finally:
                await self.uncut(followers)
            killed_ns = time.monotonic_ns()

            successor = await wait_for_new_leader(self.manager, followers, group_id, leader_id)

            async def committed_on_successor() -> Optional[int]:
                ok = self.workload.stats(within_ns=(killed_ns, time.monotonic_ns())).write_ok
                return ok if ok >= MIN_WRITES_ON_NEW_LEADER else None
            await wait_for(committed_on_successor, time.time() + 60 * self.slow,
                           label=f"{MIN_WRITES_ON_NEW_LEADER} writes committed by the successor {successor}")

            await self.restart(leader)
            # From before the cut: the old leader may have heard of the successor and
            # truncated before it was killed, which leaves the same copies on disk.
            truncated_at, _ = await leader_log.wait_for(rf"truncate_log: group_id={group_id}", from_mark=before,
                                                        timeout=60 * self.slow)
            results = await asyncio.gather(*doomed, return_exceptions=True)
            logger.info("The doomed writes ended with %s",
                        [type(r).__name__ if isinstance(r, BaseException) else "success" for r in results])

            if persist:
                # A flush of one table closes the active segment on every shard, so the
                # group's next batch lands in a new one and the group releases the
                # record before it: the first release after the truncation writes the
                # history into the descriptor.  The descriptor goes to the
                # system.raft_groups memtable only, and a crash before that memtable is
                # flushed loses it (the segment's pin keeps the entries instead), so
                # system.raft_groups is flushed once more.  The register table is never
                # flushed here, and with ROOM_FOR_HELD_SEGMENTS the commitlog stays far
                # below its flush threshold, so the segment stays dirty and on disk until
                # the crash.
                await self.manager.api.keyspace_flush(leader.ip_addr, "system", "raft_groups")
                await leader_log.wait_for(RELEASE_WITH_TRUNCATIONS.format(group_id), from_mark=truncated_at,
                                          timeout=60 * self.slow)
                await self.manager.api.keyspace_flush(leader.ip_addr, "system", "raft_groups")

            replay_mark = await leader_log.mark()
            await self.kill(leader)
            await self.restart(leader)
            self.no_flush.discard(leader.server_id)

        dropped = superseded = 0
        for _, match in await leader_log.grep(REPLAY_SUMMARY.format(group_id), from_mark=replay_mark):
            dropped += int(match.group(1))
            superseded += int(match.group(2))
        assert not persist or dropped > 0, (
            f"group {group_id}: the replay after a persisted truncation on {leader.ip_addr} dropped no copy by it "
            f"(seed={self.workload.seed})")
        logger.info("%s truncated the tail of group %s and replayed it: dropped_truncated=%d superseded=%d; "
                    "the successor was %s", leader.ip_addr, group_id, dropped, superseded, successor)
        return TailRound(leader=leader, successor=successor, dropped_truncated=dropped, superseded=superseded)

    # --- error injections in the raft log's persistence ---------------------------------

    async def disable_unless_down(self, server: ServerInfo, injection: str) -> None:
        """Turn `injection` off on `server` after an action failed half-way; a killed node lost it anyway."""
        if server.server_id not in self.down:
            await self.manager.api.disable_injection(server.ip_addr, injection)

    async def hold_stores_then_kill(self, group_id: str, victim: ServerInfo, held_s: float,
                                    down_s: float) -> DisruptionWindow:
        """Kill `victim` while its stores of `group_id` are on disk but not acknowledged.

        raft_commitlog_pause_after_store holds every store of the group on the victim
        once the entries are written to its commitlog, before raft's io fiber sends the
        batch's messages and reports commits: a follower does not answer the append, a
        leader sends its followers nothing, heartbeats included.  The victim is killed
        after `held_s` of that, so its commitlog holds entries raft never acknowledged,
        and restarted after `down_s`: replay brings them back as the tail of its log, and
        they must agree with what the group committed.  The injection dies with the
        process.  Returns the window the stores were held in.
        """
        log = self.logs[victim.server_id]
        async with self.disruption():
            mark = await log.mark()
            await self.manager.api.enable_injection(victim.ip_addr, PAUSE_AFTER_STORE, one_shot=False,
                                                    parameters={"group_id": group_id})
            try:
                await log.wait_for(f"{PAUSE_AFTER_STORE}: group_id={group_id} waits", from_mark=mark,
                                   timeout=60 * self.slow)
                async with self.workload.disruption_window("held") as held:
                    await self.sleep(held_s)
                await self.kill(victim)
            finally:
                await self.disable_unless_down(victim, PAUSE_AFTER_STORE)
            await self.sleep(down_s)
            await self.restart(victim)
        return held

    async def fail_descriptor_writes(self, group_id: str, victim: ServerInfo, failing_s: float, crash: bool) -> int:
        """Fail every descriptor write of `group_id` on `victim` for `failing_s`; then crash the
        victim if `crash`, or turn the failures off; either way its releases must succeed again.

        A release persists the group's descriptor before it lets go of a segment.  Under
        raft_groups_storage_fail_descriptor_write each of those writes on the victim fails,
        the way a write under memory pressure does: the record stays, the segment stays
        held, and the next commit, apply or flush round tries again.  A failing release
        must not be felt: this is no disruption, no request may fail meanwhile, and no
        release of the group may succeed on the victim.  A crashed victim replays from an
        older descriptor and meets segments its group meant to release.

        Needs the releases that small segments and flushes make, and room for the segments
        they keep (ROOM_FOR_HELD_SEGMENTS).  Returns how many failed writes the victim
        logged, a lower bound: the log line is rate-limited.
        """
        log = self.logs[victim.server_id]
        failed_line = rf"group_id={group_id}: writing the descriptor for segment \d+ failed"
        mark = await log.mark()
        await self.manager.api.enable_injection(victim.ip_addr, FAIL_DESCRIPTOR_WRITE, one_shot=False,
                                                parameters={"group_id": group_id})
        try:
            # Releases are synchronous, so none can succeed once the injection is on.
            failing_from = await log.mark()
            # Close the victim's segments now, so that the group tries a release within the
            # window: the flusher alone reaches this node only every few seconds.
            await self.manager.api.flush_all_keyspaces(victim.ip_addr)
            await self.sleep(failing_s)
            await log.wait_for(failed_line, from_mark=mark, timeout=60 * self.slow)
            failing_until = await log.mark()
            released = await log.grep(rf"released record for group_id={group_id}:", from_mark=failing_from)
            assert not released, (
                f"a release of group {group_id} succeeded on {victim.ip_addr} while writes failed: {released[0][0]} "
                f"(seed={self.workload.seed})")
            failed = len(await log.grep(failed_line, from_mark=mark))
            if crash:
                await self.crash(victim)
        finally:
            await self.disable_unless_down(victim, FAIL_DESCRIPTOR_WRITE)
        await self.manager.api.flush_all_keyspaces(victim.ip_addr)
        await log.wait_for(rf"released record for group_id={group_id}:", from_mark=failing_until,
                           timeout=60 * self.slow)
        logger.info("%s releases group %s again after %d failed descriptor writes", victim.ip_addr, group_id, failed)
        return failed


@asynccontextmanager
async def sc_rig(manager: ScyllaClusterManager, slow: int, *, initial_tablets: int = 1,
                 config: Optional[Mapping[str, Any]] = None, cmdline: Sequence[str] = (),
                 commitlog: CommitlogSetup = COMMITLOG_SETUPS["default"]) -> AsyncIterator[Rig]:
    """Boot the cluster and create the register table; every tablet has a leader when the block starts."""
    servers, cql = await boot_sc_cluster(manager, NUM_NODES, config={**commitlog.config, **(config or {})},
                                         cmdline=[*commitlog.cmdline, *cmdline])
    async with new_test_keyspace(manager, sc_keyspace_opts(replication_factor=NUM_NODES, initial_tablets=initial_tablets)) as ks:
        # prepare() turns the driver's retries off: one attempt per recorded operation, or
        # tolerate_reboot is wrong about what became of it.
        workload = RegisterWorkload(ks=ks, dml_pause_s=DML_PAUSE_S,
                                    exception_policy=only_during(REBOOT, tolerate_reboot_here))
        await workload.create_table(cql)
        local_read = cql.prepare(f"SELECT c FROM {workload.fqtn} WHERE pk = ?")
        local_read.consistency_level = ConsistencyLevel.ONE

        table_id = await manager.get_table_id(ks, workload.table_name)
        rows = await cql.run_async(f"SELECT last_token, raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        tablets = sorted((r.last_token, str(r.raft_group_id)) for r in rows)
        group_ids = [group_id for _, group_id in tablets]
        for group_id in group_ids:
            await wait_for_leader(manager, servers[0], group_id)
        by_host_id = {str(await manager.get_host_id(s.server_id)): s for s in servers}
        logs = {s.server_id: await manager.server_open_log(s.server_id) for s in servers}

        yield Rig(manager=manager, cql=cql, servers=servers, workload=workload, group_ids=group_ids,
                  by_host_id=by_host_id, slow=slow, local_read=local_read, logs=logs, tablets=tablets,
                  flush_every_s=commitlog.flush_every_s)


async def assert_local_reads_match(rig: Rig, server: ServerInfo, hosts: Optional[list[Host]] = None) -> None:
    """A CL=ONE (local) read on `server` answers what a linearizable read does, for every key.

    `hosts`: what rig.wait_caught_up() returned, if the caller waited already with
    nothing written since; its barrier makes `server` apply everything committed.
    """
    if hosts is None:
        hosts = await rig.wait_caught_up()
    host = hosts[rig.servers.index(server)]
    for pk in range(rig.workload.num_keys):
        linearizable = [r.c for r in await rig.cql.run_async(rig.workload.read_stmt.bind([pk]), host=host)]
        local = [r.c for r in await rig.cql.run_async(rig.local_read.bind([pk]), host=host)]
        assert local == linearizable, f"pk={pk}: {server.ip_addr} holds {local} locally, the group says {linearizable}"


async def assert_doomed_writes_absent(rig: Rig, hosts: list[Host]) -> None:
    """No replica holds a write sent into a truncated tail, locally or by a linearizable read.

    `hosts`: what rig.wait_caught_up() returned.
    """
    for server, host in zip(rig.servers, hosts):
        for pk in rig.doomed_pks:
            local = [r.c for r in await rig.cql.run_async(rig.local_read.bind([pk]), host=host)]
            assert not local, f"{server.ip_addr} holds the truncated write of pk={pk} locally: {local}"
            linearizable = [r.c for r in await rig.cql.run_async(rig.workload.read_stmt.bind([pk]), host=host)]
            assert not linearizable, f"a linearizable read on {server.ip_addr} returns the truncated write of pk={pk}: {linearizable}"


def register_key_bytes(pk: int) -> bytes:
    """The bytes by which key `pk` is found in the commitlog segments (the count and
    length of the key's components little-endian, the int itself big-endian)."""
    return struct.pack("<II", 1, 4) + struct.pack(">i", pk)


async def wait_for_keys_in_commitlog(manager: ScyllaClusterManager, server: ServerInfo, pks: Sequence[int],
                                     since: float, timeout: float) -> None:
    """Wait until a write of every key in `pks` is in `server`'s commitlog.

    Read from the segment files, so the entries were written there and a kill cannot
    lose them.  Only segments modified since `since` (a time.time()) are read.  An
    entry that never reaches raft's log -- held for memory while nothing commits --
    times this out rather than passing unseen.
    """
    workdir = await manager.server_get_workdir(server.server_id)
    patterns = {pk: register_key_bytes(pk) for pk in pks}

    def missing() -> set[int]:
        left = set(pks)
        for path in commitlog_segments(workdir):
            try:
                if os.path.getmtime(path) < since:
                    continue
            except FileNotFoundError:
                continue
            payload = commitlog_segment_payload(path)
            left = {pk for pk in left if patterns[pk] not in payload}
            if not left:
                break
        return left

    async def all_written() -> Optional[bool]:
        return True if not await asyncio.to_thread(missing) else None
    await wait_for(all_written, time.time() + timeout, period=0.2,
                   label=f"the writes of keys {list(pks)} in the commitlog of {server.ip_addr}")


async def wait_for_new_leader(manager: ScyllaClusterManager, servers: list[ServerInfo], group_id: str, old_leader: str) -> str:
    """Wait until every one of `servers` names the same leader of `group_id`, and it is not `old_leader`."""
    async def agreed() -> str | None:
        seen = {str(await manager.api.get_raft_leader(s.ip_addr, group_id)) for s in servers}
        leader = seen.pop() if len(seen) == 1 else None
        return leader if leader and leader != old_leader and uuid.UUID(leader).int != 0 else None
    return await wait_for(agreed, time.time() + 60, label=f"every replica of group {group_id} to know a leader other than {old_leader}")
