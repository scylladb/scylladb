#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Stress tests: followers of a strongly consistent tablet fill their raft log
again and again under the register workload, with linearizability checking
via Porcupine.

A follower bounds its in-memory raft log (max_log_size + snapshot_trailing_size).
Once the next entry from the leader does not fit, it refuses it and reports a
full log, its leader stops replicating to it, and a snapshot taken by the
follower's applier makes room again.  Unit tests cover that in raft; these
tests make a strongly consistent table go through it repeatedly, while clients
read and write.

In the first two the disruption is a stalled state machine: apply() waits on
an error injection on the chosen followers until the test releases it.

  - One stalled follower: the leader commits with the other one, so clients
    must not notice anything.  No error is tolerated.
  - Both followers stalled: they go on appending, so the leader commits until
    both logs are full.  Then it cannot, so writes wait (and time out) until
    apply() is released, and then they have to go through.  The writes that
    timed out have an unknown outcome, and the history is checked with them.

In both, the log of a stalled follower fills with committed entries it has
not applied, and applying them takes it over snapshot_threshold_log_size, so
the snapshot which makes room is one the thresholds call for anyway.  The
other two tests get a follower to the cases where they do not, and the
follower has to take the snapshot because its log is full (SCYLLADB-5634):

  - Below the threshold: the log of a stalled follower fills up before it
    reaches the threshold, because the entry it refuses is larger than the
    gap between the two.  The followers get a threshold close to their bound,
    and large writes go to the table besides the registers.
  - After a re-election: everything committed is applied, and the log is full
    of entries from an earlier term, which the leader cannot commit without
    the follower accepting an entry of its own term above them.  The follower
    does not answer the leader, which keeps appending, until its log is full;
    then the leader is re-elected in a new term, while it needs this follower
    for a quorum.

The logs are shrunk with sc_raft_max_log_size, so that a few thousand writes
fill a follower instead of ~21MB of them.

Strongly consistent tables have no snapshot transfer yet.  A follower which
stopped taking entries is lost for good once its leader truncates its log past
the follower's tail, i.e. after the leader writes about snapshot_trailing_size
more.  The tests release a follower as soon as the leader has seen its full
log, so that this does not happen; if it does, they fail saying so rather than
timing out.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncIterator, Callable, Sequence

import pytest
from cassandra.cluster import NoHostAvailable
from cassandra.connection import ConnectionException
from cassandra.protocol import ServerError
from cassandra.query import PreparedStatement

from test.cluster.strong_consistency.config import boot_sc_cluster, sc_keyspace_opts
from test.cluster.strong_consistency.outcomes import (
    NEVER_APPLIED,
    WRITE_MAY_HAVE_APPLIED,
    only_during,
    strict,
    tolerate_reboot,
    tolerate_unavailability,
)
from test.cluster.strong_consistency.register_workload import (
    RegisterWorkload,
    check_linearizable,
    run_workload,
)
from test.cluster.test_strong_consistency import get_table_raft_group_id, wait_for_leader
from test.cluster.util import new_test_keyspace
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

LOG_SIZE_INJECTION = "sc_raft_max_log_size"
STALL_APPLY_INJECTION = "strong_consistency_state_machine_wait_before_apply"
AVOID_LEADERSHIP_INJECTION = "avoid_being_raft_leader"
DROP_REPLIES_INJECTION = "sc_drop_append_entries_reply"


@dataclass
class LogLimits:
    """The raft log limits sc_raft_max_log_size sets on a node."""

    max_log_size: int
    snapshot_threshold_log_size: int
    snapshot_trailing_size: int

    @classmethod
    def of(cls, max_log_size: int) -> LogLimits:
        """The threshold and the trailing part the injection derives itself."""
        return cls(max_log_size, max_log_size // 4 * 3, max_log_size // 2)

    @property
    def max_follower_log_size(self) -> int:
        """fsm_config::max_follower_log_size, see raft::server_impl::start()."""
        return self.max_log_size + self.snapshot_trailing_size

    @property
    def parameters(self) -> dict[str, str]:
        return {
            "value": str(self.max_log_size),
            "snapshot_threshold_log_size": str(self.snapshot_threshold_log_size),
            "snapshot_trailing_size": str(self.snapshot_trailing_size),
        }


# Every node of the stalled-follower tests.  At least twice max_command_size
# (100K), or raft::server refuses the configuration.
DEFAULT_LIMITS = LogLimits.of(256 * 1024)
# The leader in the other two: a follower stays reachable while it writes
# 10MB more, and it never runs out of room for the large writes.
LEADER_LIMITS = LogLimits.of(20 * 1024 * 1024)
# The bound is 257K, so a follower refuses a ballast entry anywhere between
# 257K - BALLAST_SIZE and the threshold at 256K.  It refuses at or above the
# threshold only if the last ballast entry it took landed its log in the 1K
# between the two, about once in 90 cycles; a register write is too small to
# be refused below the threshold.  Not limits for a node which may lead: a
# leader needs max_log_size - snapshot_threshold_log_size of at least
# max_command_size, see the injection.
BELOW_THRESHOLD_LIMITS = LogLimits(
    max_log_size=256 * 1024, snapshot_threshold_log_size=256 * 1024, snapshot_trailing_size=1024)
# The bound is 128K above the threshold, more than any entry, so that only the
# full log can call for the snapshot.  The small trailing part leaves the log
# far below the threshold after a snapshot.
REELECTION_LIMITS = LogLimits(
    max_log_size=512 * 1024, snapshot_threshold_log_size=400 * 1024, snapshot_trailing_size=16 * 1024)

METRIC_PREFIX = "scylla_strong_consistency_raft_"

# The default is 30s, longer than a stall cycle should be.
WRITE_TIMEOUT_S = 3
# How long both followers stay full before apply() is released, in write
# timeouts: the writes stuck on the leader time out, with an unknown outcome,
# and may still be applied once the followers take entries again.
BOTH_FULL_HOLD_WRITE_TIMEOUTS = 2

# Long enough for at least two stall cycles in debug mode.  A cycle with one
# follower stalled takes a few seconds; one with both is dominated by the hold.
ONE_FOLLOWER_DURATION_S = 30
BOTH_FOLLOWERS_DURATION_S = 60
# A re-election cycle waits for the failure detector, and an election timeout.
REELECTION_DURATION_S = 60

# One ballast write is one raft entry of a bit more than this.
BALLAST_SIZE = 90 * 1024
BALLAST = bytes(BALLAST_SIZE)
# About 1.5MB/s in all: a follower full below the threshold is lost after
# several seconds, rather than within the round trip of releasing it.
BALLAST_WRITERS = 8
BALLAST_PAUSE_S = (0.2, 0.6)
# More than the bound of a follower with REELECTION_LIMITS takes.
REELECTION_BURST = 8
# The ballast writer of the writes outside a burst: its key is none of the
# burst's, which are writers 0..REELECTION_BURST-1.
LONE_WRITER = REELECTION_BURST
# The leader sends a follower which does not answer one request per raft tick,
# 100ms.
LACKING_FOR_S = 1

# The window during which both followers are stalled; the only one in which a
# client may see an error.
STALL_BOTH = "stall-both"
STALL_ONE = "stall-one"
# Two nodes are paused and the leader steps down.
REELECTION = "reelection"


@dataclass
class RaftState:
    """What one node's metrics say about the raft group of the table."""

    leaders: float
    log_memory_usage: float
    full_logs: float
    uncommitted_entries: float
    unapplied_entries: float
    # Followers this node, as the leader, does not send entries to, of those
    # its failure detector sees alive: a paused one is not counted.
    blocked_probe: float
    blocked_log_full: float
    blocked_snapshot: float


class FollowerLostError(AssertionError):
    pass


class PollTimeout(AssertionError):
    pass


@dataclass
class RaftGroup:
    """The one tablet raft group of the table; `leader` is pinned by
    avoid_being_raft_leader on the followers."""

    manager: ScyllaClusterManager
    ks: str
    table: str
    leader: ServerInfo
    followers: list[ServerInfo]
    leader_limits: LogLimits

    async def state(self, server: ServerInfo) -> RaftState:
        # Polled every few tens of milliseconds, on nodes under load: the node
        # renders and sends only these, rather than the full dump each time.
        labels = {"ks": self.ks, "cf": self.table}
        metrics = await self.manager.metrics.query(server.ip_addr, names=[METRIC_PREFIX + "*"], labels=labels)

        def gauge(name: str, **extra) -> float:
            value = metrics.get(METRIC_PREFIX + name, labels | extra)
            assert value is not None, (
                f"no {METRIC_PREFIX}{name} for {labels | extra} on {server.ip_addr}")
            return value

        return RaftState(
            leaders=gauge("leaders"),
            log_memory_usage=gauge("log_memory_usage"),
            full_logs=gauge("full_logs"),
            uncommitted_entries=gauge("uncommitted_entries"),
            unapplied_entries=gauge("unapplied_entries"),
            blocked_probe=gauge("blocked_followers", reason="probe"),
            blocked_log_full=gauge("blocked_followers", reason="log_full"),
            blocked_snapshot=gauge("blocked_followers", reason="snapshot"),
        )

    def check_not_lost(self, leader: RaftState, label: str) -> None:
        # A follower the leader has to send a snapshot to never comes back while
        # strongly consistent tables have no snapshot transfer.
        # TODO: once they have it, wait for the transfer to finish here instead
        # of failing, and add a variant which stalls a follower past the
        # trailing window on purpose, to test that it catches up from a snapshot.
        if leader.blocked_snapshot:
            raise FollowerLostError(
                f"the leader {self.leader.ip_addr} needs to send a snapshot to a follower "
                f"({label}): the follower fell behind the leader's log, and strongly "
                f"consistent tables have no snapshot transfer, so it is lost for good. "
                f"The follower took no entries while the leader wrote over "
                f"{self.leader_limits.snapshot_trailing_size} bytes past its tail: {leader}")

    async def stall(self, server: ServerInfo) -> None:
        await self.manager.api.enable_injection(server.ip_addr, STALL_APPLY_INJECTION, one_shot=False)

    async def resume(self, server: ServerInfo) -> None:
        # Disabling releases every apply() waiting on the injection.
        # Disabling one which is not enabled is a no-op.
        await self.manager.api.disable_injection(server.ip_addr, STALL_APPLY_INJECTION)


@dataclass
class PinnedCluster:
    """Three nodes, of which `leader` is the only one which may lead, and a
    keyspace with one tablet replicated on all three: one raft group, and
    every node is a member.  The injections are on before any table exists,
    so that the group never elects a follower and starts with each node's
    limits."""

    manager: ScyllaClusterManager
    cql: Any
    ks: str
    leader: ServerInfo
    followers: list[ServerInfo]
    leader_limits: LogLimits
    write_timeout_s: float

    async def raft_group(self, workload: RegisterWorkload, timeout: float) -> RaftGroup:
        """Wait for the raft group of the table, created by now, to elect `leader`."""
        group_id = await get_table_raft_group_id(self.manager, workload.ks, workload.table_name)
        await wait_for_leader(self.manager, self.leader, group_id,
                              expected_host_id=await self.manager.get_host_id(self.leader.server_id),
                              timeout=timeout)
        return RaftGroup(self.manager, workload.ks, workload.table_name,
                         self.leader, self.followers, self.leader_limits)


@contextlib.asynccontextmanager
async def pinned_cluster(manager: ScyllaClusterManager, scale_timeout: Callable[[float], float],
                         leader_limits: LogLimits, follower_limits: LogLimits,
                         ) -> AsyncIterator[PinnedCluster]:
    write_timeout_s = scale_timeout(WRITE_TIMEOUT_S)
    servers, cql = await boot_sc_cluster(manager, num_nodes=3, config={
        "write_request_timeout_in_ms": int(write_timeout_s * 1000),
    })
    leader, followers = servers[0], servers[1:]
    await pin_leader(manager, leader, followers, leader_limits, follower_limits)
    async with new_test_keyspace(manager, sc_keyspace_opts(replication_factor=3, initial_tablets=1)) as ks:
        yield PinnedCluster(manager, cql, ks, leader, followers, leader_limits, write_timeout_s)


async def check_run(workload: RegisterWorkload, group: RaftGroup,
                    task_errors: list[BaseException]) -> None:
    """What every test checks once the workload stopped, before Porcupine."""
    assert not task_errors, (
        f"Task(s) failed with unexpected exceptions (seed={workload.seed}): "
        f"{task_errors}"
    )

    # A follower lost in the last cycle, or one that stopped early, has not
    # been through poll() since.
    group.check_not_lost(await group.state(group.leader), "at the end of the run")

    workload.assert_progress(min_writes=100, min_reads=100)


async def pin_leader(manager: ScyllaClusterManager, leader: ServerInfo, followers: list[ServerInfo],
                     leader_limits: LogLimits, follower_limits: LogLimits) -> None:
    """Make `leader` the only node which may lead, and give each node its log
    limits.  Before the table is created: the group reads the limits when it
    starts, and its first election, a fast bootstrap one included, must not
    pick a follower."""
    await manager.api.enable_injection(
        leader.ip_addr, LOG_SIZE_INJECTION, one_shot=False, parameters=leader_limits.parameters)
    for follower in followers:
        await manager.api.enable_injection(
            follower.ip_addr, LOG_SIZE_INJECTION, one_shot=False, parameters=follower_limits.parameters)
        await manager.api.enable_injection(follower.ip_addr, AVOID_LEADERSHIP_INJECTION, one_shot=False)


# Takes the state of the leader, then of each victim, in that order.
StateCheck = Callable[[RaftState, list[RaftState]], bool]


async def poll(workload: RegisterWorkload, group: RaftGroup, victims: list[ServerInfo],
               check: StateCheck, timeout: float, label: str, period: float = 0.05) -> bool:
    """Poll the leader and `victims` until `check` holds.  Returns False if the
    workload stopped first.

    The leader is checked for a lost follower on every round, before `check`:
    a lost follower is not blocked on log_full any more, so a check that waits
    for that to clear would otherwise take it for a recovered one.

    Not test.pylib.util.wait_for: that one retries on exceptions, and a lost
    follower has to fail the test at once, with its own message.
    """
    deadline = time.monotonic() + timeout
    while True:
        # At once: every scrape of a round eats into the time a full follower
        # has before its leader truncates the log past its tail.
        leader, *stalled = await asyncio.gather(
            group.state(group.leader), *(group.state(v) for v in victims))
        group.check_not_lost(leader, f"while waiting for {label}")
        if check(leader, stalled):
            return True
        if workload.stop_event.is_set():
            return False
        if time.monotonic() >= deadline:
            raise PollTimeout(f"timed out after {timeout}s waiting for {label}")
        await asyncio.sleep(period)


async def wait_for_write(workload: RegisterWorkload, group: RaftGroup, since_ns: int,
                         timeout: float, label: str) -> bool:
    """Wait for a register write to succeed after `since_ns`: recovered as raft
    sees it is not enough."""
    # stats() walks the whole history, hence the longer period.
    def wrote(leader: RaftState, stalled: list[RaftState]) -> bool:
        return workload.stats(within_ns=(since_ns, time.monotonic_ns())).write_ok > 0
    return await poll(workload, group, [], wrote, timeout, label, period=0.5)


@dataclass
class Ballast:
    """Large writes to the table of the registers, on keys the workload does
    not use: the same raft group takes them, and the history does not."""

    workload: RegisterWorkload
    cql: Any
    stmt: PreparedStatement
    # Sent there rather than to a replica the driver picks, which may be a
    # paused one.
    host: Any
    written: int = 0
    failed: int = 0

    # Errors a write which did not get through can end with, a leader which
    # steps down included; whether it did does not matter.
    ERRORS = (*NEVER_APPLIED, *WRITE_MAY_HAVE_APPLIED, NoHostAvailable, ConnectionException, ServerError)

    @classmethod
    async def create_table(cls, workload: RegisterWorkload, cql, leader: ServerInfo) -> Ballast:
        """Create the table of the registers, with a column for the ballast."""
        await cql.run_async(f"CREATE TABLE {workload.fqtn} (pk int PRIMARY KEY, c int, ballast blob)")
        workload.prepare(cql)
        stmt = cql.prepare(f"UPDATE {workload.fqtn} SET ballast = ? WHERE pk = ?")
        # With host=None the driver would pick the replica itself.
        host = cql.cluster.metadata.get_host(leader.ip_addr)
        assert host is not None, f"the driver does not know {leader.ip_addr}"
        return cls(workload, cql, stmt, host)

    async def write(self, writer: int) -> None:
        try:
            # The registers are 0..num_keys-1.
            await self.cql.run_async(self.stmt.bind([BALLAST, -1 - writer]), host=self.host)
            self.written += 1
        except self.ERRORS as exc:
            self.failed += 1
            logger.debug("Ballast write %d failed: %r", writer, exc)

    async def writer_task(self, writer: int) -> None:
        rng = self.workload.rng_for(f"ballast/{writer}")
        while not self.workload.stop_event.is_set():
            await self.write(writer)
            await asyncio.sleep(rng.uniform(*BALLAST_PAUSE_S))

    def disruptors(self) -> list[tuple[str, Callable]]:
        return [(f"ballast-{i}", lambda i=i: self.writer_task(i)) for i in range(BALLAST_WRITERS)]


@dataclass
class StallerState:
    workload: RegisterWorkload
    group: RaftGroup
    follower_limits: LogLimits
    # How many followers each cycle stalls: 1 or 2.
    stalled: int
    timeout: float
    # How long both followers stay full before they are released, if a cycle
    # stalls both.
    both_full_hold_s: float = 0
    cycles: int = 0
    # Cycles in which a follower refused an entry below its snapshot threshold.
    below_threshold: int = 0

    @property
    def stalls_all(self) -> bool:
        return self.stalled == len(self.group.followers)


async def stall_cycle(state: StallerState, victims: list[ServerInfo]) -> bool:
    """Stall apply() on `victims` until they all report a full log, release it,
    and wait for the group to recover.  Returns False if the workload stopped
    before the cycle completed."""
    group, workload, limits = state.group, state.workload, state.follower_limits
    names = [v.ip_addr for v in victims]

    for victim in victims:
        await group.stall(victim)
    try:
        full_usage: list[float] = []

        def all_full(leader: RaftState, stalled: list[RaftState]) -> bool:
            for victim, s in zip(victims, stalled, strict=True):
                # Also what fails if the injection did not take effect: the
                # production bound is far above what this test writes.
                assert s.log_memory_usage <= limits.max_follower_log_size, (
                    f"{victim.ip_addr} log is past the bound {limits.max_follower_log_size}, "
                    f"is {LOG_SIZE_INJECTION} in effect? {s}")
            full_usage[:] = [s.log_memory_usage for s in stalled]
            return all(s.full_logs for s in stalled)
        if not await poll(workload, group, victims, all_full, state.timeout,
                          f"{names} to report a full log"):
            return False
        # A stalled follower stays full until released, so this is the usage
        # the refusal left.
        if any(usage < limits.snapshot_threshold_log_size for usage in full_usage):
            state.below_threshold += 1

        # The leader learns it from each follower's next append_reply.
        def leader_blocked(leader: RaftState, stalled: list[RaftState]) -> bool:
            return leader.blocked_log_full == len(victims)
        if not await poll(workload, group, [], leader_blocked, state.timeout,
                          f"the leader to stop replicating to {names}"):
            return False
        logger.info("%s full at %s bytes (threshold %d), the leader stopped replicating to them",
                    names, full_usage, limits.snapshot_threshold_log_size)

        if state.stalls_all:
            # Nothing commits past the longer of the two logs now, but the
            # leader commits, applies and snapshots up to it, so the follower
            # with the shorter log may be behind the leader's snapshot.  By no
            # more than the two logs differ when stalled, which is at most
            # snapshot_threshold_log_size - snapshot_trailing_size: a follower
            # applying normally holds between the two.  That keeps it within
            # the trailing part the leader keeps only while it is smaller than
            # snapshot_trailing_size -- the injection's ratios guarantee it.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(workload.stop_event.wait(), state.both_full_hold_s)
    finally:
        for victim in victims:
            await group.resume(victim)

    t_release_ns = time.monotonic_ns()

    def recovered(leader: RaftState, stalled: list[RaftState]) -> bool:
        return not leader.blocked_log_full and not any(s.full_logs for s in stalled)
    if not await poll(workload, group, victims, recovered, state.timeout,
                      f"{names} to accept entries again"):
        return False

    return await wait_for_write(workload, group, t_release_ns, state.timeout,
                                f"a write to succeed after releasing {names}")


async def staller_task(state: StallerState) -> None:
    rng = state.workload.rng_for("staller")
    window_name = STALL_BOTH if state.stalls_all else STALL_ONE
    logger.info("Staller started, %d follower(s) per cycle", state.stalled)

    while not state.workload.stop_event.is_set():
        victims = rng.sample(state.group.followers, state.stalled)
        logger.info("Stall cycle #%d: stalling apply() on %s at time_ns=%d",
                    state.cycles + 1, [v.ip_addr for v in victims], time.monotonic_ns())
        async with state.workload.disruption_window(window_name) as window:
            completed = await stall_cycle(state, victims)
        if not completed:
            break
        state.cycles += 1
        stats = state.workload.stats(within_ns=window.span_ns)
        logger.info("Stall cycle #%d done in %.1fs: %s", state.cycles,
                    (window.end_ns - window.start_ns) / 1e9, stats)
        if window_name == STALL_ONE:
            # The leader commits with the other follower: clients go on.
            assert stats.write_ok > 0, (
                f"no write succeeded while one follower was stalled (cycle #{state.cycles}): {stats}")
        await asyncio.sleep(rng.uniform(1.0, 3.0))

    logger.info("Staller finished: %d cycles", state.cycles)


async def run_staller(cql, state: StallerState, duration_s: int,
                      extra_disruptors: Sequence[tuple[str, Callable]] = ()) -> None:
    """Run the workload with the staller, and check what every stall test
    checks at the end."""
    workload, group = state.workload, state.group
    try:
        task_errors = await run_workload(
            workload, cql, duration_s,
            disruptors=[("staller", lambda: staller_task(state)), *extra_disruptors],
        )
    finally:
        # A raft group cannot be torn down while its applier waits on the injection.
        for follower in group.followers:
            await group.resume(follower)

    logger.info("Stats: %s | stall cycles=%d", workload.stats_line(), state.cycles)
    await check_run(workload, group, task_errors)
    assert state.cycles >= 2, (
        f"Too few stall cycles ({state.cycles}), expected >= 2: the followers "
        f"filled their logs too slowly for a {duration_s}s run"
    )


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("stalled, duration_s", [
    (1, ONE_FOLLOWER_DURATION_S),
    (2, BOTH_FOLLOWERS_DURATION_S),
], ids=["one_follower", "both_followers"])
async def test_sc_linearizability_with_full_follower_logs(
    manager: ScyllaClusterManager, tmp_path, scale_timeout, stalled: int, duration_s: int,
):
    # A stalled leader is throttled by its own log limiter, not by the
    # follower bound, so the stalled nodes must stay followers.
    async with pinned_cluster(manager, scale_timeout, DEFAULT_LIMITS, DEFAULT_LIMITS) as cluster:
        workload = RegisterWorkload(
            ks=cluster.ks,
            exception_policy=only_during(STALL_BOTH, tolerate_unavailability),
        )
        await workload.create_table(cluster.cql)
        group = await cluster.raft_group(workload, scale_timeout(60))

        state = StallerState(workload=workload, group=group, follower_limits=DEFAULT_LIMITS,
                             stalled=stalled, timeout=scale_timeout(60),
                             both_full_hold_s=BOTH_FULL_HOLD_WRITE_TIMEOUTS * cluster.write_timeout_s)
        await run_staller(cluster.cql, state, duration_s)

        if state.stalls_all:
            # Otherwise the hold no longer outlasts the write timeout, and the
            # writes left in the leader's log with an unknown outcome, the ones
            # Porcupine has to place, are not tested.
            assert workload.totals.write_indeterminate > 0, (
                f"no write timed out while both followers were full: {workload.stats_line()}")

        await check_linearizable(
            workload, output_dir=tmp_path / "porcupine-checker-output")


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_linearizability_with_follower_log_full_below_threshold(
    manager: ScyllaClusterManager, tmp_path, scale_timeout,
):
    """In at least one cycle, a stalled follower refuses the entry of a
    ballast write while its log is below the snapshot threshold.  Once
    released, it applies everything and stays below the threshold, so only its
    full log calls for the snapshot."""
    async with pinned_cluster(manager, scale_timeout, LEADER_LIMITS, BELOW_THRESHOLD_LIMITS) as cluster:
        # The other follower keeps up, so nothing may fail.
        workload = RegisterWorkload(ks=cluster.ks, exception_policy=strict)
        ballast = await Ballast.create_table(workload, cluster.cql, cluster.leader)
        group = await cluster.raft_group(workload, scale_timeout(60))

        state = StallerState(workload=workload, group=group, follower_limits=BELOW_THRESHOLD_LIMITS,
                             stalled=1, timeout=scale_timeout(60))
        await run_staller(cluster.cql, state, ONE_FOLLOWER_DURATION_S, ballast.disruptors())

        logger.info("Ballast: %d written, %d failed; %d of %d cycles below the threshold",
                    ballast.written, ballast.failed, state.below_threshold, state.cycles)
        assert state.below_threshold > 0, (
            f"no follower refused an entry below the threshold in {state.cycles} cycles")

        await check_linearizable(
            workload, output_dir=tmp_path / "porcupine-checker-output")


@dataclass
class ReelectionState:
    workload: RegisterWorkload
    cql: Any
    group: RaftGroup
    ballast: Ballast
    timeout: float
    # How long the target may take to fill its log once it stops answering.
    fill_timeout: float
    # How long the leader may take to commit once re-elected: it takes no
    # more than a round trip, unless the group is stuck.
    commit_timeout: float
    cycles: int = 0
    # Cycles in which the target did not get stuck as it has to; retried.
    missed: int = 0


class Cycle(Enum):
    DONE = "done"
    MISSED = "missed"
    STOPPED = "stopped"


async def fill_and_reelect(state: ReelectionState, target: ServerInfo, burst: list[asyncio.Task]) -> Cycle:
    """With the other follower paused: fill the log of `target` with entries
    the leader cannot commit, after it applied everything committed, and
    re-elect the leader in a new term.  Raises PollTimeout unless the leader
    commits again within commit_timeout.  MISSED if the target took entries
    instead of refusing them.  The ballast writes it starts go to `burst`."""
    group, workload, manager = state.group, state.workload, state.group.manager
    limits = REELECTION_LIMITS

    # Applying the last entries the leader commits must not take the log over
    # the threshold: the snapshot would leave nothing committed above it, and
    # with nothing it can shrink, a follower takes entries instead of refusing.
    # The leader commits with the target alone until the injection is on, and
    # the target learns the last commit index with up to two ballast entries
    # more.  The register writes are too few to take the log anywhere, so
    # ballast ones take it over the threshold, and the snapshot leaves it short.
    #
    # The leader must be streaming entries to the target, too: one probing it
    # sends the same entry again and again until it hears back, which it no
    # longer does, so the log does not fill.  A follower which was paused in
    # the previous cycle is probed until it caught up.
    deadline = time.monotonic() + state.timeout
    while True:
        leader, t = await asyncio.gather(group.state(group.leader), group.state(target))
        group.check_not_lost(leader, f"while waiting for {target.ip_addr} to have a short log")
        short = t.log_memory_usage < limits.snapshot_threshold_log_size - 2 * BALLAST_SIZE
        if short and not leader.blocked_probe and not leader.blocked_log_full:
            break
        if workload.stop_event.is_set():
            return Cycle.STOPPED
        if time.monotonic() >= deadline:
            raise PollTimeout(f"timed out after {state.timeout}s waiting for {target.ip_addr} "
                              f"to have a short log, with the leader streaming to it: {leader} {t}")
        if short:
            await asyncio.sleep(0.05)
        else:
            await state.ballast.write(LONE_WRITER)

    await manager.api.enable_injection(target.ip_addr, DROP_REPLIES_INJECTION, one_shot=False)
    try:
        # Nothing commits from here on, so these stay in the leader's log
        # until they time out, and the target takes them until it is full.
        burst.extend(asyncio.create_task(state.ballast.write(i)) for i in range(REELECTION_BURST))

        # The target refused an entry once it lacks some the leader holds, and
        # goes on lacking them: the leader, which hears nothing back, sends
        # what follows, which the target cannot append either, over the gap.
        # Neither full_logs nor the log size tells: the target forgets the
        # refused entry with the next request, and with the fix for
        # SCYLLADB-5634 it takes a snapshot of what it applied as it refuses,
        # which leaves it room the leader does not know about.  Without the
        # fix it is still full after the re-election.  Held over several
        # ticks, so as not to take entries on their way for missing ones.
        lacking_since: list[float] = []

        def stuck(leader: RaftState, s: list[RaftState]) -> bool:
            t = s[0]
            if t.unapplied_entries or not 0 < t.uncommitted_entries < leader.uncommitted_entries:
                lacking_since.clear()
                return False
            if not lacking_since:
                lacking_since.append(time.monotonic())
            return time.monotonic() - lacking_since[0] >= LACKING_FOR_S
        try:
            if not await poll(workload, group, [target], stuck, state.fill_timeout,
                              f"{target.ip_addr} to refuse entries the leader cannot commit"):
                return Cycle.STOPPED
        except PollTimeout:
            # The log went over the threshold after all, before the
            # injection was on, and the target took entries one per request.
            logger.warning("%s did not refuse entries the leader cannot commit: %s",
                           target.ip_addr, await group.state(target))
            return Cycle.MISSED
        logger.info("%s refuses entries the leader cannot commit, all it has committed applied: %s",
                    target.ip_addr, await group.state(target))

        # With both followers paused, the failure detector of the leader
        # leaves it without a quorum, and it steps down.
        await manager.server_pause(target.server_id)
        try:
            if not await poll(workload, group, [], lambda leader, s: not leader.leaders,
                              state.timeout, f"{group.leader.ip_addr} to step down"):
                return Cycle.STOPPED
        finally:
            await manager.server_unpause(target.server_id)
    finally:
        await manager.api.disable_injection(target.ip_addr, DROP_REPLIES_INJECTION)

    # The target votes for it, as its log is the longer one, and the other
    # follower is still paused, so it needs the target to commit.
    if not await poll(workload, group, [], lambda leader, s: leader.leaders == 1,
                      state.timeout, f"{group.leader.ip_addr} to be re-elected"):
        return Cycle.STOPPED
    t_elected_ns = time.monotonic_ns()
    logger.info("%s re-elected", group.leader.ip_addr)

    # Without the fix for SCYLLADB-5634 the target is still full, refuses the
    # entry of the new term, and has nothing left to apply, so nothing makes
    # room, and the leader, which needs it, commits nothing.
    #
    # A ballast write rather than a register one: the driver sends some of
    # those to the paused follower, where they wait until it is back.  Not cut
    # short by the end of the run, which is what this test is about.
    written = state.ballast.written
    deadline = time.monotonic() + state.commit_timeout
    while state.ballast.written == written:
        if time.monotonic() >= deadline:
            raise PollTimeout(
                f"the leader committed nothing in {state.commit_timeout}s after its "
                f"re-election, with {target.ip_addr} refusing entries: {await group.state(target)}")
        await state.ballast.write(LONE_WRITER)
    logger.info("A write succeeded %.1fs after the re-election",
                (time.monotonic_ns() - t_elected_ns) / 1e9)
    return Cycle.DONE


async def reelection_cycle(state: ReelectionState, target: ServerInfo, other: ServerInfo) -> Cycle:
    """Run fill_and_reelect() with `other` paused, and wait for the group and
    the driver to recover."""
    group, manager = state.group, state.group.manager
    burst: list[asyncio.Task] = []
    await manager.server_pause(other.server_id)
    try:
        cycle = await fill_and_reelect(state, target, burst)
    finally:
        await manager.server_unpause(other.server_id)
        await asyncio.gather(*burst)
    if cycle == Cycle.STOPPED:
        return cycle

    # Within the disruption window: the driver may have marked the paused
    # nodes down, and reconnects to them on its own schedule.
    await wait_for_cql_and_get_hosts(state.cql, [group.leader, *group.followers],
                                     time.time() + state.timeout)

    def recovered(leader: RaftState, s: list[RaftState]) -> bool:
        return not leader.blocked_log_full and not any(f.full_logs for f in s)
    if not await poll(state.workload, group, [target, other], recovered, state.timeout,
                      "the followers to accept entries again"):
        return Cycle.STOPPED
    return cycle


async def reelection_task(state: ReelectionState) -> None:
    rng = state.workload.rng_for("reelection")
    logger.info("Re-election disruptor started")

    while not state.workload.stop_event.is_set():
        target, other = rng.sample(state.group.followers, 2)
        logger.info("Re-election cycle #%d: filling %s, pausing %s at time_ns=%d",
                    state.cycles + 1, target.ip_addr, other.ip_addr, time.monotonic_ns())
        async with state.workload.disruption_window(REELECTION) as window:
            cycle = await reelection_cycle(state, target, other)
        if cycle == Cycle.STOPPED:
            break
        if cycle == Cycle.MISSED:
            state.missed += 1
        else:
            state.cycles += 1
        logger.info("Re-election cycle %s in %.1fs: %s", cycle.value,
                    (window.end_ns - window.start_ns) / 1e9,
                    state.workload.stats(within_ns=window.span_ns))
        await asyncio.sleep(rng.uniform(1.0, 3.0))

    logger.info("Re-election disruptor finished: %d cycles, %d missed", state.cycles, state.missed)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_linearizability_with_full_follower_log_after_reelection(
    manager: ScyllaClusterManager, tmp_path, scale_timeout,
):
    """A follower whose log is full of entries from an earlier term, with
    everything committed applied, and whose leader needs it to commit an entry
    of the new term: the leader has to commit again (SCYLLADB-5634)."""
    async with pinned_cluster(manager, scale_timeout, LEADER_LIMITS, REELECTION_LIMITS) as cluster:
        workload = RegisterWorkload(ks=cluster.ks, exception_policy=only_during(REELECTION, tolerate_reboot))
        ballast = await Ballast.create_table(workload, cluster.cql, cluster.leader)
        group = await cluster.raft_group(workload, scale_timeout(60))

        state = ReelectionState(workload=workload, cql=cluster.cql, group=group, ballast=ballast,
                                timeout=scale_timeout(60), fill_timeout=scale_timeout(10),
                                commit_timeout=scale_timeout(10))
        task_errors = await run_workload(
            workload, cluster.cql, REELECTION_DURATION_S,
            disruptors=[("reelection", lambda: reelection_task(state))],
        )

        logger.info("Stats: %s | re-election cycles=%d", workload.stats_line(), state.cycles)
        await check_run(workload, group, task_errors)
        assert state.cycles >= 1, (
            f"no re-election cycle completed in {REELECTION_DURATION_S}s, {state.missed} missed")

        await check_linearizable(
            workload, output_dir=tmp_path / "porcupine-checker-output")
