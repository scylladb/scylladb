#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A three-node strongly consistent cluster under the register workload, and the failures
the node-failure tests inflict on it.

Rig owns the cluster, the workload over its register table and the actions that break
it: nodes crashed and restarted.  A test picks the action and when to take it; Rig keeps
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
import collections
import dataclasses
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Mapping, Optional, Sequence

from cassandra import ConsistencyLevel, OperationTimedOut, Unavailable
from cassandra.pool import Host

from test.cluster.strong_consistency.config import boot_sc_cluster, sc_keyspace_opts
from test.cluster.strong_consistency.outcomes import FailureContext, Outcome, only_during, tolerate_reboot
from test.cluster.strong_consistency.register_workload import (
    DisruptionWindow, RegisterWorkload, Stats, check_linearizable, run_workload)
from test.cluster.test_strong_consistency import wait_for_leader
from test.cluster.util import new_test_keyspace
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


def total(stats: Stats) -> int:
    """Every operation the stats count, whatever became of it."""
    return sum(dataclasses.astuple(stats))


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
    flush_every_s: Optional[tuple[float, float]] = None
    # The server ids of the nodes killed and not yet caught up again; the flusher leaves them alone.
    down: set[int] = dataclasses.field(default_factory=set)
    # How many times each server was killed, by server id.
    kills: collections.Counter[int] = dataclasses.field(default_factory=collections.Counter)
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
        self.kills.update(s.server_id for s in servers)
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
            live = [s for s in self.servers if s.server_id not in self.down]
            if not live:
                continue
            server = rng.choice(live)
            kills = self.kills[server.server_id]
            try:
                await self.manager.api.flush_all_keyspaces(server.ip_addr)
            except Exception as exc:
                if self.kills[server.server_id] == kills:
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
        healed_since = healed_until = 0

        async def scenario() -> None:
            nonlocal healed_since, healed_until
            await self.sleep(WARMUP_S)
            await disrupt()
            healed_since = time.monotonic_ns()
            # Until the run's timer stops the clients, if it does so first.
            try:
                await asyncio.wait_for(self.workload.stop_event.wait(), HEALED_S * self.slow)
            except asyncio.TimeoutError:
                self.workload.stop_event.set()
            healed_until = time.monotonic_ns()

        duration_s = (WARMUP_S + disruption_s + HEALED_S) * self.slow
        disruptors = [("disruptor", scenario)]
        if self.flush_every_s:
            disruptors.append(("flusher", self.flusher))
        errors = await run_workload(self.workload, self.cql, duration_s, disruptors)
        logger.info("Run done: %s | flushes=%d", self.workload.stats_line(), self.flushes)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={self.workload.seed}): {errors}"
        healed_s = (healed_until - healed_since) / 1e9
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
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        group_ids = [str(r.raft_group_id) for r in rows]
        for group_id in group_ids:
            await wait_for_leader(manager, servers[0], group_id)
        by_host_id = {str(await manager.get_host_id(s.server_id)): s for s in servers}

        yield Rig(manager=manager, cql=cql, servers=servers, workload=workload, group_ids=group_ids,
                  by_host_id=by_host_id, slow=slow, local_read=local_read, flush_every_s=commitlog.flush_every_s)


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
