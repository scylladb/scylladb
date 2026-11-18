#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A random sequence of failures in the raft log's persistence, under the register workload.

The scenario tests in test_strong_consistency_node_failures.py each pin one failure:
a crash, a crash with stores held, a truncated tail, failing descriptor writes, the
whole cluster down.  This test draws the same rig actions from one seeded generator and
runs them one after another on the same cluster, over four tablets, with small commitlog
segments and a node flushed every second or two, so that each failure meets the state
the ones before it left behind: a replay of a log truncated two crashes ago, a release
that failed and was retried after a restart.  A crash of every node is drawn too, and
fails where SCYLLADB-2565 strands a lagging follower (see ACTIONS).

Every kind of action runs once, in a random order, and then random ones until the
disruption budget is spent.  Each action leaves the cluster whole before the next one is
drawn, and its errors are tolerated only inside its own REBOOT window: between two
actions nothing may fail.  The checker verifies the whole history at the end, and every
node's local state must agree with the group's.

The run's seed is in every assertion message; set SEED to it to replay the same choices
(not the same interleaving).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

import pytest

from test.cluster.strong_consistency.rig import (
    COMMITLOG_SETUPS, ELECT_S, FAILING_S, GAP_S, GROUPS_START_TIMEOUT_S, HELD_S, RESTART_S, ROOM_FOR_HELD_SEGMENTS,
    UNCOMMITTED_TAIL_CMDLINE, Rig, assert_doomed_writes_absent, assert_local_reads_match, sc_rig)
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)
# About 3 minutes in dev, three nodes under load with crashes and flushes every second or
# two; the action allowances assume a machine not shared with other cluster tests.
pytestmark = [pytest.mark.asyncio, pytest.mark.no_parallel]

# Set to the seed of a failed run to replay its choices.
SEED: Optional[int] = None

TABLETS = 4


@dataclass
class Nemesis:
    """Draws the targets and timings of the rig's actions; every action returns with the cluster whole."""

    rig: Rig
    rng: Any = None
    done: list[str] = field(default_factory=list)
    dropped_truncated: int = 0
    superseded: int = 0
    descriptor_failures: int = 0

    def __post_init__(self) -> None:
        self.rng = self.rig.workload.rng_for("nemesis")

    async def kill_one(self) -> str:
        victim = self.rng.choice(self.rig.servers)
        down_s = self.rng.uniform(0, GAP_S)
        await self.rig.crash(victim, down_s=down_s)
        return f"{victim.ip_addr} down for {down_s:.1f}s"

    async def kill_all(self) -> str:
        await self.rig.crash(*self.rig.servers)
        return "every node"

    async def hold_then_kill(self) -> str:
        group_id, victim = self.rng.choice(self.rig.group_ids), self.rng.choice(self.rig.servers)
        held_s, down_s = self.rng.uniform(0.5, HELD_S), self.rng.uniform(0, GAP_S)
        await self.rig.hold_stores_then_kill(group_id, victim, held_s=held_s, down_s=down_s)
        return f"group {group_id} held on {victim.ip_addr} for {held_s:.1f}s, down for {down_s:.1f}s"

    async def truncate_uncommitted_tail(self) -> str:
        group_id = self.rng.choice(self.rig.group_ids)
        persist = self.rng.random() < 0.5
        tail = await self.rig.truncate_uncommitted_tail(group_id, persist=persist)
        self.dropped_truncated += tail.dropped_truncated
        self.superseded += tail.superseded
        return (f"group {group_id} on {tail.leader.ip_addr}, truncation {'persisted' if persist else 'not persisted'}, "
                f"replay dropped_truncated={tail.dropped_truncated} superseded={tail.superseded}")

    async def fail_descriptors(self) -> str:
        group_id, victim = self.rng.choice(self.rig.group_ids), self.rng.choice(self.rig.servers)
        failing_s = self.rng.uniform(2, FAILING_S)
        crash = self.rng.random() < 0.5
        failed = await self.rig.fail_descriptor_writes(group_id, victim, failing_s=failing_s, crash=crash)
        self.descriptor_failures += failed
        return (f"group {group_id} on {victim.ip_addr} for {failing_s:.1f}s, {failed} failed writes, "
                f"{'then crashed' if crash else 'then recovered'}")

    async def flush(self) -> str:
        """Flush system.raft_groups alone on a node: descriptors become durable at a random moment.

        Not a disruption.  The flusher flushes whole nodes meanwhile.
        """
        server = self.rng.choice(self.rig.servers)
        await self.rig.manager.api.keyspace_flush(server.ip_addr, "system", "raft_groups")
        return f"system.raft_groups on {server.ip_addr}"


# name -> (allowance in seconds before `slow`, weight in the random part)
ACTIONS: dict[str, tuple[float, int]] = {
    "kill_one": (GAP_S + RESTART_S, 3),
    # Fails now and then on SCYLLADB-2565: after every node restarts, a follower that lagged
    # behind the new leader's descriptor floor needs a snapshot transfer, which strongly
    # consistent groups do not have yet, and its read barrier never passes.
    "kill_all": (GROUPS_START_TIMEOUT_S + RESTART_S, 1),
    "hold_then_kill": (HELD_S + GAP_S + RESTART_S + 10, 3),
    "truncate_uncommitted_tail": (2 * ELECT_S + 2 * RESTART_S, 2),
    "fail_descriptors": (FAILING_S + RESTART_S + 5, 2),  # +5: the release after the window
    "flush": (5, 1),
}

# Spent on actions after the warmup; an action starts only if its allowance still fits.
# Room for every action once at its allowance in any order, and 30s more: the actions
# take a fraction of their allowances, so most of the budget goes to random ones.
NEMESIS_S = sum(allowance for allowance, _ in ACTIONS.values()) + GAP_S * (len(ACTIONS) - 1) + 30


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_persistence_nemesis_under_load(manager: ScyllaClusterManager, tmp_path, slow: int):
    """Random crashes, held stores, truncated tails, failing descriptor writes and flushes, one after another.

    Every action kind runs at least once.  The checker verifies the whole history; the
    writes sent into truncated tails are not in it and must never be read back on any
    replica; every node's local reads agree with linearizable ones.
    """
    async with sc_rig(manager, slow, initial_tablets=TABLETS, commitlog=COMMITLOG_SETUPS["small-segments"],
                      config=ROOM_FOR_HELD_SEGMENTS, cmdline=UNCOMMITTED_TAIL_CMDLINE) as rig:
        if SEED is not None:
            rig.workload.seed = SEED
        nemesis = Nemesis(rig=rig)
        actions: dict[str, Callable[[], Awaitable[str]]] = {name: getattr(nemesis, name) for name in ACTIONS}

        async def disrupt() -> None:
            # A second short of the run's timer, for the scheduling delay of the warmup.
            deadline = time.monotonic() + NEMESIS_S * slow - 1
            first_pass = list(ACTIONS)
            nemesis.rng.shuffle(first_pass)
            names, weights = zip(*((name, weight) for name, (_, weight) in ACTIONS.items()))
            while not rig.workload.stop_event.is_set():
                i = len(nemesis.done)
                name = first_pass[i] if i < len(first_pass) else nemesis.rng.choices(names, weights)[0]
                gap_s = nemesis.rng.uniform(1, GAP_S) if i else 0
                await rig.sleep(gap_s)
                if time.monotonic() + ACTIONS[name][0] * slow > deadline:
                    break
                started = time.monotonic()
                what = await actions[name]()
                took = time.monotonic() - started
                nemesis.done.append(name)
                logger.info("Action %d %s (%.1fs): %s", i, name, took, what)
                assert took <= ACTIONS[name][0] * slow, (
                    f"action {i} {name} took {took:.1f}s, over its allowance of {ACTIONS[name][0] * slow}s: {what} "
                    f"(seed={rig.workload.seed})")

        await rig.run(NEMESIS_S, disrupt)
        logger.info("Actions: %s | replays dropped_truncated=%d superseded=%d | descriptor write failures=%d",
                    nemesis.done, nemesis.dropped_truncated, nemesis.superseded, nemesis.descriptor_failures)
        missing = set(ACTIONS) - set(nemesis.done)
        assert not missing, f"actions that never ran: {sorted(missing)} (seed={rig.workload.seed}): {nemesis.done}"
        await rig.check(tmp_path)

        hosts = await rig.wait_caught_up()
        await assert_doomed_writes_absent(rig, hosts)
        for server in rig.servers:
            await assert_local_reads_match(rig, server, hosts)
