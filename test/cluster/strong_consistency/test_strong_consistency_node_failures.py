#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Node failures and recovery of a strongly consistent table under the register workload.

Three nodes, RF=3, and one disruptor per test: a replica killed and restarted, random
nodes crashed and restarted again and again, the majority of a tablet's replicas lost
and restored, a leader killed with an uncommitted tail that the next leader overwrites,
the whole cluster killed and restarted.

Every test is one run of the workload in three parts: a warmup on the whole cluster,
the disruption, and a healed part on the whole cluster again, after which the run
stops.  The checker verifies the whole history at once: nothing acknowledged is lost,
nothing unacknowledged appears, and every read agrees with one order.  The cluster, the
workload and the actions that break the cluster are in rig.py; this file picks which
action to take and what to require of the cluster around it.

A crashed node brings its raft log back from its commitlog.  With the default 64 MB
segments a short run closes a segment only at a restart and no group ever releases one,
so the crash tests also run with 1 MB segments and a node flushed every second or two
(``COMMITLOG_SETUPS``): the groups then release segments in flush rounds and move their
descriptors forward, and the commitlog deletes the segments, while the nodes are being
killed.  A crash lands between a release and the flush of its descriptor, and the replay
starts from a descriptor the run moved.

The commitlog is append-only, so a raft log truncation leaves the discarded copies on
disk.  A truncation needs a leader change while the old leader holds entries nobody
else has, which a plain kill makes only by chance; the uncommitted-tail test makes one in
every round, so that replay has to drop superseded copies after crashes that land
before and after the truncation, and after releases that persisted it.
"""

from __future__ import annotations

import logging
from typing import Optional

import pytest

from test.cluster.strong_consistency.rig import (
    COMMITLOG_SETUPS, ELECT_S, GAP_S, GROUPS_START_TIMEOUT_S, RESTART_S, UNCOMMITTED_TAIL_CMDLINE,
    assert_doomed_writes_absent, assert_local_reads_match, sc_rig, total)
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)
pytestmark = [pytest.mark.asyncio, pytest.mark.no_parallel]  # 30-100s each in dev, minutes in debug


# --- a replica killed and restarted ------------------------------------------------

DOWN_S = 10
# Writes that must succeed while the node is down.  They must fit in DOWN_S, so for a
# dead leader this also bounds the pause: raft needs about 4s to notice it and elect a
# successor, the rest is room for a loaded CI machine.
MIN_WRITES_WHILE_DOWN = 50


@pytest.mark.parametrize("role", ["follower", "leader"])
async def test_node_killed_under_load(manager: ScyllaClusterManager, tmp_path, slow: int, role: str):
    """Kill one replica of a strongly consistent table under load, keep it down, restart it under load.

    A dead follower must not be felt: only the requests in flight on it may fail, at most
    one per client.  A dead leader stalls the clients until raft elects a successor; then
    writes must succeed again while the node is still down.  After the restart nothing may
    fail, and a CL=ONE (local) read on the restarted node answers what a linearizable read
    does, for every key.
    """
    async with sc_rig(manager, slow) as rig:
        victim: Optional[ServerInfo] = None
        down = None  # the window from the kill to the restart, without the restart

        async def disrupt() -> None:
            nonlocal victim, down
            leader = await rig.leader()
            victim = leader if role == "leader" else next(s for s in rig.servers if s != leader)
            logger.info("Killing %s, the %s", victim.ip_addr, role)
            async with rig.disruption():
                async with rig.workload.disruption_window("down") as down:
                    await rig.kill(victim)
                    await rig.sleep(DOWN_S)
                await rig.restart(victim)
            logger.info("%s is back and caught up", victim.ip_addr)

        await rig.run(DOWN_S + RESTART_S, disrupt)

        while_down = rig.workload.stats(within_ns=down.span_ns)
        assert while_down.write_ok >= MIN_WRITES_WHILE_DOWN, (
            f"only {while_down.write_ok} writes succeeded while the {role} was down: {while_down}")
        if role == "follower":
            # From the start, so that the requests in flight at the kill are counted; the
            # warmup allows no failure, so whatever failed, failed because of the kill.
            until_restart = rig.workload.stats(within_ns=(0, down.end_ns))
            assert until_restart.write_fail == 0 and until_restart.write_indeterminate <= rig.workload.num_writers \
                and until_restart.read_fail <= rig.workload.num_readers, (
                    f"more than the requests in flight on the dead follower failed: {until_restart}")
        await rig.check(tmp_path)
        await assert_local_reads_match(rig, victim)


# --- random nodes crashed and restarted again and again -----------------------------

CYCLES = 3


@pytest.mark.parametrize("commitlog", list(COMMITLOG_SETUPS))
async def test_repeated_crash_restart_cycles_under_load(manager: ScyllaClusterManager, tmp_path, slow: int,
                                                        commitlog: str):
    """Crash and restart random nodes again and again under load, over four tablets.

    Each cycle takes a node down for a few seconds and brings it back, so leaders of
    different tablets die, followers catch up on what they missed, and a node can die
    again soon after it rejoined.  Between two cycles the cluster is whole and nothing may
    fail; the checker verifies the whole history.

    Run over each COMMITLOG_SETUPS: with small segments and flushes the crashed node
    replays a raft log that spans several segments, of which the groups released some.
    """
    async with sc_rig(manager, slow, initial_tablets=4, commitlog=COMMITLOG_SETUPS[commitlog]) as rig:
        cycles_done = 0

        async def disrupt() -> None:
            nonlocal cycles_done
            rng = rig.workload.rng_for("killer")
            while cycles_done < CYCLES and not rig.workload.stop_event.is_set():
                if cycles_done:
                    await rig.sleep(GAP_S)
                victim = rng.choice(rig.servers)
                logger.info("Cycle %d: killing %s", cycles_done, victim.ip_addr)
                await rig.crash(victim, down_s=GAP_S)
                cycles_done += 1
                logger.info("Cycle %d: %s is back and caught up", cycles_done - 1, victim.ip_addr)

        await rig.run(CYCLES * (GAP_S + RESTART_S) + (CYCLES - 1) * GAP_S, disrupt)
        assert cycles_done == CYCLES, f"only {cycles_done} of {CYCLES} crash-restart cycles fit in the run"
        await rig.check(tmp_path)


# --- the majority of the replicas lost and restored ---------------------------------

REQUEST_TIMEOUT_MS = 2000  # short, so that many writes give up waiting for a quorum during the outage
OUTAGE_S = 5  # with the majority down
MIN_REQUESTS_IN_OUTAGE = 5  # the clients must keep trying, or "nothing acknowledged" proves nothing


async def test_majority_lost_and_restored_under_load(manager: ScyllaClusterManager, tmp_path, slow: int):
    """Lose the majority of a tablet's replicas under load, then restore it.

    Two of the three replicas are killed, the leader among them, and stay down for 5s.
    The survivor alone cannot elect a leader, so no write and no linearizable read that
    happened in that window may be acknowledged, and the clients must have kept trying.
    A write waits for the full request timeout (2s here) and its fate stays unknown: it
    may sit in the survivor's log and get committed once the majority is back, or not.
    A CL=ONE read on the survivor still answers from local state.

    The two nodes are then restarted under the same workload.  After that nothing may
    fail, and the checker verifies the whole history.
    """
    config = {'write_request_timeout_in_ms': REQUEST_TIMEOUT_MS, 'read_request_timeout_in_ms': REQUEST_TIMEOUT_MS}
    async with sc_rig(manager, slow, config=config) as rig:
        outage = None  # the majority down, until the restart begins

        async def disrupt() -> None:
            nonlocal outage
            leader = await rig.leader()
            follower, survivor = [s for s in rig.servers if s != leader]
            logger.info("Killing the leader %s and the follower %s; %s survives", leader.ip_addr, follower.ip_addr, survivor.ip_addr)
            async with rig.disruption():
                await rig.kill(leader, follower)
                async with rig.workload.disruption_window("majority-lost") as outage:
                    await rig.sleep(OUTAGE_S)
                    rows = await rig.cql.run_async(rig.local_read.bind([0]),
                                                   host=rig.cql.cluster.metadata.get_host(survivor.rpc_address))
                    logger.info("CL=ONE read of pk=0 on the survivor without a quorum: %s", [r.c for r in rows])
                await rig.restart(leader, follower)
            logger.info("The majority is back and caught up")

        await rig.run(OUTAGE_S + RESTART_S, disrupt)

        in_outage = rig.workload.stats(within_ns=outage.span_ns)
        logger.info("Outage of %.1fs: %s", (outage.end_ns - outage.start_ns) / 1e9, in_outage)
        assert total(in_outage) >= MIN_REQUESTS_IN_OUTAGE, f"only {total(in_outage)} requests happened during the outage"
        assert in_outage.write_ok == 0 and in_outage.reads == 0, f"operations were acknowledged without a quorum: {in_outage}"
        await rig.check(tmp_path)


# --- a leader killed with an uncommitted tail ---------------------------------------

ROUNDS = 2


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("commitlog", list(COMMITLOG_SETUPS))
async def test_uncommitted_tail_truncated_under_load(manager: ScyllaClusterManager, tmp_path, slow: int,
                                                  commitlog: str):
    """Kill a leader that holds entries nobody else has, under load, and let the next leader overwrite them.

    Rig.truncate_uncommitted_tail in each of two rounds: the leader is cut off, takes a
    tail of the workload's writes and of doomed writes sent straight to it, and is killed
    with the tail in its commitlog; the successor commits at the same indexes, the old
    leader comes back and truncates its tail, and is crashed again, so a replay meets
    both copies.  test_truncated_copies_are_dropped_on_replay in
    test_strong_consistency_commitlog.py takes the same path on one node without load;
    this one runs it under the register workload, with the checker over the history.

    The checker verifies the whole history; the writes cut off with the tail are
    indeterminate to it.  The doomed writes are not in it, and must never be read back:
    a CL=ONE and a linearizable read on every replica after the run find none, so no
    replay applied a truncated copy.

    Run over each COMMITLOG_SETUPS: with small segments and flushes the groups release
    segments around the truncation, and persist it as truncation records, which a later
    replay then drops copies by.
    """
    async with sc_rig(manager, slow, commitlog=COMMITLOG_SETUPS[commitlog], cmdline=UNCOMMITTED_TAIL_CMDLINE) as rig:
        [group_id] = rig.group_ids
        rounds_done = 0
        dropped_truncated = superseded = 0

        async def disrupt() -> None:
            nonlocal rounds_done, dropped_truncated, superseded
            while rounds_done < ROUNDS and not rig.workload.stop_event.is_set():
                if rounds_done:
                    await rig.sleep(GAP_S)
                tail = await rig.truncate_uncommitted_tail(group_id)
                dropped_truncated += tail.dropped_truncated
                superseded += tail.superseded
                rounds_done += 1

        await rig.run(ROUNDS * (ELECT_S + 2 * RESTART_S) + (ROUNDS - 1) * GAP_S, disrupt)
        assert rounds_done == ROUNDS, f"only {rounds_done} of {ROUNDS} rounds fit in the run"
        # Which of the two drops the replays after the truncations took depends on what the
        # groups released before each crash, so it is reported, not required.
        logger.info("Replays after the truncations: dropped_truncated=%d superseded=%d", dropped_truncated, superseded)
        await rig.check(tmp_path)
        await assert_doomed_writes_absent(rig, await rig.wait_caught_up())


# --- the whole cluster killed and restarted -----------------------------------------

MIN_WRITES_BEFORE_CRASH = 50


@pytest.mark.parametrize("commitlog", list(COMMITLOG_SETUPS))
async def test_whole_cluster_crash_under_load(manager: ScyllaClusterManager, tmp_path, slow: int, commitlog: str):
    """Kill all three nodes at once under load, then restart them together.

    The requests in flight at the crash are the only ones whose fate is unknown; the ones
    sent while every node is down never leave the client.  After the restart nothing may
    fail, and the checker verifies the history across the crash: what raft committed
    before the crash is there, what the clients were told had failed is not.  Four
    tablets, so every node brings back several raft groups.

    No replica survives to fill a gap: every committed entry comes back from some node's
    own commitlog, so this is the test of the replay that loses the most if it drops one.
    """
    async with sc_rig(manager, slow, initial_tablets=4, commitlog=COMMITLOG_SETUPS[commitlog]) as rig:
        crash = None

        async def disrupt() -> None:
            nonlocal crash
            logger.info("Killing every node")
            crash = await rig.crash(*rig.servers)
            logger.info("Every node is back and caught up")

        await rig.run(GROUPS_START_TIMEOUT_S + RESTART_S, disrupt)

        before_crash = rig.workload.stats(within_ns=(0, crash.start_ns))
        assert before_crash.write_ok >= MIN_WRITES_BEFORE_CRASH, f"too few writes before the crash: {before_crash}"
        await rig.check(tmp_path)
