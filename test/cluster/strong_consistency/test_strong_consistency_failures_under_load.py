#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Failures and recovery of a strongly consistent table under a linearizability workload.

Three nodes, RF=3, the register workload of the SC harness, and a disruptor per test:
a replica killed and left down or restarted, random nodes crashed and restarted again
and again, the majority of a tablet's replicas lost and restored, the leader stepping
down every few seconds, the whole cluster killed and restarted. Every test runs in the
two phases of ``recovery``: the clients tolerate only what the disruption causes while
it lasts, nothing may fail once the cluster is whole again, and the checker verifies
both phases as one history - nothing acknowledged is lost, nothing unacknowledged
appears, requests flow again.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.strong_consistency.config import boot_sc_cluster
from test.cluster.strong_consistency.outcomes import strict
from test.cluster.strong_consistency.recovery import (
    check_recovered, history_ops, run_phase, sc_register_table, tolerate_node_loss, wait_caught_up)
from test.cluster.strong_consistency.workload import RegisterWorkload
from test.cluster.test_strong_consistency import wait_for_leader
from test.cluster.util import trigger_stepdown
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)
pytestmark = [pytest.mark.asyncio, pytest.mark.tier2]  # 30-60s each in dev, minutes in debug

WARMUP_S = 5  # workload alone before the disruption starts
DISRUPTED_S = 20  # the disrupted phase; the majority-loss test needs a little more, see below
GAP_S = 5  # after a kill, and after a restart, before the next step
NS_PER_S = 1_000_000_000  # the history and the kill times are time.monotonic_ns()
# Every timer above and below is multiplied by this; debug boots, restarts and elects
# several times slower, so it gets several times longer.
SLOW = 1


@pytest.fixture(autouse=True)
def _time_scale(build_mode: str) -> None:
    global SLOW
    SLOW = 4 if build_mode == "debug" else 1



# --- a replica killed, left down or restarted ----------------------------------

# From the kill of a leader to the first acknowledged write. Raft needs about 4s to
# notice the dead leader and elect a new one; the rest is room for a loaded CI machine.
MAX_PAUSE_S = 20
MIN_WRITES_AFTER_KILL = 50


async def kill_nodes(manager: ScyllaClusterManager, workload: RegisterWorkload, servers: list[ServerInfo], group_ids: list[str],
                     pick: str, restart: bool, rounds: int, kills: list[tuple[ServerInfo, int]]) -> None:
    """Kill a node up to `rounds` times: the first one WARMUP_S into the run, each next step GAP_S after the previous.

    `pick` names the victim by its role for the first group, "leader" or "follower", or is "random".
    With `restart` the node is started again GAP_S after the kill and the next round waits until it
    caught up; without, it stays down. Every kill is appended to `kills` with its time.

    The clients tolerate node loss from a kill until the node is back and caught up, and nothing before
    or after: a failure while the cluster is whole is a bug, whether or not the phase timer has run out.
    """
    rng = workload.rng_for("killer")
    by_host_id = {str(await manager.get_host_id(s.server_id)): s for s in servers}
    alive = list(servers)
    await asyncio.sleep(WARMUP_S * SLOW)
    for _ in range(rounds):
        if workload.stop_event.is_set():
            break
        leader = by_host_id[await wait_for_leader(manager, alive[0], group_ids[0])]
        victim = leader if pick == "leader" else next(s for s in alive if s != leader) if pick == "follower" else rng.choice(alive)
        logger.info(f"Killing {victim.ip_addr}, the {pick} (leader of group {group_ids[0]}: {leader.ip_addr})")
        workload.exception_policy = tolerate_node_loss
        await manager.server_stop(victim.server_id, convict=False)
        kills.append((victim, time.monotonic_ns()))
        alive.remove(victim)
        await asyncio.sleep(GAP_S * SLOW)
        if restart:
            await manager.server_start(victim.server_id)
            alive.append(victim)
            await wait_caught_up(manager, alive, group_ids)
            workload.exception_policy = strict
            logger.info(f"{victim.ip_addr} is back and caught up")
            await asyncio.sleep(GAP_S * SLOW)


def assert_writes_resumed(workload: RegisterWorkload, killed_at: int) -> None:
    """Writes started after the kill succeed again: the first within MAX_PAUSE_S, and at least MIN_WRITES_AFTER_KILL of them."""
    acked = sorted(t_return for op, t_call, status, t_return in history_ops(workload) if op == "write" and status == "ok" and t_call > killed_at)
    assert len(acked) >= MIN_WRITES_AFTER_KILL, f"only {len(acked)} writes succeeded after the kill: {workload.stats()}"
    pause = (acked[0] - killed_at) / NS_PER_S
    assert pause < MAX_PAUSE_S * SLOW, f"the first write after the kill succeeded {pause:.1f}s later"


@pytest.mark.parametrize("role", ["follower", "leader"])
async def test_node_killed_under_load(manager: ScyllaClusterManager, tmp_path, role: str):
    """Kill one replica of a strongly consistent table under load and leave it down.

    A dead follower must not be felt: only the requests in flight on it may fail, at most one
    per client. A dead leader
    stalls the clients until raft elects a new one; then writes must succeed again while the
    node is still down, and the pause must be short.

    The node is then restarted and the workload runs on with no failure allowed. The checker
    verifies the whole history.
    """
    servers, cql = await boot_sc_cluster(manager, 3)
    async with sc_register_table(manager, servers) as (workload, group_ids):
        kills: list[tuple[ServerInfo, int]] = []
        await run_phase(workload, cql, DISRUPTED_S * SLOW,
                        [("killer", lambda: kill_nodes(manager, workload, servers, group_ids, role, False, 1, kills))])
        [(victim, killed_at)] = kills
        stats = workload.stats()

        if role == "follower":
            assert stats.write_fail == 0 and stats.write_indeterminate <= workload.num_writers \
                and stats.read_fail <= workload.num_readers, f"more than the requests in flight on the dead follower failed: {stats}"
            # Expect one at ~2s right after the kill: the python driver waits that long for a stream
            # on the dead node's closed connection before it tries the next host
            # (https://github.com/scylladb/python-driver/issues/1058). Not asserted on: a loaded
            # CI machine adds pauses of its own.
            slowest = sorted(((t_return - t_call) / NS_PER_S, op, status, (t_call - killed_at) / NS_PER_S)
                             for op, t_call, status, t_return in history_ops(workload) if status)[-5:]
            logger.info(f"Slowest requests as (seconds, op, status, seconds after the kill): {slowest}")
        assert_writes_resumed(workload, killed_at)

        await manager.server_start(victim.server_id)
        await wait_caught_up(manager, servers, group_ids)
        await check_recovered(workload, cql, tmp_path, SLOW)


@pytest.mark.parametrize("role", ["follower", "leader"])
async def test_node_killed_and_restarted_under_load(manager: ScyllaClusterManager, tmp_path, role: str):
    """Kill one replica under load and restart it while the workload runs.

    Same as test_node_killed_under_load, but the node comes back during the disrupted phase.
    The restart must not cost the clients anything: the healed phase allows no failure. After
    the run, a CL=ONE (local) read on the restarted node must return the same value as a
    linearizable read, for every key.
    """
    servers, cql = await boot_sc_cluster(manager, 3)
    async with sc_register_table(manager, servers) as (workload, group_ids):
        kills: list[tuple[ServerInfo, int]] = []
        await run_phase(workload, cql, DISRUPTED_S * SLOW,
                        [("killer", lambda: kill_nodes(manager, workload, servers, group_ids, role, True, 1, kills))])
        [(victim, killed_at)] = kills
        assert_writes_resumed(workload, killed_at)
        await check_recovered(workload, cql, tmp_path, SLOW)

        # The barrier makes the restarted node apply everything the healed phase committed.
        victim_host = (await wait_caught_up(manager, servers, group_ids))[servers.index(victim)]
        local_read = cql.prepare(f"SELECT c FROM {workload.fqtn} WHERE pk = ?")
        local_read.consistency_level = ConsistencyLevel.ONE
        for pk in range(workload.num_keys):
            linearizable = [r.c for r in await cql.run_async(workload.read_stmt.bind([pk]), host=victim_host)]
            local = [r.c for r in await cql.run_async(local_read.bind([pk]), host=victim_host)]
            assert local == linearizable, f"pk={pk}: the restarted node holds {local} locally, the group says {linearizable}"


async def test_repeated_crash_restart_cycles_under_load(manager: ScyllaClusterManager, tmp_path):
    """Crash and restart random nodes again and again under load, over four tablets.

    Each cycle takes a node down for a few seconds and brings it back, so leaders of
    different tablets die, followers catch up on what they missed, and a node can die
    again soon after it rejoined. The disrupted phase ends when the last cycle is done,
    however long the restarts take. After that nothing may fail, and the checker
    verifies the whole history.
    """
    servers, cql = await boot_sc_cluster(manager, 3)
    async with sc_register_table(manager, servers, initial_tablets=4) as (workload, group_ids):
        kills: list[tuple[ServerInfo, int]] = []

        async def cycles() -> None:
            await kill_nodes(manager, workload, servers, group_ids, "random", True, 3, kills)
            workload.stop_event.set()

        await run_phase(workload, cql, 6 * DISRUPTED_S * SLOW, [("killer", cycles)])
        assert len(kills) == 3, f"only {len(kills)} crash-restart cycles fit in {6 * DISRUPTED_S * SLOW}s"
        await check_recovered(workload, cql, tmp_path, SLOW)


# --- the majority of the replicas lost and restored ----------------------------

REQUEST_TIMEOUT_MS = 2000  # short, so that many writes give up waiting for a quorum during the outage
OUTAGE_S = 5  # with the majority down
MAJORITY_DISRUPTED_S = DISRUPTED_S + OUTAGE_S  # warmup, outage and the restart of two nodes
MIN_REQUESTS_IN_OUTAGE = 5  # the clients must keep trying, or "nothing acknowledged" proves nothing


async def lose_majority(manager: ScyllaClusterManager, workload: RegisterWorkload, cql, servers: list[ServerInfo], group_id: str,
                        window: list[int]) -> None:
    """Kill the leader and one follower WARMUP_S into the run, keep them down for OUTAGE_S, then start both and wait
    until every node caught up. Appends to `window` the time the majority was lost and the time the restart began.
    At the end of the outage a CL=ONE read on the survivor must answer: its local state is still served."""
    by_host_id = {str(await manager.get_host_id(s.server_id)): s for s in servers}
    await asyncio.sleep(WARMUP_S * SLOW)
    leader = by_host_id[await wait_for_leader(manager, servers[0], group_id)]
    follower, survivor = [s for s in servers if s != leader]
    logger.info(f"Killing the leader {leader.ip_addr} and the follower {follower.ip_addr}; {survivor.ip_addr} survives")
    workload.exception_policy = tolerate_node_loss  # nothing may fail before the kill, or after the restore
    for s in (leader, follower):
        await manager.server_stop(s.server_id, convict=False)
    window.append(time.monotonic_ns())

    await asyncio.sleep(OUTAGE_S * SLOW)
    relaxed_read = SimpleStatement(f"SELECT c FROM {workload.fqtn} WHERE pk = 0", consistency_level=ConsistencyLevel.ONE)
    rows = await cql.run_async(relaxed_read, host=cql.cluster.metadata.get_host(survivor.rpc_address))
    logger.info(f"CL=ONE read of pk=0 on the survivor without a quorum: {[r.c for r in rows]}")

    window.append(time.monotonic_ns())
    await asyncio.gather(*(manager.server_start(s.server_id) for s in (leader, follower)))
    await wait_caught_up(manager, servers, [group_id])
    workload.exception_policy = strict  # whole again: from here on a failure is a bug
    logger.info("The majority is back and caught up")


async def test_majority_lost_and_restored_under_load(manager: ScyllaClusterManager, tmp_path):
    """Lose the majority of a tablet's replicas under load, then restore it.

    Two of the three replicas are killed, the leader among them, and stay down for 5s.
    The survivor alone cannot elect a leader, so no write and no linearizable read started
    in that window may be acknowledged; the test checks that from the history, and that
    the clients kept trying. A write waits for the full request timeout (2s here) and its
    fate stays unknown: it may sit in the survivor's log and get committed once the
    majority is back, or not. A CL=ONE read on the survivor still answers from local state.

    The two nodes are then restarted under the same workload. After that nothing may fail,
    and the checker verifies the whole history: acknowledged writes are there, writes that
    failed for sure are not, and every read agrees with one order.
    """
    config = {'write_request_timeout_in_ms': REQUEST_TIMEOUT_MS, 'read_request_timeout_in_ms': REQUEST_TIMEOUT_MS}
    servers, cql = await boot_sc_cluster(manager, 3, config=config)
    async with sc_register_table(manager, servers) as (workload, [group_id]):
        window: list[int] = []
        await run_phase(workload, cql, MAJORITY_DISRUPTED_S * SLOW,
                        [("majority-loss", lambda: lose_majority(manager, workload, cql, servers, group_id, window))])
        lost, restart_begun = window
        in_outage = [(op, status, t_return) for op, t_call, status, t_return in history_ops(workload) if lost < t_call < restart_begun]
        assert len(in_outage) >= MIN_REQUESTS_IN_OUTAGE, f"only {len(in_outage)} requests were started during the outage"
        acked = [op for op, status, t_return in in_outage if status == "ok" and t_return < restart_begun]
        assert not acked, f"{len(acked)} operations were acknowledged without a quorum: {acked[:5]}"
        logger.info(f"Outage of {(restart_begun - lost) / NS_PER_S:.1f}s, {len(in_outage)} requests started in it: {workload.stats()}")
        await check_recovered(workload, cql, tmp_path, SLOW)


# --- the leader stepping down again and again ----------------------------------

PERIOD_S = 3  # between two stepdowns
MIN_HANDOVERS = 3  # about 5 fit in 20s on an idle machine


async def wait_for_new_leader(manager: ScyllaClusterManager, servers: list[ServerInfo], group_id: str, old_leader: str) -> str:
    """Wait until every one of `servers` names the same leader of `group_id`, and it is not `old_leader`."""
    async def agreed() -> str | None:
        seen = {str(await manager.api.get_raft_leader(s.ip_addr, group_id)) for s in servers}
        leader = seen.pop() if len(seen) == 1 else None
        return leader if leader and leader != old_leader and uuid.UUID(leader).int != 0 else None
    return await wait_for(agreed, time.time() + 60, label=f"every replica of group {group_id} to know a leader other than {old_leader}")


async def step_down_leaders(manager: ScyllaClusterManager, workload: RegisterWorkload, servers: list[ServerInfo], group_id: str,
                            handovers: list[str]) -> None:
    """Every PERIOD_S, make the current leader of `group_id` step down and wait until every replica knows the
    successor; append the successor to `handovers`. Stops with the workload."""
    by_host_id = {str(await manager.get_host_id(s.server_id)): s for s in servers}
    while not workload.stop_event.is_set():
        await asyncio.sleep(PERIOD_S * SLOW)
        old_leader = await wait_for_leader(manager, servers[0], group_id)
        await trigger_stepdown(manager, by_host_id[old_leader], group_id)
        new_leader = await wait_for_new_leader(manager, servers, group_id, old_leader)
        handovers.append(new_leader)
        logger.info(f"Handover #{len(handovers)} of group {group_id}: {old_leader} -> {new_leader}")


async def test_leader_stepdown_under_load(manager: ScyllaClusterManager, tmp_path):
    """Make the leader of a strongly consistent tablet step down every 3s under load.

    A handover must not be visible to the clients: the coordinator retries a request the old
    leader could not take, and the new leader is known within milliseconds. So no request may
    fail, during the handovers or after them, and the checker verifies the whole history.
    At least 3 handovers must happen in 20s.
    """
    servers, cql = await boot_sc_cluster(manager, 3)
    async with sc_register_table(manager, servers, exception_policy=strict) as (workload, [group_id]):
        handovers: list[str] = []
        await run_phase(workload, cql, DISRUPTED_S * SLOW,
                        [("stepdown", lambda: step_down_leaders(manager, workload, servers, group_id, handovers))])
        assert len(handovers) >= MIN_HANDOVERS, f"only {len(handovers)} leader handovers in {DISRUPTED_S}s"
        await check_recovered(workload, cql, tmp_path, SLOW)


# --- the whole cluster killed and restarted ------------------------------------

async def crash_cluster(manager: ScyllaClusterManager, workload: RegisterWorkload, servers: list[ServerInfo], group_ids: list[str]) -> None:
    """Kill every node at once WARMUP_S into the run, stop the clients, start every node again and wait until all
    of them caught up on every group."""
    await asyncio.sleep(WARMUP_S * SLOW)
    logger.info("Killing every node")
    workload.exception_policy = tolerate_node_loss  # nothing may fail before the kill, or after the restore
    await asyncio.gather(*(manager.server_stop(s.server_id, convict=False) for s in servers))
    # The requests in flight now are the ones whose fate nobody knows. A client left running
    # would only add failures of requests that never left it.
    workload.stop_event.set()
    await asyncio.gather(*(manager.server_start(s.server_id) for s in servers))
    await wait_caught_up(manager, servers, group_ids)
    workload.exception_policy = strict  # whole again: from here on a failure is a bug
    logger.info("Every node is back and caught up")


async def test_whole_cluster_crash_under_load(manager: ScyllaClusterManager, tmp_path):
    """Kill all three nodes at once under load, then restart them together.

    The requests in flight at the crash are the only ones whose fate is unknown. After the
    restart nothing may fail, and the checker verifies the history across the crash: what
    raft committed before the crash is there, what the clients were told had failed is not.
    Four tablets, so every node brings back several raft groups.
    """
    servers, cql = await boot_sc_cluster(manager, 3)
    async with sc_register_table(manager, servers, initial_tablets=4) as (workload, group_ids):
        await run_phase(workload, cql, DISRUPTED_S * SLOW, [("crash", lambda: crash_cluster(manager, workload, servers, group_ids))])
        assert workload.stats().write_ok >= 50, f"too few writes before the crash: {workload.stats()}"
        await check_recovered(workload, cql, tmp_path, SLOW)
