#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Live network partitions under strongly-consistent (SC) tables, checked with Porcupine.

A node that is stopped or frozen with SIGSTOP goes silent for everyone: its peers and the
test alike. A node behind a broken link is different. It keeps its timers, keeps taking
client requests, keeps its raft state machines ticking and keeps accumulating state it
cannot share, and the code paths that matter are the ones that run *on* it in that
condition: the leader that must notice on its own that it has no majority and step down,
the follower that must not depose a leader it merely cannot hear, the coordinator that
must park writes as hints and deliver them later, the gossiper that must agree with a
half-connected peer on who is up and still shut down cleanly.

The faults go through the per-node TCP proxies of ``test.pylib.network_proxy``: each node
advertises its proxy's address to its peers and its clients. Every scenario acts on the
internode ports, so the test keeps talking to every node over CQL and the REST API, except
the ones that cut a node off from its clients on purpose. The proxy sees connections, not
packets: a "cut" withholds the bytes of a flow or resets the connections, and "loss" is
emulated as the stalls TCP retransmissions would cause; the gossiper and the failure
detector report the nodes by their proxy addresses, which the helpers below translate
back. The SC load is the register workload of ``test.cluster.strong_consistency``
and its history is checked for linearizability at the end; a window opened with
``disruption_window`` excuses the failures a cut legitimately causes, and the same failure
outside the window fails the test.

The cluster is three nodes, RF=3, one tablet, so that there is exactly one SC raft group
and "the leader" is well defined.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

import pytest
from cassandra import ConsistencyLevel, OperationTimedOut, ReadFailure, ReadTimeout, Unavailable
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement

from test.cluster.strong_consistency.outcomes import only_during, tolerate_reboot
from test.cluster.strong_consistency.register_workload import RegisterWorkload, check_linearizable, run_workload
from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table
from test.pylib.internal_types import HostID, IPAddress, ServerInfo
from test.pylib.network_proxy import NetworkProxy
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely, wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

SC_KS_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
              " AND tablets = {'initial': 1} AND consistency = 'global'")

# One name per kind of cut, for the disruption windows and the exception policy.
PARTITION = "partition"
LOSS = "loss"
FAULT = "fault"
DELAY = "delay"
FLAP = "flap"
CLIENTS = "clients-blocked"

# The register workload draws its keys from this seed, so a run replays the same choices.
SEED = 20261106

# The raft side notices a cut-off peer through the direct failure detector, 2s by default
# (raft_group_registry.cc), and a leader without a majority steps down ELECTION_TIMEOUT
# (10 ticks of 100ms) later; a follower starts an election at most 2*ELECTION_TIMEOUT after
# the detector gave up on the leader. So a new leader exists within ~5s of a cut, and the old
# one has stepped down by then. The bound below leaves room for REST polling and a slow box.
LEADER_CHANGE_BOUND_S = 20


async def boot_sc_cluster(manager: ScyllaClusterManager, failure_detector_timeout: int, extra_cmdline: list[str] = [],
                          request_timeout_ms: Optional[int] = None,
                          extra_config: Optional[dict] = None,
                          nodes: int = 3) -> tuple[list[ServerInfo], object, list, list[HostID]]:
    """`nodes` SC-enabled nodes (three by default) in one DC, one per rack, with a short gossip
    failure detector timeout.

    The harness gives test nodes 60s request timeouts. A test that cuts a node off wants
    production-like ones instead (`request_timeout_ms`): a request that lands on the cut-off
    node then fails with a timeout and its client moves on, rather than holding the client
    until the heal lets the request complete.

    `extra_config` is merged in last, e.g. the LeaseGuard options of
    test_network_proxy_sc_leases.py.
    """
    config = DEFAULT_CONFIG | {'failure_detector_timeout_in_ms': failure_detector_timeout}
    if request_timeout_ms is not None:
        config |= {'read_request_timeout_in_ms': request_timeout_ms, 'write_request_timeout_in_ms': request_timeout_ms}
    config |= extra_config or {}
    servers = await manager.servers_add(nodes, config=config, cmdline=DEFAULT_CMDLINE + extra_cmdline, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*(manager.get_host_id(s.server_id) for s in servers))
    return servers, cql, hosts, host_ids


async def down_endpoints(manager: ScyllaClusterManager, network_proxy: NetworkProxy, server: ServerInfo) -> set[str]:
    """The nodes `server` reports DOWN, by their real addresses (gossip knows them by their proxies')."""
    real_of = {proxy: real for real, proxy in network_proxy.server.addresses.items()}
    return {real_of.get(IPAddress(ep), ep) for ep in await manager.api.get_down_endpoints(server.ip_addr)}


async def wait_for_down(manager: ScyllaClusterManager, network_proxy: NetworkProxy, server: ServerInfo, expected: set[str],
                        timeout: float = 60) -> None:
    async def done():
        return True if await down_endpoints(manager, network_proxy, server) == expected else None
    await wait_for(done, time.time() + timeout, period=0.5, label=f"{server.ip_addr} to see {expected or 'nobody'} down")


async def wait_for_all_up(manager: ScyllaClusterManager, network_proxy: NetworkProxy, servers: list[ServerInfo],
                          timeout: float = 60) -> None:
    await gather_safely(*(wait_for_down(manager, network_proxy, s, set(), timeout) for s in servers))


def log_slowest_ops(workload: RegisterWorkload, count: int = 8) -> None:
    """Log the operations that took longest, call to return, from the recorded history."""
    calls: dict[int, dict] = {}
    durations: list[tuple[float, dict, dict]] = []
    for line in workload.history.to_jsonl().splitlines():
        event = json.loads(line)
        if event["kind"] == "call":
            calls[event["id"]] = event
        elif (call := calls.get(event["id"])) is not None:
            durations.append(((event["time_ns"] - call["time_ns"]) / 1e9, call, event))
    durations.sort(key=lambda d: -d[0])
    for seconds, call, ret in durations[:count]:
        logger.info(f"slow op: {seconds:.2f}s {call['op']} pk={call['key']} client={call['client_id']} -> {ret['status']}")


def latencies_s(workload: RegisterWorkload, within_ns: Optional[tuple[int, int]] = None) -> list[float]:
    """The call-to-return times of the operations called within `within_ns` (all of them by default), sorted."""
    calls: dict[int, int] = {}
    durations: list[float] = []
    for line in workload.history.to_jsonl().splitlines():
        event = json.loads(line)
        if event["kind"] == "call":
            if within_ns is None or within_ns[0] <= event["time_ns"] < within_ns[1]:
                calls[event["id"]] = event["time_ns"]
        elif (t_call := calls.get(event["id"])) is not None:
            durations.append((event["time_ns"] - t_call) / 1e9)
    durations.sort()
    return durations


def median_latency_s(workload: RegisterWorkload, within_ns: Optional[tuple[int, int]] = None) -> float:
    durations = latencies_s(workload, within_ns)
    return durations[len(durations) // 2] if durations else float("nan")


def latency_histogram(durations: list[float]) -> dict[str, int]:
    return {"<50ms": sum(d < 0.05 for d in durations), "50-250ms": sum(0.05 <= d < 0.25 for d in durations),
            ">=250ms": sum(d >= 0.25 for d in durations)}


async def median_s(cql, host, statements: list[str]) -> float:
    """The median duration of the statements, run one after the other through `host`."""
    durations = []
    for statement in statements:
        start = time.monotonic()
        await cql.run_async(statement, host=host)
        durations.append(time.monotonic() - start)
    durations.sort()
    return durations[len(durations) // 2]


async def metric(manager: ScyllaClusterManager, server: ServerInfo, name: str) -> float:
    """A counter summed over the shards of `server`; 0 when it was never reported (skip_when_empty)."""
    return (await manager.metrics.query(server.ip_addr)).get(name) or 0.0


@dataclass
class LeaderWatch:
    """One node's view of the group's leader, polled over REST; every change is recorded.

    `changes` is the sequence of distinct values the node reported, with the time each one
    was first seen; None is "no leader known", which a follower reports between losing a
    leader and hearing from the next one, and a cut-off candidate reports for as long as
    it campaigns.
    """
    manager: ScyllaClusterManager
    server: ServerInfo
    group_id: str
    period_s: float = 0.25
    changes: list[tuple[float, Optional[HostID]]] = field(default_factory=list)
    stop: asyncio.Event = field(default_factory=asyncio.Event)

    async def run(self) -> None:
        last: object = ()
        while not self.stop.is_set():
            leader = await self.manager.api.get_raft_leader(self.server.ip_addr, self.group_id)
            if leader != last:
                self.changes.append((time.monotonic(), leader))
                logger.info(f"leader of group {self.group_id} as seen by {self.server.ip_addr}: {leader}")
                last = leader
            await asyncio.sleep(self.period_s)

    @property
    def leaders(self) -> list[Optional[HostID]]:
        return [leader for _, leader in self.changes]

    def distinct_leaders(self, before: Optional[float] = None) -> list[HostID]:
        """The leaders reported, in order of first appearance, "none" left out; only those first
        seen before the `time.monotonic()` value `before`, when given."""
        seen: list[HostID] = []
        for t, leader in self.changes:
            if before is not None and t >= before:
                break
            if leader is not None and leader not in seen:
                seen.append(leader)
        return seen


class PinnedSession:
    """The register workload's view of the session, with every request pinned to one of `hosts`.

    The workload calls `prepare()` and `run_async(statement)` only. Round robin over the
    given hosts, so a test can keep the load on the nodes it wants and probe the others
    itself.
    """

    def __init__(self, cql, hosts: list):
        self._cql = cql
        self._hosts = list(hosts)
        self._next = 0

    def prepare(self, query: str):
        return self._cql.prepare(query)

    def run_async(self, statement, *args, **kwargs):
        host = self._hosts[self._next % len(self._hosts)]
        self._next += 1
        return self._cql.run_async(statement, *args, host=host, **kwargs)


async def wait_for_new_leader(manager: ScyllaClusterManager, server: ServerInfo, group_id: str, old_leader: HostID,
                              timeout: float) -> HostID:
    """Wait until `server` reports a leader other than `old_leader`."""
    async def new_leader():
        leader = await manager.api.get_raft_leader(server.ip_addr, group_id)
        return leader if leader is not None and leader != old_leader else None
    return await wait_for(new_leader, time.time() + timeout, period=0.25,
                          label=f"{server.ip_addr} to report a leader of {group_id} other than {old_leader}")


async def wait_for_leader_change(manager: ScyllaClusterManager, server: ServerInfo, group_id: str, old_leader: HostID,
                                 timeout: float) -> Optional[HostID]:
    """Wait until `server` stops reporting `old_leader`; returns what it reports instead (None: nobody)."""
    async def changed():
        leader = await manager.api.get_raft_leader(server.ip_addr, group_id)
        return (leader,) if leader != old_leader else None
    (leader,) = await wait_for(changed, time.time() + timeout, period=0.25,
                               label=f"{server.ip_addr} to stop reporting {old_leader} as the leader of {group_id}")
    return leader


async def wait_for_no_leader(manager: ScyllaClusterManager, server: ServerInfo, group_id: str, timeout: float) -> None:
    """Wait until `server` reports no leader for the group: it stepped down, or gave up on the one it had."""
    async def no_leader():
        return True if await manager.api.get_raft_leader(server.ip_addr, group_id) is None else None
    await wait_for(no_leader, time.time() + timeout, period=0.25, label=f"{server.ip_addr} to report no leader of {group_id}")


@pytest.mark.parametrize("mode", ["drop", "reset"])
async def test_isolated_live_leader_steps_down_and_history_stays_linearizable(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, mode: str, tmp_path) -> None:
    """The SC leader is cut off from both followers while it keeps running and keeps taking
    client requests. The majority elects a new leader, the old one notices on its own that it
    has no majority (fsm::tick_leader) and steps down, and the whole history, requests served
    by the old leader in the window included, is linearizable: a write the old leader
    accepted into its log but could not commit is either reported as failed/unknown to the
    client, or ends up applied exactly once through the new leader.

    Through the window the clients keep hitting every node, the cut-off one included. Its
    requests time out (the policy excuses that inside the window), the others' go on once the
    new leader is up. After the heal the old leader rejoins as a follower; from then on
    nothing may fail.

    `drop` leaves the old leader's connections hanging, `reset` fails them at once.

    The cut stays up until gossip on both sides has convicted the other, which drops the RPC
    connections between them (storage_proxy's connection_dropped, messaging_service): a heal
    before that would have the conviction land on a whole cluster.
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, request_timeout_ms=3000)
    leader, *followers = servers
    leader_id = host_ids[0]

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(PARTITION, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await ensure_raft_group_leader_on(manager, leader, group_id)
        logger.info(f"group {group_id}: leader {leader_id} on {leader.ip_addr}")

        watch = LeaderWatch(manager, followers[0], group_id)
        timings: dict[str, float] = {}
        outcome: dict[str, object] = {}

        async def partition_the_leader() -> None:
            await asyncio.sleep(5)
            async with workload.disruption_window(PARTITION) as window:
                t0 = time.monotonic()
                logger.info(f"Cutting the leader {leader.ip_addr} off from {[f.ip_addr for f in followers]} ({mode})")
                await network_proxy.isolate(leader.ip_addr, [f.ip_addr for f in followers], mode=mode)

                new_leader = await wait_for_new_leader(manager, followers[0], group_id, leader_id, LEADER_CHANGE_BOUND_S)
                timings["new_leader_s"] = time.monotonic() - t0
                outcome["new_leader"] = new_leader
                # The cut-off leader is still running and answers REST: it must have stepped down.
                await wait_for_no_leader(manager, leader, group_id, LEADER_CHANGE_BOUND_S)
                timings["stepdown_s"] = time.monotonic() - t0
                logger.info(f"new leader {new_leader} after {timings['new_leader_s']:.1f}s, "
                            f"old leader stepped down after {timings['stepdown_s']:.1f}s")

                # Keep serving through the cut until gossip has convicted the cut-off node on both
                # sides: requests keep reaching the old leader all along.
                await gather_safely(*(wait_for_down(manager, network_proxy, f, {leader.ip_addr}) for f in followers))
                await wait_for_down(manager, network_proxy, leader, {f.ip_addr for f in followers})
                timings["gossip_down_s"] = time.monotonic() - t0

                logger.info("Removing the cut")
                outcome["heal_t"] = time.monotonic()
                await network_proxy.heal()
                await wait_for_all_up(manager, network_proxy, servers)
                # Once healed, the old leader is an ordinary member again: Raft may well
                # elect it, or a leadership transfer may move the group back to it, so
                # the cluster only has to agree on some leader.
                await wait_for_one_agreed_leader(manager, servers, group_id, LEADER_CHANGE_BOUND_S)
                await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            outcome["window"] = window
            timings["window_s"] = (window.end_ns - window.start_ns) / 1e9
            # The tail: a whole cluster again, nothing may fail now.
            await asyncio.sleep(8)
            workload.stop_event.set()
            watch.stop.set()

        await wait_for_leader(manager, followers[0], group_id, expected_host_id=leader_id)
        errors = await run_workload(workload, cql, duration_s=45,
                                    disruptors=[("partition", partition_the_leader), ("leader-watch", watch.run)])
        logger.info(f"Stats: {workload.stats_line()} | timings {timings} | leaders seen by {followers[0].ip_addr}: {watch.leaders}")
        log_slowest_ops(workload)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"
        assert "window" in outcome, "the partition never completed"
        window = outcome["window"]

        assert timings["stepdown_s"] <= LEADER_CHANGE_BOUND_S
        assert outcome["new_leader"] in host_ids[1:]
        # Until the heal, the follower saw the old leader first, then never again: a split
        # vote may add an election, but leadership never goes back to the cut-off node while
        # it is cut off. After the heal it may (see above), so those changes do not count.
        distinct = watch.distinct_leaders(before=outcome["heal_t"])
        assert distinct[0] == leader_id and leader_id not in distinct[1:], f"leaders seen: {watch.changes}"
        assert distinct[-1] in host_ids[1:], f"leaders seen: {watch.changes}"

        in_window = workload.stats(within_ns=window.span_ns)
        logger.info(f"Inside the partition window ({timings['window_s']:.1f}s): {in_window}")
        # The majority kept serving while the old leader was cut off.
        assert in_window.write_ok >= 20 and in_window.reads >= 20, f"too little got through the window: {in_window}"
        # And the cut-off leader did fail some: otherwise the window exercised nothing on it.
        assert in_window.write_indeterminate + in_window.write_fail + in_window.read_fail >= 1, in_window

        after = workload.stats(within_ns=(window.end_ns, time.monotonic_ns()))
        assert after.write_ok >= 50 and after.reads >= 50, f"too little after the heal: {after}"

        workload.assert_progress(min_writes=200, min_reads=200)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")


BRIDGE_IDLE_XFAIL = (
    "raft grants a pre-vote while it follows a live leader (fsm::request_vote has no leader-lease check), so the "
    "cut-off follower deposes the leader through the bridge node's vote as soon as their logs are equal; see the "
    "test's docstring")


@pytest.mark.parametrize("load", [pytest.param("idle", marks=pytest.mark.xfail(strict=True, reason=BRIDGE_IDLE_XFAIL)), "loaded"])
async def test_bridge_partition_does_not_depose_a_leader_that_keeps_its_majority(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, load: str, tmp_path) -> None:
    """A partial partition: the leader A and the follower C cannot reach each other, while B
    reaches both. A keeps a majority (A, B) and must keep leading; C, which cannot hear A,
    gives up on it, campaigns, and must not be able to depose A through B's vote: raft's
    pre-vote exists precisely so that a node recovering from a partition cannot disrupt a
    working leader (Raft thesis, 4.2.3, "disruptive servers").

    Only a live, partial cut exercises this: a stopped node does not campaign, and the RPC
    injections that drop AppendEntries leave the failure detector's pings alone, so a
    follower keeps considering the leader alive and never starts an election
    (fsm::tick, has_stable_leader).

    `idle`: no client load, so the logs of B and C stay identical and B has no reason, other
    than hearing from A, to refuse C. `loaded`: the register workload runs through A and B,
    whose logs then grow past C's.

    The idle variant fails today, and is marked so: fsm::request_vote grants a pre-vote for a
    higher term whenever the candidate's log is up to date, without asking whether this node
    currently follows a live leader (the "leader lease" rule of Raft 4.2.3 that pre-vote is
    meant to be combined with). B grants C's pre-vote while following A, then the real vote,
    C leads in the next term, A learns the term from B's replies and steps down, and stays a
    candidate for the rest of the cut: B rejects its pre-votes because C's log, with the new
    term's entry, is now ahead. The majority-side leader is deposed by a node that cannot
    reach it. Under load the window closes by itself: C's log falls behind within the ~3s it
    takes C to give up on A, and B refuses it.
    """
    extra_cmdline = ['--logger-log-level', 'raft=trace'] if load == "idle" else []
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, extra_cmdline, request_timeout_ms=3000)
    a, b, c = servers
    a_id, b_id, c_id = host_ids
    host_a, host_b, host_c = hosts

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(PARTITION, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await ensure_raft_group_leader_on(manager, a, group_id)
        await wait_for_leader(manager, b, group_id, expected_host_id=a_id)
        await wait_for_leader(manager, c, group_id, expected_host_id=a_id)
        logger.info(f"group {group_id}: leader {a_id} on {a.ip_addr}; cutting {a.ip_addr} <-> {c.ip_addr}, {b.ip_addr} bridges")

        watch_b = LeaderWatch(manager, b, group_id)
        watch_c = LeaderWatch(manager, c, group_id)
        probe: dict[str, object] = {}

        async def bridge_partition() -> None:
            await asyncio.sleep(3)
            async with workload.disruption_window(PARTITION):
                t0 = time.monotonic()
                await network_proxy.partition(a.ip_addr, c.ip_addr)
                # C gives up on A: the failure detector, then its election timer. What it reports
                # then, nobody or itself, is what the assertions below are about.
                probe["c_reports"] = await wait_for_leader_change(manager, c, group_id, a_id, LEADER_CHANGE_BOUND_S)
                probe["c_gave_up_after_s"] = time.monotonic() - t0
                if load == "loaded":
                    # A request at C has nowhere to go: C knows no leader, and could not reach A anyway.
                    stmt = SimpleStatement(f"SELECT c FROM {workload.fqtn} WHERE pk = 0", consistency_level=ConsistencyLevel.QUORUM,
                                           retry_policy=FallthroughRetryPolicy())
                    try:
                        await cql.run_async(stmt, host=host_c)
                        probe["c_read"] = "ok"
                    except (ReadTimeout, ReadFailure, Unavailable, OperationTimedOut) as exc:
                        probe["c_read"] = exc
                # Long enough for a few of C's election timeouts to pass, and for gossip on A and C
                # to convict each other, so that the heal is a heal of a convicted cut.
                await wait_for_down(manager, network_proxy, a, {c.ip_addr})
                await wait_for_down(manager, network_proxy, c, {a.ip_addr})
                await asyncio.sleep(3)
                logger.info("Removing the cut")
                await network_proxy.heal()
                await wait_for_all_up(manager, network_proxy, servers)
                # Whoever it is: the assertions below say who it had to be.
                await wait_for_leader(manager, c, group_id, timeout=LEADER_CHANGE_BOUND_S)
                await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            await asyncio.sleep(5)
            workload.stop_event.set()
            watch_b.stop.set()
            watch_c.stop.set()

        if load == "loaded":
            session = PinnedSession(cql, [host_a, host_b])
            errors = await run_workload(workload, session, duration_s=40,
                                        disruptors=[("bridge", bridge_partition), ("watch-b", watch_b.run), ("watch-c", watch_c.run)])
            logger.info(f"Stats: {workload.stats_line()} | probe {probe}")
            log_slowest_ops(workload)
            assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"
            assert probe.get("c_read") != "ok", "a read at the cut-off follower succeeded without a leader"
        else:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(bridge_partition())
                tg.create_task(watch_b.run())
                tg.create_task(watch_c.run())

        logger.info(f"leaders seen by B: {watch_b.leaders}; by C: {watch_c.leaders}; probe {probe}")
        assert watch_b.distinct_leaders() == [a_id], (
            f"B saw the leadership leave A although A kept a majority with B: {watch_b.leaders}")
        # C ends up back under A, and never believed anyone else led.
        assert watch_c.distinct_leaders() == [a_id], f"C saw: {watch_c.leaders}"

        if load == "loaded":
            workload.assert_progress(min_writes=200, min_reads=200)
            # Pinned to A and B, which stayed connected: the window must have been free of failures.
            totals = workload.stats()
            assert totals.write_fail + totals.write_indeterminate + totals.read_fail + totals.read_indeterminate == 0, totals
            await check_linearizable(workload, output_dir=tmp_path / "porcupine")


async def test_isolated_coordinator_stores_hints_and_delivers_them_after_heal(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int) -> None:
    """A node cut off from both peers keeps taking CL=ONE writes for a regular table, as a
    coordinator and the only reachable replica. It stores a hint per unreachable replica
    (storage_proxy::hint_to_dead_endpoints), and once the cut is removed hinted handoff
    delivers them: the two other replicas then hold the rows without a read repair having
    to copy them over.

    The proof is in the metrics: `written` on the coordinator grows by two hints per write,
    the majority reads none of the rows while the cut is up, `sent_total` catches up with
    `written` after the heal, and a read at ALL afterwards returns every row without the
    coordinator's read-repair diff counter moving.
    """
    config = {'failure_detector_timeout_in_ms': failure_detector_timeout}
    servers = await manager.servers_add(3, config=config, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    a, b, c = servers
    host_a, _, host_c = hosts
    writes = 20

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            written_before = await metric(manager, c, "scylla_hints_manager_written")
            sent_before = await metric(manager, c, "scylla_hints_manager_sent_total")

            logger.info(f"Cutting {c.ip_addr} off from {a.ip_addr} and {b.ip_addr}")
            await network_proxy.isolate(c.ip_addr, [a.ip_addr, b.ip_addr])
            await wait_for_down(manager, network_proxy, c, {a.ip_addr, b.ip_addr})
            await wait_for_down(manager, network_proxy, a, {c.ip_addr})
            await wait_for_down(manager, network_proxy, b, {c.ip_addr})

            for pk in range(writes):
                await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk})", consistency_level=ConsistencyLevel.ONE),
                                    host=host_c)

            async def hints_written():
                written = await metric(manager, c, "scylla_hints_manager_written") - written_before
                return written if written >= 2 * writes else None
            written = await wait_for(hints_written, time.time() + 60, label=f"{2 * writes} hints written on {c.ip_addr}")
            logger.info(f"{c.ip_addr} wrote {written} hints")

            # The majority has none of it: a QUORUM read through A is answered by A and B.
            pks = ", ".join(str(pk) for pk in range(writes))
            rows = await cql.run_async(SimpleStatement(f"SELECT pk FROM {table} WHERE pk IN ({pks})", consistency_level=ConsistencyLevel.QUORUM),
                                       host=host_a)
            assert rows == [], f"the majority already has {[r.pk for r in rows]}"

            logger.info("Removing the cut")
            await network_proxy.heal()
            await wait_for_all_up(manager, network_proxy, servers)

            async def hints_sent():
                sent = await metric(manager, c, "scylla_hints_manager_sent_total") - sent_before
                return sent if sent >= written else None
            sent = await wait_for(hints_sent, time.time() + 120, label=f"{written} hints sent by {c.ip_addr}")
            logger.info(f"{c.ip_addr} sent {sent} hints")

            diff_before = await metric(manager, a, "scylla_storage_proxy_coordinator_read_repairs_diff_found_total")
            rows = await cql.run_async(SimpleStatement(f"SELECT pk, v FROM {table} WHERE pk IN ({pks})", consistency_level=ConsistencyLevel.ALL),
                                       host=host_a)
            assert sorted((r.pk, r.v) for r in rows) == [(pk, pk) for pk in range(writes)]
            diff_after = await metric(manager, a, "scylla_storage_proxy_coordinator_read_repairs_diff_found_total")
            assert diff_after == diff_before, f"the replicas disagreed at ALL: hints did not deliver the rows ({diff_before} -> {diff_after})"


async def test_one_way_cut_converges_to_down_both_ways_and_graceful_stop_completes(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int) -> None:
    """A link that works in one direction only: A's bytes to B are withheld, B's reach A.
    Neither node's echo gets a reply, so each marks the other DOWN
    (gossiper::failure_detector_loop_for_node), and that view holds steady: no flapping while
    the cut is up, and the third node sees nobody down. A then shuts down gracefully with the
    half-open peer still there: the shutdown must not wait on a connection to B that can never
    complete (SCYLLADB-4693), and the peers must see A leave. After the heal A comes back and
    everyone is UP.
    """
    config = {'failure_detector_timeout_in_ms': failure_detector_timeout}
    servers = await manager.servers_add(3, config=config, auto_rack_dc='dc1')
    cql, _ = await manager.get_ready_cql(servers)
    a, b, c = servers
    a_id, b_id, _ = await gather_safely(*(manager.get_host_id(s.server_id) for s in servers))
    await wait_for_all_up(manager, network_proxy, servers)
    await asyncio.sleep(1)
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]

    logger.info(f"Cutting packets from {a.ip_addr} to {b.ip_addr}; the other way stays open")
    await network_proxy.cut(a.ip_addr, b.ip_addr)
    await wait_for_down(manager, network_proxy, a, {b.ip_addr})
    await wait_for_down(manager, network_proxy, b, {a.ip_addr})
    converged_at = time.monotonic()
    assert await down_endpoints(manager, network_proxy, c) == set()

    # Steady: every sample over the next while shows the same view.
    while time.monotonic() - converged_at < 8:
        views = await gather_safely(*(down_endpoints(manager, network_proxy, s) for s in servers))
        assert views == [{b.ip_addr}, {a.ip_addr}, set()], f"views changed: {views}"
        await asyncio.sleep(0.5)

    transitions = []
    for s, log, mark in zip(servers, logs, marks):
        lines = await log.grep(r"InetAddress .* is now (UP|DOWN)", from_mark=mark)
        transitions.append([line.strip() for line, _ in lines])
        logger.info(f"{s.ip_addr} gossip transitions: {transitions[-1]}")
    # One DOWN on each side of the cut, nothing on the third node, and no UP anywhere.
    assert all("is now UP" not in line for t in transitions for line in t), transitions
    assert len(transitions[0]) == 1 and b_id in transitions[0][0], transitions[0]
    assert len(transitions[1]) == 1 and a_id in transitions[1][0], transitions[1]
    assert transitions[2] == [], transitions[2]

    logger.info(f"Gracefully stopping {a.ip_addr} with the cut in place")
    t0 = time.monotonic()
    await manager.server_stop_gracefully(a.server_id, timeout=120)
    stop_s = time.monotonic() - t0
    logger.info(f"{a.ip_addr} stopped in {stop_s:.1f}s")
    assert stop_s < 60, f"graceful stop took {stop_s:.1f}s"
    await wait_for_down(manager, network_proxy, b, {a.ip_addr})
    await wait_for_down(manager, network_proxy, c, {a.ip_addr})

    logger.info("Removing the cut and starting the node again")
    await network_proxy.heal()
    await manager.server_start(a.server_id)
    await wait_for_all_up(manager, network_proxy, servers)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)


async def test_lossy_links_keep_the_sc_leader_and_the_history_linearizable(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, tmp_path) -> None:
    """Every internode link "loses" 20% of its traffic in each direction while the register
    workload runs: the proxy stalls that share of the chunks the way TCP would repair lost
    packets, a quick repair for most, a retransmission timeout doubled for each repair lost
    in turn for the rest, which is what lost packets look like to the nodes. They see latency,
    not failures: no other node becomes the leader, no request fails, the history is
    linearizable, and once the loss stops everybody is UP for everybody. A follower may lose
    sight of the leader for a moment when a stall outlasts its election timeout (1s); it may
    not find another one.

    What 20% loss does cost is measured and logged rather than asserted away: a run of lost
    retransmits stalls a connection for seconds (TCP's RTO doubles each time), which the
    direct failure detector, with its 2s threshold, reports as a peer briefly dead, and which
    can hold a gossip echo past its deadline and flap a node DOWN and UP. The test tolerates
    such a flap but not a conviction that lasts: no node may be DOWN for two consecutive
    samples, and every "dead" mark must be undone by the end of the run.

    The disruption window covers the tail too: an echo that stalled under loss can time out
    and convict a peer seconds after the loss stopped, and the test counts that as part of
    the disruption rather than as a failure of a healthy cluster. The zero-failure assertion
    below is what keeps the test strict.
    """
    await lossy_links_scenario(manager, network_proxy, failure_detector_timeout, tmp_path)


async def lossy_links_scenario(manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int,
                               tmp_path, extra_config: Optional[dict] = None,
                               on_boot: Optional[Callable[[object, list], Awaitable[None]]] = None) -> None:
    """The body of test_lossy_links_keep_the_sc_leader_and_the_history_linearizable.

    `extra_config` goes to every node; `on_boot(cql, hosts)` runs once the cluster is up,
    before anything else (a check that the config took effect, say).
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, extra_config=extra_config)
    if on_boot is not None:
        await on_boot(cql, hosts)
    a, b, c = servers

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(LOSS, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await ensure_raft_group_leader_on(manager, a, group_id)
        leader_id = host_ids[0]
        await wait_for_leader(manager, b, group_id, expected_host_id=leader_id)

        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await log.mark() for log in logs]
        watch = LeaderWatch(manager, b, group_id)
        samples: list[list[set[str]]] = []

        async def lossy_links() -> None:
            await asyncio.sleep(3)
            async with workload.disruption_window(LOSS):
                for x, y in ((a, b), (a, c), (b, c)):
                    await network_proxy.partition(x.ip_addr, y.ip_addr, loss=20)
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    samples.append(await gather_safely(*(down_endpoints(manager, network_proxy, s) for s in servers)))
                    await asyncio.sleep(0.5)
                await network_proxy.heal()
                await asyncio.sleep(5)
                await wait_for_all_up(manager, network_proxy, servers)
            workload.stop_event.set()
            watch.stop.set()

        errors = await run_workload(workload, cql, duration_s=30, disruptors=[("loss", lossy_links), ("leader-watch", watch.run)])
        logger.info(f"Stats: {workload.stats_line()} | leaders seen by {b.ip_addr}: {watch.leaders}")
        log_slowest_ops(workload)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"

        flaps = [sample for sample in samples if any(sample)]
        logger.info(f"{len(flaps)} of {len(samples)} samples saw a node DOWN: {flaps}")
        for first, second in zip(samples, samples[1:]):
            for s, view_1, view_2 in zip(servers, first, second):
                assert not (view_1 & view_2), f"{s.ip_addr} kept {view_1 & view_2} DOWN across two samples under loss"
        for s, log, mark in zip(servers, logs, marks):
            dead = await log.grep(r"marking Raft server .* as dead for raft groups", from_mark=mark)
            alive = await log.grep(r"marking Raft server .* as alive for raft groups", from_mark=mark)
            logger.info(f"{s.ip_addr} direct FD under loss: {len(dead)} dead marks, {len(alive)} alive marks")
            assert len(alive) >= len(dead), f"{s.ip_addr} left a peer dead: {[line.strip() for line, _ in dead]}"
        assert watch.distinct_leaders() == [leader_id], f"the leader changed under loss: {watch.leaders}"
        logger.info(f"{b.ip_addr} lost sight of the leader {watch.leaders.count(None)} time(s) under loss")

        totals = workload.stats()
        assert totals.write_fail + totals.write_indeterminate + totals.read_fail + totals.read_indeterminate == 0, (
            f"requests failed under 20% loss: {totals}")
        workload.assert_progress(min_writes=200, min_reads=200)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")


@dataclass
class Fault:
    """One network fault of the random sequence: how to apply it, and what gossip must say before it is healed."""
    name: str
    apply: Callable[[], Awaitable[None]]
    # What each node's list of DOWN endpoints must become before the heal, per node; an empty
    # dict is a fault gossip is not expected to notice (loss, clients blocked).
    convicted: dict[str, set[str]]
    hold_s: float


# The menu: a node cut off from its peers, with hanging or reset connections; a pair of nodes cut from
# each other, both ways or one way; lossy links everywhere; a node unreachable for its clients only.
FAULT_KINDS = ["isolate-drop", "isolate-reset", "pair-partition", "one-way-cut", "loss", "clients-blocked"]


def random_fault(rng: random.Random, network_proxy: NetworkProxy, servers: list[ServerInfo], kind: str) -> Fault:
    """A fault of the given kind on nodes drawn at random."""
    x, y, z = rng.sample(servers, 3)
    if kind.startswith("isolate"):
        mode = kind.split("-")[1]
        return Fault(f"{kind} {x.ip_addr}", lambda: network_proxy.isolate(x.ip_addr, [y.ip_addr, z.ip_addr], mode=mode),
                     {x.ip_addr: {y.ip_addr, z.ip_addr}, y.ip_addr: {x.ip_addr}, z.ip_addr: {x.ip_addr}}, hold_s=6)
    if kind == "pair-partition":
        return Fault(f"{kind} {x.ip_addr}|{y.ip_addr}", lambda: network_proxy.partition(x.ip_addr, y.ip_addr),
                     {x.ip_addr: {y.ip_addr}, y.ip_addr: {x.ip_addr}, z.ip_addr: set()}, hold_s=6)
    if kind == "one-way-cut":
        # Gossip convicts both ways: the other side's echoes get no answer either.
        return Fault(f"{kind} {x.ip_addr}->{y.ip_addr}", lambda: network_proxy.cut(x.ip_addr, y.ip_addr),
                     {x.ip_addr: {y.ip_addr}, y.ip_addr: {x.ip_addr}, z.ip_addr: set()}, hold_s=6)
    if kind == "loss":
        percent = rng.randint(20, 30)

        async def lossy() -> None:
            for s, t in ((x, y), (x, z), (y, z)):
                await network_proxy.partition(s.ip_addr, t.ip_addr, loss=percent)
        return Fault(f"{kind} {percent}%", lossy, {}, hold_s=10)
    return Fault(f"{kind} {x.ip_addr}", lambda: network_proxy.block_clients(x.ip_addr, mode="reset"), {}, hold_s=8)


async def wait_for_one_agreed_leader(manager: ScyllaClusterManager, servers: list[ServerInfo], group_id: str,
                                     timeout: float) -> HostID:
    """Wait until every node reports the same, existing leader for the group."""
    async def agreed():
        leaders = set(await gather_safely(*(manager.api.get_raft_leader(s.ip_addr, group_id) for s in servers)))
        return leaders.pop() if len(leaders) == 1 and None not in leaders else None
    return await wait_for(agreed, time.time() + timeout, period=0.25, label=f"all nodes to agree on a leader of {group_id}")


async def test_random_network_faults_keep_the_history_linearizable(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, tmp_path) -> None:
    """Every kind of network fault once, in random order and on random nodes, each healed before the
    next, under the register workload: a node cut off from its peers with hanging or reset
    connections, a pair of nodes cut from each other both ways or one way, lossy links everywhere, a
    node cut off from its clients only. Each fault is held until gossip has convicted what it should (where it should
    notice at all), then healed, and the cluster must come back whole: everybody UP, one leader
    everybody agrees on, the driver connected to every node. Only then does the fault's window
    close, so a failure between two faults is a finding, and the whole history, the requests the
    faults disturbed included, must be linearizable.

    The faults are drawn with a fixed seed, so a run replays the same sequence; the interleaving
    with the clients is not reproducible, as with every test of this family.
    """
    await random_faults_scenario(manager, network_proxy, failure_detector_timeout, tmp_path)


async def random_faults_scenario(manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int,
                                 tmp_path, extra_config: Optional[dict] = None,
                                 on_boot: Optional[Callable[[object, list], Awaitable[None]]] = None) -> None:
    """The body of test_random_network_faults_keep_the_history_linearizable; the arguments as for
    lossy_links_scenario."""
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, request_timeout_ms=3000,
                                                          extra_config=extra_config)
    if on_boot is not None:
        await on_boot(cql, hosts)
    rng = random.Random(f"{SEED}/faults")
    kinds = list(FAULT_KINDS)
    rng.shuffle(kinds)
    faults = [random_fault(rng, network_proxy, servers, kind) for kind in kinds]
    rounds = len(faults)
    logger.info(f"fault sequence: {[f.name for f in faults]}")

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(FAULT, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await wait_for_one_agreed_leader(manager, servers, group_id, LEADER_CHANGE_BOUND_S)
        watch = LeaderWatch(manager, servers[0], group_id)
        done: list[dict] = []

        async def faults_one_by_one() -> None:
            await asyncio.sleep(3)
            for fault in faults:
                t0 = time.monotonic()
                async with workload.disruption_window(FAULT) as window:
                    logger.info(f"Applying fault: {fault.name}")
                    await fault.apply()
                    await gather_safely(*(wait_for_down(manager, network_proxy, s, fault.convicted[s.ip_addr]) for s in servers
                                          if s.ip_addr in fault.convicted))
                    convicted_s = time.monotonic() - t0
                    await asyncio.sleep(fault.hold_s)
                    logger.info(f"Healing: {fault.name}")
                    await network_proxy.heal()
                    await wait_for_all_up(manager, network_proxy, servers)
                    leader = await wait_for_one_agreed_leader(manager, servers, group_id, LEADER_CHANGE_BOUND_S)
                    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
                done.append({"fault": fault.name, "window_s": round((window.end_ns - window.start_ns) / 1e9, 1),
                             "convicted_s": round(convicted_s, 1), "leader_after": leader,
                             "in_window": workload.stats(within_ns=window.span_ns)})
                logger.info(f"Round done: {done[-1]}")
                # Whole again: nothing may fail from here to the next fault.
                await asyncio.sleep(4)
            await asyncio.sleep(8)
            workload.stop_event.set()
            watch.stop.set()

        errors = await run_workload(workload, cql, duration_s=300,
                                    disruptors=[("faults", faults_one_by_one), ("leader-watch", watch.run)])
        logger.info(f"Stats: {workload.stats_line()} | leaders seen by {servers[0].ip_addr}: {watch.leaders}")
        for round_info in done:
            logger.info(f"round: {round_info}")
        log_slowest_ops(workload)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"
        assert len(done) == rounds, f"only {len(done)} of {rounds} faults completed: {done}"

        for round_info in done:
            # Two of the three nodes were always connected to each other, so the group kept serving.
            in_window = round_info["in_window"]
            assert in_window.write_ok >= 5 and in_window.reads >= 5, f"too little got through during {round_info}"
            assert round_info["leader_after"] in host_ids, round_info

        last_window = workload.windows(FAULT)[-1]
        tail = workload.stats(within_ns=(last_window.end_ns, time.monotonic_ns()))
        assert tail.write_ok >= 30 and tail.reads >= 30, f"too little after the last heal: {tail}"
        assert await wait_for_one_agreed_leader(manager, servers, group_id, LEADER_CHANGE_BOUND_S) in host_ids
        await wait_for_all_up(manager, network_proxy, servers)

        workload.assert_progress(min_writes=300, min_reads=300)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")


async def test_delayed_links_keep_the_leader_and_cost_what_they_should(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, tmp_path) -> None:
    """Every internode link carries 150ms +/- 30ms of one-way delay for a while, under the
    register workload. That is well under raft's election timeout (1s) and both failure
    detectors' thresholds (2s direct, 5s gossip here), so nothing may fail, the leader must not
    change and the history must stay linearizable. What the delay costs is measured against the
    undelayed baseline of the same run: a write pays at least the append and its ack (two
    one-way delays, 300ms), and after the heal the latency must come back down.

    A packet filter cannot do this: it decides pass or drop per packet, it cannot hold one.
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, request_timeout_ms=3000)
    a, b, _ = servers
    leader_id = host_ids[0]

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(DELAY, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await ensure_raft_group_leader_on(manager, a, group_id)
        await wait_for_leader(manager, b, group_id, expected_host_id=leader_id)
        watch = LeaderWatch(manager, b, group_id)
        spans: dict[str, tuple[int, int]] = {}

        async def delayed_links() -> None:
            await asyncio.sleep(6)
            async with workload.disruption_window(DELAY) as window:
                for s in servers:
                    await network_proxy.delay(s.ip_addr, 150, jitter_ms=30, direction="upstream")
                await asyncio.sleep(8)
                logger.info(f"proxy state under delay: {await network_proxy.server.state()}")
                logger.info(f"so far in the window: {workload.stats(within_ns=(window.start_ns, time.monotonic_ns()))}")
                await asyncio.sleep(7)
                heal_ns = time.monotonic_ns()
                await network_proxy.heal()
                # Chunks already held back are delivered late; keep them inside the window.
                await asyncio.sleep(1)
            spans["before"] = (0, window.start_ns)
            spans["during"] = (window.start_ns, heal_ns)
            await asyncio.sleep(8)
            spans["after"] = (window.end_ns, time.monotonic_ns())
            workload.stop_event.set()
            watch.stop.set()

        errors = await run_workload(workload, cql, duration_s=40, disruptors=[("delay", delayed_links), ("leader-watch", watch.run)])
        latency = {name: median_latency_s(workload, span) for name, span in spans.items()}
        histograms = {name: latency_histogram(latencies_s(workload, span)) for name, span in spans.items()}
        logger.info(f"Stats: {workload.stats_line()} | median latency {latency} | histograms {histograms} | "
                    f"leaders seen by {b.ip_addr}: {watch.leaders}")
        log_slowest_ops(workload)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"
        assert spans.keys() == {"before", "during", "after"}, "the delay window never completed"

        assert watch.leaders == [leader_id], f"the leader changed under delay: {watch.leaders}"
        totals = workload.stats()
        assert totals.write_fail + totals.write_indeterminate + totals.read_fail + totals.read_indeterminate == 0, (
            f"requests failed under 150ms delay: {totals}")
        assert latency["during"] >= latency["before"] + 0.25, f"the delay did not show in the latency: {latency}"
        assert latency["after"] <= latency["during"] - 0.2, f"the latency did not come back down after the heal: {latency}"
        workload.assert_progress(min_writes=200, min_reads=200)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")


async def test_slow_follower_does_not_slow_the_group(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, tmp_path) -> None:
    """One follower's links carry 200ms of delay each way; the leader and the other follower
    stay fast. Raft commits with the fast majority, so the workload, pinned to the two fast
    nodes, must not slow down by more than a fraction of the delay, and requests through the
    slow follower, probed separately, pay the forward to the leader and the reply (two delays).
    The slow follower keeps hearing the leader's heartbeats, late but regularly spaced, and its
    pings to the peers (400ms round trip) stay under the direct failure detector's timeout
    (direct_failure_detector_ping_timeout_in_ms, 600ms), so it never gives up on the leader:
    no election, the leader does not change, nothing fails, and the history is linearizable.
    See test_follower_beyond_the_ping_timeout_loses_the_leader_and_rejoins for the other side
    of that line.
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, request_timeout_ms=3000)
    a, b, c = servers
    host_a, host_b, host_c = hosts
    leader_id = host_ids[0]

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(DELAY, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await ensure_raft_group_leader_on(manager, a, group_id)
        await wait_for_leader(manager, c, group_id, expected_host_id=leader_id)
        watch_c = LeaderWatch(manager, c, group_id)
        spans: dict[str, tuple[int, int]] = {}
        probe: dict[str, float] = {}
        selects = [f"SELECT c FROM {workload.fqtn} WHERE pk = {pk}" for pk in range(10)]

        async def slow_follower() -> None:
            await asyncio.sleep(6)
            probe["via_slow_before"] = await median_s(cql, host_c, selects)
            async with workload.disruption_window(DELAY) as window:
                await network_proxy.delay(c.ip_addr, 200, direction="both")
                await asyncio.sleep(3)
                try:
                    probe["via_slow_during"] = await median_s(cql, host_c, selects)
                except Exception as exc:  # noqa: BLE001 - recorded, the assertions below judge it
                    probe["via_slow_during_error"] = repr(exc)
                logger.info(f"proxy state under delay: {await network_proxy.server.state()}")
                logger.info(f"so far in the window: {workload.stats(within_ns=(window.start_ns, time.monotonic_ns()))}")
                await asyncio.sleep(8)
                await network_proxy.heal()
                await asyncio.sleep(1)
            spans["before"] = (0, window.start_ns)
            spans["during"] = window.span_ns
            await asyncio.sleep(6)
            workload.stop_event.set()
            watch_c.stop.set()

        session = PinnedSession(cql, [host_a, host_b])
        errors = await run_workload(workload, session, duration_s=40, disruptors=[("slow-follower", slow_follower), ("watch-c", watch_c.run)])
        latency = {name: median_latency_s(workload, span) for name, span in spans.items()}
        logger.info(f"Stats: {workload.stats_line()} | fast side median latency {latency} | probe via the slow follower {probe} "
                    f"| leaders seen by {c.ip_addr}: {watch_c.leaders}")
        log_slowest_ops(workload)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"
        assert spans.keys() == {"before", "during"}, "the delay window never completed"

        assert watch_c.leaders == [leader_id], f"the slow follower lost or changed the leader: {watch_c.leaders}"
        totals = workload.stats()
        assert totals.write_fail + totals.write_indeterminate + totals.read_fail + totals.read_indeterminate == 0, totals
        assert latency["during"] <= latency["before"] + 0.15, f"a slow follower slowed the fast majority: {latency}"
        assert "via_slow_during" in probe, f"a request through the slow follower failed: {probe}"
        assert probe["via_slow_during"] >= probe["via_slow_before"] + 0.35, f"requests through the slow follower did not pay the delay: {probe}"
        workload.assert_progress(min_writes=200, min_reads=200)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")


async def test_follower_beyond_the_ping_timeout_loses_the_leader_and_rejoins(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, tmp_path) -> None:
    """One follower's links carry 400ms of delay each way: an 800ms round trip, beyond the
    direct failure detector's ping timeout (direct_failure_detector_ping_timeout_in_ms, 600ms),
    while gossip's detector (5s here) is content. Every ping of the slow follower times out, so
    2s later it marks both peers dead for raft and drops its leader ("marking Raft server ...
    as dead for raft groups"), and it cannot win one back: its log is behind the others'. From
    then on a request at that node has no leader to go to and times out, for as long as the
    delay lasts; the fast majority is unaffected (the workload runs through it with nothing
    failing), and once the delay is gone the follower marks its peers alive and follows the
    same leader again. Documented behaviour of the detector, pinned here so that a change to
    the timeout or to how a follower treats a dead leader shows up.
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, request_timeout_ms=3000)
    a, b, c = servers
    host_a, host_b, host_c = hosts
    leader_id = host_ids[0]

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(DELAY, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await ensure_raft_group_leader_on(manager, a, group_id)
        await wait_for_leader(manager, c, group_id, expected_host_id=leader_id)
        watch_b = LeaderWatch(manager, b, group_id)
        watch_c = LeaderWatch(manager, c, group_id)
        log_c = await manager.server_open_log(c.server_id)
        mark_c = await log_c.mark()
        timings: dict[str, float] = {}
        probe: dict[str, object] = {}

        async def far_follower() -> None:
            await asyncio.sleep(5)
            async with workload.disruption_window(DELAY):
                t0 = time.monotonic()
                await network_proxy.delay(c.ip_addr, 400, direction="both")
                await wait_for_no_leader(manager, c, group_id, LEADER_CHANGE_BOUND_S)
                timings["leaderless_s"] = time.monotonic() - t0
                stmt = SimpleStatement(f"SELECT c FROM {workload.fqtn} WHERE pk = 0", retry_policy=FallthroughRetryPolicy())
                try:
                    await cql.run_async(stmt, host=host_c)
                    probe["read_at_c"] = "ok"
                except (ReadTimeout, ReadFailure, Unavailable, OperationTimedOut) as exc:
                    probe["read_at_c"] = type(exc).__name__
                await asyncio.sleep(5)
                await network_proxy.heal()
                await wait_for_leader(manager, c, group_id, expected_host_id=leader_id, timeout=LEADER_CHANGE_BOUND_S)
                timings["rejoined_s"] = time.monotonic() - t0
                await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            await asyncio.sleep(6)
            workload.stop_event.set()
            watch_b.stop.set()
            watch_c.stop.set()

        session = PinnedSession(cql, [host_a, host_b])
        errors = await run_workload(workload, session, duration_s=60,
                                    disruptors=[("far-follower", far_follower), ("watch-b", watch_b.run), ("watch-c", watch_c.run)])
        dead = [line.strip() for line, _ in await log_c.grep(r"marking Raft server .* as dead for raft groups", from_mark=mark_c)]
        alive = [line.strip() for line, _ in await log_c.grep(r"marking Raft server .* as alive for raft groups", from_mark=mark_c)]
        logger.info(f"Stats: {workload.stats_line()} | timings {timings} | probe {probe} | leaders seen by B: {watch_b.leaders}; "
                    f"by C: {watch_c.leaders} | C marked dead: {len(dead)}, alive: {len(alive)}")
        log_slowest_ops(workload)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"
        assert "rejoined_s" in timings, f"the far follower never rejoined: {timings}"

        assert len(dead) >= 2 and len(alive) >= len(dead), f"C's direct failure detector: dead {dead}, alive {alive}"
        assert probe["read_at_c"] != "ok", "a read at the leaderless follower succeeded"
        assert watch_b.leaders == [leader_id], f"the majority side changed its leader: {watch_b.leaders}"
        assert watch_c.distinct_leaders() == [leader_id] and None in watch_c.leaders, f"C saw: {watch_c.leaders}"
        totals = workload.stats()
        assert totals.write_fail + totals.write_indeterminate + totals.read_fail + totals.read_indeterminate == 0, (
            f"the fast majority failed requests because of a far follower: {totals}")
        workload.assert_progress(min_writes=200, min_reads=200)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")


async def test_flapping_cuts_keep_the_history_linearizable(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, tmp_path) -> None:
    """Short cuts in a row: a node drawn at random is cut off from both peers for 2s, healed,
    and left alone for 3s, eight times, hanging and reset connections alternating. 2s is too
    short for gossip to convict anyone (12s and more here), but long enough for raft's direct
    failure detector and election timer, so a flap that hits the leader may cost an election,
    and the next flap comes before the group has settled. Everything inside the flapping is
    excused; once it stops the cluster must come back whole (one leader everybody agrees on,
    everybody UP, the driver connected to all) and from then on nothing may fail; the whole
    history is linearizable.
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, request_timeout_ms=3000)
    rng = random.Random(f"{SEED}/flaps")

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(FLAP, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await wait_for_one_agreed_leader(manager, servers, group_id, LEADER_CHANGE_BOUND_S)
        watch = LeaderWatch(manager, servers[0], group_id)
        flaps: list[str] = []
        outcome: dict[str, object] = {}

        async def flapping() -> None:
            await asyncio.sleep(4)
            async with workload.disruption_window(FLAP) as window:
                for i in range(8):
                    node = rng.choice(servers)
                    peers = [s.ip_addr for s in servers if s is not node]
                    mode = "drop" if i % 2 == 0 else "reset"
                    flaps.append(f"{mode} {node.ip_addr}")
                    await network_proxy.isolate(node.ip_addr, peers, mode=mode)
                    await asyncio.sleep(2)
                    await network_proxy.heal()
                    await asyncio.sleep(3)
                outcome["leader"] = await wait_for_one_agreed_leader(manager, servers, group_id, LEADER_CHANGE_BOUND_S)
                await wait_for_all_up(manager, network_proxy, servers)
                await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            outcome["window"] = window
            await asyncio.sleep(8)
            workload.stop_event.set()
            watch.stop.set()

        errors = await run_workload(workload, cql, duration_s=90, disruptors=[("flaps", flapping), ("leader-watch", watch.run)])
        logger.info(f"Stats: {workload.stats_line()} | flaps {flaps} | leaders seen by {servers[0].ip_addr}: {watch.leaders}")
        log_slowest_ops(workload)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"
        assert "window" in outcome, "the flapping never completed"
        window = outcome["window"]
        assert outcome["leader"] in host_ids

        in_window = workload.stats(within_ns=window.span_ns)
        logger.info(f"Inside the flapping window: {in_window}; distinct leaders: {watch.distinct_leaders()}")
        assert in_window.write_ok >= 50 and in_window.reads >= 50, f"too little got through the flaps: {in_window}"
        tail = workload.stats(within_ns=(window.end_ns, time.monotonic_ns()))
        assert tail.write_ok >= 30 and tail.reads >= 30, f"too little after the flaps: {tail}"
        workload.assert_progress(min_writes=300, min_reads=300)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")


async def test_leader_cut_off_from_its_clients_keeps_serving_through_its_followers(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, tmp_path) -> None:
    """The SC leader's CQL ports are closed to its clients (connections reset) for 10s while its
    peers still reach it. The driver marks the node down and sends everything to the followers,
    which forward each request to the leader over the internode link: the group is whole, so the
    leader must not change, the forwarded requests must go through, and once the driver has
    moved on nothing may fail. After the heal the driver reconnects to the leader. The history,
    the requests that died with their connections included, is linearizable.
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, request_timeout_ms=3000)
    leader, *followers = servers
    leader_id = host_ids[0]

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(CLIENTS, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await ensure_raft_group_leader_on(manager, leader, group_id)
        await wait_for_leader(manager, followers[0], group_id, expected_host_id=leader_id)
        watch = LeaderWatch(manager, followers[0], group_id)
        driver_host = cql.cluster.metadata.get_host(leader.rpc_address)
        assert driver_host is not None, f"the driver knows no host at {leader.rpc_address}"
        timings: dict[str, float] = {}
        outcome: dict[str, object] = {}

        async def host_is(up: bool):
            return True if driver_host.is_up == up else None

        async def block_the_leaders_clients() -> None:
            await asyncio.sleep(5)
            async with workload.disruption_window(CLIENTS) as window:
                t0 = time.monotonic()
                await network_proxy.block_clients(leader.ip_addr, mode="reset")
                await wait_for(lambda: host_is(False), time.time() + 30, period=0.1, label=f"the driver to mark {leader.rpc_address} down")
                timings["driver_down_s"] = time.monotonic() - t0
                await asyncio.sleep(10)
                await network_proxy.heal()
                await wait_for(lambda: host_is(True), time.time() + 60, period=0.2, label=f"the driver to mark {leader.rpc_address} up")
                await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            outcome["window"] = window
            await asyncio.sleep(8)
            workload.stop_event.set()
            watch.stop.set()

        errors = await run_workload(workload, cql, duration_s=45,
                                    disruptors=[("block-clients", block_the_leaders_clients), ("leader-watch", watch.run)])
        logger.info(f"Stats: {workload.stats_line()} | timings {timings} | leaders seen by {followers[0].ip_addr}: {watch.leaders}")
        log_slowest_ops(workload)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"
        assert "window" in outcome, "the cut never completed"
        window = outcome["window"]

        assert watch.leaders == [leader_id], f"the leader changed although its peers still reached it: {watch.leaders}"
        in_window = workload.stats(within_ns=window.span_ns)
        logger.info(f"Inside the window: {in_window} (driver marked the leader down after {timings['driver_down_s']:.1f}s)")
        # The followers forwarded for the whole window: plenty got through.
        assert in_window.write_ok >= 50 and in_window.reads >= 50, f"too little got through via the followers: {in_window}"
        after = workload.stats(within_ns=(window.end_ns, time.monotonic_ns()))
        assert after.write_ok >= 30 and after.reads >= 30, f"too little after the heal: {after}"
        workload.assert_progress(min_writes=200, min_reads=200)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")
