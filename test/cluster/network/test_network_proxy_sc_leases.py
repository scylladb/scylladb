#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""LeaseGuard leader leases of strongly-consistent (SC) tables under live network partitions.

With leases on (strong_consistency_enable_leases), the leader of an SC tablet group serves a
linearizable read locally while its lease is valid: its newest committed entry is from its
own term and definitely younger than the lease duration Δ. A new leader defers committing
until the newest entry of the previous terms in its log is definitely older than Δ, so that
a deposed leader which still believes it leads can no longer serve a stale read by then
(docs/dev/strong_consistency.md, "Leader leases").

Both halves are about a deposed leader that is *alive*: it keeps its timers and keeps
taking client requests. A stopped or killed leader serves nothing, so it tests neither. The
per-node proxies of ``test.pylib.network_proxy`` give exactly such a leader. The scenarios
here are the partition family of test_network_proxy_sc.py, run with leases on, plus the checks
that only make sense with leases:

* the cut-off leader keeps answering reads, locally, for a while after the cut, and every
  one of those reads is part of a history that must stay linearizable;
* the majority's first acknowledged write after the cut comes no earlier than Δ after it;
* lease renewals, which a read-only load makes the leader append, keep a follower that
  cannot hear the leader from deposing it through a bridge node (the pre-vote hole that the
  idle variant of the bridge test in test_network_proxy_sc.py reproduces);
* reads forwarded to a leader that its clients cannot reach take the lease path.

Every test is parametrized over leases off and on. The off variants run on any build and
pin today's behavior, which the on variants are compared against. The on variants skip:

* when the scylla binary does not know the lease options (built without
  scylladb/scylladb#30777, which wires LeaseGuard into SC tables), detected after boot
  through system.config, since scylla only warns about an unknown option;
* when the host's kernel clock cannot bound its error tightly enough for the lease
  duration the test uses (the default adjtimex backend reads ntp_adjtime(2); the nodes and
  the test share the host clock).
"""

from __future__ import annotations

import asyncio
import ctypes
import ctypes.util
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Literal, Optional

import pytest
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement

from test.cluster.strong_consistency.outcomes import Outcome, only_during, tolerate_reboot
from test.cluster.strong_consistency.register_workload import RegisterWorkload, check_linearizable, run_workload
from test.cluster.network.test_network_proxy_sc import (
    BRIDGE_IDLE_XFAIL, LEADER_CHANGE_BOUND_S, PARTITION, SC_KS_OPTS, SEED, LeaderWatch, PinnedSession, boot_sc_cluster,
    log_slowest_ops, lossy_links_scenario, random_faults_scenario, wait_for_all_up, wait_for_down, wait_for_leader_change,
    median_s, wait_for_new_leader, wait_for_no_leader, wait_for_one_agreed_leader)
from test.cluster.test_strong_consistency import get_table_raft_group_id, wait_for_leader
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerInfo
from test.pylib.network_proxy import NetworkProxy
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.skip_types import skip_env
from test.pylib.util import gather_safely, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

Leases = Literal["off", "on"]
LEASES = ["off", "on"]

CLIENTS_BLOCKED = "clients-blocked"

# The lease duration of the tests that do not need a particular one: 3x the default (1s),
# so that a host whose NTP daemon updates the kernel every few minutes still has the clock
# error budget below, and renewals (every Δ/2 under a read-only load) still come faster
# than a follower cut off from the leader gives up on it (failure detector, then an
# election timeout: 3s and more).
LEASE_MS = 3000

# The lease duration of the deferred-commit test: longer than it takes the majority to
# elect a new leader after a cut (LEADER_CHANGE_BOUND_S says ~5s), so that the commit
# deferral, not the election, decides when the first write after the cut is acknowledged.
# 8s is the longest duration scylla takes without a warning.
LONG_LEASE_MS = 8000

# A lease is usable only while its age plus twice the clock's error bound is under Δ
# (raft::time_bounds::younger_than). The tests want the error to take a small part of it.
LEASE_CLOCK_MAX_ERROR_FRACTION = 0.25

# ntp_adjtime(2) reports TIME_ERROR (sys/timex.h) while the kernel clock is not synchronized.
_TIME_ERROR = 5


class _Timex(ctypes.Structure):
    """struct timex as ntp_adjtime(2) fills it in; only `maxerror` is read, the rest gives it its size."""
    _fields_ = [("modes", ctypes.c_uint), ("offset", ctypes.c_long), ("freq", ctypes.c_long), ("maxerror", ctypes.c_long),
                ("esterror", ctypes.c_long), ("status", ctypes.c_int), ("constant", ctypes.c_long),
                ("precision", ctypes.c_long), ("tolerance", ctypes.c_long), ("time_sec", ctypes.c_long),
                ("time_usec", ctypes.c_long), ("tick", ctypes.c_long), ("ppsfreq", ctypes.c_long), ("jitter", ctypes.c_long),
                ("shift", ctypes.c_int), ("stabil", ctypes.c_long), ("jitcnt", ctypes.c_long), ("calcnt", ctypes.c_long),
                ("errcnt", ctypes.c_long), ("stbcnt", ctypes.c_long), ("tai", ctypes.c_int), ("_pad", ctypes.c_char * 44)]


def require_lease_capable_clock(lease_ms: int) -> None:
    """Skip unless the host's kernel clock is synchronized and its error fits a lease of `lease_ms`.

    The same reading the adjtimex backend takes (service/raft/bounded_clock_adjtimex.cc in
    #30777); the nodes run on this host, so it is theirs too.
    """
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    tx = _Timex()
    state = libc.ntp_adjtime(ctypes.byref(tx))
    if state < 0 or state == _TIME_ERROR:
        skip_env(f"host clock is not synchronized (ntp_adjtime state {state}), LeaseGuard cannot hold a lease")
    budget_us = lease_ms * 1000 * LEASE_CLOCK_MAX_ERROR_FRACTION
    if 2 * tx.maxerror > budget_us:
        skip_env(f"host clock error too wide for a {lease_ms}ms lease: 2*maxerror={2 * tx.maxerror}us > {int(budget_us)}us")


def lease_config(leases: Leases, lease_ms: int) -> dict:
    """The node config of a variant. Nothing at all for `off`, so that variant runs on builds without leases."""
    if leases == "off":
        return {}
    require_lease_capable_clock(lease_ms)
    return {'strong_consistency_enable_leases': True, 'strong_consistency_lease_duration_in_ms': lease_ms}


async def require_leases_enabled(cql, hosts: list) -> None:
    """Skip unless every node runs with leases on.

    Scylla only warns about an option it does not know, so a build without #30777 boots
    with leases silently off; system.config has no row for the option then.
    """
    for host in hosts:
        rows = await cql.run_async("SELECT value FROM system.config WHERE name = 'strong_consistency_enable_leases'", host=host)
        if not rows:
            pytest.skip("this scylla build has no LeaseGuard for SC tables (scylladb/scylladb#30777)")
        assert rows[0].value == "true", f"{host} runs with strong_consistency_enable_leases={rows[0].value}"


def boot_check(leases: Leases):
    """The on_boot hook of the shared scenarios: the lease check for the `on` variant, nothing for `off`."""
    async def check(cql, hosts) -> None:
        if leases == "on":
            await require_leases_enabled(cql, hosts)
    return check


@dataclass
class Op:
    """One operation of the recorded history: call and return times (monotonic ns), None when it never returned."""
    client_id: int
    op: str
    key: int
    value: int
    call_ns: int
    return_ns: Optional[int]
    status: Optional[str]


def history_ops(workload: RegisterWorkload) -> list[Op]:
    """The operations of the workload's history, paired up, in call order."""
    calls: dict[int, dict] = {}
    returns: dict[int, dict] = {}
    for line in workload.history.to_jsonl().splitlines():
        event = json.loads(line)
        (calls if event["kind"] == "call" else returns)[event["id"]] = event
    ops = []
    for op_id, call in calls.items():
        ret = returns.get(op_id)
        ops.append(Op(call["client_id"], call["op"], call["key"], ret["value"] if ret else call["value"], call["time_ns"],
                      ret["time_ns"] if ret else None, ret["status"] if ret else None))
    return ops


@dataclass
class PinnedReader:
    """Reads of random keys through one node only, recorded into the workload's history under their own client id.

    The workload's own clients go through the session the test gives it; this one is how a
    test keeps asking one particular node, the cut-off leader, and still has every answer
    checked by Porcupine with the rest.
    """
    workload: RegisterWorkload
    cql: object
    host: object
    client_id: int
    stop: asyncio.Event = field(default_factory=asyncio.Event)

    async def run(self) -> None:
        rng = self.workload.rng_for(f"pinned-reader/{self.client_id}")
        while not self.stop.is_set() and not self.workload.stop_event.is_set():
            pk = rng.randint(0, self.workload.num_keys - 1)
            op_id, t_call_ns = self.workload.history.record_call(self.client_id, "read", pk)
            try:
                rows = await self.cql.run_async(self.workload.read_stmt.bind([pk]), host=self.host)
                value = 0 if not rows or rows[0].c is None else rows[0].c
                self.workload.history.record_return(op_id, self.client_id, "read", pk, value, Outcome.OK)
            except Exception as exc:
                self.workload.record_failure(exc, op_id=op_id, client_id=self.client_id, op="read", key=pk, value=0,
                                             t_call_ns=t_call_ns)
            await asyncio.sleep(rng.uniform(*self.workload.dml_pause_s))


# Cuts the old leader's side (the leader first, then the followers cut off with it) from the rest.
Cut = Callable[[NetworkProxy, list[ServerInfo], list[ServerInfo]], Awaitable[None]]

SC_KS_OPTS_RF5 = SC_KS_OPTS.replace("'replication_factor': 3", "'replication_factor': 5")

async def deposed_leader_scenario(manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int,
                                  leases: Leases, tmp_path, *, cut: Cut, nodes: int = 3, cut_off: int = 1,
                                  lease_ms: int = LONG_LEASE_MS) -> None:
    """The SC leader A and `cut_off - 1` followers are cut from the other nodes by `cut`, and keep
    running; one extra client per cut-off node keeps reading through it all along, the register
    workload writes and reads through the other side, which holds the majority.

    With leases off, a read on the cut-off side needs a quorum read barrier, so from the cut
    until the heal that side answers no read at all. With leases on, A still holds a lease
    (writes renewed it until the cut) and answers reads locally, its own and the ones its
    cut-off followers forward to it, until it notices that it has no majority and steps down,
    which is about an election timeout. Those answers come from a side that can no longer
    learn about new writes, so they are exactly the reads LeaseGuard has to make safe: the new
    leader must not commit anything until A's lease, which is no newer than the newest entry
    of A's that reached the majority's logs, has expired. The history, the cut-off side's
    reads included, must be linearizable, and the first write the majority acknowledges after
    the cut comes no earlier than Δ after it.

    Δ is 8s by default, longer than the election takes, so that a missing deferral shows up
    as a first write ~5s after the cut instead of ~8s. A steps down by check-quorum before
    the new leader is elected.

    A answers no read that starts after the new leader's first acknowledged write: such a
    read could only come from a lease that should have expired.
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, request_timeout_ms=3000,
                                                          extra_config=lease_config(leases, lease_ms), nodes=nodes)
    if leases == "on":
        await require_leases_enabled(cql, hosts)
    leader = servers[0]
    leader_id = host_ids[0]
    side, rest = servers[:cut_off], servers[cut_off:]
    side_ips, rest_ips = {s.ip_addr for s in side}, {s.ip_addr for s in rest}
    ks_opts = SC_KS_OPTS if nodes == 3 else SC_KS_OPTS_RF5
    assert nodes in (3, 5), "the keyspace options cover three and five nodes"

    async with new_test_keyspace(manager, ks_opts) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(PARTITION, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await ensure_raft_group_leader_on(manager, leader, group_id)
        await gather_safely(*(wait_for_leader(manager, s, group_id, expected_host_id=leader_id) for s in servers[1:]))
        logger.info(f"group {group_id}: leader {leader_id} on {leader.ip_addr}, cut off with {sorted(side_ips - {leader.ip_addr})}, "
                    f"leases {leases} (delta {lease_ms}ms)")

        readers = [PinnedReader(workload, cql, hosts[i], client_id=workload.reset_client_id + 1 + i) for i in range(cut_off)]
        watch = LeaderWatch(manager, rest[0], group_id)
        marks: dict[str, int] = {}
        timings: dict[str, float] = {}

        async def depose_the_leader() -> None:
            await asyncio.sleep(5)
            async with workload.disruption_window(PARTITION):
                marks["cut_ns"] = time.monotonic_ns()
                await cut(network_proxy, side, rest)
                marks["cut_done_ns"] = time.monotonic_ns()
                new_leader = await wait_for_new_leader(manager, rest[0], group_id, leader_id, LEADER_CHANGE_BOUND_S)
                marks["new_leader_ns"] = time.monotonic_ns()
                timings["new_leader_s"] = (marks["new_leader_ns"] - marks["cut_ns"]) / 1e9
                await wait_for_no_leader(manager, leader, group_id, LEADER_CHANGE_BOUND_S)
                timings["stepdown_s"] = (time.monotonic_ns() - marks["cut_ns"]) / 1e9
                logger.info(f"new leader {new_leader} after {timings['new_leader_s']:.1f}s, "
                            f"{leader.ip_addr} stepped down after {timings['stepdown_s']:.1f}s")
                # Hold the cut past the deferral, so that it plays out while A is still cut off,
                # and until gossip has convicted the other side on both sides (a one-way cut
                # convicts both ways too: the echoes of one side get no answer).
                hold_until_ns = marks["cut_ns"] + int((lease_ms / 1000 + 3) * 1e9)
                await asyncio.sleep(max(0.0, (hold_until_ns - time.monotonic_ns()) / 1e9))
                await gather_safely(*(wait_for_down(manager, network_proxy, s, side_ips) for s in rest),
                                    *(wait_for_down(manager, network_proxy, s, rest_ips) for s in side))
                marks["heal_ns"] = time.monotonic_ns()
                await network_proxy.heal()
                await wait_for_all_up(manager, network_proxy, servers)
                # Healed, A may be elected again: the cluster only has to agree on a leader.
                await wait_for_one_agreed_leader(manager, servers, group_id, LEADER_CHANGE_BOUND_S)
                await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            await asyncio.sleep(8)
            workload.stop_event.set()
            watch.stop.set()

        errors = await run_workload(workload, PinnedSession(cql, hosts[cut_off:]), duration_s=60,
                                    disruptors=[("depose", depose_the_leader), ("leader-watch", watch.run)]
                                    + [(f"reader-at-{side[i].ip_addr}", r.run) for i, r in enumerate(readers)])
        logger.info(f"Stats: {workload.stats_line()} | timings {timings} | leaders seen by {rest[0].ip_addr}: {watch.leaders}")
        log_slowest_ops(workload)
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"
        assert "heal_ns" in marks, "the partition never completed"
        cut_ns, heal_ns = marks["cut_ns"], marks["heal_ns"]

        ops = history_ops(workload)
        # Only requests sent after the cut had surely taken effect count: the proxy rules are
        # in place when the cut returns, not when it is called (a request sent in between
        # goes through, a run without leases saw a write acknowledged "0.00s after the
        # cut"), and the margin covers a request that was already on its way.
        cut_settled_ns = marks["cut_done_ns"] + int(0.2e9)
        served: dict[str, list[Op]] = {}
        for i, reader in enumerate(readers):
            served[side[i].ip_addr] = [op for op in ops if op.client_id == reader.client_id and op.status == Outcome.OK
                                       and op.call_ns >= cut_settled_ns and op.return_ns < heal_ns]
        for ip, answered in served.items():
            last_s = max(((op.return_ns - cut_ns) / 1e9 for op in answered), default=None)
            logger.info(f"{ip} answered {len(answered)} reads while cut off, the last one {last_s}s after the cut")
        served_cut_off = [op for answered in served.values() for op in answered]

        writes_after_cut = [op for op in ops if op.op == "write" and op.status == Outcome.OK and op.call_ns >= cut_settled_ns]
        assert writes_after_cut, "the majority acknowledged no write after the cut"
        first_write_ns = min(op.return_ns for op in writes_after_cut)
        first_write_s = (first_write_ns - cut_ns) / 1e9
        # The lease safety property itself, independent of the values read: the cut-off side
        # serves nothing that starts once the majority has committed past it.
        late = [op for op in served_cut_off if op.call_ns > first_write_ns]
        assert not late, (f"the cut-off side answered {len(late)} read(s) that started after the new leader's first "
                          f"acknowledged write, e.g. {late[:3]}")
        logger.info(f"first write acknowledged after the cut: {first_write_s:.2f}s after it "
                    f"(new leader after {timings['new_leader_s']:.1f}s, delta {lease_ms / 1000}s)")

        if leases == "off":
            assert not served_cut_off, f"the cut-off side answered reads without a quorum: {served_cut_off[:5]}"
        else:
            assert served[leader.ip_addr], f"{leader.ip_addr} answered no read from its lease after the cut"
            # The deposed lease is no newer than the newest entry of A's in the majority's
            # logs, which is no earlier than the cut; 1s covers the clock error and polling.
            assert first_write_s >= lease_ms / 1000 - 1, (
                f"the new leader acknowledged a write {first_write_s:.2f}s after the cut, before the deposed "
                f"leader's {lease_ms}ms lease could have expired")

        # Until the heal: the old leader, then one from the majority side. After it A may lead again.
        cut_phase = watch.distinct_leaders(before=marks["heal_ns"] / 1e9)
        rest_ids = set(host_ids[cut_off:])
        assert cut_phase[0] == leader_id and cut_phase[-1] in rest_ids, watch.changes
        workload.assert_progress(min_writes=200, min_reads=200)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")


async def isolate_both_ways(network_proxy: NetworkProxy, side: list[ServerInfo], rest: list[ServerInfo]) -> None:
    await network_proxy.partition([s.ip_addr for s in side], [s.ip_addr for s in rest], mode="drop")


@pytest.mark.parametrize("leases", LEASES)
async def test_cut_off_leader_serves_lease_reads_and_successor_defers_its_commit(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, leases: Leases, tmp_path) -> None:
    """The SC leader A is cut off from both followers, both ways, and keeps running: the
    scenario of deposed_leader_scenario on three nodes."""
    await deposed_leader_scenario(manager, network_proxy, failure_detector_timeout, leases, tmp_path, cut=isolate_both_ways)


@pytest.mark.parametrize("leases", LEASES)
async def test_majority_split_with_the_leader_in_the_minority(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, leases: Leases, tmp_path) -> None:
    """Five nodes, RF=5, split 2|3 with the leader A and one follower F on the minority side
    (Jepsen's `:majority` partition, the one its ScyllaDB suite runs).

    What three nodes cannot show: F is a follower that can still reach the deposed leader.
    F's reads are read barriers requested from A, so with leases on A answers them from its
    lease (fsm::start_read_barrier with another node as the requester) for as long as it
    still leads, and F serves them from its own replica. Those reads must stay linearizable
    against the writes the majority side commits after the deferral, like A's own.
    """
    await deposed_leader_scenario(manager, network_proxy, failure_detector_timeout, leases, tmp_path,
                                  cut=isolate_both_ways, nodes=5, cut_off=2)


async def cut_into_the_leader(network_proxy: NetworkProxy, side: list[ServerInfo], rest: list[ServerInfo]) -> None:
    await network_proxy.cut([s.ip_addr for s in rest], side[0].ip_addr)


async def cut_out_of_the_leader(network_proxy: NetworkProxy, side: list[ServerInfo], rest: list[ServerInfo]) -> None:
    await network_proxy.cut(side[0].ip_addr, [s.ip_addr for s in rest])


@pytest.mark.parametrize("leases", LEASES)
@pytest.mark.parametrize("direction", ["into", "out-of"])
async def test_one_way_cut_of_the_leader(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, leases: Leases,
        direction: str, tmp_path) -> None:
    """Only one direction of the leader's links is cut.

    `into`: the followers' bytes to A are withheld, A's bytes to them go through. The
    followers keep getting A's appends, including the entries A creates for client writes
    after the cut, but A gets no acknowledgement: it can commit nothing, so it cannot renew
    its lease either, and it steps down by check-quorum. The followers give up on A through
    the direct failure detector (their pings get no answer) and elect a new leader whose log
    holds A's post-cut entries: the deposed lease it has to wait out is the one of the newest
    of those, not of the last entry before the cut, and it commits A's entries along with its
    own. A's clients saw those writes time out; the history must still be linearizable.

    `out-of`: A's bytes to the followers are withheld. They hear nothing from A, elect a new
    leader, whose appends reach A and depose it at once.

    Either way the successor defers its first commit by Δ and the history is linearizable.
    """
    cut = cut_into_the_leader if direction == "into" else cut_out_of_the_leader
    await deposed_leader_scenario(manager, network_proxy, failure_detector_timeout, leases, tmp_path, cut=cut)


@pytest.mark.parametrize("leases", [pytest.param("off", marks=pytest.mark.xfail(strict=True, reason=BRIDGE_IDLE_XFAIL)), "on"])
async def test_bridge_partition_under_read_only_load_keeps_the_leader(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, leases: Leases) -> None:
    """The bridge partition of test_network_proxy_sc.py (A, the leader, and C cannot reach
    each other, B reaches both) with a read-only load through A.

    Reads append nothing to the raft log when leases are off, so the logs of B and C stay
    equal, as in the idle variant there, and B grants the pre-vote of C, which cannot hear
    A: C deposes the leader that kept its majority. Marked xfail, for the same reason.

    With leases on, a read-only load makes A renew its lease with a no-op entry every Δ/2
    (fsm::tick, "LeaseGuard renewing lease"). Those entries reach B but not C, so by the
    time C gives up on A its log is behind B's and B refuses it, as under a write load. A
    must keep leading and every read through A must succeed.
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout, request_timeout_ms=3000,
                                                          extra_config=lease_config(leases, LEASE_MS))
    if leases == "on":
        await require_leases_enabled(cql, hosts)
    a, b, c = servers
    a_id = host_ids[0]

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int)")
        group_id = await get_table_raft_group_id(manager, ks, "t")
        await ensure_raft_group_leader_on(manager, a, group_id)
        await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (0, 1)", host=hosts[0])
        await wait_for_leader(manager, b, group_id, expected_host_id=a_id)
        await wait_for_leader(manager, c, group_id, expected_host_id=a_id)

        watch_b = LeaderWatch(manager, b, group_id)
        watch_c = LeaderWatch(manager, c, group_id)
        reads = {"ok": 0, "failed": []}
        stop = asyncio.Event()

        async def read_through_a() -> None:
            stmt = SimpleStatement(f"SELECT c FROM {table} WHERE pk = 0", retry_policy=FallthroughRetryPolicy())
            while not stop.is_set():
                try:
                    rows = await cql.run_async(stmt, host=hosts[0])
                    assert rows and rows[0].c == 1, rows
                    reads["ok"] += 1
                except Exception as exc:
                    reads["failed"].append(repr(exc))
                await asyncio.sleep(0.01)

        async def bridge_partition() -> None:
            # Reads for a few lease halves first, so that A renews while C still hears it.
            await asyncio.sleep(LEASE_MS / 1000 * 1.5)
            await network_proxy.partition(a.ip_addr, c.ip_addr)
            c_reports = await wait_for_leader_change(manager, c, group_id, a_id, LEADER_CHANGE_BOUND_S)
            logger.info(f"{c.ip_addr} gave up on {a.ip_addr}, reports {c_reports}")
            await wait_for_down(manager, network_proxy, a, {c.ip_addr})
            await wait_for_down(manager, network_proxy, c, {a.ip_addr})
            await asyncio.sleep(3)
            await network_proxy.heal()
            await wait_for_all_up(manager, network_proxy, servers)
            await wait_for_leader(manager, c, group_id, timeout=LEADER_CHANGE_BOUND_S)
            await asyncio.sleep(2)
            stop.set()
            watch_b.stop.set()
            watch_c.stop.set()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(read_through_a())
            tg.create_task(bridge_partition())
            tg.create_task(watch_b.run())
            tg.create_task(watch_c.run())

        logger.info(f"reads through A: {reads['ok']} ok, {len(reads['failed'])} failed; "
                    f"leaders seen by B: {watch_b.leaders}; by C: {watch_c.leaders}")
        assert watch_b.distinct_leaders() == [a_id], f"B saw the leadership leave A although A kept a majority: {watch_b.leaders}"
        assert watch_c.distinct_leaders() == [a_id], f"C saw: {watch_c.leaders}"
        assert not reads["failed"], f"reads through A failed although A kept its majority: {reads['failed'][:5]}"
        assert reads["ok"] >= 100, reads["ok"]


@pytest.mark.parametrize("leases", LEASES)
async def test_reads_forwarded_to_a_leader_its_clients_cannot_reach(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, leases: Leases, tmp_path) -> None:
    """The SC leader A is cut off from its clients only (CQL ports, connections reset); its
    peers and the REST API still reach it. The workload runs through B and C, which forward
    every request to A over the internode connection.

    Nothing may fail and the leader must not change: the group is whole. What leases change
    is how A answers the forwarded reads, which the raft trace log of A shows: from its
    lease ("start_read_barrier[...] lease read") when on, through a quorum read barrier
    and never from a lease when off.
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout,
                                                          extra_config=lease_config(leases, LEASE_MS))
    if leases == "on":
        await require_leases_enabled(cql, hosts)
    leader = servers[0]
    leader_id = host_ids[0]

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(CLIENTS_BLOCKED, tolerate_reboot))
        await workload.create_table(cql)
        group_id = await get_table_raft_group_id(manager, ks, workload.table_name)
        await ensure_raft_group_leader_on(manager, leader, group_id)
        await wait_for_leader(manager, servers[1], group_id, expected_host_id=leader_id)

        watch = LeaderWatch(manager, servers[1], group_id)
        log = await manager.server_open_log(leader.server_id)
        # The API answers with a JSON string, quotes included.
        raft_level = (await manager.api.get_logger_level(leader.ip_addr, "raft")).strip('"')
        counts: dict[str, int] = {}

        async def block_the_leaders_clients() -> None:
            await asyncio.sleep(3)
            async with workload.disruption_window(CLIENTS_BLOCKED):
                await network_proxy.block_clients(leader.ip_addr, mode="reset")
                await manager.api.set_logger_level(leader.ip_addr, "raft", "trace")
                mark = await log.mark()
                try:
                    await asyncio.sleep(10)
                finally:
                    await manager.api.set_logger_level(leader.ip_addr, "raft", raft_level)
                counts["lease"] = len(await log.grep(r"start_read_barrier\[[^\]]*\] lease read", from_mark=mark))
                counts["quorum"] = len(await log.grep(r"broadcast_read_quorum\[", from_mark=mark))
                await network_proxy.heal()
                await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            await asyncio.sleep(4)
            workload.stop_event.set()
            watch.stop.set()

        errors = await run_workload(workload, PinnedSession(cql, hosts[1:]), duration_s=40,
                                    disruptors=[("clients-blocked", block_the_leaders_clients), ("leader-watch", watch.run)])
        logger.info(f"Stats: {workload.stats_line()} | read barriers on {leader.ip_addr} while its clients were cut: {counts}")
        assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"
        assert counts, "the cut never completed"
        assert watch.leaders == [leader_id], f"the leader changed although only its clients were cut off: {watch.leaders}"
        totals = workload.stats()
        assert totals.write_fail + totals.write_indeterminate + totals.read_fail + totals.read_indeterminate == 0, totals
        if leases == "on":
            assert counts["lease"] > 0, f"{leader.ip_addr} answered no forwarded read from its lease: {counts}"
        else:
            assert counts["lease"] == 0 and counts["quorum"] > 0, counts
        workload.assert_progress(min_writes=200, min_reads=200)
        await check_linearizable(workload, output_dir=tmp_path / "porcupine")


async def test_lossy_links_with_leases(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, tmp_path) -> None:
    """test_lossy_links_keep_the_sc_leader_and_the_history_linearizable with leases on: 20% loss on
    every link must not change the leader, fail a request or break linearizability.

    A spurious leader change would cost more here than without leases (the new leader defers
    its commits for up to Δ), which the zero-failure assertion of the scenario catches.
    """
    await lossy_links_scenario(manager, network_proxy, failure_detector_timeout, tmp_path,
                               extra_config=lease_config("on", LEASE_MS), on_boot=boot_check("on"))


async def test_random_network_faults_with_leases(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, tmp_path) -> None:
    """test_random_network_faults_keep_the_history_linearizable with leases on: every kind of fault
    once, in random order, under the register workload, and the history stays linearizable.

    Here the deposed leaders that keep serving lease reads after a cut, and the successors
    that have to wait their leases out, come in every combination the faults produce.
    """
    await random_faults_scenario(manager, network_proxy, failure_detector_timeout, tmp_path,
                                 extra_config=lease_config("on", LEASE_MS), on_boot=boot_check("on"))


@pytest.mark.parametrize("leases", LEASES)
async def test_lease_reads_skip_the_quorum_round_trip_under_delayed_links(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, leases: Leases) -> None:
    """200ms of one-way delay on every internode link makes the cost of a read visible. A
    write at the leader pays the append and its ack, two delays, with leases on or off. A
    read at the leader with leases off needs a quorum read barrier, one round trip to a
    follower, so it pays two delays too; with leases on the leader answers from its lease, so
    it pays none. A read at a follower adds the forward to the leader and the reply, two more.

    The lease has to be valid: the writes that precede the reads renew it, and the reads
    themselves make the leader renew it every delta/2.
    """
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout,
                                                          extra_config=lease_config(leases, LEASE_MS))
    if leases == "on":
        await require_leases_enabled(cql, hosts)
    a, b, _ = servers
    host_a, host_b, _ = hosts

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split(".")[1])
            await ensure_raft_group_leader_on(manager, a, group_id)
            await wait_for_leader(manager, b, group_id, expected_host_id=host_ids[0])
            inserts = [f"INSERT INTO {table} (pk, v) VALUES ({pk}, {pk})" for pk in range(10)]
            selects = [f"SELECT v FROM {table} WHERE pk = {pk}" for pk in range(10)]
            await median_s(cql, host_a, inserts)
            base = {"write_leader": await median_s(cql, host_a, inserts), "read_leader": await median_s(cql, host_a, selects),
                    "read_follower": await median_s(cql, host_b, selects)}

            for s in servers:
                await network_proxy.delay(s.ip_addr, 200, direction="upstream")
            delayed = {"write_leader": await median_s(cql, host_a, inserts), "read_leader": await median_s(cql, host_a, selects),
                       "read_follower": await median_s(cql, host_b, selects)}
            await network_proxy.heal()
            cost = {k: delayed[k] - base[k] for k in base}
            logger.info(f"leases {leases}: baseline {base}, with 200ms links {delayed}, cost {cost}")

            assert cost["write_leader"] >= 0.35, f"a write at the leader did not pay the append and its ack: {cost}"
            if leases == "off":
                assert cost["read_leader"] >= 0.35, f"a read at the leader did not pay a quorum round trip: {cost}"
            else:
                assert cost["read_leader"] < 0.15, f"a read at the leader paid a round trip although it holds a lease: {cost}"
            assert cost["read_follower"] >= cost["read_leader"] + 0.35, f"a read at a follower did not pay the forward and the reply: {cost}"


async def test_lease_renewal_under_slow_commits_stays_one_no_op_at_a_time(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int) -> None:
    """A read-only load through the leader while every internode hop takes 220ms, with Δ=800ms.

    A commit then takes an append and its ack, 440ms, longer than Δ/2: every renewal no-op is
    past half the lease by the time it commits, so the leader wants to renew on every tick
    (fsm::tick_leader, "LeaseGuard renewing lease"). What keeps that from piling up is the
    rule that it renews only with nothing in flight: at most one no-op per commit round trip.
    The lease a renewal buys still holds for Δ - 440ms minus the clock error, so some reads
    are answered from it and the rest through a quorum read barrier; none may fail, and the
    leader must not change. 440ms stays under the direct failure detector's 600ms ping timeout.
    """
    lease_ms, hop_ms, window_s = 800, 220, 10
    servers, cql, hosts, host_ids = await boot_sc_cluster(manager, failure_detector_timeout,
                                                          extra_config=lease_config("on", lease_ms))
    await require_leases_enabled(cql, hosts)
    leader, b, _ = servers
    leader_id = host_ids[0]

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            group_id = await get_table_raft_group_id(manager, ks, table.split(".")[1])
            await ensure_raft_group_leader_on(manager, leader, group_id)
            await wait_for_leader(manager, b, group_id, expected_host_id=leader_id)
            await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 1)", host=hosts[0])

            watch = LeaderWatch(manager, b, group_id)
            log = await manager.server_open_log(leader.server_id)
            raft_level = (await manager.api.get_logger_level(leader.ip_addr, "raft")).strip('"')
            reads = {"ok": 0, "failed": []}
            counts: dict[str, int] = {}
            stop = asyncio.Event()

            async def read_through_the_leader() -> None:
                stmt = SimpleStatement(f"SELECT v FROM {table} WHERE pk = 0", retry_policy=FallthroughRetryPolicy())
                while not stop.is_set():
                    try:
                        rows = await cql.run_async(stmt, host=hosts[0])
                        assert rows and rows[0].v == 1, rows
                        reads["ok"] += 1
                    except Exception as exc:
                        reads["failed"].append(repr(exc))
                    await asyncio.sleep(0.01)

            async def slow_links() -> None:
                for s in servers:
                    await network_proxy.delay(s.ip_addr, hop_ms, direction="upstream")
                # Let the reads in flight before the delay drain, then count over the window.
                await asyncio.sleep(2)
                await manager.api.set_logger_level(leader.ip_addr, "raft", "trace")
                mark = await log.mark()
                try:
                    await asyncio.sleep(window_s)
                finally:
                    await manager.api.set_logger_level(leader.ip_addr, "raft", raft_level)
                counts["renewals"] = len(await log.grep(r"LeaseGuard renewing lease with a no-op", from_mark=mark))
                counts["lease_reads"] = len(await log.grep(r"start_read_barrier\[[^\]]*\] lease read", from_mark=mark))
                counts["barriers"] = len(await log.grep(r"start_read_barrier\[[^\]]*\] starting read barrier", from_mark=mark))
                await network_proxy.heal()
                await asyncio.sleep(1)
                stop.set()
                watch.stop.set()

            async with asyncio.TaskGroup() as tg:
                tg.create_task(read_through_the_leader())
                tg.create_task(slow_links())
                tg.create_task(watch.run())

    logger.info(f"{window_s}s of reads with {hop_ms}ms hops and a {lease_ms}ms lease: {counts}; "
                f"reads {reads['ok']} ok, {len(reads['failed'])} failed; leaders seen by {b.ip_addr}: {watch.leaders}")
    assert watch.leaders == [leader_id], f"the leader changed under slow commits: {watch.leaders}"
    assert not reads["failed"], f"reads failed under slow commits: {reads['failed'][:5]}"
    commit_round_trip_s = 2 * hop_ms / 1000
    assert counts["renewals"] >= 3, f"the leader did not keep renewing its lease under a read-only load: {counts}"
    assert counts["renewals"] <= window_s / commit_round_trip_s + 2, (
        f"more renewals than one per commit round trip ({commit_round_trip_s}s): no-ops piled up: {counts}")
    assert counts["lease_reads"] > 0, f"no read was answered from a renewed lease: {counts}"
