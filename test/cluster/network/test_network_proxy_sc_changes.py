#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Schema changes on a strongly-consistent (SC) table while one link of the cluster is cut, checked with Porcupine.

An SC table has two raft groups in play: group0, which holds the schema, and the table's own
group, which holds the data. Their leaders need not be the same node, and the data group's
entries carry the schema version they were written with. A replica that applies an entry
with a schema version it does not know runs a group0 read barrier first (the barrier trigger
in state_machine::apply, schema_store::resolve_and_upgrade); if the version is still unknown
after the barrier it is an internal error ("couldn't find schema").

That barrier is not unbounded. It is start_group0_operation (migration_manager.cc), which runs
under the default raft_timeout, group0_raft_op_timeout_in_ms (a live-updatable option, 60s in
production; test.py raises it to 300s, scylla_server.py). When it expires the barrier throws
raft_operation_timeout_error, apply wraps it as "error while applying mutations", the raft
server stops the applier fiber and calls on_background_error, which for an SC group is
on_internal_error (groups_manager.cc), and the harness runs nodes with abort-on-internal-error.

A node cut off from group0's leader only, and not from the data group's leader, is exactly the
node whose barrier cannot complete while its data group keeps feeding it entries. Stopping a
node (SIGSTOP or shutdown) silences it for everyone; only a pair-wise cut of the per-node proxy
(test.pylib.network_proxy) puts a live node in this state.

The cluster is three nodes, one SC keyspace with RF=3, one table with one tablet, so one data
group, and three distinct roles pinned before every fault:

* G leads group0 (ensure_group0_leader_on); every DDL is issued at G.
* L leads the table's raft group (ensure_raft_group_leader_on with the table's group id).
* F leads nothing: it runs with the `avoid_being_raft_leader` injection from startup. The
  injection is in raft::fsm::become_candidate, so it applies to every raft group of F,
  group0 included: F never campaigns, in any group. That also means F cannot depose G in
  group0 through L's vote while F<->G is cut (raft pre-vote has no leader stickiness,
  scylladb#23953): the S1 scenarios are free of that confounder by construction, and the
  group0 leader is still watched and asserted on.

Scenarios (the c register workload runs throughout; a second register workload writes and
reads the column c2 once it exists):

S1 (test_follower_cut_from_group0_leader_waits_for_schema_and_catches_up): F<->G is cut both
ways, F<->L and L<->G stay. G adds c2; both workloads run through L (c through G too). L and G
are a majority of both groups, so nothing may fail and the writes must keep being acknowledged.
F gets the new entries from L but cannot learn the new schema: its apply waits in the group0
barrier (schema_barrier_waits grows by one, the debug line says so, the barrier does not
complete, committed entries pile up unapplied on F). The cut stays well under F's group0
operation timeout. After the heal F's barrier completes, F applies everything, and a CL=ONE
(local) read on every node returns what a linearizable read does, for every key and both
columns.

S1b-stop (test_follower_waiting_for_schema_stops_cleanly): as S1 up to the point where F waits
in the barrier; then F is stopped gracefully. The stop must finish within a bound and abort the
waiting apply ("execution ... aborted"), with no internal error or abort in F's log; the
histories are linearizable. F stays down.

S1b-restart (test_follower_waiting_for_schema_restarts_and_catches_up): as S1b-stop, then the cut
is healed and F started again; it must catch up as in S1. Today the restart aborts in the
commitlog replay of the SC group, which resolves a committed entry's schema without a barrier
(raft_commitlog_replay_buffer.cc): that is SCYLLADB-5027. The test fails naming 5027 on exactly
that signature and with a different message on anything else; it turns green once 5027 is fixed.

S1c (test_follower_cut_from_group0_leader_longer_than_the_group0_op_timeout): as S1, with F's
group0_raft_op_timeout_in_ms lowered to 5s (live update, confirmed over REST before the cut).
The cut is held until F logs that its barrier timed out (the precondition, with the measured
time from the wait line to the timeout line logged), then one more timeout. The design contract
is that a follower that cannot reach group0 stays up and catches up once it can (F's barrier
completes after the heal, as in S1); the code path above says the applier dies and the node
aborts. The test checks everything that does not depend on F first,
then fails naming exactly that chain (timeout, "applier fiber stopped", "error while applying
mutations" in F's log, F dead or its applier stopped), with a different message on any other
outcome; once fixed, F survives and the S1 catch-up checks make it green.

Every scenario checks that no node logged "couldn't find schema", an abort or a failed apply,
except where the scenario is about exactly that.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from datetime import datetime
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pytest
from cassandra import ConsistencyLevel
from cassandra.policies import FallthroughRetryPolicy, WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test.cluster.network.sc_util import (LeaderWatch, PARTITION, PinnedSession, SC_KS_OPTS, assert_workload_running,
    history_ops, log_slowest_ops, workload_cap_s)
from test.cluster.network.util import (SEED, metric, wait_for_all_up, wait_for_down)
from test.cluster.strong_consistency.outcomes import ExceptionPolicy, Outcome, only_during, tolerate_reboot
from test.cluster.strong_consistency.register_workload import RegisterWorkload, check_linearizable, run_workload
from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, get_table_raft_group_id, wait_for_leader
from test.cluster.util import ensure_group0_leader_on, ensure_raft_group_leader_on, new_test_keyspace
from test.pylib.internal_types import HostID, ServerInfo
from test.pylib.network_proxy import NetworkProxy
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely, wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

# The bounds below are tuned for dev builds; a debug build is too slow for the 3s request timeout.
pytestmark = pytest.mark.skip_mode(mode='debug', reason='timing bounds are tuned for dev builds')

# Production-like request timeouts, so that a request stuck behind the cut fails and its client moves on.
REQUEST_TIMEOUT_MS = 3000
# The longest a healthy majority may go without acknowledging a single write of a register: two request timeouts.
AVAILABILITY_BOUND_S = 2 * REQUEST_TIMEOUT_MS / 1000
# The fixed part of the cut, after the DDL and after the clients of the new column started. The cut then lasts
# until gossip on both sides has convicted the other too, so that the heal is a heal of a convicted cut.
HOLD_S = 8
# The workload runs this long before the fault, and this long after the cluster is whole again.
WARMUP_S = 3
TAIL_S = 5
# Bound on a graceful stop of the node waiting in the group0 barrier (S1b).
STOP_BOUND_S = 60
# Bound on every catch-up step after the heal: barriers, the driver reconnecting, applies.
CATCH_UP_TIMEOUT_S = 60
# At least this many writes of the new column must be acknowledged in the stretch the scenario measures.
MIN_NEW_COLUMN_ACKS = 10
# S1c: F's group0 operation timeout, and how many of them to wait for the timeout line once F waits in the barrier.
SHORT_GROUP0_OP_TIMEOUT_MS = 5000
TIMEOUTS_HELD = 3
GROUP0_OP_TIMEOUT = "group0_raft_op_timeout_in_ms"

NEW_COLUMN = "c2"
SCHEMA_BARRIER_WAITS = "scylla_strong_consistency_state_machine_schema_barrier_waits"
UNAPPLIED_ENTRIES = "scylla_strong_consistency_raft_unapplied_entries"
# Debug lines of sc_state_machine (DEFAULT_CMDLINE turns that logger to debug).
BARRIER_WAIT_LINE = r"apply\(\): waiting for the group0 barrier for tablet \S+, group_id={group_id}"
BARRIER_DONE_LINE = r"Table \S+ has schema version \S+ after the group0 read barrier"
APPLY_ABORTED_LINE = r"apply\(\): execution for tablet \S+, group_id={group_id} aborted"
MISSING_SCHEMA = "couldn't find schema"
# What no node may log in these scenarios: the internal error of an unresolvable schema (state_machine.cc,
# schema_store::resolve_and_upgrade), any abort, and an apply that failed rather than being aborted.
BAD_LOG_LINE = rf"{MISSING_SCHEMA}|Aborting on shard|error while applying mutations"
# S1c: the group0 barrier's own timeout (raft_group_registry.cc, timeout_message), and what an applier or a process
# that died of it logs (raft/server.cc applier fiber, state_machine.cc apply's catch-all, groups_manager.cc
# on_background_error, seastar's abort). Classified by these lines only: the death lines quote the timeout too.
BARRIER_TIMEOUT_LINE = r"raft operation \[read_barrier\] timed out"
APPLIER_STOPPED = "applier fiber stopped"
APPLIER_DEATH = rf"{APPLIER_STOPPED}|error while applying mutations|background error|Aborting on shard"
# The known failure's chain, every link of which must be in F's log to call it that failure.
SC_APPLIER_DEATH_CHAIN = (BARRIER_TIMEOUT_LINE, APPLIER_STOPPED, "error while applying mutations")
# S1b-restart: lines that tell a startup replay failure from a live one.
INIT_COMPLETED_LINE = r"initialization completed"
BARRIER_WAIT_ANY_LINE = r"apply\(\): waiting for the group0 barrier"
SCYLLADB_5027 = "SCYLLADB-5027: commitlog replay aborts on a committed SC entry whose schema version is unknown locally"
# Not filed yet: replace with the issue key once it is.
SC_APPLIER_DIES_ON_BARRIER_TIMEOUT = ("SC applier dies when its group0 schema barrier times out: on_background_error -> "
                                      "on_internal_error (unfiled; related SCYLLADB-2578)")
# Scylla log lines start with the level and "YYYY-MM-DD HH:MM:SS,mmm".
LOG_TIME = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[,.]\d{3}")


@dataclass
class Roles:
    """The three nodes by role: G leads group0, L leads the table's raft group, F leads nothing."""
    g: ServerInfo
    l: ServerInfo
    f: ServerInfo
    g_id: HostID
    l_id: HostID
    f_id: HostID
    host_g: object
    host_l: object
    host_f: object

    @property
    def servers(self) -> list[ServerInfo]:
        return [self.g, self.l, self.f]

    def name(self, host_id: Optional[HostID]) -> str:
        """The role of a host id, for the messages; None is "nobody"."""
        return {self.g_id: "G", self.l_id: "L", self.f_id: "F"}.get(host_id, str(host_id)) if host_id is not None else "none"

    def name_of(self, server: ServerInfo) -> str:
        return {self.g.server_id: "G", self.l.server_id: "L", self.f.server_id: "F"}[server.server_id]

    def names(self, watch: LeaderWatch) -> list[str]:
        """The leaders a watch saw, in order, by role."""
        return [self.name(leader) for leader in watch.leaders]


async def boot_roles(manager: ScyllaClusterManager, failure_detector_timeout: int) -> tuple[Roles, object]:
    """Three SC nodes, one per rack; the third (F) never campaigns in any raft group.

    F gets its own config: it needs the injection at startup."""
    config = {'failure_detector_timeout_in_ms': failure_detector_timeout,
              'read_request_timeout_in_ms': REQUEST_TIMEOUT_MS,
              'write_request_timeout_in_ms': REQUEST_TIMEOUT_MS}
    g, l = await manager.servers_add(2, config=config, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    f = await manager.server_add(config=config | {'error_injections_at_startup': ['avoid_being_raft_leader']},
                                 cmdline=DEFAULT_CMDLINE, property_file={'dc': 'dc1', 'rack': 'rack3'})
    cql, (host_g, host_l, host_f) = await manager.get_ready_cql([g, l, f])
    g_id, l_id, f_id = await gather_safely(*(manager.get_host_id(s.server_id) for s in (g, l, f)))
    return Roles(g, l, f, g_id, l_id, f_id, host_g, host_l, host_f), cql


async def group0_id(cql) -> str:
    return (await cql.run_async("SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'"))[0].value


async def pin_roles(manager: ScyllaClusterManager, roles: Roles, g0_id: str, group_id: str) -> None:
    """Make G lead group0 and L lead the table's group, and check that every node agrees."""
    await ensure_group0_leader_on(manager, roles.g)
    await ensure_raft_group_leader_on(manager, roles.l, group_id)
    for s in roles.servers:
        await wait_for_leader(manager, s, g0_id, expected_host_id=roles.g_id)
        await wait_for_leader(manager, s, group_id, expected_host_id=roles.l_id)
    leaders = {s.ip_addr: (roles.name(await manager.api.get_raft_leader(s.ip_addr, g0_id)),
                           roles.name(await manager.api.get_raft_leader(s.ip_addr, group_id))) for s in roles.servers}
    assert all(v == ("G", "L") for v in leaders.values()), (
        f"roles not pinned, (group0 leader, SC leader) per node: {leaders}; retry: election race before the ALTER, not a finding")
    logger.info(f"roles: G={roles.g.ip_addr} leads group0 {g0_id}, L={roles.l.ip_addr} leads SC group {group_id}, "
                f"F={roles.f.ip_addr} leads nothing")


async def assert_group0_led_by_g(manager: ScyllaClusterManager, roles: Roles, g0_id: str, scenario: str, when: str) -> None:
    leader = await manager.api.get_raft_leader(roles.g.ip_addr, g0_id)
    assert leader == roles.g_id, (f"{scenario}: group0 is led by {roles.name(leader)}, not G, {when}; "
                                  f"retry: election race before the ALTER (raft pre-vote, scylladb#23953), not a finding")


def ddl_session(manager: ScyllaClusterManager, server: ServerInfo):
    """A session that talks to `server` only and does not wait for schema agreement after a DDL, which would
    stall on the node that cannot learn the change. Same as test_network_proxy_schema.session:
    a proxied node advertises its proxy's address, so the whitelist names that one."""
    addr = server.rpc_address
    cluster = manager.con_gen([addr], manager.port, manager.use_ssl, manager.auth_provider, WhiteListRoundRobinPolicy([addr]))
    manager.exclusive_clusters.append(cluster)
    session = cluster.connect()
    session.cluster.max_schema_agreement_wait = 0
    return session


async def alter_add_new_column(ddl, fqtn: str) -> None:
    await ddl.run_async(SimpleStatement(f"ALTER TABLE {fqtn} ADD {NEW_COLUMN} int", retry_policy=FallthroughRetryPolicy()))


async def knows_column(cql, host, ks: str, table: str, column: str) -> bool:
    """Whether the node behind `host` has `column` in its local system_schema."""
    rows = await cql.run_async(SimpleStatement(
        f"SELECT column_name FROM system_schema.columns WHERE keyspace_name = '{ks}' AND table_name = '{table}'",
        consistency_level=ConsistencyLevel.ONE), host=host)
    return column in {r.column_name for r in rows}


async def who_knows_column(cql, roles: Roles, ks: str, table: str) -> dict[str, bool]:
    return {r: await knows_column(cql, h, ks, table, NEW_COLUMN)
            for r, h in (("G", roles.host_g), ("L", roles.host_l), ("F", roles.host_f))}


async def table_metric(manager: ScyllaClusterManager, server: ServerInfo, name: str, ks: str, table: str) -> float:
    """A per-table metric of `server`; 0 when it was never reported (skip_when_empty)."""
    return await metric(manager, server, name, {'ks': ks, 'cf': table})


async def wait_for_log(log, pattern: str, mark: int, timeout: float, what: str) -> str:
    """The first line matching `pattern` after `mark`; fails with `what` if none shows up in time."""
    try:
        _, matches = await log.wait_for(pattern, from_mark=mark, timeout=timeout)
    except TimeoutError:
        raise AssertionError(f"{what}: no log line matching {pattern!r} within {timeout}s") from None
    return matches[0][0].strip()


async def grep_lines(log, pattern: str, mark: int) -> list[str]:
    return [line.strip() for line, _ in await log.grep(pattern, from_mark=mark)]


def log_seconds_between(first: str, second: str) -> Optional[float]:
    """Seconds between the timestamps of two node log lines; None if either has none."""
    stamps = [LOG_TIME.search(line) for line in (first, second)]
    if not all(stamps):
        return None
    t1, t2 = (datetime.strptime(m.group(0).replace(".", ","), "%Y-%m-%d %H:%M:%S,%f") for m in stamps)
    return round((t2 - t1).total_seconds(), 3)


def process_alive(pid: int) -> bool:
    """Whether the process exists and is not a zombie (test.py runs the nodes as its own children)."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return False
    return stat.rsplit(")", 1)[1].split()[0] not in ("Z", "X")


async def group0_op_timeout_s(manager: ScyllaClusterManager, server: ServerInfo) -> float:
    return float(await manager.api.get_config(server.ip_addr, GROUP0_OP_TIMEOUT)) / 1000


def ack_times_ns(workload: RegisterWorkload, op: str = "write") -> list[int]:
    """The return times of the operations of kind `op` that succeeded, from the recorded history."""
    return sorted(o.return_ns for o in history_ops(workload)
                  if o.op == op and o.status == Outcome.OK and o.client_id != workload.reset_client_id)


def longest_gap_s(times_ns: list[int], start_ns: int, end_ns: int) -> float:
    """The longest stretch of [start_ns, end_ns] without any of `times_ns` in it, in seconds."""
    points = [start_ns] + [t for t in sorted(times_ns) if start_ns <= t <= end_ns] + [end_ns]
    return max(b - a for a, b in zip(points, points[1:])) / 1e9


def count_between(times_ns: list[int], start_ns: int, end_ns: int) -> int:
    return sum(start_ns <= t <= end_ns for t in times_ns)


@dataclass
class NewColumnClients:
    """Writers and readers of the new column, every request pinned to one node, recorded into their own register
    workload (each key's c2 is a register of its own, independent of its c) for Porcupine.

    Not RegisterWorkload's own clients: those prepare their statements, and the driver prepares on every node,
    the one that does not know the column included. Plain statements, one attempt each, go to `host` only.
    """
    workload: RegisterWorkload
    cql: object
    host: object
    errors: Counter = field(default_factory=Counter)
    tasks: list[asyncio.Task] = field(default_factory=list)
    started_ns: int = 0
    failed: list[BaseException] = field(default_factory=list)

    def start(self) -> None:
        self.started_ns = time.monotonic_ns()
        w = self.workload
        for i in range(w.num_writers):
            self.tasks.append(asyncio.create_task(self._client(i, "write"), name=f"{NEW_COLUMN}-writer-{i}"))
        for i in range(w.num_readers):
            self.tasks.append(asyncio.create_task(self._client(w.num_writers + i, "read"), name=f"{NEW_COLUMN}-reader-{i}"))

    def failures(self) -> list[BaseException]:
        """What the clients that already ended raised: a failure the exception policy did not accept."""
        return self.failed + [t.exception() for t in self.tasks if t.done() and not t.cancelled() and t.exception() is not None]

    async def stop(self) -> None:
        self.workload.stop_event.set()
        if self.tasks:
            async with asyncio.timeout(CATCH_UP_TIMEOUT_S):
                results = await asyncio.gather(*self.tasks, return_exceptions=True)
            self.failed += [r for r in results if isinstance(r, BaseException)]
            self.tasks = []
        self.workload.totals = self.workload.stats()

    async def _client(self, client_id: int, op: str) -> None:
        w = self.workload
        rng = w.rng_for(f"{op}/{client_id}")
        while not w.stop_event.is_set():
            pk = rng.randint(0, w.num_keys - 1)
            value = w.next_write_value() if op == "write" else 0
            op_id, t_call_ns = w.history.record_call(client_id, op, pk, value)
            text = (f"UPDATE {w.fqtn} SET {NEW_COLUMN} = {value} WHERE pk = {pk}" if op == "write"
                    else f"SELECT {NEW_COLUMN} FROM {w.fqtn} WHERE pk = {pk}")
            try:
                rows = await self.cql.run_async(SimpleStatement(text, retry_policy=FallthroughRetryPolicy()), host=self.host)
                if op == "read":
                    value = 0 if not rows or getattr(rows[0], NEW_COLUMN) is None else getattr(rows[0], NEW_COLUMN)
                w.history.record_return(op_id, client_id, op, pk, value, Outcome.OK)
            except Exception as exc:  # noqa: BLE001 - classified by the workload's policy, which raises on anything unexpected
                self.errors[f"{op}: {type(exc).__name__}: {str(exc)[:200]}"] += 1
                w.record_failure(exc, op_id=op_id, client_id=client_id, op=op, key=pk, value=value, t_call_ns=t_call_ns)
            await asyncio.sleep(rng.uniform(*w.dml_pause_s))


def new_column_workload(ks: str, table_name: str, num_keys: int, policy: ExceptionPolicy) -> RegisterWorkload:
    return RegisterWorkload(ks=ks, table_name=table_name, name=NEW_COLUMN, num_keys=num_keys, num_writers=2, num_readers=2,
                            seed=SEED, exception_policy=policy)


def assert_tasks_ok(scenario: str, errors: list[BaseException], clients: NewColumnClients, seed: int) -> None:
    """The first check after a run: a task that died, the clients of the new column included, explains every
    later symptom (a missing ack, a timed-out wait), so it is reported before any of them."""
    client_failures = clients.failures()
    assert not errors and not client_failures, (
        f"{scenario}: task(s) failed (seed={seed}): {errors}; {NEW_COLUMN} clients failed: {client_failures}; "
        f"{NEW_COLUMN} errors seen: {dict(clients.errors)}")


async def assert_converged(manager: ScyllaClusterManager, cql, roles: Roles, servers: list[ServerInfo], ks: str, table: str,
                           group_id: str, num_keys: int) -> None:
    """Each of `servers` knows the new column, has applied everything its group committed, and a CL=ONE (local)
    read on it returns what a linearizable read does, for every key and both registers."""
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + CATCH_UP_TIMEOUT_S)
    for s, host in zip(servers, hosts):
        role = roles.name_of(s)
        await read_barrier(manager.api, s.ip_addr, timeout=CATCH_UP_TIMEOUT_S)
        assert await knows_column(cql, host, ks, table, NEW_COLUMN), f"{role} ({s.ip_addr}) does not know {NEW_COLUMN} after the heal"
        # The barrier on the SC group makes the node apply everything committed so far (a timeout is a must:
        # without one the node aborts, SCYLLADB-4759).
        await read_barrier(manager.api, s.ip_addr, group_id=group_id, timeout=CATCH_UP_TIMEOUT_S)

        async def applied_all():
            return True if await table_metric(manager, s, UNAPPLIED_ENTRIES, ks, table) == 0 else None
        await wait_for(applied_all, time.time() + CATCH_UP_TIMEOUT_S, period=0.5,
                       label=f"{role} ({s.ip_addr}) to apply every committed entry of group {group_id}")

        mismatches = []
        for pk in range(num_keys):
            query = f"SELECT c, {NEW_COLUMN} FROM {ks}.{table} WHERE pk = {pk}"
            linearizable = [tuple(r) for r in await cql.run_async(SimpleStatement(
                query, consistency_level=ConsistencyLevel.QUORUM, retry_policy=FallthroughRetryPolicy()), host=host)]
            local = [tuple(r) for r in await cql.run_async(SimpleStatement(
                query, consistency_level=ConsistencyLevel.ONE, retry_policy=FallthroughRetryPolicy()), host=host)]
            if local != linearizable:
                mismatches.append(f"pk={pk}: local {local}, group {linearizable}")
        assert not mismatches, (f"{role} ({s.ip_addr}) holds locally other (c, {NEW_COLUMN}) than the group says, "
                                f"{len(mismatches)} of {num_keys} keys: {mismatches[:10]}")
        logger.info(f"{role} ({s.ip_addr}): local reads match the linearizable ones for all {num_keys} keys")


async def assert_no_bad_log_lines(manager: ScyllaClusterManager, roles: Roles, marks: dict[str, int],
                                  servers: Optional[list[ServerInfo]] = None, until: Optional[dict[str, int]] = None) -> None:
    """No bad line in the logs of `servers` (all three by default) from their mark on, and, for a node in `until`,
    only up to that later mark."""
    for s in servers or roles.servers:
        log = await manager.server_open_log(s.server_id)
        bad = await grep_lines(log, BAD_LOG_LINE, marks[s.ip_addr])
        if until and s.ip_addr in until:
            later = Counter(await grep_lines(log, BAD_LOG_LINE, until[s.ip_addr]))
            bad = list((Counter(bad) - later).elements())
        errors = await log.grep_for_errors(distinct_errors=True, from_mark=marks[s.ip_addr])
        if errors:
            logger.info(f"ERROR lines on {s.ip_addr} since the scenario began: {errors[:20]}")
        assert not bad, f"{roles.name_of(s)} ({s.ip_addr}) logged an internal error, a missing schema or an abort: {bad[:10]}"


# The variants of the follower scenario.
RUNNING = "running"     # S1: F stays up through the cut.
STOP = "stop"           # S1b-stop: F is stopped while it waits, and stays down.
RESTART = "restart"     # S1b-restart: F is stopped while it waits, and started after the heal.
LONG = "long"           # S1c: the cut outlasts F's group0 operation timeout.


async def follower_cut_scenario(manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int,
                                build_mode: str, tmp_path, variant: str) -> None:
    """S1, S1b and S1c: F<->G cut, ALTER at G, both registers through L (c through G too)."""
    scenario = {RUNNING: "S1", STOP: "S1b-stop", RESTART: "S1b-restart", LONG: "S1c"}[variant]
    roles, cql = await boot_roles(manager, failure_detector_timeout)
    g, l, f = roles.g, roles.l, roles.f
    g0_id = await group0_id(cql)

    if variant == LONG:
        # A live option (db/config.cc), read per operation (raft_group_registry.cc, resolve_deadline):
        # reloaded on SIGHUP, which server_update_config sends.
        await manager.server_update_config(f.server_id, GROUP0_OP_TIMEOUT, SHORT_GROUP0_OP_TIMEOUT_MS)

        async def timeout_lowered():
            return True if await group0_op_timeout_s(manager, f) * 1000 == SHORT_GROUP0_OP_TIMEOUT_MS else None
        await wait_for(timeout_lowered, time.time() + CATCH_UP_TIMEOUT_S, period=0.5,
                       label=f"{scenario}: F's {GROUP0_OP_TIMEOUT} to become {SHORT_GROUP0_OP_TIMEOUT_MS}")
    op_timeout_s = await group0_op_timeout_s(manager, f)
    logger.info(f"{scenario}: F's group0 operation timeout is {op_timeout_s:.0f}s")

    async with new_test_keyspace(manager, SC_KS_OPTS) as ks:
        workload = RegisterWorkload(ks=ks, seed=SEED, exception_policy=only_during(PARTITION, tolerate_reboot))
        await workload.create_table(cql)
        table = workload.table_name
        group_id = await get_table_raft_group_id(manager, ks, table)
        await pin_roles(manager, roles, g0_id, group_id)
        ddl = ddl_session(manager, g)
        clients = NewColumnClients(new_column_workload(ks, table, workload.num_keys, only_during(PARTITION, tolerate_reboot)),
                                   cql, roles.host_l)

        logs = {s.ip_addr: await manager.server_open_log(s.server_id) for s in roles.servers}
        marks = {ip: await log.mark() for ip, log in logs.items()}
        log_f = logs[f.ip_addr]
        g0_watch_g = LeaderWatch(manager, g, g0_id)
        g0_watch_l = LeaderWatch(manager, l, g0_id)
        sc_watch_g = LeaderWatch(manager, g, group_id)
        watches = [g0_watch_g, g0_watch_l, sc_watch_g]
        outcome: dict[str, object] = {}

        async def cut_alter_and_heal() -> None:
            healed = False
            try:
                await asyncio.sleep(WARMUP_S)
                async with workload.disruption_window(PARTITION) as window, clients.workload.disruption_window(PARTITION):
                    waits_before = await table_metric(manager, f, SCHEMA_BARRIER_WAITS, ks, table)
                    mark_f = outcome["mark_f"] = await log_f.mark()
                    logger.info(f"{scenario}: cutting F ({f.ip_addr}) <-> G ({g.ip_addr}) both ways; F<->L and L<->G stay")
                    await network_proxy.partition(f.ip_addr, g.ip_addr)
                    outcome["cut_ns"] = time.monotonic_ns()

                    await assert_group0_led_by_g(manager, roles, g0_id, scenario, "at the DDL")
                    logger.info(f"{scenario}: ALTER TABLE ADD {NEW_COLUMN} at G ({g.ip_addr})")
                    await alter_add_new_column(ddl, workload.fqtn)
                    await assert_group0_led_by_g(manager, roles, g0_id, scenario, "right after the DDL")

                    # L reaches G: a group0 barrier there makes it apply the change before its clients use the column.
                    await read_barrier(manager.api, l.ip_addr, timeout=CATCH_UP_TIMEOUT_S)
                    known = await who_knows_column(cql, roles, ks, table)
                    assert known == {"G": True, "L": True, "F": False}, (
                        f"{scenario}: who knows {NEW_COLUMN} right after the DDL: {known}; F must not, or the cut did not hold")
                    clients.start()
                    logger.info(f"{scenario}: clients of {NEW_COLUMN} started through L ({l.ip_addr})")

                    # F gets entries with the new schema version from L and must wait for group0 to learn it.
                    async def f_waits():
                        delta = await table_metric(manager, f, SCHEMA_BARRIER_WAITS, ks, table) - waits_before
                        return delta if delta >= 1 else None
                    await wait_for(f_waits, time.time() + CATCH_UP_TIMEOUT_S, period=0.25,
                                   label=f"{scenario}: F's ({f.ip_addr}) SC apply to wait for the group0 barrier")
                    outcome["waiting_line"] = await wait_for_log(
                        log_f, BARRIER_WAIT_LINE.format(group_id=group_id), mark_f, CATCH_UP_TIMEOUT_S,
                        f"{scenario}: F ({f.ip_addr}) never logged that its apply waits for the group0 barrier")
                    wait_t = time.monotonic()

                    if variant == LONG:
                        # The precondition: the barrier did time out. Then hold one more timeout past it.
                        bound_s = TIMEOUTS_HELD * op_timeout_s + CATCH_UP_TIMEOUT_S
                        timeout_line = await wait_for_log(
                            log_f, BARRIER_TIMEOUT_LINE, mark_f, bound_s,
                            f"{scenario}: F's group0 barrier never timed out within {bound_s:.0f}s of waiting (timeout "
                            f"{op_timeout_s:.0f}s), so the scenario did not test the expiry")
                        expired_after = log_seconds_between(outcome["waiting_line"], timeout_line)
                        outcome["barrier_expired_after_s"] = expired_after
                        logger.info(f"{scenario}: F's barrier timed out {expired_after}s after the wait line "
                                    f"(timeout {op_timeout_s:.0f}s): {timeout_line}")
                        await asyncio.sleep(op_timeout_s)
                        outcome["blocked_s"] = time.monotonic() - wait_t
                        outcome["f_alive"] = process_alive(f.pid)
                        outcome["f_death"] = await grep_lines(log_f, APPLIER_DEATH, mark_f)
                        logger.info(f"{scenario}: {outcome['blocked_s']:.1f}s after F began to wait: F alive "
                                    f"{outcome['f_alive']}; applier/process death lines {outcome['f_death'][:10]}")
                        if outcome["f_alive"] and not outcome["f_death"]:
                            await wait_for_down(manager, network_proxy, f, {g.ip_addr})
                            await wait_for_down(manager, network_proxy, g, {f.ip_addr})
                    else:
                        await asyncio.sleep(HOLD_S)
                        await wait_for_down(manager, network_proxy, f, {g.ip_addr})
                        await wait_for_down(manager, network_proxy, g, {f.ip_addr})

                        # The precondition of the rest: F is still blocked, right now.
                        done = await grep_lines(log_f, BARRIER_DONE_LINE, mark_f)
                        assert not done, f"{scenario}: F's group0 barrier completed although F<->G was cut: {done}"
                        assert not await knows_column(cql, roles.host_f, ks, table, NEW_COLUMN), (
                            f"{scenario}: F learned {NEW_COLUMN} although cut from G")
                        waits = await table_metric(manager, f, SCHEMA_BARRIER_WAITS, ks, table) - waits_before
                        assert waits == 1, (f"{scenario}: F's schema_barrier_waits grew by {waits} during the cut; one apply "
                                            f"blocked for the whole cut adds exactly one")
                        unapplied = await table_metric(manager, f, UNAPPLIED_ENTRIES, ks, table)
                        assert unapplied >= 1, f"{scenario}: F has no committed entry left unapplied, so its apply is not blocked"
                        # The barrier has a deadline (see the module docstring); this scenario is about the wait, not the
                        # expiry, which S1c covers.
                        blocked_s = time.monotonic() - wait_t
                        assert blocked_s < op_timeout_s / 2, (
                            f"{scenario}: F waited {blocked_s:.1f}s in the group0 barrier, too close to its group0 operation "
                            f"timeout ({op_timeout_s:.0f}s): the run would test the timeout (S1c), not the wait")
                        logger.info(f"{scenario}: F blocked in the group0 barrier for {blocked_s:.1f}s: {outcome['waiting_line']}; "
                                    f"{unapplied:.0f} committed entries unapplied on F")

                    if variant in (STOP, RESTART):
                        mark_stop = outcome["mark_stop"] = await log_f.mark()
                        logger.info(f"{scenario}: stopping F ({f.ip_addr}) gracefully while it waits in the barrier")
                        t0 = time.monotonic()
                        try:
                            await manager.server_stop_gracefully(f.server_id, timeout=STOP_BOUND_S)
                        except TimeoutError:
                            raise AssertionError(f"{scenario}: the graceful stop of F ({f.ip_addr}) did not finish within "
                                                 f"{STOP_BOUND_S}s while its SC apply waited for the group0 barrier") from None
                        outcome["stop_s"] = time.monotonic() - t0
                        logger.info(f"{scenario}: F stopped in {outcome['stop_s']:.1f}s")
                        bad = await grep_lines(log_f, BAD_LOG_LINE, mark_stop)
                        assert not bad, f"{scenario}: F's shutdown with an apply waiting for group0 logged: {bad[:10]}"
                        aborted = await log_f.grep(APPLY_ABORTED_LINE.format(group_id=group_id), from_mark=mark_stop)
                        assert aborted, f"{scenario}: F stopped without aborting the apply that waited for the group0 barrier"
                        outcome["mark_restart"] = await log_f.mark()

                    outcome["heal_ns"] = time.monotonic_ns()
                    logger.info(f"{scenario}: healing the cut")
                    await network_proxy.heal()
                    healed = True

                    f_up = variant == RUNNING or (variant == LONG and outcome["f_alive"] and not outcome["f_death"])
                    if variant == RESTART:
                        try:
                            await manager.server_start(f.server_id)
                            f_up = True
                        except (RuntimeError, TimeoutError) as exc:
                            outcome["restart_error"] = exc
                            logger.info(f"{scenario}: F failed to start: {exc!r}")
                    if f_up:
                        await wait_for_all_up(manager, network_proxy, roles.servers)
                        if variant in (RUNNING, LONG):
                            outcome["done_line"] = await wait_for_log(
                                log_f, BARRIER_DONE_LINE, mark_f, CATCH_UP_TIMEOUT_S,
                                f"{scenario}: F ({f.ip_addr}) never completed its group0 barrier after the heal")
                        await read_barrier(manager.api, f.ip_addr, timeout=CATCH_UP_TIMEOUT_S)
                        await wait_for_cql_and_get_hosts(cql, roles.servers, time.time() + CATCH_UP_TIMEOUT_S)
                    outcome["f_up"] = f_up
                outcome["window"] = window
                # The tail: nothing may fail now.
                await asyncio.sleep(TAIL_S)
                assert_workload_running(workload)
            finally:
                if not healed:
                    await network_proxy.heal()
                await clients.stop()
                workload.stop_event.set()
                for watch in watches:
                    watch.stop.set()

        errors = await run_workload(workload, PinnedSession(cql, [roles.host_l, roles.host_g]), duration_s=workload_cap_s(build_mode, 240),
                                    disruptors=[("cut-alter-heal", cut_alter_and_heal)]
                                    + [(f"watch-{i}", w.run) for i, w in enumerate(watches)])
        logger.info(f"{scenario}: stats c: {workload.stats_line()} | {NEW_COLUMN}: {clients.workload.stats()} | "
                    f"group0 leaders seen by G {roles.names(g0_watch_g)}, by L {roles.names(g0_watch_l)} | "
                    f"SC leaders seen by G {roles.names(sc_watch_g)} | {NEW_COLUMN} client errors {dict(clients.errors)} | "
                    f"outcome { {k: v for k, v in outcome.items() if k != 'window'} }")
        log_slowest_ops(workload)
        assert_tasks_ok(scenario, errors, clients, workload.seed)
        assert "window" in outcome, f"{scenario}: the cut never completed"

        await check_linearizable(workload, output_dir=tmp_path / "porcupine")
        await check_linearizable(clients.workload, output_dir=tmp_path / f"porcupine-{NEW_COLUMN}")

        def assert_leaders_and_availability() -> None:
            # Nothing here depends on F, so it runs before an expected failure of F as well.
            heal_ns = outcome["heal_ns"]
            assert (g0_watch_g.distinct_leaders(before_ns=heal_ns) == [roles.g_id]
                    and g0_watch_l.distinct_leaders(before_ns=heal_ns) == [roles.g_id]), (
                f"{scenario}: group0 leadership left G while F<->G was cut (seen by G: {roles.names(g0_watch_g)}, by L: "
                f"{roles.names(g0_watch_l)}). F cannot campaign (avoid_being_raft_leader), so this is a finding: compare raft "
                f"pre-vote without leader stickiness, scylladb#23953")
            assert sc_watch_g.distinct_leaders(before_ns=heal_ns) == [roles.l_id], (
                f"{scenario}: SC leadership left L although L kept a majority with G: {roles.names(sc_watch_g)}")

            cut_ns = outcome["cut_ns"]
            c2_acks = ack_times_ns(clients.workload)
            c_gap = longest_gap_s(ack_times_ns(workload), cut_ns, heal_ns)
            c2_gap = longest_gap_s(c2_acks, clients.started_ns, heal_ns)
            c2_acked = count_between(c2_acks, clients.started_ns, heal_ns)
            logger.info(f"{scenario}: longest stretch without an acknowledged write during the cut: c {c_gap:.2f}s, "
                        f"{NEW_COLUMN} {c2_gap:.2f}s; {NEW_COLUMN} writes acknowledged during the cut: {c2_acked}")
            for name, w in (("c", workload), (NEW_COLUMN, clients.workload)):
                totals = w.stats()
                assert totals.write_fail + totals.write_indeterminate + totals.read_fail + totals.read_indeterminate == 0, (
                    f"{scenario}: requests on {name} through L and G failed although they had a majority of both groups: {totals}; "
                    f"{NEW_COLUMN} errors {dict(clients.errors)}")
            assert c_gap <= AVAILABILITY_BOUND_S, (
                f"{scenario}: no write of c acknowledged for {c_gap:.1f}s while F<->G was cut and L, G (a majority of both "
                f"groups) were connected")
            assert c2_gap <= AVAILABILITY_BOUND_S and c2_acked >= MIN_NEW_COLUMN_ACKS, (
                f"{scenario}: writes of {NEW_COLUMN} through L stalled while F<->G was cut: longest gap {c2_gap:.1f}s, "
                f"{c2_acked} acknowledged; errors {dict(clients.errors)}")
            workload.assert_progress(min_writes=200, min_reads=200)
            clients.workload.assert_progress(min_writes=MIN_NEW_COLUMN_ACKS, min_reads=MIN_NEW_COLUMN_ACKS)

        if not outcome["f_up"] and variant in (RESTART, LONG):
            # F is down (or its applier is): first everything that does not depend on it.
            await assert_converged(manager, cql, roles, [g, l], ks, table, group_id, workload.num_keys)
            until = {f.ip_addr: outcome["mark_restart"]} if variant == RESTART else {}
            await assert_no_bad_log_lines(manager, roles, marks, servers=[g, l, f] if variant == RESTART else [g, l], until=until)
            assert_leaders_and_availability()

        if variant == RESTART and not outcome["f_up"]:
            # 5027 is the replay of the SC log at startup: the missing schema comes before F finishes starting and
            # before any live apply waits for the group0 barrier; a live-apply failure is not 5027.
            first = await grep_lines(log_f, rf"{MISSING_SCHEMA}|{INIT_COMPLETED_LINE}|{BARRIER_WAIT_ANY_LINE}", outcome["mark_restart"])
            if first and re.search(MISSING_SCHEMA, first[0]):
                pytest.fail(f"{scenario}: {SCYLLADB_5027}: {first[0]}")
            pytest.fail(f"{scenario}: F failed to start after the heal, without the {SCYLLADB_5027} signature: "
                        f"{outcome['restart_error']!r}; F's first startup/schema lines since the restart: {first[:5]}; "
                        f"bad lines: {await grep_lines(log_f, BAD_LOG_LINE, outcome['mark_restart'])}")
        if variant == LONG and not outcome["f_up"]:
            death = await grep_lines(log_f, APPLIER_DEATH, outcome["mark_f"])
            report = (f"F ({f.ip_addr}, pid {f.pid}) cut from the group0 leader {outcome['blocked_s']:.1f}s, its group0 barrier "
                      f"timed out after {outcome['barrier_expired_after_s']}s (timeout {op_timeout_s:.0f}s): alive at the heal "
                      f"{outcome['f_alive']}, alive now {process_alive(f.pid)}; F logged: {death[:10]}")
            if all(any(re.search(p, line) for line in death) for p in SC_APPLIER_DEATH_CHAIN):
                pytest.fail(f"{scenario}: {SC_APPLIER_DIES_ON_BARRIER_TIMEOUT}: {report}")
            pytest.fail(f"{scenario}: {report}; not the known chain {SC_APPLIER_DEATH_CHAIN}")

        await assert_converged(manager, cql, roles, roles.servers if outcome["f_up"] else [g, l], ks, table, group_id,
                               workload.num_keys)
        await assert_no_bad_log_lines(manager, roles, marks)

        assert_leaders_and_availability()



@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_follower_cut_from_group0_leader_waits_for_schema_and_catches_up(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, build_mode: str, tmp_path) -> None:
    """S1: a follower of the SC group cut off from group0's leader only waits in its apply for the schema of the
    new entries without holding the group back, and catches up after the heal. See the module docstring."""
    await follower_cut_scenario(manager, network_proxy, failure_detector_timeout, build_mode, tmp_path, RUNNING)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_follower_waiting_for_schema_stops_cleanly(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, build_mode: str, tmp_path) -> None:
    """S1b-stop: the follower of S1 is stopped gracefully while its apply waits for the group0 barrier; the stop
    is bounded and aborts the apply cleanly. See the module docstring."""
    await follower_cut_scenario(manager, network_proxy, failure_detector_timeout, build_mode, tmp_path, STOP)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_follower_waiting_for_schema_restarts_and_catches_up(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, build_mode: str, tmp_path) -> None:
    """S1b-restart: as S1b-stop, then the follower is started after the heal and catches up. Fails today on
    the SCYLLADB-5027 signature. See the module docstring."""
    await follower_cut_scenario(manager, network_proxy, failure_detector_timeout, build_mode, tmp_path, RESTART)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_follower_cut_from_group0_leader_longer_than_the_group0_op_timeout(
        manager: ScyllaClusterManager, network_proxy: NetworkProxy, failure_detector_timeout: int, build_mode: str, tmp_path) -> None:
    """S1c: as S1, with the cut outlasting F's group0 operation timeout while F's apply waits for the barrier;
    F must stay up and catch up. Fails today on the known applier-death chain. See the module docstring."""
    await follower_cut_scenario(manager, network_proxy, failure_detector_timeout, build_mode, tmp_path, LONG)
