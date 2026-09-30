#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Helpers for linearizability tests that kill or restart nodes.

The register workload and the checker come from ``register_workload``. This module adds:

  * ``sc_register_table`` - the table with its workload ready to run, and the raft group
    id of every tablet;
  * ``tolerate_node_loss`` - how the errors a dying node causes are reported to the
    checker, and ``NODE_LOSS``, the window a disruptor opens while it kills;
  * ``wait_caught_up`` - when a restarted node is fully back;
  * ``run_phase`` and ``check_recovered`` - a disrupted phase, then a phase where
    nothing may fail, then the checker over both.

Every test here runs in those two phases. A disruptor opens a ``NODE_LOSS`` window
(``RegisterWorkload.disruption_window``) around a kill and closes it once the cluster
is whole again, so a failure at any other time fails the test. The operations of the
healed phase go into the same history as the disrupted one, so the checker verifies
both at once: acknowledged writes are there, writes that failed for sure are not, and
requests flow again.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Optional, Sequence

from cassandra import OperationTimedOut, Unavailable
from cassandra.pool import Host

from test.cluster.strong_consistency.config import sc_keyspace_opts
from test.cluster.strong_consistency.outcomes import FailureContext, Outcome, only_during, tolerate_reboot
from test.cluster.strong_consistency.register_workload import (
    DisruptorFactory, RegisterWorkload, check_linearizable, run_workload)
from test.cluster.test_strong_consistency import wait_for_leader
from test.cluster.util import new_test_keyspace
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

# Pause between two operations of one client. Slower than the harness default on
# purpose: while a tablet has no leader, every write fails at once, and each of those
# is an operation the checker must keep open (see RegisterWorkload.record_failure for
# its per-key budget). At this pace a few seconds of that leave one or two per key.
DML_PAUSE_S = (0.05, 0.1)

# The healed phase: this long, and at least this many writes and reads must succeed in it.
HEALED_S = 5
MIN_HEALED_OPS = 50

# The window a disruptor opens from a kill until the cluster is whole again. Only the
# operations that overlap it may fail, and only as tolerate_node_loss allows.
NODE_LOSS = "node-loss"


def tolerate_node_loss(exc: BaseException, ctx: FailureContext) -> Optional[Outcome]:
    """tolerate_reboot, minus two errors that in these tests can only mean a bug.

    Unavailable: every node is a replica, so no coordinator can lack one.
    OperationTimedOut: the driver gave up before the server answered, although the server
    has its own, shorter, request timeouts.

    Correct only with driver retries off, which the workload guarantees: each recorded
    operation is one attempt at one node, and its error says what became of it.
    """
    if isinstance(exc, (Unavailable, OperationTimedOut)):
        return None
    return tolerate_reboot(exc, ctx)


@asynccontextmanager
async def sc_register_table(manager: ScyllaClusterManager, servers: Sequence[ServerInfo], initial_tablets: int = 1,
                            **workload_kwargs) -> AsyncIterator[tuple[RegisterWorkload, list[str]]]:
    """A strongly consistent register table (RF=3) with its workload ready to run.

    Yields (workload, raft group ids). The keyspace lives for the block. Every tablet has
    a leader when the block starts, so the first operations do not wait for one. Unless
    `workload_kwargs` says otherwise, the workload tolerates node loss inside a NODE_LOSS
    window and nothing outside one.
    """
    cql = manager.get_cql()
    workload_kwargs.setdefault("exception_policy", only_during(NODE_LOSS, tolerate_node_loss))
    async with new_test_keyspace(manager, sc_keyspace_opts(replication_factor=3, initial_tablets=initial_tablets)) as ks:
        workload = RegisterWorkload(ks=ks, dml_pause_s=DML_PAUSE_S, **workload_kwargs)
        await workload.create_table(cql)
        table_id = await manager.get_table_id(ks, workload.table_name)
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        group_ids = [str(r.raft_group_id) for r in rows]
        for group_id in group_ids:
            await wait_for_leader(manager, servers[0], group_id)
        yield workload, group_ids


async def wait_caught_up(manager: ScyllaClusterManager, servers: Sequence[ServerInfo], group_ids: Sequence[str]) -> list[Host]:
    """Wait until the cluster is whole again after a restart. `servers` must be every node, all up.

    Three steps, in this order: the nodes see each other through gossip, they answer CQL,
    and each passes a read barrier of every group (its raft server is registered, the group
    has a leader, and it has applied everything committed so far). Returns the driver Host
    of each server.

    The gossip step is a workaround for SCYLLADB-4722: when a node learns through gossip that
    a peer restarted, it drops its connections to that peer, and a request in flight on them
    fails - a forwarded write fails with WriteFailure. A read barrier can pass before that,
    since raft already talks to the peer.
    """
    await manager.servers_see_each_other(list(servers))
    hosts = await wait_for_cql_and_get_hosts(manager.get_cql(), list(servers), time.time() + 60)
    for server in servers:
        for group_id in group_ids:
            async def barrier() -> bool:  # raises until the group's raft server is registered; wait_for retries on that
                await read_barrier(manager.api, server.ip_addr, group_id, timeout=60)
                return True
            await wait_for(barrier, time.time() + 120, label=f"{server.ip_addr} to pass a read barrier of group {group_id}")
    return hosts


def history_ops(workload: RegisterWorkload) -> list[tuple[str, int, str | None, int | None]]:
    """Every operation of the history as (op, call time_ns, status, return time_ns).

    An operation that never returned has status None and return time None.
    """
    events = [json.loads(line) for line in workload.history.to_jsonl().splitlines()]
    calls = {e["id"]: (e["op"], e["time_ns"]) for e in events if e["kind"] == "call"}
    returns = {e["id"]: (e["status"], e["time_ns"]) for e in events if e["kind"] == "return"}
    return [(op, t_call, *returns.get(op_id, (None, None))) for op_id, (op, t_call) in calls.items()]


async def read_every_key(workload: RegisterWorkload, cql) -> None:
    """Read every key once, as one more client in the history.

    The random readers may never read a key after its last write. This read makes the
    checker see the final value of every register, so a lost write cannot hide.
    """
    client_id = workload.reset_client_id + 1
    for pk in range(workload.num_keys):
        op_id, _ = workload.history.record_call(client_id, "read", pk)
        rows = await cql.run_async(workload.read_stmt.bind([pk]))
        value = 0 if not rows or rows[0].c is None else rows[0].c
        workload.history.record_return(op_id, client_id, "read", pk, value, Outcome.OK)


async def run_phase(workload: RegisterWorkload, cql, duration_s: float, disruptors: Sequence[tuple[str, DisruptorFactory]] = ()) -> None:
    """Run the workload for `duration_s` with a fresh stop event. A task that raised fails the test."""
    workload.stop_event = asyncio.Event()
    errors = await run_workload(workload, cql, duration_s, disruptors)
    logger.info(f"Phase done: {workload.stats_line()}")
    assert not errors, f"Task(s) failed with unexpected exceptions (seed={workload.seed}): {errors}"


async def check_recovered(workload: RegisterWorkload, cql, tmp_path: Path, slow: int = 1) -> None:
    """Run the workload for HEALED_S (times `slow`) more, read every key once more, and check
    the whole history - the disrupted phase included - with the linearizability checker.

    Nothing may fail in this phase: every disruption window has closed, so the policy
    excuses nothing."""
    before = workload.stats()
    await run_phase(workload, cql, HEALED_S * slow)
    after = workload.stats()
    assert after.write_ok - before.write_ok >= MIN_HEALED_OPS, f"only {after.write_ok - before.write_ok} writes succeeded after recovery"
    assert after.reads - before.reads >= MIN_HEALED_OPS, f"only {after.reads - before.reads} reads succeeded after recovery"
    await read_every_key(workload, cql)
    await check_linearizable(workload, output_dir=tmp_path / "porcupine-checker-output")
