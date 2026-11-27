#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Helpers of the strongly-consistent (SC) network proxy tests of this directory.

They run the register workload of ``test.cluster.strong_consistency`` on a cluster of
SC-enabled nodes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from test import MODES_TIMEOUT_FACTOR
from test.cluster.strong_consistency.register_workload import RegisterWorkload
from test.pylib.internal_types import HostID, ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

SC_KS_OPTS = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
              " AND tablets = {'initial': 1} AND consistency = 'global'")

# The name of the cut, for the disruption window and the exception policy.
PARTITION = "partition"

# How often a LeaderWatch asks its node for the leader.
LEADER_POLL_PERIOD_S = 0.25

def workload_cap_s(build_mode: str, seconds: float) -> float:
    """The `duration_s` of a run_workload whose disruptor ends the run: a cap that only stops a
    disruptor that hangs, scaled to the build mode like every other timeout."""
    return seconds * MODES_TIMEOUT_FACTOR[build_mode]


def assert_workload_running(workload: RegisterWorkload) -> None:
    """Fail if run_workload's `duration_s` already stopped the clients: the checks after the
    disruption would then have nothing to check."""
    assert not workload.stop_event.is_set(), "duration_s expired before the disruption finished"


@dataclass
class HistoryOp:
    """One operation of the recorded history: call and return times (monotonic ns), None when it never returned."""
    client_id: int
    op: str
    key: int
    value: int
    call_ns: int
    return_ns: Optional[int]
    status: Optional[str]


def history_ops(workload: RegisterWorkload) -> list[HistoryOp]:
    """The operations of the workload's history, paired up, in call order."""
    calls: dict[int, dict] = {}
    returns: dict[int, dict] = {}
    for line in workload.history.to_jsonl().splitlines():
        event = json.loads(line)
        (calls if event["kind"] == "call" else returns)[event["id"]] = event
    ops = []
    for op_id, call in calls.items():
        ret = returns.get(op_id)
        ops.append(HistoryOp(call["client_id"], call["op"], call["key"], ret["value"] if ret else call["value"], call["time_ns"],
                             ret["time_ns"] if ret else None, ret["status"] if ret else None))
    return ops


def log_slowest_ops(workload: RegisterWorkload) -> None:
    """Log the eight operations that took longest, call to return, from the recorded history."""
    returned = sorted((op for op in history_ops(workload) if op.return_ns is not None),
                      key=lambda op: op.return_ns - op.call_ns, reverse=True)
    for op in returned[:8]:
        logger.info(f"slow op: {(op.return_ns - op.call_ns) / 1e9:.2f}s {op.op} pk={op.key} client={op.client_id} -> {op.status}")


@dataclass
class LeaderWatch:
    """One node's view of the group's leader, polled over REST; every change is recorded.

    `changes` is the sequence of distinct values the node reported, with the time.monotonic_ns()
    at which each one was first seen; None is "no leader known", which a follower reports
    between losing a leader and hearing from the next one, and a cut-off candidate reports for
    as long as it campaigns.
    """
    manager: ScyllaClusterManager
    server: ServerInfo
    group_id: str
    changes: list[tuple[int, Optional[HostID]]] = field(default_factory=list)
    stop: asyncio.Event = field(default_factory=asyncio.Event)

    async def run(self) -> None:
        last: object = ()
        while not self.stop.is_set():
            leader = await self.manager.api.get_raft_leader(self.server.ip_addr, self.group_id)
            if leader != last:
                self.changes.append((time.monotonic_ns(), leader))
                logger.info(f"leader of group {self.group_id} as seen by {self.server.ip_addr}: {leader}")
                last = leader
            await asyncio.sleep(LEADER_POLL_PERIOD_S)

    @property
    def leaders(self) -> list[Optional[HostID]]:
        return [leader for _, leader in self.changes]

    def distinct_leaders(self, before_ns: Optional[int] = None) -> list[HostID]:
        """The leaders reported, in order of first appearance, "none" left out; only those first
        seen before the time.monotonic_ns() value `before_ns`, when given."""
        seen: list[HostID] = []
        for t, leader in self.changes:
            if before_ns is not None and t >= before_ns:
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
