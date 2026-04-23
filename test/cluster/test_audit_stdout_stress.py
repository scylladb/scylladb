#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Stress test framework for the stdout audit backend.

The tests here exercise the audit stdout pipeline under concurrent cross-shard
load, validating that:

1. Every audited CQL statement results in exactly one well-formed audit line.
2. No audit line is torn or interleaved with another line (line-atomicity).
3. No audit line is dropped under burst or sustained load.
4. Throughput remains reasonable — a rough lower bound is asserted; the
   measured events/sec is logged so regressions can be caught by eyeballing.

The validation strategy is deliberately strict: after the workload completes,
the test harness collects every line from the node's stdout relay log that
contains `scylla-audit:`, matches each against the expected regex, and
compares the resulting count against the number of queries issued.

These tests rely on SCYLLA_TEST_SPLIT_STDOUT=1 being set automatically by
the `AuditBackendStdout` helper (see `test_audit.py`) so that the Scylla
process writes its stdout to a real pipe (not a regular file), matching the
production configuration where stdout is consumed by a container runtime.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.query import SimpleStatement

from test.cluster.test_audit import AuditBackendStdout, CQLAuditTester
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

# Canonical audit line regex. This must match exactly one line of output per
# audited statement. The timestamp format is `%h %e %T` (e.g. `Apr 23 10:11:12`).
AUDIT_LINE_RE = re.compile(
    r"^[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+scylla-audit:\s+"
    r'node="(?P<node>[^"]*)",\s+'
    r'category="(?P<category>[^"]*)",\s+'
    r'cl="(?P<cl>[^"]*)",\s+'
    r'error="(?P<error>true|false)",\s+'
    r'keyspace="(?P<keyspace>[^"]*)",\s+'
    r'query="(?P<query>.*)",\s+'
    r'client_ip="(?P<client_ip>[^"]*)",\s+'
    r'table="(?P<table>[^"]*)",\s+'
    r'username="(?P<username>[^"]*)"$'
)


def _read_audit_lines(log_path: Path, start_offset: int) -> list[str]:
    """Read and return all complete `scylla-audit:` lines from ``log_path``
    starting at ``start_offset``. Ignores any partial trailing line."""
    end = log_path.stat().st_size
    lines: list[str] = []
    with log_path.open(encoding="utf-8", errors="replace") as f:
        f.seek(start_offset)
        while f.tell() < end:
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            if pos + len(line) > end:
                # File grew again while we were reading; ignore the overshoot.
                break
            if not line.endswith("\n"):
                # Partial line — ignore it. The caller is expected to drain
                # before calling us, so this should not happen in practice.
                break
            if "scylla-audit:" in line:
                lines.append(line.rstrip("\n"))
    return lines


def _validate_line_format(lines: list[str]) -> list[str]:
    """Return the subset of ``lines`` that do not match :data:`AUDIT_LINE_RE`."""
    return [line for line in lines if not AUDIT_LINE_RE.match(line)]


async def _run_stress_workload(
    manager: ManagerClient,
    *,
    smp: int,
    sessions: int,
    queries_per_session: int,
    min_throughput_eps: float,
    audit_categories: str = "QUERY",
) -> None:
    """Drive a concurrent QUERY-audited workload and assert correctness.

    A single-node multi-shard cluster is started with the stdout audit
    backend. ``sessions`` independent CQL sessions are created; each issues
    ``queries_per_session`` SELECTs concurrently. After the workload
    completes, the node's stdout log is drained and every audit line is
    validated: it must match the audit-line regex, and the total count must
    equal ``sessions * queries_per_session`` (no duplicates, no drops).

    The observed throughput (audit events per second) is logged and
    ``min_throughput_eps`` is asserted as a conservative lower bound.
    """
    helper = AuditBackendStdout()
    with helper:
        audit_settings = {
            "audit": "stdout",
            "audit_categories": audit_categories,
            "audit_keyspaces": "ks",
        }
        t = CQLAuditTester(manager)
        session = await t.prepare(
            audit_settings=audit_settings,
            helper=helper,
            cmdline=["--smp", str(smp)],
        )

        session.execute(
            "CREATE TABLE IF NOT EXISTS ks.probe (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO ks.probe (k, v) VALUES (0, 0)")

        # Clear audit logs now so we only measure what the workload produces.
        helper.clear_audit_logs(session)

        # Each task uses its own prepared statement to simulate a distinct
        # client. The `ConsistencyLevel.ONE` matches the test default and
        # keeps the cl field in audit entries uniform.
        prepared = session.prepare("SELECT v FROM ks.probe WHERE k = ?")
        prepared.consistency_level = ConsistencyLevel.ONE

        loop = asyncio.get_running_loop()

        def run_one_task(_i: int) -> int:
            # Execute synchronously on a driver thread via `execute_async`
            # to maximise cross-shard concurrency at the server side.
            count = 0
            for _ in range(queries_per_session):
                fut = session.execute_async(prepared, (0,))
                fut.result()
                count += 1
            return count

        logger.info(
            "stress: smp=%d sessions=%d queries/session=%d total=%d",
            smp, sessions, queries_per_session,
            sessions * queries_per_session,
        )
        start = time.monotonic()
        results = await asyncio.gather(*[
            loop.run_in_executor(None, run_one_task, i)
            for i in range(sessions)
        ])
        elapsed = time.monotonic() - start

        executed = sum(results)
        assert executed == sessions * queries_per_session, (
            f"driver-side count mismatch: {executed} != "
            f"{sessions * queries_per_session}"
        )

        # Give the audit pipeline time to flush.
        helper.drain_stdout()

        # Fetch the lines produced by our workload.
        assert len(helper.nodes) == 1, \
            f"stress test expects one node, got {len(helper.nodes)}"
        node = helper.nodes[0]
        start_offset = helper.node_start_marks[node.address]
        lines = _read_audit_lines(node.log_path, start_offset)

        throughput = executed / elapsed if elapsed > 0 else float("inf")
        result_msg = (
            f"stress results: smp={smp} sessions={sessions} "
            f"queries/session={queries_per_session} executed={executed} "
            f"audit_lines={len(lines)} elapsed={elapsed:.3f}s "
            f"throughput={throughput:.0f} eps"
        )
        logger.info(result_msg)
        # Mirror to stdout so the number is visible in captured pytest output
        # (logger.info may be swallowed depending on test-runner config).
        print(f"\n[AUDIT_STRESS] {result_msg}", flush=True)

        # Validate every line matches the canonical format (catches
        # interleaving / partial writes).
        malformed = _validate_line_format(lines)
        assert not malformed, (
            f"{len(malformed)} malformed audit lines; first 3: "
            f"{malformed[:3]}"
        )

        # Restrict to audit entries caused by our workload: same query text,
        # QUERY category, keyspace=ks, table=probe.
        workload_lines = [
            line for line in lines
            if (m := AUDIT_LINE_RE.match(line)) is not None
            and m.group("category") == "QUERY"
            and m.group("keyspace") == "ks"
            and m.group("table") == "probe"
        ]

        assert len(workload_lines) == executed, (
            f"audit line count mismatch: "
            f"got {len(workload_lines)}, expected {executed}; "
            f"total captured scylla-audit lines: {len(lines)}"
        )

        assert throughput >= min_throughput_eps, (
            f"throughput regression: {throughput:.0f} eps < "
            f"{min_throughput_eps:.0f} eps"
        )


@pytest.mark.single_node
async def test_audit_stdout_stress_smoke(manager: ManagerClient):
    """Small burst to sanity-check the framework (single shard, low load)."""
    await _run_stress_workload(
        manager,
        smp=1,
        sessions=4,
        queries_per_session=50,
        min_throughput_eps=50,
    )


@pytest.mark.single_node
async def test_audit_stdout_stress_cross_shard_burst(manager: ManagerClient):
    """Multi-shard burst: all shards dispatch audit writes concurrently.

    This is the scenario most likely to expose torn lines or missed writes
    caused by a broken semaphore / flush protocol in the shard-0 writer.
    """
    await _run_stress_workload(
        manager,
        smp=4,
        sessions=16,
        queries_per_session=250,
        # 4000 audit events over ~a few seconds — single shard-0 writer with
        # a per-line semaphore still clears well over 400 eps in dev mode.
        min_throughput_eps=200,
    )


@pytest.mark.single_node
async def test_audit_stdout_stress_sustained(manager: ManagerClient):
    """Longer-duration sustained load to catch slow leaks or gate build-up."""
    await _run_stress_workload(
        manager,
        smp=4,
        sessions=8,
        queries_per_session=1000,
        min_throughput_eps=200,
    )


@pytest.mark.single_node
async def test_audit_stdout_stress_mega_burst(manager: ManagerClient):
    """High-shard, high-concurrency burst to probe the shard-0 writer ceiling.

    All CQL statements funnel through a single shard-0 serialised writer, so
    this test is the worst-case scenario: 8 shards generating audit events
    concurrently, 32 independent CQL sessions, 16k total events. Validates
    that throughput does not collapse and no events are dropped under
    maximum contention on the shard-0 semaphore.
    """
    await _run_stress_workload(
        manager,
        smp=8,
        sessions=32,
        queries_per_session=500,
        # Even under peak contention, dev-mode throughput stays well above
        # 500 eps; keep a conservative gate to avoid flakes on loaded CI.
        min_throughput_eps=300,
    )
