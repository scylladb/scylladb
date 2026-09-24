#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Shared harness for linearizability stress tests over a fixed set of keys.

The workload models each partition key as an independent register holding an
integer:

  * writers issue ``UPDATE <table> SET c = ? WHERE pk = ?`` with a globally
    unique, monotonically increasing value;
  * readers issue ``SELECT c FROM <table> WHERE pk = ?``;
  * an empty result or ``c IS NULL`` is read as the register's initial value 0,
    which is correct both for a never-written key and for a key whose table was
    just recreated.

Every call/return pair is recorded into a :class:`HistoryRecorder` and the whole
history is handed to the Porcupine checker (see ``test.cluster.tools.porcupine``)
in a single pass at the end of the test.

A test supplies its own *disruptor* — a coroutine that misbehaves in some
interesting way while the workload runs (random schema changes, ALTER TYPE,
node kills, tablet migrations, leader stepdowns, ...) — and an
:data:`~test.cluster.strong_consistency.outcomes.ExceptionPolicy` saying
which failures that disruption may legitimately cause.  The harness itself
knows only about the register model and about one disruption primitive that
changes the model: a *reset*, i.e. an operation that returns every register to
0 (DROP+CREATE of the table or of the whole keyspace).  Use
:meth:`RegisterWorkload.reset_window` around such an operation; it emits a
synthetic ``write(0)`` per key spanning the whole window, so Porcupine accepts
the reset happening at any instant inside it, and it marks the window so the
policy can tell a legitimate table-absence error from a real bug.

Typical usage::

    workload = RegisterWorkload(ks=ks, num_keys=100)
    await cql.run_async(f"CREATE TABLE {workload.fqtn} (pk int PRIMARY KEY, c int)")
    workload.prepare(cql)

    errors = await run_workload(
        workload, cql, duration_s=60,
        disruptors=[("schema-changer", lambda: schema_changer_task(state, cql))],
    )
    assert not errors, f"Task(s) failed: {errors}"
    workload.assert_progress(min_writes=100, min_reads=100)
    await check_linearizable(workload, output_dir=tmp_path / "porcupine")

The table must have the shape the statements above assume:
``(pk int PRIMARY KEY, c int)``, and it is left empty: a key that was never
written reads as the register's default value 0, which is exactly the path a
reader takes after a reset, so pre-initializing the rows would only hide that
path from the first half of the run.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator, Awaitable, Callable, Optional, Sequence

from cassandra.query import PreparedStatement

from test.cluster.tools.porcupine import run_porcupine_checker
from test.cluster.strong_consistency.outcomes import (
    ExceptionPolicy,
    FailureContext,
    Outcome,
    tolerate_reset_windows,
)

logger = logging.getLogger(__name__)


class HistoryRecorder:
    """Records call/return events for the Porcupine linearizability checker.

    Produces JSON-lines output consumable by the Go ``porcupine_checker``.
    Safe to use from coroutines in a single-threaded asyncio loop.

    .. note:: ``list[dict]`` is not the most efficient representation;
       if history sizes grow significantly, consider ``msgspec.Struct``
       with ``msgspec.json.encode`` for lower overhead.
    """

    def __init__(self) -> None:
        self._events: list[dict] = []
        self._next_id = 0
        self._pending = 0

    def record_call(self, client_id: int, op: str, key: int,
                    value: int = 0) -> tuple[int, int]:
        """Returns (op_id, absolute_time_ns)."""
        op_id = self._next_id
        self._next_id += 1
        t = time.monotonic_ns()
        self._events.append({
            "id": op_id,
            "client_id": client_id,
            "kind": "call",
            "op": op,
            "key": key,
            "value": value,
            "time_ns": t,
        })
        return op_id, t

    def record_return(self, op_id: int, client_id: int, op: str,
                      key: int, value: int, status: str,
                      time_ns: Optional[int] = None) -> int:
        """Returns absolute_time_ns.

        ``time_ns`` lets a caller that already took the timestamp — because it
        needed it to classify a failure — record the real return time rather
        than the time classification finished.
        """
        t = time.monotonic_ns() if time_ns is None else time_ns
        self._events.append({
            "id": op_id,
            "client_id": client_id,
            "kind": "return",
            "op": op,
            "key": key,
            "value": value,
            "time_ns": t,
            "status": status,
        })
        return t

    def leave_pending(self, op_id: int) -> None:
        """Deliberately record *no* return for an operation.

        This is how an indeterminate outcome is expressed.  Writing a return
        with status ``unknown`` would say two things: "we do not know whether
        it applied" and "…but if it did, it applied before this instant".  The
        client knows neither — for a client-side timeout the server never even
        learned the client gave up, and the write may reach the log after a
        later acknowledged write.

        An unpaired call is the encoding the checker is built for: it appends
        a synthetic indeterminate return *after every other event*, leaving the
        operation open to the end of the history.  That window is wide, which
        costs search time, but it is the only one that claims nothing the
        client could not observe.

        The cost is worth a number, because the checker reports exceeding its
        10s-per-key budget as ``valid: false``, which reads like a violation.
        Measured on the CI image, worst case — all pending operations mutually
        concurrent on one key, their values observed by later reads — 8 per key
        take 0.16s, 10 take 0.30s, 12 take 4.2s and 14 exhaust the budget.  A
        workload whose clients hold one operation in flight at a time cannot
        get near that: the whole run leaves at most
        ``num_writers * duration_s / request_timeout_s`` operations pending.
        Hence a note and not an assertion — one that cannot fire reads as
        protection while providing none.  A workload with concurrency *inside*
        a client removes that bound and should watch :attr:`pending_count`.
        """
        self._pending += 1

    def record_reset(self, client_id: int, keys: range,
                     t_call_ns: int, t_return_ns: int) -> tuple[int, int]:
        """Emit synthetic write(0) call/return pairs for every key.

        Models a DROP+RECREATE as resetting all registers to their initial
        value (0).  Each key gets its own operation with a unique op_id but
        shares the same call/return timestamps so that Porcupine sees the
        reset window identically for every key.

        Returns the (first, last) op_id of the emitted operations, so that the
        caller can point at them from its log line.
        """
        first_op_id = self._next_id
        for key in keys:
            op_id = self._next_id
            self._next_id += 1
            self._events.append({
                "id": op_id,
                "client_id": client_id,
                "kind": "call",
                "op": "write",
                "key": key,
                "value": 0,
                "time_ns": t_call_ns,
            })
            self._events.append({
                "id": op_id,
                "client_id": client_id,
                "kind": "return",
                "op": "write",
                "key": key,
                "value": 0,
                "time_ns": t_return_ns,
                "status": Outcome.OK,
            })

        return first_op_id, self._next_id - 1

    def to_jsonl(self) -> str:
        """Serialize all recorded events to a JSON-lines string."""
        return "".join(
            json.dumps(event, separators=(",", ":")) + "\n"
            for event in self._events
        )

    @property
    def event_count(self) -> int:
        return len(self._events)

    @property
    def op_count(self) -> int:
        return self._next_id

    @property
    def pending_count(self) -> int:
        """Operations left without a return; the checker reports the same
        number on stderr as 'N unpaired call events treated as indeterminate'."""
        return self._pending


def intervals_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return a_start <= b_end and b_start <= a_end


@dataclass
class ResetWindow:
    """Handle for one reset, yielded by :meth:`RegisterWorkload.reset_window`.

    The op ids are only filled in once the window closes, so they mean nothing
    until the ``async with`` block is left.  They let a test point its log line
    at the synthetic operations the reset added to ``history.jsonl`` — the file
    the checker dumps into its artifacts directory next to the visualization.
    """

    first_op_id: int = -1
    last_op_id: int = -1

    def __str__(self) -> str:
        return f"#{self.first_op_id}-#{self.last_op_id}"


@dataclass
class RegisterWorkload:
    """State shared by the writer/reader tasks of one linearizability test."""

    ks: str
    table_name: str = "main"
    # Tells this workload apart from the others when a test runs more than one
    # at a time -- a strongly consistent table beside an eventually consistent
    # one, or several SC keyspaces.  It is part of every RNG stream name, so
    # two workloads in one run do not replay the same choices.  Defaults to the
    # table name.  Whatever it is set to must be stable across runs, which
    # rules out the keyspace: new_test_keyspace() gives it a fresh unique name
    # every time, and the seed would then reproduce nothing.
    name: str = ""
    num_keys: int = 100
    num_writers: int = 4
    num_readers: int = 4
    # Pause between two consecutive DML operations of a single client.
    dml_pause_s: tuple[float, float] = (0.005, 0.02)
    # Decides how a failed operation is recorded; see the outcomes module.
    # The default tolerates only the harness's own disruption primitive.
    exception_policy: ExceptionPolicy = tolerate_reset_windows

    # Master seed for every RNG in the run: each task derives its own stream
    # from it via rng_for(), so adding or removing a task leaves the others
    # producing what they produced before.  A fresh one per run by default; it
    # is logged when the workload starts and quoted in the assertion messages,
    # and passing it back in replays the same choices.  Only the *choices* are
    # reproducible — the interleaving of the concurrent clients and of the
    # disruptor is not, so a replay reproduces a timing bug only by luck.
    seed: int = field(default_factory=lambda: random.randrange(sys.maxsize))

    history: HistoryRecorder = field(default_factory=HistoryRecorder)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)

    write_stmt: Optional[PreparedStatement] = field(default=None, repr=False)
    read_stmt: Optional[PreparedStatement] = field(default=None, repr=False)

    write_success: int = 0
    write_failed: int = 0
    write_indeterminate: int = 0

    read_success: int = 0
    read_empty: int = 0
    read_failed: int = 0
    read_indeterminate: int = 0

    reset_count: int = 0

    _next_value: int = field(default=1, init=False)
    # Closed windows for completed resets (DROP+CREATE sequences).
    _reset_intervals: list[tuple[int, int]] = field(default_factory=list, init=False)
    # Non-zero while a reset is in progress; interpreted as [start, +inf).
    _reset_start_ns: int = field(default=0, init=False)

    @property
    def fqtn(self) -> str:
        return f"{self.ks}.{self.table_name}"

    @property
    def reset_client_id(self) -> int:
        """Client id used for the synthetic write(0) operations of a reset.

        Distinct from every writer and reader id, so that within one key the
        reset never looks like a second concurrent operation of a client that
        already has one in flight.
        """
        return self.num_writers + self.num_readers + 1

    def rng_for(self, stream: str) -> random.Random:
        """A generator for one task, derived from the run's master seed."""
        return random.Random(f"{self.seed}/{self.name or self.table_name}/{stream}")

    def next_write_value(self) -> int:
        """Globally unique value; makes every write distinguishable in the history."""
        v = self._next_value
        self._next_value += 1
        return v

    def overlaps_reset(self, t_call_ns: int, t_return_ns: int) -> bool:
        """True if [t_call_ns, t_return_ns] intersects any reset window."""
        for iv_start, iv_end in self._reset_intervals:
            if intervals_overlap(t_call_ns, t_return_ns, iv_start, iv_end):
                return True

        if self._reset_start_ns:
            return t_return_ns >= self._reset_start_ns

        return False

    def prepare(self, cql) -> None:
        """(Re-)prepare the read/write statements.

        Must be called again after the table is recreated: the driver tags
        prepared statements with the table's UUID, which changes on recreate.
        """
        self.write_stmt = cql.prepare(f"UPDATE {self.fqtn} SET c = ? WHERE pk = ?")
        self.read_stmt = cql.prepare(f"SELECT c FROM {self.fqtn} WHERE pk = ?")
        logger.debug("Prepared read/write statements for %s", self.fqtn)

    @contextlib.asynccontextmanager
    async def reset_window(self) -> AsyncIterator[ResetWindow]:
        """Wrap an operation that resets every register to 0.

        Opens the window *before* any DDL is issued, so that DML errors caused
        by the table being absent can be recognised from the very first moment.
        On success, emits a synthetic write(0) per key whose [call, return]
        interval spans the whole window, so Porcupine knows the reset could
        have taken effect at any point inside it.

        The body is responsible for recreating the table and calling
        :meth:`prepare` again::

            async with workload.reset_window() as reset:
                await cql.run_async(f"DROP TABLE {workload.fqtn}")
                await cql.run_async(f"CREATE TABLE {workload.fqtn} {SCHEMA}")
                workload.prepare(cql)
            logger.info("recreated; reset ops %s", reset)

        Not re-entrant: one reset at a time per workload.
        """
        window = ResetWindow()
        window_start_ns = time.monotonic_ns()
        self._reset_start_ns = window_start_ns

        try:
            yield window
        finally:
            window_end_ns = time.monotonic_ns()
            self._reset_intervals.append((window_start_ns, window_end_ns))
            self._reset_start_ns = 0

        window.first_op_id, window.last_op_id = self.history.record_reset(
            self.reset_client_id, range(self.num_keys),
            window_start_ns, window_end_ns)

        self.reset_count += 1

    def classify(self, exc: BaseException, ctx: FailureContext) -> Outcome:
        """Ask the policy how to record a failed operation.

        Raises AssertionError if the policy does not recognise the exception:
        an unexpected failure is the finding the test is looking for, not
        something to be swallowed.
        """
        outcome = self.exception_policy(exc, ctx)
        if outcome is None:
            raise AssertionError(
                f"{ctx.op} by client {ctx.client_id} for pk={ctx.key}: "
                f"unexpected {type(exc).__name__}, not accepted by the "
                f"exception policy (overlaps_reset="
                f"{self.overlaps_reset(ctx.t_call_ns, ctx.t_return_ns)}): {exc!r}"
            ) from exc
        return outcome

    def stats_line(self) -> str:
        return (
            f"writes ok={self.write_success} fail={self.write_failed} "
            f"indeterminate={self.write_indeterminate} | "
            f"reads ok={self.read_success} empty={self.read_empty} "
            f"fail={self.read_failed} indeterminate={self.read_indeterminate} | "
            f"resets={self.reset_count} | "
            f"ops={self.history.op_count} events={self.history.event_count} "
            f"pending={self.history.pending_count}"
        )

    def assert_progress(self, min_writes: int, min_reads: int = 0) -> None:
        """Guard against a test that passed only because nothing happened."""
        assert self.write_success >= min_writes, (
            f"Too few successful writes ({self.write_success}), "
            f"expected >= {min_writes}"
        )
        assert self.read_success + self.read_empty >= min_reads, (
            f"Too few successful reads ({self.read_success + self.read_empty}), "
            f"expected >= {min_reads}"
        )


async def writer_task(workload: RegisterWorkload, cql, writer_id: int) -> None:
    rng = workload.rng_for(f"writer/{writer_id}")
    logger.info("Writer %d started", writer_id)

    local_writes = 0
    local_failed = 0
    local_indeterminate = 0
    while not workload.stop_event.is_set():
        pk = rng.randint(0, workload.num_keys - 1)
        value = workload.next_write_value()

        op_id, t_call_ns = workload.history.record_call(writer_id, "write", pk, value)

        try:
            bound = workload.write_stmt.bind([value, pk])
            await cql.run_async(bound)

            workload.history.record_return(
                op_id, writer_id, "write", pk, value, Outcome.OK)
            workload.write_success += 1
            local_writes += 1

        except Exception as exc:
            t_return_ns = time.monotonic_ns()
            ctx = FailureContext(
                workload=workload, op="write", client_id=writer_id, key=pk,
                t_call_ns=t_call_ns, t_return_ns=t_return_ns,
            )
            try:
                outcome = workload.classify(exc, ctx)
            except AssertionError:
                # Unexpected failure: the test is over, but leave the write
                # pending first — we do not know whether it applied, and a
                # history claiming otherwise would mislead the post-mortem.
                workload.history.leave_pending(op_id)
                raise

            if outcome is Outcome.UNKNOWN:
                workload.history.leave_pending(op_id)
                workload.write_indeterminate += 1
                local_indeterminate += 1
            else:
                workload.history.record_return(
                    op_id, writer_id, "write", pk, value, outcome, t_return_ns)
                workload.write_failed += 1
                local_failed += 1

        # Paced even on the failure path: a disruption that keeps failing (a
        # node that stays down) would otherwise spin the client at full speed
        # and bury the history under junk operations.
        await asyncio.sleep(rng.uniform(*workload.dml_pause_s))

    logger.info(
        "Writer %d finished: writes=%d failed=%d indeterminate=%d",
        writer_id, local_writes, local_failed, local_indeterminate,
    )


async def reader_task(workload: RegisterWorkload, cql, reader_id: int) -> None:
    rng = workload.rng_for(f"reader/{reader_id}")
    client_id = workload.num_writers + reader_id
    logger.info("Reader %d started (client_id=%d)", reader_id, client_id)

    local_reads = 0
    local_failed = 0
    local_indeterminate = 0

    while not workload.stop_event.is_set():
        pk = rng.randint(0, workload.num_keys - 1)

        op_id, t_call_ns = workload.history.record_call(client_id, "read", pk)

        try:
            bound = workload.read_stmt.bind([pk])
            rows = await cql.run_async(bound)

            if not rows:
                # Empty result = register has default value 0.
                # This is correct both for never-written keys and after
                # DROP+RECREATE (the synthetic write(0) resets the model).
                value = 0
                workload.read_empty += 1
            else:
                # A row can exist with c = NULL: a disruptor that writes some
                # other column of the table — an ADD COLUMN followed by a
                # write to the new column, say — creates the row without
                # touching c.  Nothing was written to the register in that
                # case either, so it means the same as an absent row: the
                # default value 0.
                row = rows[0]
                value = 0 if row.c is None else row.c
                workload.read_success += 1

            workload.history.record_return(
                op_id, client_id, "read", pk, value, Outcome.OK)
            local_reads += 1

        except Exception as exc:
            t_return_ns = time.monotonic_ns()
            ctx = FailureContext(
                workload=workload, op="read", client_id=client_id, key=pk,
                t_call_ns=t_call_ns, t_return_ns=t_return_ns,
            )
            try:
                outcome = workload.classify(exc, ctx)
            except AssertionError:
                # See writer_task: keep the history truthful, then fail.
                workload.history.leave_pending(op_id)
                raise

            if outcome is Outcome.UNKNOWN:
                workload.history.leave_pending(op_id)
                workload.read_indeterminate += 1
                local_indeterminate += 1
            else:
                workload.history.record_return(
                    op_id, client_id, "read", pk, 0, outcome, t_return_ns)
                workload.read_failed += 1
                local_failed += 1

        # See writer_task: pace the failure path too.
        await asyncio.sleep(rng.uniform(*workload.dml_pause_s))

    logger.info(
        "Reader %d finished: reads=%d failed=%d indeterminate=%d",
        reader_id, local_reads, local_failed, local_indeterminate,
    )


# A disruptor is started together with the workload and is expected to stop
# itself once workload.stop_event is set.
DisruptorFactory = Callable[[], Awaitable[None]]


async def run_workload(
    workload: RegisterWorkload,
    cql,
    duration_s: float,
    disruptors: Sequence[tuple[str, DisruptorFactory]] = (),
) -> list[BaseException]:
    """Run writers, readers and the given disruptors for ``duration_s``.

    Returns the exceptions raised by the tasks; an empty list means every task
    finished cleanly.  Errors are returned rather than raised so that the
    caller can still log statistics and run the linearizability check on the
    history collected so far.
    """
    logger.info(
        "Starting workload for %.0fs: writers=%d readers=%d keys=%d "
        "disruptors=%s seed=%d",
        duration_s, workload.num_writers, workload.num_readers,
        workload.num_keys, [name for name, _ in disruptors], workload.seed,
    )

    task_errors: list[BaseException] = []
    try:
        async with asyncio.TaskGroup() as tg:
            async def stop_timer():
                try:
                    await asyncio.sleep(duration_s)
                finally:
                    workload.stop_event.set()

            tg.create_task(stop_timer(), name="stop-timer")
            for i in range(workload.num_writers):
                tg.create_task(writer_task(workload, cql, i), name=f"writer-{i}")
            for i in range(workload.num_readers):
                tg.create_task(reader_task(workload, cql, i), name=f"reader-{i}")
            for name, factory in disruptors:
                tg.create_task(factory(), name=name)
    except* Exception as eg:
        # 'return' is not allowed inside an except* block.
        task_errors = list(eg.exceptions)
        logger.error("Task(s) failed: %s", task_errors)

    return task_errors


async def check_linearizable(
    workload: RegisterWorkload,
    output_dir: Path | None = None,
) -> dict:
    """Run the Porcupine checker over the recorded history and assert validity."""
    logger.info(
        "Running Porcupine linearizability check: %d ops, %d events; artifacts_dir=%s",
        workload.history.op_count, workload.history.event_count, output_dir,
    )

    result = await run_porcupine_checker(
        workload.history.to_jsonl(),
        output_dir=output_dir,
    )

    if result.get("visualization"):
        logger.info("Visualization: %s", result["visualization"])

    # The checker has one way to say "not valid", and it uses it both for a
    # real violation and for "I ran out of time deciding".  Only the message
    # tells them apart, so don't report the second as the first.
    error = str(result.get("error") or "")
    assert not (not result["valid"] and "timed out" in error), (
        f"The linearizability check did not finish, so nothing was proven: "
        f"{error}\n"
        f"This is the checker's 10s-per-key budget, not a violation. It is "
        f"usually caused by too many indeterminate ('unknown') writes on one "
        f"key — see HistoryRecorder.leave_pending for the measured limits.\n"
        f"seed={workload.seed}\n"
        f"artifacts_dir={result.get('artifacts_dir')}"
    )

    assert result["valid"], (
        f"Linearizability violation: {result.get('error')}\n"
        f"seed={workload.seed}\n"
        f"keys_checked={result.get('keys_checked')}, "
        f"total_ops={result.get('total_ops')}\n"
        f"artifacts_dir={result.get('artifacts_dir')}\n"
        f"visualization={result.get('visualization')}\n"
        f"full_result={result}"
    )

    logger.info(
        "Linearizable (keys_checked=%d, total_ops=%d)",
        result.get("keys_checked", 0), result.get("total_ops", 0),
    )
    return result
