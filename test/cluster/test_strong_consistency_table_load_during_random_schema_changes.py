#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Stress test: randomized schema changes concurrent with SC (strongly-consistent)
table read/write workload, with linearizability checking via Porcupine.

The test performs a mix of reads and writes on a single SC table, while a
  - DROP+RECREATE is modeled as a synthetic write(0) per key in the
    Porcupine history, eliminating the need for generation tracking.
    The entire operation history is checked in a single Porcupine pass.
  - Empty SELECT results are treated as read(0) (the register's default),
    which is correct both for the initial empty table and after a recreate.
  - InvalidRequest from read/write is classified using actual overlap with the
    DROP+CREATE recreate window, but ONLY for table-absence errors.
  - Unexpected InvalidRequest outside the recreate window fails the test.

Context: scylladb/scylladb#28546
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid as _uuid
from dataclasses import dataclass, field
from typing import Awaitable, Optional, Callable

import pytest
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import InvalidRequest
from cassandra.query import PreparedStatement
from test.cluster.tools.porcupine import run_porcupine_checker
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


NUM_KEYS = 100
NUM_WRITERS = 4
NUM_READERS = 4
STRESS_DURATION_S = 60
NUM_TABLETS = 10


_CQL_LITERAL_GENERATORS: dict[str, Callable[[random.Random], str]] = {
    "int":       lambda rng: str(rng.randint(1, 10000)),
    "bigint":    lambda rng: str(rng.randint(1, 10**12)),
    "varint":    lambda rng: str(rng.randint(1, 10**12)),
    "text":      lambda rng: f"'sanity_{rng.randint(0, 9999)}'",
    "blob":      lambda rng: f"0x{rng.getrandbits(32):08x}",
    "boolean":   lambda rng: rng.choice(["true", "false"]),
    "uuid":      lambda rng: str(_uuid.UUID(int=rng.getrandbits(128))),
    "timestamp": lambda rng: f"'{2020 + rng.randint(0, 5)}-06-15T12:00:00Z'",
}

COLUMN_TYPES = list(_CQL_LITERAL_GENERATORS.keys())
ALTER_TYPE_TARGET = "varint"

TABLE_SCHEMA = "(pk int PRIMARY KEY, c int)"

# Dedicated client_id for synthetic reset events emitted by DROP+RECREATE.
RESET_CLIENT_ID = NUM_WRITERS + NUM_READERS + 1

# Exceptions tolerated during DROP+RECREATE windows.
_RECREATE_TOLERANT_EXCEPTIONS = (InvalidRequest, NoHostAvailable)


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
                      key: int, value: int, status: str) -> int:
        """Returns absolute_time_ns."""
        t = time.monotonic_ns()
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

    def record_reset(self, client_id: int, keys: range,
                     t_call_ns: int, t_return_ns: int) -> None:
        """Emit synthetic write(0) call/return pairs for every key.

        Models a DROP+RECREATE as resetting all registers to their initial
        value (0).  Each key gets its own operation with a unique op_id but
        shares the same call/return timestamps so that Porcupine sees the
        reset window identically for every key.
        """
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
                "status": "ok",
            })

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


def _intervals_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return a_start <= b_end and b_start <= a_end

# TODO(SCYLLADB-1450): these substring checks have not been validated
# against all possible server error messages for absent tables.
def _is_table_absence_invalid_request(exc: InvalidRequest) -> bool:
    msg = str(exc).lower()
    return (
        "unconfigured table" in msg
        or "does not exist" in msg
        or "unknown table" in msg
        or "undefined table" in msg
    )


def _is_stale_table_uuid(exc: NoHostAvailable) -> bool:
    """Detect NoHostAvailable caused by a prepared statement referencing a
    dropped table's UUID.  After DROP+RECREATE the new table gets a new UUID,
    but the driver may still send requests tagged with the old one."""
    for inner in exc.errors.values():
        msg = str(inner).lower()
        if "can't find a column family" in msg:
            return True
    return False


def _is_table_absence_error(exc: Exception) -> bool:
    """Return True if the exception indicates the table is absent (dropped/recreated)."""
    if isinstance(exc, InvalidRequest):
        return _is_table_absence_invalid_request(exc)
    if isinstance(exc, NoHostAvailable):
        return _is_stale_table_uuid(exc)
    return False


@dataclass
class SCSchemaTestState:
    ks: str
    table_name: str = "main"
    history: HistoryRecorder = field(default_factory=HistoryRecorder)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)

    _next_value: int = 1

    added_columns: list[str] = field(default_factory=list)
    current_c_type: str = "int"
    col_counter: int = 0

    write_stmt: Optional[PreparedStatement] = field(default=None, repr=False)
    read_stmt: Optional[PreparedStatement] = field(default=None, repr=False)

    write_success: int = 0
    writer_table_absent: int = 0

    read_success: int = 0
    read_empty: int = 0
    reader_table_absent: int = 0
    schema_ops: int = 0
    recreate_count: int = 0

    # Closed windows for completed DROP+CREATE sequences.
    _recreate_intervals: list[tuple[int, int]] = field(default_factory=list, init=False)
    # Non-zero while DROP+CREATE is in progress; interpreted as [start, +inf).
    _recreate_start_ns: int = field(default=0, init=False)

    @property
    def fqtn(self) -> str:
        return f"{self.ks}.{self.table_name}"

    def next_write_value(self) -> int:
        v = self._next_value
        self._next_value += 1
        return v

    def next_col_name(self) -> str:
        self.col_counter += 1
        return f"col_{self.col_counter}"

    def overlaps_recreate(self, t_call_ns: int, t_return_ns: int) -> bool:
        for iv_start, iv_end in self._recreate_intervals:
            if _intervals_overlap(t_call_ns, t_return_ns, iv_start, iv_end):
                return True

        if self._recreate_start_ns:
            return t_return_ns >= self._recreate_start_ns

        return False


def prepare_rw_statements(state: SCSchemaTestState, cql) -> None:
    state.write_stmt = cql.prepare(
        f"UPDATE {state.fqtn} SET c = ? WHERE pk = ?"
    )
    state.read_stmt = cql.prepare(
        f"SELECT c FROM {state.fqtn} WHERE pk = ?"
    )
    logger.debug("Prepared read/write statements for %s", state.fqtn)


async def writer_task(state: SCSchemaTestState, cql, writer_id: int) -> None:
    rng = random.Random(writer_id)
    logger.info("Writer %d started", writer_id)

    local_writes = 0
    local_indeterminate = 0
    while not state.stop_event.is_set():
        pk = rng.randint(0, NUM_KEYS - 1)
        value = state.next_write_value()

        op_id, t_call_ns = state.history.record_call(writer_id, "write", pk, value)

        try:
            bound = state.write_stmt.bind([value, pk])
            await cql.run_async(bound)

            state.history.record_return(
                op_id, writer_id, "write", pk, value, "ok")
            state.write_success += 1
            local_writes += 1

        except _RECREATE_TOLERANT_EXCEPTIONS as exc:
            t_return_ns = state.history.record_return(
                op_id, writer_id, "write", pk, value, "fail")
            if (
                _is_table_absence_error(exc)
                and state.overlaps_recreate(t_call_ns, t_return_ns)
            ):
                state.writer_table_absent += 1
                local_indeterminate += 1
                continue

            raise AssertionError(
                f"Writer {writer_id}: unexpected {type(exc).__name__} for pk={pk}, "
                f"overlaps_recreate="
                f"{state.overlaps_recreate(t_call_ns, t_return_ns)}: {exc!r}"
            ) from exc

        await asyncio.sleep(rng.uniform(0.005, 0.02))

    logger.info(
        "Writer %d finished: writes=%d indeterminate=%d",
        writer_id, local_writes, local_indeterminate,
    )


async def reader_task(state: SCSchemaTestState, cql, reader_id: int) -> None:
    rng = random.Random(1000 + reader_id)
    client_id = NUM_WRITERS + reader_id
    logger.info("Reader %d started (client_id=%d)", reader_id, client_id)

    local_reads = 0
    local_indeterminate = 0

    while not state.stop_event.is_set():
        pk = rng.randint(0, NUM_KEYS - 1)

        op_id, t_call_ns = state.history.record_call(client_id, "read", pk)

        try:
            bound = state.read_stmt.bind([pk])
            rows = await cql.run_async(bound)

            if not rows:
                # Empty result = register has default value 0.
                # This is correct both for never-written keys and after
                # DROP+RECREATE (the synthetic write(0) resets the model).
                state.history.record_return(
                    op_id, client_id, "read", pk, 0, "ok")
                state.read_empty += 1
                local_reads += 1
                continue

            # Treat NULL in c the same as an absent row: logical register value 0.
            row = rows[0]
            value = 0 if row.c is None else row.c

            state.history.record_return(
                op_id, client_id, "read", pk, value, "ok")
            state.read_success += 1
            local_reads += 1

        except _RECREATE_TOLERANT_EXCEPTIONS as exc:
            t_return_ns = state.history.record_return(
                op_id, client_id, "read", pk, 0, "fail")
            if (
                _is_table_absence_error(exc)
                and state.overlaps_recreate(t_call_ns, t_return_ns)
            ):
                state.reader_table_absent += 1
                local_indeterminate += 1
                continue

            raise AssertionError(
                f"Reader {reader_id}: unexpected {type(exc).__name__} for pk={pk}, "
                f"overlaps_recreate="
                f"{state.overlaps_recreate(t_call_ns, t_return_ns)}: {exc!r}"
            ) from exc

        await asyncio.sleep(rng.uniform(0.005, 0.02))

    logger.info(
        "Reader %d finished: reads=%d indeterminate=%d",
        reader_id, local_reads, local_indeterminate,
    )


SchemaOpHandler = Callable[
    ["SCSchemaTestState", object, random.Random],
    Awaitable[Optional[str]],
]


def _random_cql_literal(col_type: str, rng: random.Random) -> str:
    return _CQL_LITERAL_GENERATORS[col_type](rng)


async def op_add_column_and_sanity_check(
    state: SCSchemaTestState, cql, rng: random.Random,
) -> Optional[str]:
    col_name = state.next_col_name()
    col_type = rng.choice(COLUMN_TYPES)
    await cql.run_async(f"ALTER TABLE {state.fqtn} ADD {col_name} {col_type}")
    state.added_columns.append(col_name)
    logger.info("DDL: ADD COLUMN %s %s", col_name, col_type)

    # Sanity: write a value to the new column, read it back.
    pk = rng.randint(0, NUM_KEYS - 1)
    literal = _random_cql_literal(col_type, rng)
    await cql.run_async(
        f"UPDATE {state.fqtn} SET {col_name} = {literal} WHERE pk = {pk}")
    rows = await cql.run_async(
        f"SELECT {col_name} FROM {state.fqtn} WHERE pk = {pk}")
    assert rows, (
        f"Sanity check failed: no row returned after writing "
        f"{col_name}={literal} at pk={pk}"
    )
    assert getattr(rows[0], col_name) is not None, (
        f"Sanity check failed: {col_name} is NULL after writing "
        f"{literal} at pk={pk}"
    )
    logger.info("DDL: ADD COLUMN %s %s — sanity OK (pk=%d)", col_name, col_type, pk)

    return "add_column"


async def op_drop_column(
    state: SCSchemaTestState, cql, rng: random.Random,
) -> Optional[str]:
    if not state.added_columns:
        return None
    col_name = rng.choice(state.added_columns)
    await cql.run_async(f"ALTER TABLE {state.fqtn} DROP {col_name}")
    state.added_columns.remove(col_name)
    logger.info("DDL: DROP COLUMN %s", col_name)
    return "drop_column"


async def op_alter_type_c(
    state: SCSchemaTestState, cql, rng: random.Random,
) -> Optional[str]:
    await cql.run_async(
        f"ALTER TABLE {state.fqtn} ALTER c TYPE {ALTER_TYPE_TARGET}")
    state.current_c_type = ALTER_TYPE_TARGET
    logger.info("DDL: ALTER TYPE c int -> %s", ALTER_TYPE_TARGET)
    return "alter_type"


async def op_alter_properties(
    state: SCSchemaTestState, cql, rng: random.Random,
) -> Optional[str]:
    props = rng.choice([
        f"comment = 'stress_{rng.randint(0, 9999)}'",
        f"gc_grace_seconds = {rng.randint(100, 100000)}",
        f"default_time_to_live = {rng.randint(50, 3600)}",
    ])
    await cql.run_async(f"ALTER TABLE {state.fqtn} WITH {props}")
    logger.info("DDL: ALTER TABLE WITH %s", props)
    return "alter_props"


async def op_drop_recreate(
    state: SCSchemaTestState, cql, rng: random.Random,
) -> Optional[str]:
    logger.info("DDL: DROP+RECREATE starting")

    # Open recreate window BEFORE any DDL.
    # While open, all DML errors for table absence → "fail".
    window_start_ns = time.monotonic_ns()
    state._recreate_start_ns = window_start_ns

    try:
        await cql.run_async(f"DROP TABLE {state.fqtn}")
        logger.info("DDL: DROP TABLE done")

        await cql.run_async(f"CREATE TABLE {state.fqtn} {TABLE_SCHEMA}")
        state.added_columns.clear()
        state.current_c_type = "int"
        prepare_rw_statements(state, cql)
    finally:
        window_end_ns = time.monotonic_ns()
        state._recreate_intervals.append((window_start_ns, window_end_ns))
        state._recreate_start_ns = 0

    # Emit synthetic write(0) for every key.  The [call, return] interval
    # spans the entire DROP+CREATE window so Porcupine knows the reset
    # could have happened at any point within.
    state.history.record_reset(
        RESET_CLIENT_ID, range(NUM_KEYS), window_start_ns, window_end_ns)

    state.recreate_count += 1
    logger.info("DDL: DROP+RECREATE done (recreate #%d)", state.recreate_count)
    return "drop_recreate"


SCHEMA_OPS = [
    ("add_column", 3),
    ("drop_column", 2),
    ("alter_type", 1),
    ("alter_props", 2),
    ("drop_recreate", 2),
]


SCHEMA_OP_HANDLERS: dict[str, SchemaOpHandler] = {
    "add_column": op_add_column_and_sanity_check,
    "drop_column": op_drop_column,
    "alter_type": op_alter_type_c,
    "alter_props": op_alter_properties,
    "drop_recreate": op_drop_recreate,
}


async def schema_changer_task(state: SCSchemaTestState, cql) -> None:
    rng = random.Random(42)
    logger.info("Schema changer started")
    # Keep picking until we either execute a real schema change
    # or the test is asked to stop.
    while not state.stop_event.is_set():
        result = None

        while result is None and not state.stop_event.is_set():
            available_ops = [
                (name, weight) for name, weight in SCHEMA_OPS
                if not (name == "alter_type" and state.current_c_type != "int")
                if not (name == "drop_column" and not state.added_columns)
            ]
            op_names = [name for name, _ in available_ops]
            op_weights = [weight for _, weight in available_ops]

            [op_name] = rng.choices(op_names, weights=op_weights)
            result = await SCHEMA_OP_HANDLERS[op_name](state, cql, rng)

        if result is None:
            break

        state.schema_ops += 1
        logger.info("Schema op #%d: %s", state.schema_ops, result)
        await asyncio.sleep(rng.uniform(1.0, 3.0))

    logger.info("Schema changer finished: %d ops", state.schema_ops)


@pytest.mark.asyncio
async def test_sc_linearizability_with_schema_changes(
    manager: ManagerClient, tmp_path,
):
    logger.info("Bootstrapping cluster")
    config = {
        "experimental_features": ["strongly-consistent-tables"],
    }
    cmdline = [
        "--logger-log-level", "sc_groups_manager=debug",
        "--logger-log-level", "sc_coordinator=debug",
    ]

    servers = await manager.servers_add(
        6,
        config=config,
        cmdline=cmdline,
        auto_rack_dc="my_dc",
    )

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    ks_opts = (
        "WITH replication = "
        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
        f"AND tablets = {{'initial': {NUM_TABLETS}}} "
        "AND consistency = 'global'"
    )

    async with new_test_keyspace(manager, ks_opts) as ks:
        state = SCSchemaTestState(ks=ks, table_name="main")

        await cql.run_async(f"CREATE TABLE {state.fqtn} {TABLE_SCHEMA}")
        prepare_rw_statements(state, cql)

        logger.info("Pre-initializing %d rows with c=0", NUM_KEYS)
        await asyncio.gather(*[
            cql.run_async(state.write_stmt.bind([0, pk]))
            for pk in range(NUM_KEYS)
        ])
        logger.info(
            "Starting stress phase (%ds): writers=%d readers=%d "
            "keys=%d tablets=%d + schema changer",
            STRESS_DURATION_S, NUM_WRITERS, NUM_READERS,
            NUM_KEYS, NUM_TABLETS,
        )

        task_errors = []
        try:
            async with asyncio.TaskGroup() as tg:
                async def stop_timer():
                    try:
                        await asyncio.sleep(STRESS_DURATION_S)
                    finally:
                        state.stop_event.set()

                tg.create_task(stop_timer(), name="stop-timer")
                for i in range(NUM_WRITERS):
                    tg.create_task(
                        writer_task(state, cql, i), name=f"writer-{i}")
                for i in range(NUM_READERS):
                    tg.create_task(
                        reader_task(state, cql, i), name=f"reader-{i}")
                tg.create_task(
                    schema_changer_task(state, cql), name="schema-changer")
        except* Exception as eg:
            task_errors = list(eg.exceptions)
            logger.error("Task(s) failed: %s", task_errors)

        logger.info("Stress phase complete")
        logger.info(
            "Stats: writes ok=%d table_absent=%d | "
            "reads ok=%d empty=%d table_absent=%d | "
            "schema_ops=%d recreates=%d | "
            "ops=%d events=%d",
            state.write_success,
            state.writer_table_absent,
            state.read_success, state.read_empty,
            state.reader_table_absent,
            state.schema_ops, state.recreate_count,
            state.history.op_count, state.history.event_count,
        )

        assert not task_errors, (
            f"Task(s) failed with unexpected exceptions: {task_errors}"
        )

        assert state.write_success >= 100, (
            f"Too few successful writes ({state.write_success}), expected >= 100"
        )
        assert state.schema_ops >= 3, (
            f"Too few schema operations ({state.schema_ops}), expected >= 3"
        )

        checker_output_dir = tmp_path / "porcupine-checker-output"
        logger.info(
            "Running Porcupine linearizability check: %d ops, %d events; artifacts_dir=%s",
            state.history.op_count,
            state.history.event_count,
            checker_output_dir,
        )

        result = await run_porcupine_checker(
            state.history.to_jsonl(),
            output_dir=checker_output_dir,
        )

        if result.get("visualization"):
            logger.info("Visualization: %s", result["visualization"])

        assert result["valid"], (
            f"Linearizability violation: {result.get('error')}\n"
            f"keys_checked={result.get('keys_checked')}, "
            f"total_ops={result.get('total_ops')}\n"
            f"artifacts_dir={result.get('artifacts_dir')}\n"
            f"visualization={result.get('visualization')}\n"
            f"full_result={result}"
        )

        logger.info(
            "Test passed — linearizable (keys_checked=%d, total_ops=%d)",
            result.get("keys_checked", 0),
            result.get("total_ops", 0),
        )
