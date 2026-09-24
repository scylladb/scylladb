#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Stress test: randomized schema changes concurrent with SC (strongly-consistent)
table read/write workload, with linearizability checking via Porcupine.

The test performs a mix of reads and writes on a single SC table, while a
background task performs randomized schema changes.  The cluster/keyspace setup
and the register workload itself (writers, readers, history recording, the
Porcupine run) come from ``test.cluster.strong_consistency``; this module
only defines the schema changer and the assertions specific to it.

The only failures tolerated here are the ones a DROP+RECREATE can cause, which
is the default exception policy.  A test that also kills nodes or moves tablets
must widen that explicitly (see ``outcomes.tolerate_timeouts``).

  - DROP+RECREATE is modeled as a synthetic write(0) per key in the
    Porcupine history, eliminating the need for generation tracking.
    The entire operation history is checked in a single Porcupine pass.
  - Empty SELECT results are treated as read(0) (the register's default),
    which is correct both for the initial empty table and after a recreate.
    The table is left empty on purpose, so that a reader takes the same path
    from the first second of the run as it does after every DROP+RECREATE.
  - InvalidRequest from read/write is classified using actual overlap with the
    DROP+CREATE recreate window, but ONLY for table-absence errors.
  - Unexpected InvalidRequest outside the recreate window fails the test.

Context: SCYLLADB-975
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid as _uuid
from dataclasses import dataclass, field
from typing import Awaitable, Optional, Callable

import pytest
from test.cluster.strong_consistency.config import boot_sc_cluster, sc_keyspace_opts
from test.cluster.strong_consistency.workload import (
    RegisterWorkload,
    check_linearizable,
    run_workload,
)
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


# --- Cluster and schema configuration ----------------------------------------

NUM_NODES = 6
REPLICATION_FACTOR = 3
NUM_TABLETS = 10

KEYSPACE_OPTS = sc_keyspace_opts(
    replication_factor=REPLICATION_FACTOR, initial_tablets=NUM_TABLETS)

TABLE_NAME = "main"

# --- Workload configuration --------------------------------------------------

NUM_KEYS = 100
NUM_WRITERS = 4
NUM_READERS = 4
STRESS_DURATION_S = 60

# Range for the randomized `default_time_to_live` property.  It applies to
# every subsequent write, so a row written during the run must not be able to
# expire before a later read: the reader would record the missing row as
# read(0), while the Porcupine register model has no notion of expiration.
# Derived from the run duration so the margin survives a change of
# STRESS_DURATION_S.
DEFAULT_TTL_RANGE_S = (10 * STRESS_DURATION_S, 100 * STRESS_DURATION_S)

# Pause between two consecutive operations of a single reader/writer.
DML_PAUSE_S = (0.005, 0.02)
# Pause between two consecutive schema changes.
SCHEMA_OP_PAUSE_S = (1.0, 3.0)

# Sanity thresholds: the run is only meaningful if it actually did some work.
MIN_EXPECTED_WRITES = 100
MIN_EXPECTED_READS = 100
MIN_EXPECTED_SCHEMA_OPS = 3


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
# Disabled together with the `alter_type` schema op, see BUG_ALTER_TYPE below.
# ALTER_TYPE_TARGET = "varint"

TABLE_SCHEMA = "(pk int PRIMARY KEY, c int)"


@dataclass
class SchemaChangerState:
    """State owned by the schema changer.

    The register workload it runs against (history, prepared statements,
    reset windows, counters) lives in :attr:`workload`.
    """

    workload: RegisterWorkload
    # Kept so that the keyspace can be recreated with the same options.
    ks_opts: str = KEYSPACE_OPTS

    added_columns: list[str] = field(default_factory=list)
    current_c_type: str = "int"
    col_counter: int = 0
    schema_ops: int = 0

    @property
    def ks(self) -> str:
        return self.workload.ks

    @property
    def fqtn(self) -> str:
        return self.workload.fqtn

    def next_col_name(self) -> str:
        self.col_counter += 1
        return f"col_{self.col_counter}"


SchemaOpHandler = Callable[
    ["SchemaChangerState", object, random.Random],
    Awaitable[Optional[str]],
]


def _random_cql_literal(col_type: str, rng: random.Random) -> str:
    return _CQL_LITERAL_GENERATORS[col_type](rng)


async def op_add_column_and_sanity_check(
    state: SchemaChangerState, cql, rng: random.Random,
) -> Optional[str]:
    col_name = state.next_col_name()
    col_type = rng.choice(COLUMN_TYPES)
    await cql.run_async(f"ALTER TABLE {state.fqtn} ADD {col_name} {col_type}")
    state.added_columns.append(col_name)
    logger.info("DDL: ADD COLUMN %s %s", col_name, col_type)

    # Sanity: write a value to the new column, read it back.
    pk = rng.randint(0, state.workload.num_keys - 1)
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
    state: SchemaChangerState, cql, rng: random.Random,
) -> Optional[str]:
    if not state.added_columns:
        return None
    col_name = rng.choice(state.added_columns)
    await cql.run_async(f"ALTER TABLE {state.fqtn} DROP {col_name}")
    state.added_columns.remove(col_name)
    logger.info("DDL: DROP COLUMN %s", col_name)
    return "drop_column"


# BUG_ALTER_TYPE: `ALTER TABLE ... ALTER c TYPE varint` on an SC table makes an
# existing row transiently invisible to readers, which the Porcupine checker
# reports as a linearizability violation.  There is a race between the memtable
# schema update and the prepared statement cache invalidation in
# `schema_applier::post_commit()`: for a short window a read executes with the
# old schema (c: int) against data already stored in the new format (c: varint),
# and the asymmetric type compatibility check in the schema upgrader silently
# drops such cells.
#
# See SCYLLADB-1563.  The whole `alter_type` schema operation is
# commented out below so that the rest of the stress test stays usable; once the
# bug is fixed, uncomment everything marked with BUG_ALTER_TYPE.
#
# async def op_alter_type_c(
#     state: SchemaChangerState, cql, rng: random.Random,
# ) -> Optional[str]:
#     await cql.run_async(
#         f"ALTER TABLE {state.fqtn} ALTER c TYPE {ALTER_TYPE_TARGET}")
#     state.current_c_type = ALTER_TYPE_TARGET
#     logger.info("DDL: ALTER TYPE c int -> %s", ALTER_TYPE_TARGET)
#     return "alter_type"


async def op_alter_properties(
    state: SchemaChangerState, cql, rng: random.Random,
) -> Optional[str]:
    # Only properties that cannot make an already-written value disappear
    # during the run — see DEFAULT_TTL_RANGE_S.
    props = rng.choice([
        f"comment = 'stress_{rng.randint(0, 9999)}'",
        f"gc_grace_seconds = {rng.randint(100, 100000)}",
        f"default_time_to_live = {rng.randint(*DEFAULT_TTL_RANGE_S)}",
    ])
    await cql.run_async(f"ALTER TABLE {state.fqtn} WITH {props}")
    logger.info("DDL: ALTER TABLE WITH %s", props)
    return "alter_props"


async def op_drop_recreate(
    state: SchemaChangerState, cql, rng: random.Random,
) -> Optional[str]:
    # reset_window() opens the window before any DDL is issued, closes it once
    # the table is back, and emits the synthetic write(0) per key.
    async with state.workload.reset_window() as reset:
        await cql.run_async(f"DROP TABLE {state.fqtn}")
        logger.info("DDL: DROP TABLE done")

        await cql.run_async(f"CREATE TABLE {state.fqtn} {TABLE_SCHEMA}")
        state.added_columns.clear()
        state.current_c_type = "int"
        state.workload.prepare(cql)

    logger.info("DDL: DROP+RECREATE done (reset #%d), reset ops %s",
                state.workload.reset_count, reset)
    return "drop_recreate"


async def op_drop_recreate_keyspace(
    state: SchemaChangerState, cql, rng: random.Random,
) -> Optional[str]:
    """DROP KEYSPACE + CREATE KEYSPACE + CREATE TABLE.

    More destructive than op_drop_recreate: removes the entire keyspace,
    then recreates it with the same options and recreates the table.
    From the Porcupine model perspective this is identical to drop_recreate:
    all registers reset to 0.
    """
    async with state.workload.reset_window() as reset:
        await cql.run_async(f"DROP KEYSPACE {state.ks}")
        logger.info("DDL: DROP KEYSPACE done")

        await cql.run_async(f"CREATE KEYSPACE {state.ks} {state.ks_opts}")
        logger.info("DDL: CREATE KEYSPACE done")

        await cql.run_async(f"CREATE TABLE {state.fqtn} {TABLE_SCHEMA}")
        state.added_columns.clear()
        state.current_c_type = "int"
        state.workload.prepare(cql)

    logger.info(
        "DDL: DROP+RECREATE KEYSPACE done (reset #%d), reset ops %s",
        state.workload.reset_count, reset)
    return "drop_recreate_ks"


SCHEMA_OPS = [
    ("add_column", 3),
    ("drop_column", 2),
    # ("alter_type", 1),  # BUG_ALTER_TYPE
    ("alter_props", 2),
    ("drop_recreate", 2),
    ("drop_recreate_ks", 1),
]


SCHEMA_OP_HANDLERS: dict[str, SchemaOpHandler] = {
    "add_column": op_add_column_and_sanity_check,
    "drop_column": op_drop_column,
    # "alter_type": op_alter_type_c,  # BUG_ALTER_TYPE
    "alter_props": op_alter_properties,
    "drop_recreate": op_drop_recreate,
    "drop_recreate_ks": op_drop_recreate_keyspace,
}


async def schema_changer_task(state: SchemaChangerState, cql) -> None:
    rng = state.workload.rng_for("schema-changer")
    logger.info("Schema changer started")
    # Keep picking until we either execute a real schema change
    # or the test is asked to stop.
    while not state.workload.stop_event.is_set():
        result = None

        while result is None and not state.workload.stop_event.is_set():
            available_ops = [
                (name, weight) for name, weight in SCHEMA_OPS
                # BUG_ALTER_TYPE:
                # if not (name == "alter_type" and state.current_c_type != "int")
                if not (name == "drop_column" and not state.added_columns)
            ]
            op_names = [name for name, _ in available_ops]
            op_weights = [weight for _, weight in available_ops]

            [op_name] = rng.choices(op_names, weights=op_weights)
            # Log the start and the end of every schema change with the raw
            # timestamps the history records, so that it can be located in
            # history.jsonl, which the checker dumps into its artifacts
            # directory next to the visualization.  Logging the start
            # separately also keeps it in the log when the operation raises and
            # there is no end.
            logger.info("DDL: %s starting at time_ns=%d",
                        op_name, time.monotonic_ns())
            result = await SCHEMA_OP_HANDLERS[op_name](state, cql, rng)
            t_end_ns = time.monotonic_ns()

        if result is None:
            break

        state.schema_ops += 1
        logger.info("DDL: %s (schema op #%d) done at time_ns=%d",
                    result, state.schema_ops, t_end_ns)
        await asyncio.sleep(rng.uniform(*SCHEMA_OP_PAUSE_S))

    logger.info("Schema changer finished: %d ops", state.schema_ops)


@pytest.mark.asyncio
@pytest.mark.no_parallel
async def test_sc_linearizability_with_schema_changes(
    manager: ScyllaClusterManager, tmp_path,
):
    _servers, cql = await boot_sc_cluster(manager, NUM_NODES)

    async with new_test_keyspace(manager, KEYSPACE_OPTS) as ks:
        workload = RegisterWorkload(
            ks=ks,
            table_name=TABLE_NAME,
            num_keys=NUM_KEYS,
            num_writers=NUM_WRITERS,
            num_readers=NUM_READERS,
            dml_pause_s=DML_PAUSE_S,
        )
        state = SchemaChangerState(workload=workload)

        await cql.run_async(f"CREATE TABLE {workload.fqtn} {TABLE_SCHEMA}")
        workload.prepare(cql)

        logger.info(
            "Starting stress phase (%ds): writers=%d readers=%d "
            "keys=%d tablets=%d + schema changer",
            STRESS_DURATION_S, NUM_WRITERS, NUM_READERS,
            NUM_KEYS, NUM_TABLETS,
        )

        task_errors = await run_workload(
            workload, cql, STRESS_DURATION_S,
            disruptors=[("schema-changer", lambda: schema_changer_task(state, cql))],
        )

        logger.info("Stress phase complete")
        logger.info("Stats: %s | schema_ops=%d",
                    workload.stats_line(), state.schema_ops)

        assert not task_errors, (
            f"Task(s) failed with unexpected exceptions (seed={workload.seed}): "
            f"{task_errors}"
        )

        workload.assert_progress(
            min_writes=MIN_EXPECTED_WRITES, min_reads=MIN_EXPECTED_READS)
        assert state.schema_ops >= MIN_EXPECTED_SCHEMA_OPS, (
            f"Too few schema operations ({state.schema_ops}), "
            f"expected >= {MIN_EXPECTED_SCHEMA_OPS}"
        )

        await check_linearizable(
            workload, output_dir=tmp_path / "porcupine-checker-output")
