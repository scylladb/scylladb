# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1


import pytest

from contextlib import ExitStack
from cassandra import WriteFailure
from cassandra.protocol import SyntaxException

from .util import config_value_context, new_test_table
from . import nodetool


@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass


@pytest.fixture(scope="function")
def test_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        yield table


# --- DELETE with BYPASS (the only supported statement type) ---


def test_delete_bypass_accepted(cql, test_table):
    """BYPASS LARGE_DATA_GUARDRAILS is syntactically valid on DELETE."""
    cql.execute(f"INSERT INTO {test_table} (pk, v) VALUES (20, 0x00)")
    cql.execute(
        f"DELETE FROM {test_table} WHERE pk = 20 BYPASS LARGE_DATA_GUARDRAILS")


def test_delete_bypass_with_using_clause(cql, test_table):
    """BYPASS works together with USING TIMESTAMP on DELETE."""
    cql.execute(f"INSERT INTO {test_table} (pk, v) VALUES (21, 0x00)")
    cql.execute(
        f"DELETE FROM {test_table} USING TIMESTAMP 12345 WHERE pk = 21 "
        f"BYPASS LARGE_DATA_GUARDRAILS")


def test_lwt_delete_bypass_accepted(cql, test_table):
    """BYPASS is accepted on DELETE IF EXISTS (CAS path)."""
    cql.execute(f"INSERT INTO {test_table} (pk, v) VALUES (22, 0x00)")
    cql.execute(
        f"DELETE FROM {test_table} WHERE pk = 22 IF EXISTS "
        f"BYPASS LARGE_DATA_GUARDRAILS")


# ---------------------------------------------------------------------------
# Replica-side BYPASS
# ---------------------------------------------------------------------------


def test_replica_bypass_delete_from_large_partition(cql, test_keyspace):
    """BYPASS lets a DELETE remove data from an oversized partition that the
    replica-side partition-size guardrail would otherwise reject."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '1'))

        schema = "pk int, ck int, v1 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            for ck in range(3):
                cql.execute(insert, [1, ck, bytes(400 * 1024)])
            nodetool.flush(cql, tbl)

            # Without bypass: a DELETE touching the oversized partition is
            # rejected by the replica.
            with pytest.raises(WriteFailure):
                cql.execute(f"DELETE FROM {tbl} WHERE pk = 1 AND ck = 0")

            # With bypass: the DELETE is allowed.
            cql.execute(
                f"DELETE FROM {tbl} WHERE pk = 1 AND ck = 1 "
                f"BYPASS LARGE_DATA_GUARDRAILS")


def test_replica_bypass_delete_large_row(cql, test_keyspace):
    """BYPASS lets a DELETE target a row that the replica-side row-size
    guardrail would otherwise reject."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_row_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_row_fail_threshold_mb', '1'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            # Build a >1MB row by writing v1 and v2 in separate mutations
            # (each below the coordinator threshold).
            cql.execute(cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)"),
                        [1, 0, bytes(600 * 1024)])
            cql.execute(cql.prepare(f"INSERT INTO {tbl} (pk, ck, v2) VALUES (?, ?, ?)"),
                        [1, 0, bytes(600 * 1024)])
            nodetool.flush(cql, tbl)

            # Without bypass: a DELETE of the oversized row is rejected.
            with pytest.raises(WriteFailure):
                cql.execute(f"DELETE FROM {tbl} WHERE pk = 1 AND ck = 0")

            # With bypass: the DELETE is allowed.
            cql.execute(
                f"DELETE FROM {tbl} WHERE pk = 1 AND ck = 0 "
                f"BYPASS LARGE_DATA_GUARDRAILS")


# --- BYPASS is rejected on all other statement types ---


def test_insert_bypass_rejected(cql, test_table):
    """BYPASS LARGE_DATA_GUARDRAILS is not supported on INSERT."""
    with pytest.raises(SyntaxException):
        cql.execute(
            f"INSERT INTO {test_table} (pk, v) VALUES (1, 0x00) "
            f"BYPASS LARGE_DATA_GUARDRAILS")


def test_insert_json_bypass_rejected(cql, test_table):
    """BYPASS LARGE_DATA_GUARDRAILS is not supported on INSERT JSON."""
    with pytest.raises(SyntaxException):
        cql.execute(
            f"INSERT INTO {test_table} JSON '{{\"pk\": 1, \"v\": \"0x00\"}}' "
            f"BYPASS LARGE_DATA_GUARDRAILS")


def test_update_bypass_rejected(cql, test_table):
    """BYPASS LARGE_DATA_GUARDRAILS is not supported on UPDATE."""
    with pytest.raises(SyntaxException):
        cql.execute(
            f"UPDATE {test_table} SET v = 0x00 WHERE pk = 1 "
            f"BYPASS LARGE_DATA_GUARDRAILS")


def test_batch_bypass_rejected(cql, test_table):
    """BYPASS LARGE_DATA_GUARDRAILS is not supported on BATCH."""
    with pytest.raises(SyntaxException):
        cql.execute(
            f"BEGIN BATCH "
            f"INSERT INTO {test_table} (pk, v) VALUES (1, 0x00) "
            f"APPLY BATCH BYPASS LARGE_DATA_GUARDRAILS")
