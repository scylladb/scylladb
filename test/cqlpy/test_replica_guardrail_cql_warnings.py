# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for CQL protocol warnings originating from the REPLICA-side large data
# guardrail (db::large_data_guardrail::check()).
#
# Unlike the coordinator-side tests in test_coordinator_guardrail_cql_warnings.py
# which check mutation content directly (cell sizes, row memory, collection
# counts), these tests verify that violations detected by the REPLICA — using
# the SSTable metadata index or memtable cache — propagate back through the wire
# transport and are surfaced as CQL protocol warnings to the client.
#
# The pattern: write data that accumulates past the soft threshold across
# multiple mutations (each mutation individually below coordinator limits), then
# flush (or rely on memtable cache), then write a tiny mutation to the same
# partition/row/collection. The replica's check() detects the accumulated size,
# and the violation flag travels back to the coordinator via the MUTATION_DONE
# response.

import re
from contextlib import ExitStack

import pytest

from .util import config_value_context, new_test_table
from . import nodetool


WARNING_RE = re.compile(r"(?i)large data guardrail.*soft limit violation")


@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass


def get_warnings(result):
    """Extract CQL protocol warnings from a result set, or empty list."""
    warnings = result.response_future.warnings
    return warnings if warnings else []


def make_oversized_partition(cql, tbl, pk, num_rows, value_size_bytes):
    """Insert `num_rows` rows of `value_size_bytes` blobs under partition `pk`.
    Each individual mutation is small enough to pass coordinator checks."""
    insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
    for ck in range(num_rows):
        cql.execute(insert, [pk, ck, bytes(value_size_bytes)])


def make_large_row(cql, tbl, pk, ck, value_size_bytes):
    """Build a row whose total size is `value_size_bytes` under (pk, ck),
    split across v1 and v2 in separate mutations so each is below coordinator
    thresholds."""
    half = value_size_bytes // 2
    insert_v1 = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
    insert_v2 = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v2) VALUES (?, ?, ?)")
    cql.execute(insert_v1, [pk, ck, bytes(half)])
    cql.execute(insert_v2, [pk, ck, bytes(value_size_bytes - half)])


# ---------------------------------------------------------------------------
# Partition size soft limit — CQL warning after flush
# ---------------------------------------------------------------------------


def test_partition_size_cql_warning_after_flush(cql, test_keyspace):
    """A tiny write to a partition whose on-disk size already exceeds the
    soft limit must produce a CQL protocol warning mentioning 'partition'."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '0'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'true'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            # Build a >1 MB partition (each write is ~600 KB, well below any
            # coordinator threshold).
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            # This tiny write triggers the replica check.
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            result = cql.execute(insert, [1, 99, b"\x00"])
            warnings = get_warnings(result)
            assert any(WARNING_RE.search(w) for w in warnings), \
                f"Expected replica-side CQL warning, got: {warnings}"
            assert any("partition" in w.lower() for w in warnings)


def test_partition_size_no_warning_when_below_threshold(cql, test_keyspace):
    """A write to a partition below the soft limit produces no CQL warning."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '0'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'true'))

        schema = "pk int, ck int, v1 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 0, bytes(100)])
            nodetool.flush(cql, tbl)

            result = cql.execute(insert, [1, 1, b"\x00"])
            warnings = get_warnings(result)
            assert not any(WARNING_RE.search(w) for w in warnings), \
                f"Unexpected CQL warning: {warnings}"


# ---------------------------------------------------------------------------
# Partition row-count soft limit — CQL warning after flush
# ---------------------------------------------------------------------------


def test_partition_row_count_cql_warning_after_flush(cql, test_keyspace):
    """A write to a partition whose row count already exceeds the soft limit
    must produce a CQL protocol warning mentioning 'partition'."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_rows_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'rows_count_fail_threshold', '0'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'true'))
        # Set partition size thresholds high so they don't interfere.
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '100'))

        schema = "pk int, ck int, v1 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            for ck in range(60):
                cql.execute(insert, [1, ck, b"\x00"])
            nodetool.flush(cql, tbl)

            result = cql.execute(insert, [1, 99, b"\x00"])
            warnings = get_warnings(result)
            assert any(WARNING_RE.search(w) for w in warnings), \
                f"Expected replica-side CQL warning for row count, got: {warnings}"
            assert any("partition" in w.lower() for w in warnings)


# ---------------------------------------------------------------------------
# Row size soft limit — CQL warning after flush
# ---------------------------------------------------------------------------


def test_row_size_cql_warning_after_flush(cql, test_keyspace):
    """A tiny write to a row whose on-disk size already exceeds the soft limit
    must produce a CQL protocol warning mentioning 'row'."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_row_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_row_fail_threshold_mb', '0'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'true'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            # Build a >1 MB row split across v1 and v2 (each write ~600 KB,
            # below coordinator threshold so coordinator check does not fire).
            make_large_row(cql, tbl, pk=1, ck=0, value_size_bytes=1200 * 1024)
            nodetool.flush(cql, tbl)

            # Tiny write to same row triggers replica check.
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            result = cql.execute(insert, [1, 0, b"\x00"])
            warnings = get_warnings(result)
            assert any(WARNING_RE.search(w) for w in warnings), \
                f"Expected replica-side CQL warning for row size, got: {warnings}"
            assert any("row" in w.lower() for w in warnings)


def test_row_size_cql_warning_from_memtable(cql, test_keyspace):
    """A row exceeding the soft limit in the memtable (no flush) must produce
    a CQL protocol warning on the next write to the same row."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_row_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_row_fail_threshold_mb', '10'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'true'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            # Build a >1 MB row (memtable cache tracks accumulated size).
            make_large_row(cql, tbl, pk=1, ck=0, value_size_bytes=1200 * 1024)

            # No flush — memtable cache triggers the warning.
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            result = cql.execute(insert, [1, 0, b"\x00"])
            warnings = get_warnings(result)
            assert any(WARNING_RE.search(w) for w in warnings), \
                f"Expected memtable-level CQL warning for row size, got: {warnings}"
            assert any("row" in w.lower() for w in warnings)


# ---------------------------------------------------------------------------
# Collection element count soft limit — CQL warning after flush
# ---------------------------------------------------------------------------


def test_collection_cql_warning_after_flush(cql, test_keyspace):
    """A write to a collection whose element count already exceeds the soft
    limit must produce a CQL protocol warning mentioning 'collection'."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_collection_elements_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'large_collection_elements_fail_threshold', '0'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'true'))

        schema = "pk int, ck int, s set<int>, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            # Build a collection with 60 elements (> 50 threshold), each write
            # adds 30 elements (below any coordinator limit).
            update = cql.prepare(f"UPDATE {tbl} SET s = s + ? WHERE pk = ? AND ck = ?")
            cql.execute(update, [set(range(30)), 1, 0])
            cql.execute(update, [set(range(30, 60)), 1, 0])
            nodetool.flush(cql, tbl)

            # Tiny write to same collection triggers the replica check.
            result = cql.execute(update, [{998}, 1, 0])
            warnings = get_warnings(result)
            assert any(WARNING_RE.search(w) for w in warnings), \
                f"Expected replica-side CQL warning for collection, got: {warnings}"
            assert any("collection" in w.lower() for w in warnings)


def test_collection_cql_warning_from_memtable(cql, test_keyspace):
    """A collection exceeding the soft limit in the memtable (no flush) must
    produce a CQL protocol warning on the next write to the same collection."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_collection_elements_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'large_collection_elements_fail_threshold', '10000'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'true'))

        schema = "pk int, ck int, s set<int>, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            # Grow the collection across two writes.  The merge of the second
            # write into the memtable row caches the combined count (60 > 50).
            update = cql.prepare(f"UPDATE {tbl} SET s = s + ? WHERE pk = ? AND ck = ?")
            cql.execute(update, [set(range(30)), 1, 0])
            cql.execute(update, [set(range(30, 60)), 1, 0])

            # No flush — memtable cache should trigger the CQL warning.
            result = cql.execute(update, [{998}, 1, 0])
            warnings = get_warnings(result)
            assert any(WARNING_RE.search(w) for w in warnings), \
                f"Expected memtable-level CQL warning for collection, got: {warnings}"
            assert any("collection" in w.lower() for w in warnings)


# ---------------------------------------------------------------------------
# Multi-category violations
# ---------------------------------------------------------------------------


def test_partition_and_row_combined_warning(cql, test_keyspace):
    """A write triggering both partition size and row size replica-side soft
    limit violations produces a CQL warning listing both categories."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '0'))
        cfg.enter_context(config_value_context(cql,
            'compaction_large_row_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_row_fail_threshold_mb', '0'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'true'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            # Build a >1 MB partition with a >1 MB row (each write ~600 KB,
            # below coordinator threshold so coordinator check does not fire).
            make_large_row(cql, tbl, pk=1, ck=0, value_size_bytes=1200 * 1024)
            nodetool.flush(cql, tbl)

            # Tiny write triggers both checks.
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            result = cql.execute(insert, [1, 0, b"\x00"])
            warnings = get_warnings(result)
            assert any(WARNING_RE.search(w) for w in warnings), \
                f"Expected multi-category warning, got: {warnings}"
            warning_text = "\n".join(warnings).lower()
            assert "partition" in warning_text
            assert "row" in warning_text


# ---------------------------------------------------------------------------
# Feature flag / guardrail disabled
# ---------------------------------------------------------------------------


def test_no_cql_warning_when_flag_disabled(cql, test_keyspace):
    """No CQL protocol warning when large_data_cql_warnings is disabled, even
    if the replica-side soft limit is exceeded."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '0'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'false'))

        schema = "pk int, ck int, v1 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            result = cql.execute(insert, [1, 99, b"\x00"])
            warnings = get_warnings(result)
            assert not any(WARNING_RE.search(w) for w in warnings), \
                f"CQL warning should not appear when feature is disabled: {warnings}"


def test_no_cql_warning_when_guardrails_disabled_on_table(cql, test_keyspace):
    """No CQL warning when large_data_guardrails_enabled is false on the table."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '0'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'true'))

        schema = "pk int, ck int, v1 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = false") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            result = cql.execute(insert, [1, 99, b"\x00"])
            warnings = get_warnings(result)
            assert not any(WARNING_RE.search(w) for w in warnings), \
                f"CQL warning should not appear when table guardrails disabled: {warnings}"


# ---------------------------------------------------------------------------
# Different partition/row not affected
# ---------------------------------------------------------------------------


def test_warning_only_for_affected_partition(cql, test_keyspace):
    """A write to a different partition must not inherit the warning from
    a partition that exceeds the soft limit."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '0'))
        cfg.enter_context(config_value_context(cql,
            'large_data_cql_warnings', 'true'))

        schema = "pk int, ck int, v1 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            # Different partition key — should not warn.
            result = cql.execute(insert, [2, 0, b"\x00"])
            warnings = get_warnings(result)
            assert not any(WARNING_RE.search(w) for w in warnings), \
                f"Different partition should not produce a warning: {warnings}"
