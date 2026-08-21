# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import re
from contextlib import ExitStack

import pytest

from cassandra.protocol import InvalidRequest

from .util import config_value_context, new_test_table


WARNING_RE = re.compile(r"(?i)large data guardrail.*soft limit violation")


@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass


def make_blob(size_bytes):
    return bytes(size_bytes)


def make_set(n):
    """Create a set with n elements."""
    return set(range(n))


def get_warnings(result):
    """Extract CQL protocol warnings from a result set, or empty list."""
    warnings = result.response_future.warnings
    return warnings if warnings else []


# --- Row soft limit CQL warnings ---


def test_row_soft_limit_returns_cql_warning(cql, test_keyspace):
    """A write exceeding the row soft limit returns a CQL warning."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_row_fail_threshold_mb", "0"))
            result = cql.execute(insert, [1, make_blob(1048576 + 1)])
            warnings = get_warnings(result)
            assert len(warnings) > 0, "Expected a CQL warning for row soft limit violation"
            assert any(WARNING_RE.search(w) for w in warnings)
            assert any("row" in w.lower() for w in warnings)


def test_row_soft_limit_no_warning_when_below_threshold(cql, test_keyspace):
    """A write within the row soft limit produces no CQL warning."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_row_fail_threshold_mb", "0"))
            result = cql.execute(insert, [1, b"\x00"])
            warnings = get_warnings(result)
            assert not any(WARNING_RE.search(w) for w in warnings), \
                f"Unexpected CQL warning: {warnings}"


# --- Cell soft limit CQL warnings ---


def test_cell_soft_limit_returns_cql_warning(cql, test_keyspace):
    """A write exceeding the cell soft limit returns a CQL warning."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_large_cell_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_cell_fail_threshold_mb", "0"))
            result = cql.execute(insert, [1, make_blob(1048576 + 1)])
            warnings = get_warnings(result)
            assert len(warnings) > 0, "Expected a CQL warning for cell soft limit violation"
            assert any(WARNING_RE.search(w) for w in warnings)
            assert any("cell" in w.lower() for w in warnings)


def test_cell_soft_limit_no_warning_when_below_threshold(cql, test_keyspace):
    """A write within the cell soft limit produces no CQL warning."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_large_cell_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_cell_fail_threshold_mb", "0"))
            result = cql.execute(insert, [1, b"\x00"])
            warnings = get_warnings(result)
            assert not any(WARNING_RE.search(w) for w in warnings), \
                f"Unexpected CQL warning: {warnings}"


# --- Collection soft limit CQL warnings ---


def test_collection_soft_limit_returns_cql_warning(cql, test_keyspace):
    """A write exceeding the collection element count soft limit returns a CQL warning."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v set<int>",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_collection_elements_count_warning_threshold", "5"))
            cfg.enter_context(config_value_context(cql, "large_collection_elements_fail_threshold", "0"))
            result = cql.execute(insert, [1, make_set(6)])
            warnings = get_warnings(result)
            assert len(warnings) > 0, "Expected a CQL warning for collection soft limit violation"
            assert any(WARNING_RE.search(w) for w in warnings)
            assert any("collection" in w.lower() for w in warnings)


def test_collection_soft_limit_no_warning_when_below_threshold(cql, test_keyspace):
    """A write within the collection element count soft limit produces no CQL warning."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v set<int>",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_collection_elements_count_warning_threshold", "5"))
            cfg.enter_context(config_value_context(cql, "large_collection_elements_fail_threshold", "0"))
            result = cql.execute(insert, [1, make_set(3)])
            warnings = get_warnings(result)
            assert not any(WARNING_RE.search(w) for w in warnings), \
                f"Unexpected CQL warning: {warnings}"


# --- Multi-category violations ---


def test_row_and_cell_combined_warning(cql, test_keyspace):
    """A single write exceeding both row and cell soft limits returns a CQL
    warning listing both categories."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_row_fail_threshold_mb", "0"))
            cfg.enter_context(config_value_context(cql, "compaction_large_cell_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_cell_fail_threshold_mb", "0"))
            result = cql.execute(insert, [1, make_blob(1048576 + 1)])
            warnings = get_warnings(result)
            assert len(warnings) > 0
            warning_text = "\n".join(warnings).lower()
            assert "row" in warning_text
            assert "cell" in warning_text


# --- Feature flag disabled ---


def test_no_cql_warning_when_flag_disabled(cql, test_keyspace):
    """No CQL warning is returned when large_data_cql_warnings is disabled,
    even if the soft limit is exceeded."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "false"))
            cfg.enter_context(config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_row_fail_threshold_mb", "0"))
            result = cql.execute(insert, [1, make_blob(1048576 + 1)])
            warnings = get_warnings(result)
            assert not any(WARNING_RE.search(w) for w in warnings), \
                f"CQL warning should not appear when feature is disabled: {warnings}"


def test_no_cql_warning_when_guardrails_disabled_on_table(cql, test_keyspace):
    """No CQL warning when large_data_guardrails_enabled is false on the table,
    even if the global flag is on and the soft limit is exceeded."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = false") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_row_fail_threshold_mb", "0"))
            result = cql.execute(insert, [1, make_blob(1048576 + 1)])
            warnings = get_warnings(result)
            assert not any(WARNING_RE.search(w) for w in warnings), \
                f"CQL warning should not appear when table guardrails are disabled: {warnings}"


# --- Batch statement CQL warnings ---


def test_batch_row_soft_limit_returns_cql_warning(cql, test_keyspace):
    """A batch write exceeding the row soft limit returns a CQL warning."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        batch = cql.prepare(f"BEGIN BATCH INSERT INTO {table} (pk, v) VALUES (1, ?) APPLY BATCH")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_row_fail_threshold_mb", "0"))
            result = cql.execute(batch, [make_blob(1048576 + 1)])
            warnings = get_warnings(result)
            assert len(warnings) > 0, "Expected a CQL warning for batch row soft limit violation"
            assert any(WARNING_RE.search(w) for w in warnings)


# --- LWT CQL warnings ---


def test_lwt_insert_soft_limit_returns_cql_warning(cql, test_keyspace):
    """An LWT INSERT exceeding the row soft limit returns a CQL warning."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert_if = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?) IF NOT EXISTS")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_row_fail_threshold_mb", "0"))
            result = cql.execute(insert_if, [1, make_blob(1048576 + 1)])
            warnings = get_warnings(result)
            assert len(warnings) > 0, "Expected a CQL warning for LWT soft limit violation"
            assert any(WARNING_RE.search(w) for w in warnings)


def test_lwt_update_soft_limit_returns_cql_warning(cql, test_keyspace):
    """An LWT UPDATE exceeding the row soft limit returns a CQL warning."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        update_if = cql.prepare(f"UPDATE {table} SET v = ? WHERE pk = ? IF v != null")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"))
            cfg.enter_context(config_value_context(cql, "large_row_fail_threshold_mb", "0"))
            cql.execute(insert, [1, b"\x00"])
            result = cql.execute(update_if, [make_blob(1048576 + 1), 1])
            warnings = get_warnings(result)
            assert len(warnings) > 0, "Expected a CQL warning for LWT UPDATE soft limit violation"
            assert any(WARNING_RE.search(w) for w in warnings)


# --- Hard limit still rejects even with CQL warnings enabled ---


def test_hard_limit_still_rejects_with_cql_warnings_enabled(cql, test_keyspace):
    """Hard limit rejection takes precedence; enabling CQL warnings does not
    suppress hard limit enforcement."""
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with ExitStack() as cfg:
            cfg.enter_context(config_value_context(cql, "large_data_cql_warnings", "true"))
            cfg.enter_context(config_value_context(cql, "large_row_fail_threshold_mb", "1"))
            with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
                cql.execute(insert, [1, make_blob(1048576 + 1)])
