# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import io
import re
import time

import pytest

from cassandra.protocol import InvalidRequest

from .test_logs import logfile_path, logfile, wait_for_log
from .util import config_value_context, new_test_table


WARN_RE = r"WARN .+large_data - Large data guardrail: (?:static )?row size .+ exceeds soft limit"


def assert_no_log(logfile, pattern, action, settle=0.5):
    logfile.seek(0, io.SEEK_END)
    action()
    time.sleep(settle)
    contents = logfile.read()
    assert not re.search(pattern, contents), \
        f"Unexpected log message matching {pattern}"


@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass


@pytest.fixture(scope="function")
def test_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        yield table


@pytest.fixture(scope="function")
def test_table_with_static(cql, test_keyspace):
    with new_test_table(
        cql,
        test_keyspace,
        "pk int, ck int, s blob STATIC, v blob, PRIMARY KEY (pk, ck)",
        " WITH large_data_guardrails_enabled = true",
    ) as table:
        yield table


def make_blob(size_bytes):
    return bytes(size_bytes)


# --- Hard limit tests (rejection) ---


def test_hard_limit_rejects_large_row_insert(cql, test_table):
    insert = cql.prepare(f"INSERT INTO {test_table} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
        cql.execute(insert, [1, b"\x00"])
        with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
            cql.execute(insert, [2, make_blob(1048576 + 1)])


def test_hard_limit_rejects_large_row_update(cql, test_table):
    update = cql.prepare(f"UPDATE {test_table} SET v = ? WHERE pk = ?")
    with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
        with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
            cql.execute(update, [make_blob(1048576 + 1), 1])


def test_hard_limit_rejects_batch(cql, test_table):
    batch = cql.prepare(f"BEGIN BATCH INSERT INTO {test_table} (pk, v) VALUES (1, ?) APPLY BATCH")
    with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
        with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
            cql.execute(batch, [make_blob(1048576 + 1)])


def test_hard_limit_disabled_when_zero(cql, test_table):
    insert = cql.prepare(f"INSERT INTO {test_table} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_row_fail_threshold_mb", "0"):
        cql.execute(insert, [1, make_blob(2 * 1024 * 1024)])


def test_hard_limit_includes_table_info(cql, test_table):
    insert = cql.prepare(f"INSERT INTO {test_table} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
        with pytest.raises(InvalidRequest, match="(?i)large data guardrail") as exc_info:
            cql.execute(insert, [1, make_blob(1048576 + 1)])
        msg = str(exc_info.value).lower()
        assert "keyspace=" in msg
        assert "table=" in msg


# --- Soft limit tests ---


def test_soft_limit_write_succeeds(cql, test_table):
    insert = cql.prepare(f"INSERT INTO {test_table} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"):
        with config_value_context(cql, "large_row_fail_threshold_mb", "0"):
            cql.execute(insert, [1, make_blob(1048576 + 1)])


def test_soft_limit_no_rejection_when_below_threshold_default(cql, test_table):
    insert = cql.prepare(f"INSERT INTO {test_table} (pk, v) VALUES (?, ?)")
    cql.execute(insert, [1, make_blob(100)])


def test_soft_limit_logs_warning(cql, test_keyspace, logfile):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"):
            with config_value_context(cql, "large_row_fail_threshold_mb", "0"):
                cql.execute(insert, [1, make_blob(1048576 + 1)])
        wait_for_log(logfile, WARN_RE, timeout=5)


def test_soft_limit_no_warning_below_threshold(cql, test_keyspace, logfile):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v blob",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"):
            with config_value_context(cql, "large_row_fail_threshold_mb", "0"):
                assert_no_log(logfile, WARN_RE,
                              lambda: cql.execute(insert, [1, b"\x00"]))


# --- Static row tests ---


def test_hard_limit_rejects_large_static_row(cql, test_table_with_static):
    insert = cql.prepare(f"INSERT INTO {test_table_with_static} (pk, ck, s) VALUES (?, ?, ?)")
    with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
        with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
            cql.execute(insert, [1, 1, make_blob(1048576 + 1)])


def test_soft_limit_allows_large_static_row(cql, test_table_with_static):
    insert = cql.prepare(f"INSERT INTO {test_table_with_static} (pk, ck, s) VALUES (?, ?, ?)")
    with config_value_context(cql, "compaction_large_row_warning_threshold_mb", "1"):
        with config_value_context(cql, "large_row_fail_threshold_mb", "0"):
            cql.execute(insert, [1, 1, make_blob(1048576 + 1)])


# --- DELETE tests ---


def test_delete_always_allowed(cql, test_table):
    insert = cql.prepare(f"INSERT INTO {test_table} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
        cql.execute(insert, [1, b"\x00"])
        cql.execute(f"DELETE FROM {test_table} WHERE pk = 1")


def test_delete_in_batch_always_allowed(cql, test_table):
    insert = cql.prepare(f"INSERT INTO {test_table} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
        cql.execute(insert, [1, b"\x00"])
        cql.execute(f"BEGIN BATCH DELETE FROM {test_table} WHERE pk = 1 APPLY BATCH")


# --- LWT rejection tests ---

def test_lwt_insert_rejected_by_coordinator_guardrail(cql, test_table):
    """LWT INSERT with large row must be rejected by coordinator guardrail."""
    insert_if = cql.prepare(f"INSERT INTO {test_table} (pk, v) VALUES (?, ?) IF NOT EXISTS")
    with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
        with pytest.raises(InvalidRequest, match="Large data guardrail"):
            cql.execute(insert_if, [100, make_blob(1048576 + 1)])


def test_lwt_update_rejected_by_coordinator_guardrail(cql, test_table):
    """LWT UPDATE with large row must be rejected by coordinator guardrail."""
    insert = cql.prepare(f"INSERT INTO {test_table} (pk, v) VALUES (?, ?)")
    update_if = cql.prepare(f"UPDATE {test_table} SET v = ? WHERE pk = ? IF v != null")
    with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
        cql.execute(insert, [102, b"\x00"])
        with pytest.raises(InvalidRequest, match="Large data guardrail"):
            cql.execute(update_if, [make_blob(1048576 + 1), 102])


# --- Per-table guardrail toggle tests ---


def test_per_table_guardrails_disabled_allows_large_row(cql, test_keyspace):
    with new_test_table(
        cql,
        test_keyspace,
        "pk int PRIMARY KEY, v blob",
        extra=" WITH large_data_guardrails_enabled = false",
    ) as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
            cql.execute(insert, [1, make_blob(1048576 + 1)])


def test_per_table_guardrails_enabled_rejects_large_row(cql, test_keyspace):
    with new_test_table(
        cql,
        test_keyspace,
        "pk int PRIMARY KEY, v blob",
        extra=" WITH large_data_guardrails_enabled = true",
    ) as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
            with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
                cql.execute(insert, [1, make_blob(1048576 + 1)])


def test_alter_table_disable_guardrails(cql, test_keyspace):
    with new_test_table(
        cql,
        test_keyspace,
        "pk int PRIMARY KEY, v blob",
        extra=" WITH large_data_guardrails_enabled = true",
    ) as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
            with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
                cql.execute(insert, [1, make_blob(1048576 + 1)])
            cql.execute(f"ALTER TABLE {table} WITH large_data_guardrails_enabled = false")
            cql.execute(insert, [2, make_blob(1048576 + 1)])


def test_alter_table_reenable_guardrails(cql, test_keyspace):
    with new_test_table(
        cql,
        test_keyspace,
        "pk int PRIMARY KEY, v blob",
    ) as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "large_row_fail_threshold_mb", "1"):
            cql.execute(insert, [1, make_blob(1048576 + 1)])
            cql.execute(f"ALTER TABLE {table} WITH large_data_guardrails_enabled = true")
            with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
                cql.execute(insert, [2, make_blob(1048576 + 1)])
