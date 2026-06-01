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


WARN_RE = r"WARN .+large_data - Large data guardrail: collection element count .+ exceeds soft limit"


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


@pytest.fixture(scope="module")
def test_table_set(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v set<int>",
                        " WITH large_data_guardrails_enabled = true") as table:
        yield table


@pytest.fixture(scope="module")
def test_table_with_static(cql, test_keyspace):
    with new_test_table(
        cql,
        test_keyspace,
        "pk int, ck int, s set<int> STATIC, v int, PRIMARY KEY (pk, ck)",
        " WITH large_data_guardrails_enabled = true",
    ) as table:
        yield table


def make_set(n):
    """Create a set with n elements."""
    return set(range(n))


# --- Hard limit tests (rejection) ---


def test_hard_limit_rejects_large_set_insert(cql, test_table_set):
    insert = cql.prepare(f"INSERT INTO {test_table_set} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
        cql.execute(insert, [1, make_set(3)])
        with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
            cql.execute(insert, [2, make_set(6)])


def test_hard_limit_rejects_large_list_insert(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v list<int>",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
            with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
                cql.execute(insert, [1, list(range(6))])


def test_hard_limit_rejects_large_map_insert(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v map<int, int>",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
            with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
                cql.execute(insert, [1, {i: i for i in range(6)}])


def test_hard_limit_rejects_large_collection_update(cql, test_table_set):
    update = cql.prepare(f"UPDATE {test_table_set} SET v = ? WHERE pk = ?")
    with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
        with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
            cql.execute(update, [make_set(6), 1])


def test_hard_limit_rejects_batch(cql, test_table_set):
    batch = cql.prepare(f"BEGIN BATCH INSERT INTO {test_table_set} (pk, v) VALUES (1, ?) APPLY BATCH")
    with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
        with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
            cql.execute(batch, [make_set(6)])


def test_hard_limit_disabled_when_zero(cql, test_table_set):
    insert = cql.prepare(f"INSERT INTO {test_table_set} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_collection_elements_fail_threshold", "0"):
        cql.execute(insert, [1, make_set(100)])


def test_hard_limit_includes_column_info(cql, test_table_set):
    insert = cql.prepare(f"INSERT INTO {test_table_set} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
        with pytest.raises(InvalidRequest, match="(?i)column=") as exc_info:
            cql.execute(insert, [1, make_set(6)])
        msg = str(exc_info.value).lower()
        assert "keyspace=" in msg
        assert "table=" in msg


def test_at_hard_limit_succeeds(cql, test_table_set):
    insert = cql.prepare(f"INSERT INTO {test_table_set} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
        cql.execute(insert, [1, make_set(5)])


# --- Soft limit tests ---


def test_soft_limit_write_succeeds(cql, test_table_set):
    insert = cql.prepare(f"INSERT INTO {test_table_set} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "compaction_collection_elements_count_warning_threshold", "5"):
        with config_value_context(cql, "large_collection_elements_fail_threshold", "0"):
            cql.execute(insert, [1, make_set(6)])


def test_soft_limit_no_rejection_when_below_threshold_default(cql, test_table_set):
    insert = cql.prepare(f"INSERT INTO {test_table_set} (pk, v) VALUES (?, ?)")
    cql.execute(insert, [1, make_set(3)])


def test_soft_limit_logs_warning(cql, test_keyspace, logfile):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v set<int>",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "compaction_collection_elements_count_warning_threshold", "5"):
            with config_value_context(cql, "large_collection_elements_fail_threshold", "0"):
                cql.execute(insert, [1, make_set(6)])
        wait_for_log(logfile, WARN_RE, timeout=5)


def test_soft_limit_no_warning_below_threshold(cql, test_keyspace, logfile):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v set<int>",
                        " WITH large_data_guardrails_enabled = true") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "compaction_collection_elements_count_warning_threshold", "5"):
            with config_value_context(cql, "large_collection_elements_fail_threshold", "0"):
                assert_no_log(logfile, WARN_RE,
                              lambda: cql.execute(insert, [1, make_set(3)]))


# --- Static collection tests ---


def test_hard_limit_rejects_large_static_collection(cql, test_table_with_static):
    insert = cql.prepare(f"INSERT INTO {test_table_with_static} (pk, ck, s) VALUES (?, ?, ?)")
    with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
        with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
            cql.execute(insert, [1, 1, make_set(6)])


def test_soft_limit_allows_large_static_collection(cql, test_table_with_static):
    insert = cql.prepare(f"INSERT INTO {test_table_with_static} (pk, ck, s) VALUES (?, ?, ?)")
    with config_value_context(cql, "compaction_collection_elements_count_warning_threshold", "5"):
        with config_value_context(cql, "large_collection_elements_fail_threshold", "0"):
            cql.execute(insert, [1, 1, make_set(6)])


# --- DELETE tests ---


def test_delete_always_allowed(cql, test_table_set):
    insert = cql.prepare(f"INSERT INTO {test_table_set} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
        cql.execute(insert, [1, make_set(3)])
        cql.execute(f"DELETE FROM {test_table_set} WHERE pk = 1")


def test_delete_in_batch_always_allowed(cql, test_table_set):
    insert = cql.prepare(f"INSERT INTO {test_table_set} (pk, v) VALUES (?, ?)")
    with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
        cql.execute(insert, [1, make_set(3)])
        cql.execute(f"BEGIN BATCH DELETE FROM {test_table_set} WHERE pk = 1 APPLY BATCH")


# --- LWT exemption tests ---
# LWT (Paxos) writes are exempt from coordinator-side guardrails because
# the mutation is not available until after the Paxos prepare round, and
# abandoning mid-round would violate linearizability (see issue #6299).


def test_lwt_insert_exempt_from_coordinator_guardrail(cql, test_table_set):
    """LWT INSERT with large collection must not be rejected by coordinator guardrail."""
    insert_if = cql.prepare(f"INSERT INTO {test_table_set} (pk, v) VALUES (?, ?) IF NOT EXISTS")
    with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
        cql.execute(insert_if, [100, make_set(6)])


# --- Per-table guardrail toggle tests ---


def test_per_table_guardrails_disabled_allows_large_collection(cql, test_keyspace):
    with new_test_table(
        cql,
        test_keyspace,
        "pk int PRIMARY KEY, v set<int>",
        extra=" WITH large_data_guardrails_enabled = false",
    ) as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
            cql.execute(insert, [1, make_set(6)])


def test_per_table_guardrails_enabled_rejects_large_collection(cql, test_keyspace):
    with new_test_table(
        cql,
        test_keyspace,
        "pk int PRIMARY KEY, v set<int>",
        extra=" WITH large_data_guardrails_enabled = true",
    ) as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
            with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
                cql.execute(insert, [1, make_set(6)])


def test_alter_table_disable_guardrails(cql, test_keyspace):
    with new_test_table(
        cql,
        test_keyspace,
        "pk int PRIMARY KEY, v set<int>",
        extra=" WITH large_data_guardrails_enabled = true",
    ) as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
            with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
                cql.execute(insert, [1, make_set(6)])
            cql.execute(f"ALTER TABLE {table} WITH large_data_guardrails_enabled = false")
            cql.execute(insert, [2, make_set(6)])


def test_alter_table_reenable_guardrails(cql, test_keyspace):
    with new_test_table(
        cql,
        test_keyspace,
        "pk int PRIMARY KEY, v set<int>",
    ) as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        with config_value_context(cql, "large_collection_elements_fail_threshold", "5"):
            cql.execute(insert, [1, make_set(6)])
            cql.execute(f"ALTER TABLE {table} WITH large_data_guardrails_enabled = true")
            with pytest.raises(InvalidRequest, match="(?i)large data guardrail"):
                cql.execute(insert, [2, make_set(6)])
