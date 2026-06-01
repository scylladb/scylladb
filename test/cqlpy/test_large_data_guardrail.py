#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for the large-data guardrail: write-path rejection and warnings for
# partitions, rows, and collections that exceed configured thresholds.

import pytest
import re
import time
import io
from contextlib import ExitStack
from cassandra import WriteFailure

from .util import new_test_table, config_value_context
from . import nodetool
from .test_logs import logfile, logfile_path, wait_for_log

# All tests are Scylla-only (guardrails don't exist in Cassandra).
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REJECT_PARTITION_RE = "(?i)Write rejected.*(partition size|partition row count)"
REJECT_ROW_RE = "(?i)Write rejected.*row size"
REJECT_COLLECTION_RE = "(?i)Write rejected.*collection"
WARN_RE = "Large data guardrail"


def make_oversized_partition(cql, tbl, pk, num_rows, value_size_bytes):
    """Insert `num_rows` rows of `value_size_bytes` blobs under partition key `pk`."""
    insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
    for ck in range(num_rows):
        cql.execute(insert, [pk, ck, bytes(value_size_bytes)])


def make_large_row(cql, tbl, pk, ck, value_size_bytes):
    """Insert a single row whose total size is `value_size_bytes` under (pk, ck),
    split across v1 and v2 in separate mutations to keep each below coordinator
    thresholds."""
    half = value_size_bytes // 2
    insert_v1 = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
    insert_v2 = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v2) VALUES (?, ?, ?)")
    cql.execute(insert_v1, [pk, ck, bytes(half)])
    cql.execute(insert_v2, [pk, ck, bytes(value_size_bytes - half)])


def build_large_collection(cql, tbl, pk, ck, num_elements):
    """Build a set<int> with `num_elements` entries under (pk, ck)."""
    update = cql.prepare(f"UPDATE {tbl} SET s = s + ? WHERE pk = ? AND ck = ?")
    for i in range(num_elements):
        cql.execute(update, [{i}, pk, ck])


def assert_no_log(logfile, pattern, after_action, timeout=0.5):
    """Run `after_action`, then verify `pattern` does NOT appear in the log."""
    logfile.seek(0, io.SEEK_END)
    after_action()
    time.sleep(timeout)
    new_content = logfile.read()
    assert not re.search(pattern, new_content), \
        f"Expected no match for '{pattern}', got: {new_content}"


def wait_for_all_logs(logfile, patterns, timeout=5):
    """Wait until every regex in `patterns` has appeared in the log."""
    contents = logfile.read()
    remaining = set(patterns)
    remaining = {p for p in remaining if not re.search(p, contents)}
    if not remaining:
        return
    end = time.time() + timeout
    while remaining and time.time() < end:
        s = logfile.read()
        if s:
            contents += s
            remaining = {p for p in remaining if not re.search(p, contents)}
            if not remaining:
                return
        time.sleep(0.1)
    pytest.fail(f'Timed out ({timeout}s) waiting for patterns: {remaining}')


# ---------------------------------------------------------------------------
# Partition size threshold
# ---------------------------------------------------------------------------


def test_partition_size_rejects_after_flush(cql, test_keyspace, logfile):
    """A partition whose on-disk size exceeds the hard limit must be rejected
    after flush."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '1'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            with pytest.raises(WriteFailure, match=REJECT_PARTITION_RE):
                cql.execute(insert, [1, 99, b"\x00"])

            # Different partition still succeeds.
            cql.execute(insert, [2, 0, b"\x00"])


def test_partition_size_disabled_when_zero(cql, test_keyspace):
    """When fail threshold is 0, no rejection even above the soft limit."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '0'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 99, b"\x00"])


# ---------------------------------------------------------------------------
# Partition row-count threshold
# ---------------------------------------------------------------------------


def test_partition_rows_rejects_after_flush(cql, test_keyspace):
    """A partition exceeding rows_count_fail_threshold must be rejected."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_rows_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'rows_count_fail_threshold', '50'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=60,
                                    value_size_bytes=8)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            with pytest.raises(WriteFailure, match=REJECT_PARTITION_RE):
                cql.execute(insert, [1, 999, b"\x00"])


# ---------------------------------------------------------------------------
# Soft-limit warnings — partition
# ---------------------------------------------------------------------------


def test_partition_size_soft_limit_logs_warning(cql, test_keyspace, logfile):
    """A partition above the detection threshold but below the hard limit must
    produce a warning log entry."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '10'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 99, b"\x00"])

            wait_for_log(logfile, WARN_RE, timeout=5)


def test_partition_rows_soft_limit_logs_warning(cql, test_keyspace, logfile):
    """A partition above the row-count detection threshold but below the hard
    limit must produce a warning log entry."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_rows_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'rows_count_fail_threshold', '1000'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=60,
                                    value_size_bytes=8)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 999, b"\x00"])

            wait_for_log(logfile, WARN_RE, timeout=5)


def test_partition_no_warning_below_soft_limit(cql, test_keyspace, logfile):
    """A small partition must not produce any warning."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '10'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 0, b"\x00"])
            nodetool.flush(cql, tbl)

            assert_no_log(logfile, WARN_RE,
                          lambda: cql.execute(insert, [1, 1, b"\x00"]))


# ---------------------------------------------------------------------------
# Row size threshold
# ---------------------------------------------------------------------------


def test_row_size_rejects_after_flush(cql, test_keyspace):
    """A row whose on-disk size exceeds the hard limit must be rejected."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_row_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_row_fail_threshold_mb', '1'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_large_row(cql, tbl, pk=1, ck=0,
                           value_size_bytes=1200 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            with pytest.raises(WriteFailure, match=REJECT_ROW_RE):
                cql.execute(insert, [1, 0, b"\x00"])

            # Different CK succeeds.
            cql.execute(insert, [1, 99, b"\x00"])
            # Different partition succeeds.
            cql.execute(insert, [2, 0, b"\x00"])


def test_row_size_disabled_when_zero(cql, test_keyspace):
    """When fail threshold is 0, no rejection even above the soft limit."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_row_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_row_fail_threshold_mb', '0'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_large_row(cql, tbl, pk=1, ck=0,
                           value_size_bytes=1200 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 0, b"\x00"])


# ---------------------------------------------------------------------------
# Soft-limit warnings — row
# ---------------------------------------------------------------------------


def test_row_size_soft_limit_logs_warning(cql, test_keyspace, logfile):
    """A row above the detection threshold but below the hard limit must
    produce a warning log entry."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_row_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_row_fail_threshold_mb', '10'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_large_row(cql, tbl, pk=1, ck=0,
                           value_size_bytes=1200 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 0, b"\x00"])

            wait_for_log(logfile, WARN_RE, timeout=5)


def test_row_no_warning_below_soft_limit(cql, test_keyspace, logfile):
    """A small row must not produce any warning."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_row_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_row_fail_threshold_mb', '10'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 0, b"\x00"])
            nodetool.flush(cql, tbl)

            assert_no_log(logfile, WARN_RE,
                          lambda: cql.execute(insert, [1, 1, b"\x00"]))


# ---------------------------------------------------------------------------
# Collection element count threshold
# ---------------------------------------------------------------------------


def test_collection_rejects_after_flush(cql, test_keyspace):
    """A collection exceeding the element-count hard limit must be rejected."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_collection_elements_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'compaction_large_cell_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_collection_elements_fail_threshold', '50'))

        schema = "pk int, ck int, s set<int>, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            build_large_collection(cql, tbl, pk=1, ck=0, num_elements=60)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, s) VALUES (?, ?, ?)")
            with pytest.raises(WriteFailure, match=REJECT_COLLECTION_RE):
                cql.execute(insert, [1, 0, {0}])

            # Different CK succeeds.
            cql.execute(insert, [1, 99, {0}])
            # Different partition succeeds.
            cql.execute(insert, [2, 0, {0}])


def test_collection_disabled_when_zero(cql, test_keyspace):
    """When fail threshold is 0, no rejection even above the soft limit."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_collection_elements_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'compaction_large_cell_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_collection_elements_fail_threshold', '0'))

        schema = "pk int, ck int, s set<int>, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            build_large_collection(cql, tbl, pk=1, ck=0, num_elements=60)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, s) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 0, {0}])


# ---------------------------------------------------------------------------
# Soft-limit warnings — collection
# ---------------------------------------------------------------------------


def test_collection_soft_limit_logs_warning(cql, test_keyspace, logfile):
    """A collection above the detection threshold but below the hard limit
    must produce a warning log entry."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_collection_elements_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'compaction_large_cell_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_collection_elements_fail_threshold', '10000'))

        schema = "pk int, ck int, s set<int>, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            build_large_collection(cql, tbl, pk=1, ck=0, num_elements=60)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, s) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 0, {0}])

            wait_for_log(logfile, WARN_RE, timeout=5)


def test_collection_no_warning_below_soft_limit(cql, test_keyspace, logfile):
    """A small collection must not produce any warning."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_collection_elements_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'compaction_large_cell_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_collection_elements_fail_threshold', '10000'))

        schema = "pk int, ck int, s set<int>, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, s) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 0, {1, 2, 3}])
            nodetool.flush(cql, tbl)

            assert_no_log(logfile, WARN_RE,
                          lambda: cql.execute(insert, [1, 1, {4, 5}]))


# ---------------------------------------------------------------------------
# Per-table enable/disable toggle
# ---------------------------------------------------------------------------


def test_per_table_disabled_at_create(cql, test_keyspace):
    """A table created with guardrails disabled must never reject writes."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '1'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = false") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 99, b"\x00"])


def test_per_table_alter_disable(cql, test_keyspace):
    """ALTER TABLE ... WITH large_data_guardrails_enabled = false must stop
    rejection."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '1'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            with pytest.raises(WriteFailure, match=REJECT_PARTITION_RE):
                cql.execute(insert, [1, 99, b"\x00"])

            cql.execute(f"ALTER TABLE {tbl} WITH large_data_guardrails_enabled = false")
            cql.execute(insert, [1, 99, b"\x00"])


def test_per_table_alter_reenable(cql, test_keyspace):
    """After disabling then re-enabling guardrails, writes to a large
    partition must be rejected again."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '1'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = false") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 99, b"\x00"])

            cql.execute(f"ALTER TABLE {tbl} WITH large_data_guardrails_enabled = true")
            with pytest.raises(WriteFailure, match=REJECT_PARTITION_RE):
                cql.execute(insert, [1, 100, b"\x00"])


# ---------------------------------------------------------------------------
# Exemptions
# ---------------------------------------------------------------------------


def test_lwt_paxos_learn_is_exempt(cql, test_keyspace):
    """LWT (Paxos) commit writes must bypass the guardrail."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '1'))

        schema = "pk int, ck int, v1 blob, v2 blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            # Non-LWT write is rejected.
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            with pytest.raises(WriteFailure, match=REJECT_PARTITION_RE):
                cql.execute(insert, [1, 99, b"\x00"])

            # LWT write succeeds (Paxos learn bypasses guardrail).
            lwt = cql.prepare(
                f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?) IF NOT EXISTS")
            cql.execute(lwt, [1, 100, b"\x00"])


# ---------------------------------------------------------------------------
# Multi-category: all guardrails fire independently
# ---------------------------------------------------------------------------


def test_all_guardrails_warn_independently(cql, test_keyspace, logfile):
    """When an SSTable has records in all three categories, each guardrail
    must warn independently (fail=0 means warn-only)."""
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
            'compaction_collection_elements_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'compaction_large_cell_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_collection_elements_fail_threshold', '0'))

        schema = "pk int, ck int, v1 blob, v2 blob, s set<int>, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            # Build data large in all three dimensions under pk=1, ck=0.
            # Split the row across v1/v2 in separate mutations so each
            # stays below coordinator thresholds.
            insert_v1 = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")
            insert_v2 = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v2) VALUES (?, ?, ?)")
            cql.execute(insert_v1, [1, 0, bytes(600 * 1024)])
            cql.execute(insert_v2, [1, 0, bytes(600 * 1024)])
            update_s = cql.prepare(
                f"UPDATE {tbl} SET s = s + ? WHERE pk = ? AND ck = ?")
            for i in range(60):
                cql.execute(update_s, [{i}, 1, 0])

            nodetool.flush(cql, tbl)

            insert_both = cql.prepare(
                f"INSERT INTO {tbl} (pk, ck, v1, s) VALUES (?, ?, ?, ?)")
            cql.execute(insert_both, [1, 0, b"\x00", {999}])

            wait_for_all_logs(logfile, [
                r"partition size.*exceeds",
                r"row size.*exceeds",
                r"collection element count.*exceeds",
            ], timeout=5)


def test_each_guardrail_rejects_independently(cql, test_keyspace):
    """Each hard limit must reject independently, with separate PKs isolating
    each category."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'compaction_large_row_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'compaction_collection_elements_count_warning_threshold', '50'))
        cfg.enter_context(config_value_context(cql,
            'compaction_large_cell_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '2'))
        cfg.enter_context(config_value_context(cql,
            'rows_count_fail_threshold', '0'))
        cfg.enter_context(config_value_context(cql,
            'large_row_fail_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_collection_elements_fail_threshold', '50'))

        schema = "pk int, ck int, v1 blob, v2 blob, s set<int>, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            insert_v1 = cql.prepare(
                f"INSERT INTO {tbl} (pk, ck, v1) VALUES (?, ?, ?)")

            # pk=1: 20 rows x 150 KB ~ 3 MB partition, each row < 1 MB.
            for ck in range(20):
                cql.execute(insert_v1, [1, ck, bytes(150 * 1024)])

            # pk=2: single 1.2 MB row split across v1/v2 in separate
            # mutations so each stays below coordinator thresholds.
            insert_v2 = cql.prepare(
                f"INSERT INTO {tbl} (pk, ck, v2) VALUES (?, ?, ?)")
            cql.execute(insert_v1, [2, 0, bytes(600 * 1024)])
            cql.execute(insert_v2, [2, 0, bytes(600 * 1024)])

            # pk=3: 60-element collection; row and partition tiny.
            update_s = cql.prepare(
                f"UPDATE {tbl} SET s = s + ? WHERE pk = ? AND ck = ?")
            for i in range(60):
                cql.execute(update_s, [{i}, 3, 0])

            nodetool.flush(cql, tbl)

            # Partition rejection.
            with pytest.raises(WriteFailure, match="(?i)partition size.*exceeds"):
                cql.execute(insert_v1, [1, 99, b"\x00"])

            # Row rejection.
            with pytest.raises(WriteFailure, match="(?i)row size.*exceeds"):
                cql.execute(insert_v1, [2, 0, b"\x00"])
            # Different CK in pk=2 succeeds.
            cql.execute(insert_v1, [2, 99, b"\x00"])

            # Collection rejection.
            insert_s = cql.prepare(
                f"INSERT INTO {tbl} (pk, ck, s) VALUES (?, ?, ?)")
            with pytest.raises(WriteFailure,
                    match="(?i)collection element count.*exceeds"):
                cql.execute(insert_s, [3, 0, {999}])
            # Writing without the collection column succeeds.
            cql.execute(insert_v1, [3, 0, b"\x00"])
            # Different CK in pk=3 succeeds.
            cql.execute(insert_s, [3, 99, {999}])
