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
    insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
    for ck in range(num_rows):
        cql.execute(insert, [pk, ck, bytes(value_size_bytes)])


def make_large_row(cql, tbl, pk, ck, value_size_bytes):
    """Insert a single row with a blob of `value_size_bytes` under (pk, ck)."""
    insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
    cql.execute(insert, [pk, ck, bytes(value_size_bytes)])


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

        schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
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

        schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
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

        schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=60,
                                    value_size_bytes=8)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
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

        schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=2,
                                    value_size_bytes=600 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
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

        schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_oversized_partition(cql, tbl, pk=1, num_rows=60,
                                    value_size_bytes=8)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 999, b"\x00"])

            wait_for_log(logfile, WARN_RE, timeout=5)


def test_partition_no_warning_below_soft_limit(cql, test_keyspace, logfile):
    """A small partition must not produce any warning."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_partition_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_partition_fail_threshold_mb', '10'))

        schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
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

        schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_large_row(cql, tbl, pk=1, ck=0,
                           value_size_bytes=1200 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
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

        schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_large_row(cql, tbl, pk=1, ck=0,
                           value_size_bytes=1200 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
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

        schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            make_large_row(cql, tbl, pk=1, ck=0,
                           value_size_bytes=1200 * 1024)
            nodetool.flush(cql, tbl)

            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 0, b"\x00"])

            wait_for_log(logfile, WARN_RE, timeout=5)


def test_row_no_warning_below_soft_limit(cql, test_keyspace, logfile):
    """A small row must not produce any warning."""
    with ExitStack() as cfg:
        cfg.enter_context(config_value_context(cql,
            'compaction_large_row_warning_threshold_mb', '1'))
        cfg.enter_context(config_value_context(cql,
            'large_row_fail_threshold_mb', '10'))

        schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
        with new_test_table(cql, test_keyspace, schema,
                extra=" WITH large_data_guardrails_enabled = true") as tbl:
            insert = cql.prepare(f"INSERT INTO {tbl} (pk, ck, v) VALUES (?, ?, ?)")
            cql.execute(insert, [1, 0, b"\x00"])
            nodetool.flush(cql, tbl)

            assert_no_log(logfile, WARN_RE,
                          lambda: cql.execute(insert, [1, 1, b"\x00"]))
