# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# TEMPORARY - verification of failed-test log collection/linking.
#
# This test is intentionally *flaky*: it fails on the FIRST execution and
# passes on every subsequent execution. That is exactly what is needed to
# exercise the flaky-retry path in scylla-pkg's checkIfFlakyTest (test.groovy),
# which re-runs a failed test with `--repeat=10 --maxfail=3`:
#   - the first CI run fails            -> the test is fed into checkIfFlakyTest
#   - the re-run fails only once (<3)   -> classified FLAKY -> build UNSTABLE
# The single failing repetition still triggers pytest_runtest_makereport in
# test/pylib/runner.py, which copies the leased scylla_cluster server log into
# <mode>/failed_test/<test>/ and records TEST_LOGS/PYTEST_LOG links using the
# `--artifacts_dir_url` forwarded by scylla-pkg PR #6452.
#
# A cross-process counter file is used (not an in-process global) because the
# flaky re-run executes with `-n=8` xdist workers, so state must be shared on
# disk. REMOVE before merging - this is a DONT MERGE debug branch.

import os
import tempfile

_COUNTER_FILE = os.path.join(
    tempfile.gettempdir(), "scylla_verify_failed_logs_cqlpy.counter"
)


def _next_run_index() -> int:
    """Atomically append one byte to the counter file and return the pre-append
    length, i.e. a 0-based index of this execution across all workers/repeats."""
    fd = os.open(_COUNTER_FILE, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        # Serialize concurrent xdist workers with a simple advisory lock.
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX)
        except Exception:
            pass
        index = os.fstat(fd).st_size
        os.write(fd, b"x")
        return index
    finally:
        os.close(fd)


def test_verify_failed_log_collection_cqlpy(cql):
    # Touch the cluster so a session/server log exists, then fail only on the
    # very first execution to produce a *flaky* signal.
    cql.execute("SELECT release_version FROM system.local")
    if _next_run_index() == 0:
        assert False, "intentional first-run failure to verify flaky failed-test log collection (cqlpy)"
