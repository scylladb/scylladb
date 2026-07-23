# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# TEMPORARY - verification of failed-test log collection/linking (PR #25967).
# This test intentionally fails so that pytest_runtest_makereport in
# test/pylib/runner.py copies the leased scylla_cluster server log into
# <mode>/failed_test/<test>/ and records TEST_LOGS/PYTEST_LOG links.
# REMOVE before merging the real change.


def test_verify_failed_log_collection_cqlpy(cql):
    # Touch the cluster so a session/server log exists, then fail on purpose.
    cql.execute("SELECT release_version FROM system.local")
    assert False, "intentional failure to verify failed-test log collection (cqlpy)"
