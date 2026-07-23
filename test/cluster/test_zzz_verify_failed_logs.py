#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# TEMPORARY - verification of failed-test log collection/linking (PR #25967).
# This test intentionally fails so that the `manager` fixture in
# test/cluster/conftest.py gathers server logs into <mode>/failed_test/<test>/
# and records TEST_LOGS/PYTEST_LOG links. It starts a server first so
# gather_related_logs has an actual scylla-*.log to copy.
# REMOVE before merging the real change.

import logging

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


async def test_verify_failed_log_collection_cluster(manager: ManagerClient):
    # The manager fixture leases an empty cluster; add a server so there is a
    # running Scylla whose log (scylla-*.log) can be gathered on failure.
    await manager.servers_add(1)
    cql = manager.get_cql()
    cql.execute("SELECT release_version FROM system.local")
    assert False, "intentional failure to verify failed-test log collection (cluster)"
