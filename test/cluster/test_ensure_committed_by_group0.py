#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Test that ensure_committed_by_group0() fixes tables missing the flag on boot,
and that it skips its scan once a "done" marker is persisted.
"""
import pytest
import logging
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

DONE_MARKER_KEY = "ensure_committed_by_group0_done"
FOUND_MISSING_LOG_MSG = "table\\(s\\) missing committed_by_group0"
ALL_SET_LOG_MSG = "All non-system tables have committed_by_group0 set"
SKIPPED_LOG_MSG = "Skipping committed_by_group0 check"


@pytest.mark.asyncio
async def test_ensure_committed_by_group0(manager: ScyllaClusterManager):
    """Tables with committed_by_group0 = null or false get fixed on restart,
    and a plain restart afterwards doesn't rescan scylla_tables."""
    servers = await manager.servers_add(1)
    (cql, _) = await manager.get_ready_cql(servers)

    await cql.run_async("CREATE KEYSPACE ks WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.tbl_null (pk int PRIMARY KEY)")
    await cql.run_async("CREATE TABLE ks.tbl_false (pk int PRIMARY KEY)")

    # Verify both have committed_by_group0 = true initially
    for tbl in ['tbl_null', 'tbl_false']:
        rows = await cql.run_async(
            f"SELECT committed_by_group0 FROM system_schema.scylla_tables "
            f"WHERE keyspace_name = 'ks' AND table_name = '{tbl}'")
        assert rows[0].committed_by_group0 == True

    # Simulate pre-group0 table (null) and recovery-mode table (false), deleting the
    # "done" marker like the recovery procedure does (see docs/dev/raft-in-scylla.md).
    await cql.run_async(
        "DELETE committed_by_group0 FROM system_schema.scylla_tables "
        "WHERE keyspace_name = 'ks' AND table_name = 'tbl_null'")
    await cql.run_async(
        "UPDATE system_schema.scylla_tables SET committed_by_group0 = false "
        "WHERE keyspace_name = 'ks' AND table_name = 'tbl_false'")
    await cql.run_async(f"DELETE FROM system.scylla_local WHERE key = '{DONE_MARKER_KEY}'")

    # Restart — ensure_committed_by_group0() should fix both on boot
    log = await manager.server_open_log(servers[0].server_id)
    mark = await log.mark()
    await manager.server_restart(servers[0].server_id)
    (cql, _) = await manager.get_ready_cql(servers)

    # Positive control: the scan really ran on this boot (keeps the absence
    # assertions below honest if the log message text ever changes).
    assert await log.grep(FOUND_MISSING_LOG_MSG, from_mark=mark), \
        "expected ensure_committed_by_group0 to scan and find unflagged tables"

    # Verify fixup happened for both tables
    for tbl in ['tbl_null', 'tbl_false']:
        rows = await cql.run_async(
            f"SELECT committed_by_group0 FROM system_schema.scylla_tables "
            f"WHERE keyspace_name = 'ks' AND table_name = '{tbl}'")
        assert rows[0].committed_by_group0 == True, \
            f"committed_by_group0 not fixed for {tbl}"

    # The fixup above should have persisted the "done" marker.
    rows = await cql.run_async(f"SELECT value FROM system.scylla_local WHERE key = '{DONE_MARKER_KEY}'")
    assert rows and rows[0].value == "true", "ensure_committed_by_group0_done marker not set after fixup"

    # A further plain restart, with nothing broken, should skip the scan entirely.
    mark = await log.mark()
    await manager.server_restart(servers[0].server_id)
    await manager.get_ready_cql(servers)

    assert await log.grep(SKIPPED_LOG_MSG, from_mark=mark), \
        "ensure_committed_by_group0 did not take the skip path despite the done marker"
    assert not await log.grep(FOUND_MISSING_LOG_MSG, from_mark=mark), \
        "ensure_committed_by_group0 rescanned scylla_tables despite the done marker"
    assert not await log.grep(ALL_SET_LOG_MSG, from_mark=mark), \
        "ensure_committed_by_group0 rescanned scylla_tables despite the done marker"
