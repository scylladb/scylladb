#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Test that ensure_committed_by_group0() fixes tables missing the flag on boot.
"""
import pytest
import logging
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_ensure_committed_by_group0(manager: ManagerClient):
    """Tables with committed_by_group0 = null or false get fixed on restart."""
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

    # Simulate pre-group0 table (null) and recovery-mode table (false)
    await cql.run_async(
        "DELETE committed_by_group0 FROM system_schema.scylla_tables "
        "WHERE keyspace_name = 'ks' AND table_name = 'tbl_null'")
    await cql.run_async(
        "UPDATE system_schema.scylla_tables SET committed_by_group0 = false "
        "WHERE keyspace_name = 'ks' AND table_name = 'tbl_false'")

    # Restart — ensure_committed_by_group0() should fix both on boot
    await manager.server_restart(servers[0].server_id)
    (cql, _) = await manager.get_ready_cql(servers)

    # Verify fixup happened for both tables
    for tbl in ['tbl_null', 'tbl_false']:
        rows = await cql.run_async(
            f"SELECT committed_by_group0 FROM system_schema.scylla_tables "
            f"WHERE keyspace_name = 'ks' AND table_name = '{tbl}'")
        assert rows[0].committed_by_group0 == True, \
            f"committed_by_group0 not fixed for {tbl}"
