#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest
import logging

from test.pylib.manager_client import ManagerClient
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_permissions_removal_and_restart(manager: ManagerClient) -> None:
    """Test that a node boots successfully when role_permissions contains a
    ghost row with role and resource set but the permissions column missing.

    The auth v2 migration (now removed) used INSERT to copy permission rows
    from the legacy table, which created CQL row markers. Normal GRANT uses
    UPDATE, which only writes collection cells without row markers. When
    permissions were later revoked, the collection cells were tombstoned but
    the row marker from the migration INSERT persisted. That leaves a row
    with role and resource but no permissions column.

    This test simulates that scenario:
    1. INSERT permissions with row marker (simulating auth v2 migration)
    2. REVOKE ALL permissions (tombstones the cells, marker survives)
    3. Restart and verify the node boots successfully
    """
    servers = await manager.servers_add(1, config=auth_config)
    cql, _ = await manager.get_ready_cql(servers)
    server = servers[0]

    await cql.run_async("CREATE ROLE scylla_admin WITH PASSWORD = 'x' AND LOGIN = true")
    await cql.run_async("CREATE ROLE scylla_manager WITH PASSWORD = 'x' AND LOGIN = true")

    # Simulate auth v2 migration: INSERT creates a row marker alongside the
    # permission cells, unlike GRANT which uses UPDATE (no row marker).
    await cql.run_async(
        "INSERT INTO system.role_permissions (role, resource, permissions) "
        "VALUES ('scylla_admin', 'roles/scylla_manager', {'ALTER', 'AUTHORIZE', 'DROP'})")

    # Revoke all permissions — tombstones the collection cells, but the
    # row marker from the INSERT survives, creating a ghost row.
    await cql.run_async("REVOKE ALL ON ROLE scylla_manager FROM scylla_admin")

    # Additional check: a row with an explicitly empty permissions set.
    await cql.run_async("CREATE ROLE test_empty_perms WITH PASSWORD = 'x' AND LOGIN = true")
    await cql.run_async(
        "INSERT INTO system.role_permissions (role, resource) "
        "VALUES ('test_empty_perms', 'roles/scylla_manager')")

    # Restart — the auth cache loads the ghost row and must not crash
    logger.info("Restarting node")
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_start(server.server_id)

    await manager.driver_connect()
    cql, _ = await manager.get_ready_cql(servers)
    rows = await cql.run_async("SELECT * FROM system.local")
    assert len(rows) == 1, "Node should be functional after restart"
    logger.info("Node restarted successfully")
