#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import contextlib
import logging

import pytest

from test.cluster.util import new_test_keyspace, reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_create_tablet_table_replayed_from_group0_log_on_restart(manager: ScyllaClusterManager) -> None:
    """Reproducer for SCYLLADB-5012.

    A node commits the group0 entry creating a table in a tablets keyspace,
    but is killed before applying it. On restart, the entry is applied during
    the early group0 log replay, which only writes it to the system tables.
    The table must then be loaded with the non-system keyspaces, which needs
    its tablet map to be loaded into memory as well.
    """
    srv = await manager.server_add()
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                                          "AND tablets = {'enabled': true}") as ks:
        await manager.api.enable_injection(srv.ip_addr, "group0_state_machine::block_apply", one_shot=True)

        # The node coordinates the schema change and never applies it, so the
        # statement does not complete.
        create_table = cql.run_async(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY, v int)")
        # Raft stores the commit index in system.raft before it passes the
        # committed entries to apply(), so the entry is committed once apply()
        # blocks.
        await manager.api.wait_for_injection_enter(srv.ip_addr, "group0_state_machine::block_apply")
        # On restart, the early group0 log replay applies the entries only up to
        # the stored commit index. Flush system.raft so that the commit index
        # write survives the kill even if it was not yet synced to the commitlog.
        # Otherwise the entry is committed again only after the user keyspaces
        # are loaded, and the bug is not reached.
        await manager.api.flush_keyspace(srv.ip_addr, "system")

        logger.info("Killing %s with the CREATE TABLE entry committed but not applied", srv)
        await manager.server_stop(srv.server_id, convict=False)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(create_table, timeout=60)

        await manager.server_start(srv.server_id)

        # The table must have been loaded on restart.
        cql = await reconnect_driver(manager)
        await cql.run_async(f"INSERT INTO {ks}.tbl (pk, v) VALUES (1, 1)")
