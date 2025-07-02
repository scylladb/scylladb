#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import logging
import asyncio
import time

from test.pylib.manager_client import ManagerClient
from cassandra.auth import PlainTextAuthProvider
from test.pylib.util import wait_for
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


async def repeat_until_success(f):
    async def try_execute(f):
        try:
            await f()
            return True
        except:
            return None
    return await wait_for(lambda: try_execute(f), time.time() + 60)

"""
Test if superuser is recreated after manual sstable delete (password reset procedure).
"""
@pytest.mark.asyncio
async def test_auth_after_reset(manager: ManagerClient) -> None:
    servers = await manager.servers_add(3, config=auth_config)
    cql, _ = await manager.get_ready_cql(servers)
    await cql.run_async("ALTER ROLE cassandra WITH PASSWORD = 'forgotten_pwd'")

    logging.info("Stopping cluster")
    await asyncio.gather(*[manager.server_stop_gracefully(server.server_id) for server in servers])

    logging.info("Deleting sstables")
    for table in ["roles", "role_members", "role_attributes", "role_permissions"]:
        await asyncio.gather(*[manager.server_wipe_sstables(server.server_id, "system", table) for server in servers])

    logging.info("Starting cluster")
    # Don't try connect to the servers yet, with deleted superuser it will be possible only after
    # quorum is reached.
    await asyncio.gather(*[manager.server_start(server.server_id, connect_driver=False) for server in servers])

    logging.info("Waiting for CQL connection")
    await repeat_until_success(lambda: manager.driver_connect(auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra")))
    await manager.get_ready_cql(servers)
