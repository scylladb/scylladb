#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import logging

from cassandra.cluster import NoHostAvailable
from test.pylib.manager_client import ManagerClient
from test.pylib.util import unique_name
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from cassandra.auth import PlainTextAuthProvider


"""
Checks whether the default superuser is replaced by a custom one,
and that the default superuser is not present in the system.
"""
async def test_auth_default_superuser_replaced(manager: ManagerClient) -> None:
    servers = await manager.servers_add(3, config=auth_config, auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    logging.info("Creating non default superuser")
    role = "r" + unique_name()
    await cql.run_async(f"CREATE ROLE {role} WITH SUPERUSER = true AND PASSWORD = '{role}' AND LOGIN = true")

    logging.info("Removing default superuser")
    await manager.driver_connect(auth_provider=PlainTextAuthProvider(username=role, password=role))
    cql = manager.get_cql()
    await cql.run_async(f"DROP ROLE cassandra")

    logging.info("Rolling restart")
    await manager.rolling_restart(servers, wait_for_cql=False)

    logging.info("Non-rolling restart")
    for s in servers:
        logging.info(f"Stopping server {s.server_id}")
        await manager.server_stop_gracefully(s.server_id)
    for s in servers:
        logging.info(f"Starting server {s.server_id}")
        await manager.server_start(s.server_id, auth_provider={
            "authenticator": "cassandra.auth.PlainTextAuthProvider",
            "kwargs": {
                "username": role,
                "password": role
            }}
        )

    logging.info("Waiting for CQL")
    await manager.driver_connect(auth_provider=PlainTextAuthProvider(username=role, password=role))
    cql, _ = await manager.get_ready_cql(servers)

    logging.info("Checking for expected roles")
    for s in servers:
        logging.info(f"Checking if default removed superuser is not present on server {s.server_id}")
        with pytest.raises(NoHostAvailable, match="Bad credentials"):
            await manager.driver_connect(server=s,
                auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'))

        logging.info(f"Checking if added superuser works on server {s.server_id}")
        await manager.driver_connect(server=s,
            auth_provider=PlainTextAuthProvider(username=role, password=role))
