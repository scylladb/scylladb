#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import logging
import time

from cassandra.cluster import NoHostAvailable
from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient, ServerUpState
from test.pylib.util import wait_for
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


async def repeat_if_host_unavailable(f):
    async def try_execute(f):
        try:
            await f()
            return True
        except NoHostAvailable:
            return None
    return await wait_for(lambda: try_execute(f), time.time() + 60)

"""
Test CQL is served only after superuser default password is created.
After CQL is served, user is properily authenticated as superuser (not annonymous user)
"""
@pytest.mark.asyncio
@skip_mode('release', 'error injection is disabled in release mode')
async def test_auth_password_ensured(manager: ManagerClient) -> None:
    config = {
        **auth_config,
        'authenticator': "com.scylladb.auth.TransitionalAuthenticator",
        'error_injections_at_startup': ['password_authenticator_start_pause'],
    }
    server = await manager.server_add(config=config, expected_server_up_state=ServerUpState.HOST_ID_QUERIED, connect_driver=False)

    logging.info("Waiting until PasswordAuthenticator pauses on the injected error")
    server_log = await manager.server_open_log(server.server_id)
    await server_log.wait_for("password_authenticator_start_pause: waiting for message")

    with pytest.raises(NoHostAvailable, match="Unable to connect to any servers"):
        logging.info("Expecting driver connection failure, because password_authenticator_start_pause blocks serving CQL")
        await manager.driver_connect()

    await manager.api.message_injection(server.ip_addr, 'password_authenticator_start_pause')
    await repeat_if_host_unavailable(manager.driver_connect)

    cql, _ = await manager.get_ready_cql([server])

    logging.info("Run CREATE USER to confirm successful superuser authentication")
    await cql.run_async("CREATE USER normal WITH PASSWORD '123456' NOSUPERUSER")

