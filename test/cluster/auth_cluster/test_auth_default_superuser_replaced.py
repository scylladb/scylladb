#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest
import logging

from cassandra.cluster import NoHostAvailable
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name
from test.pylib.internal_types import ServerUpState
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from cassandra.auth import PlainTextAuthProvider
from test.cluster.auth_cluster.test_auth_switch_authenticator import (
    DtestSessions, assert_auth_failed, assert_unauthorized, dtest_auth_prepare)


"""
Checks whether the default superuser is replaced by a custom one,
and that the default superuser is not present in the system.
"""
async def test_auth_default_superuser_replaced(manager: ScyllaClusterManager) -> None:
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


# Ported from scylla-dtest auth_test.py::TestAuth::test_auth_username_password_parameters.
# Three nodes with the superuser configured by auth_superuser_name and
# auth_superuser_salted_password (the hash of "gris") instead of the default
# one: logins without credentials, with an unknown user or with the wrong
# password fail, while the configured superuser can log in and create roles
# that then have no permissions of their own.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_auth_superuser_from_config(manager: ScyllaClusterManager) -> None:
    config = {
        "auth_superuser_name": "gris",  # not 'cassandra'
        "auth_superuser_salted_password": "$6$IcPWfCigHWVhHTf.$h3.30m5R2CnYqIeniCumbXCBxBxvtYPP3MbZVsjKcu268ESOcrUtSJwf1iO1s83KUT3waITRtTiexBdSWEI0Q/",
    }
    # The test framework would wait for the default superuser to log in.
    servers = await dtest_auth_prepare(manager, enable_auth=True, wait_for_superuser=True, config=config,
                                       expected_server_up_state=ServerUpState.HOST_ID_QUERIED, connect_driver=False)
    node1 = servers[0]
    sessions = DtestSessions(manager)
    try:
        for user, password in [(None, None), ("normal", "wrongpwd"), ("gris", "tuta")]:
            with pytest.raises(NoHostAvailable) as exc:
                sessions.get(node1, user, password, patient=False)
            assert_auth_failed(exc)

        gris = sessions.get(node1, "gris", "gris")
        gris.execute("CREATE USER normal WITH PASSWORD '123456' NOSUPERUSER")
        gris.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        gris.execute("CREATE TABLE ks.cf (id int primary key)")

        session = sessions.get(node1, "normal", "123456")
        assert_unauthorized("User normal has no SELECT permission on <table ks.cf> or any of its parents", session, "SELECT * FROM ks.cf")
        assert_unauthorized("User normal has no AUTHORIZE permission on <table ks.cf> or any of its parents", session, "REVOKE SELECT ON ks.cf from normal")
    finally:
        sessions.close()
