#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests that system.clients shows connection_stage = 'READY' after a successful
authentication, regardless of which auth path was taken in process_startup:

  1. No-auth path (AllowAllAuthenticator): require_authentication() = false,
     connection is immediately marked ready.

  2. SASL/password path (PasswordAuthenticator): client sends AUTH_RESPONSE,
     process_auth_response() marks the connection ready.

  3. Certificate-bypass path (CertificateAuthenticator with
     transport_early_auth_bypass injection): authenticate(session_dn_func)
     returns a user immediately, process_startup() marks the connection ready
     without a SASL round-trip.

     This is the path introduced by commit 20e9619bb1 that was missing the
     _ready = true / update_scheduling_group() / on_connection_ready() calls.
"""

import logging
import time
import pytest
from cassandra.auth import PlainTextAuthProvider
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for


logger = logging.getLogger(__name__)


def make_server_config(auth_type: str) -> dict:
    """Return the scylla config dict for a given auth type."""
    if auth_type == "allow_all":
        return {
            "authenticator": "AllowAllAuthenticator",
            "authorizer": "AllowAllAuthorizer",
        }
    if auth_type == "password":
        return {
            "authenticator": "PasswordAuthenticator",
            "authorizer": "CassandraAuthorizer",
        }
    if auth_type == "cert_bypass":
        # CertificateAuthenticator with transport_early_auth_bypass active
        # from the very first connection (including the server-add readiness
        # check), so no TLS socket is required.  The injection makes
        # certificate_authenticator::authenticate() return the cassandra
        # superuser immediately, bypassing the TLS certificate check.
        return {
            "authenticator": "CertificateAuthenticator",
            "authorizer": "CassandraAuthorizer",
            # Minimal valid config; regex is never reached due to injection.
            "auth_certificate_role_queries": [{"source": "SUBJECT", "query": "CN=(.+)"}],
            "error_injections_at_startup": [
                {"name": "transport_early_auth_bypass", "value": "cassandra"},
            ],
        }
    raise ValueError(f"Unknown auth_type: {auth_type!r}")


@pytest.mark.asyncio
@pytest.mark.parametrize("auth_type", [
    "allow_all",
    "password",
    "cert_bypass",
])
@pytest.mark.skip_mode("release", reason="error injections are not supported in release mode")
async def test_connection_stage_ready_after_auth(manager: ManagerClient, auth_type: str) -> None:
    """After a successful authentication via any code path in process_startup,
    the connection must be reported as READY in system.clients."""
    server = await manager.server_add(config=make_server_config(auth_type))

    # CertificateAuthenticator bypassed by injection returns a user without a
    # SASL challenge, so the driver must not attempt a password handshake.
    auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra") \
        if auth_type == "password" else None

    await manager.driver_connect(server=server, auth_provider=auth_provider)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

    async def all_connections_ready():
        rows = list(cql.execute(
            "SELECT connection_stage FROM system.clients WHERE client_type = 'cql' ALLOW FILTERING"
        ))
        if not rows:
            return None
        if any(r.connection_stage != "READY" for r in rows):
            return None
        return rows

    rows = await wait_for(all_connections_ready, time.time() + 30)
    assert rows, "No CQL connections found in system.clients"
    logger.info("auth_type=%s: all %d connection(s) are READY", auth_type, len(rows))
