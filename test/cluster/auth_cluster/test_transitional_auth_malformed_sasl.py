#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

from cassandra import ConsistencyLevel
from cassandra.auth import AuthProvider, Authenticator, PlainTextAuthProvider
from cassandra.query import SimpleStatement

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config

logger = logging.getLogger(__name__)

TRANSITIONAL_AUTHENTICATOR = "com.scylladb.auth.TransitionalAuthenticator"
TRANSITIONAL_AUTHORIZER = "com.scylladb.auth.TransitionalAuthorizer"

# SASL PLAIN encodes credentials as authzId<NUL>authnId<NUL>password. None of
# these carry a non-empty authnId (or password), so the server-side PLAIN parser
# rejects them. In transitional mode that rejection is swallowed and the
# challenge is reported complete, which used to leave the coordinator reading
# credentials that were never parsed.
MALFORMED_TOKENS = [b"", b"\x00", b"\x00\x00", b"onlypassword"]

# The name every other path reports for a user that has none of its own.
ANONYMOUS = "anonymous"


class _MalformedSaslAuthenticator(Authenticator):
    def __init__(self, token: bytes) -> None:
        self._token = token

    def initial_response(self):
        return self._token

    def evaluate_challenge(self, challenge):
        return b""


class _MalformedSaslAuthProvider(AuthProvider):
    def __init__(self, token: bytes) -> None:
        self._token = token

    def new_authenticator(self, host) -> Authenticator:
        return _MalformedSaslAuthenticator(self._token)


# Reproduces SCYLLADB-4166
async def test_transitional_auth_malformed_sasl_token(manager: ScyllaClusterManager) -> None:
    server = await manager.server_add(config={**auth_config,
                                              "authenticator": TRANSITIONAL_AUTHENTICATOR,
                                              "authorizer": TRANSITIONAL_AUTHORIZER,
                                              "audit": "table",
                                              "audit_categories": "AUTH"})

    for token in MALFORMED_TOKENS:
        logger.info("offering malformed SASL token %r", token)
        await manager.driver_connect(server=server, auth_provider=_MalformedSaslAuthProvider(token))
        await manager.get_cql().run_async("SELECT release_version FROM system.local")
        manager.driver_close()

    logger.info("a well-formed login must still authenticate as itself")
    await manager.driver_connect(server=server,
                                 auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))
    cql = manager.get_cql()

    # The audit keyspace is created with RF=3, more than this cluster has nodes.
    rows = list(await cql.run_async(SimpleStatement(
        "SELECT username, error FROM audit.audit_log WHERE category = 'AUTH' AND operation = 'LOGIN' ALLOW FILTERING",
        consistency_level=ConsistencyLevel.ONE)))
    logger.info("audited logins: %s", sorted({(r.username, r.error) for r in rows}))

    # A credential the authenticator could not parse is recorded under the name
    # every other anonymous login is recorded under, and not as a failure:
    # transitional mode accepted the connection.
    anonymous_logins = [r for r in rows if r.username == ANONYMOUS]
    assert len(anonymous_logins) >= len(MALFORMED_TOKENS)
    assert not any(r.error for r in anonymous_logins)
    assert all(r.username for r in rows)
    assert any(r.username == "cassandra" for r in rows)
