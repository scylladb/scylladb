#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""End-to-end tests of TransitionalAuthenticator and TransitionalAuthorizer,
the pair meant for moving a running cluster between no authentication and
PasswordAuthenticator/CassandraAuthorizer.

Under TransitionalAuthenticator a login with the right password logs in as its
role, and one that fails (a wrong password, an unknown user) logs in as the
anonymous user instead of being refused. Only a client that offers no
credentials at all is turned away, by the driver itself. TransitionalAuthorizer
lets everyone, anonymous included, CREATE, ALTER, DROP, SELECT and MODIFY
anything, but never AUTHORIZE, so nobody but a superuser can GRANT or REVOKE.

test_transitional_auth_malformed_sasl.py covers SASL tokens that cannot be
parsed; these tests cover well-formed logins and switching authenticators
across restarts.
"""

import logging
from contextlib import contextmanager

import pytest
from cassandra import AuthenticationFailed, Unauthorized
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable
from cassandra.policies import RoundRobinPolicy

from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

TRANSITIONAL = {
    "authenticator": "com.scylladb.auth.TransitionalAuthenticator",
    "authorizer": "com.scylladb.auth.TransitionalAuthorizer",
}
STRICT = {
    "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
    "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
}
ALLOW_ALL = {
    "authenticator": "AllowAllAuthenticator",
    "authorizer": "AllowAllAuthorizer",
}

NORMAL, NORMAL_PASSWORD = "normal", "123456"

ANONYMOUS_REFUSED = "You have to be logged in and not anonymous to perform this request"
CREATE_KEYSPACE = "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
CREATE_TABLE = "CREATE TABLE ks.cf (id int PRIMARY KEY)"


@contextmanager
def session_as(manager: ScyllaClusterManager, server, username: str | None = None, password: str | None = None):
    """A CQL session on `server`, logged in as `username` (or offering no
    credentials when it is None), closed on exit."""
    auth_provider = PlainTextAuthProvider(username=username, password=password) if username else None
    # A policy of its own: con_gen's default RoundRobinPolicy is one object shared
    # by every cluster it creates, so a failed login's cluster would take the
    # host away from sessions that are still open.
    cluster = manager.con_gen([server.ip_addr], manager.port, manager.use_ssl, auth_provider,
                              load_balancing_policy=RoundRobinPolicy())
    try:
        yield cluster.connect()
    finally:
        safe_driver_shutdown(cluster)


def assert_unauthorized(session, query: str, message: str):
    with pytest.raises(Unauthorized) as exc_info:
        session.execute(query)
    assert message in str(exc_info.value)


def assert_login_fails(manager: ScyllaClusterManager, server, username: str | None = None, password: str | None = None):
    """The login is refused with an authentication error.

    Without credentials the driver gives up by itself once the server asks for
    them; with wrong ones the server refuses them. Either way the driver reports
    an AuthenticationFailed for the host."""
    with pytest.raises(NoHostAvailable) as exc_info:
        with session_as(manager, server, username, password):
            pass
    # Read the per-host errors directly: str(NoHostAvailable) shows them through
    # repr(), which escapes quotes in the server's messages.
    errors = list(exc_info.value.errors.values())
    assert errors and all(isinstance(e, AuthenticationFailed) for e in errors), errors


async def switch_auth(manager: ScyllaClusterManager, server, config: dict):
    """Restart `server` with another authenticator and authorizer.

    The graceful stop also runs the outgoing authenticator's and authorizer's
    stop(), which test.py's SIGKILL at teardown never does.
    """
    logger.info("restarting with %s", config)
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_update_config(server.server_id, config_options=config)
    await manager.server_start(server.server_id)
    # The harness logs in as cassandra/cassandra. PasswordAuthenticator restores
    # that password at startup from auth_superuser_salted_password.
    await manager.driver_connect(server=server)


async def test_transitional_auth_from_default(manager: ScyllaClusterManager):
    """Move a node from AllowAll to Transitional and on to PasswordAuthenticator
    and CassandraAuthorizer. Ported from dtest's
    auth_test.py::TestAuth::test_transitional_auth_from_default."""
    server = await manager.server_add(config=ALLOW_ALL)

    await switch_auth(manager, server, TRANSITIONAL)
    await manager.get_cql().run_async(f"CREATE USER {NORMAL} WITH PASSWORD '{NORMAL_PASSWORD}' NOSUPERUSER")

    # A wrong password logs in, but as the anonymous user.
    with session_as(manager, server, NORMAL, "wrongpwd") as session:
        assert_unauthorized(session, "LIST USERS", ANONYMOUS_REFUSED)
        # TransitionalAuthorizer grants CREATE, ALTER, DROP, SELECT and MODIFY
        # to everyone, anonymous included...
        session.execute(CREATE_KEYSPACE)
        session.execute(CREATE_TABLE)
        session.execute("INSERT INTO ks.cf (id) VALUES (1)")
        assert [row.id for row in session.execute("SELECT * FROM ks.cf")] == [1]
        session.execute("ALTER TABLE ks.cf WITH comment = 'altered by anonymous'")
        # ...but GRANT and REVOKE need a user that is logged in.
        assert_unauthorized(session, f"GRANT SELECT ON ks.cf TO {NORMAL}", ANONYMOUS_REFUSED)
        assert_unauthorized(session, f"REVOKE SELECT ON ks.cf FROM {NORMAL}", ANONYMOUS_REFUSED)

    # A client offering no credentials at all is still refused.
    assert_login_fails(manager, server)

    await switch_auth(manager, server, STRICT)

    assert_login_fails(manager, server)
    assert_login_fails(manager, server, NORMAL, "wrongpwd")
    with session_as(manager, server, NORMAL, NORMAL_PASSWORD) as session:
        # Nothing was granted to normal: the keyspace was created anonymously.
        assert_unauthorized(session, "SELECT * FROM ks.cf",
                            f"User {NORMAL} has no SELECT permission on <table ks.cf> or any of its parents")
        assert_unauthorized(session, f"REVOKE SELECT ON ks.cf FROM {NORMAL}",
                            f"User {NORMAL} has no AUTHORIZE permission on <table ks.cf> or any of its parents")

    await manager.server_stop_gracefully(server.server_id)


async def test_transitional_auth_from_pwdauth(manager: ScyllaClusterManager):
    """Move a node from PasswordAuthenticator and CassandraAuthorizer to
    Transitional and on to AllowAll, the reverse of the intended order.
    Ported from dtest's auth_test.py::TestAuth::test_transitional_auth_from_pwdauth."""
    server = await manager.server_add(config=STRICT)
    await manager.get_cql().run_async(f"CREATE USER {NORMAL} WITH PASSWORD '{NORMAL_PASSWORD}' NOSUPERUSER")

    with session_as(manager, server, NORMAL, NORMAL_PASSWORD) as session:
        # A non-superuser lists only the roles granted to it: its own.
        assert [row.name for row in session.execute("LIST USERS")] == [NORMAL]
    assert_login_fails(manager, server, NORMAL, "wrongpwd")

    await switch_auth(manager, server, TRANSITIONAL)

    with session_as(manager, server, NORMAL, NORMAL_PASSWORD) as session:
        # The right password still logs in as normal.
        assert [row.name for row in session.execute("LIST USERS")] == [NORMAL]
        session.execute(CREATE_KEYSPACE)
        session.execute(CREATE_TABLE)
        # Creating them granted normal every permission on them, but
        # TransitionalAuthorizer never reports AUTHORIZE.
        assert_unauthorized(session, f"GRANT ALTER ON ks.cf TO {NORMAL}",
                            f"User {NORMAL} has no AUTHORIZE permission on <table ks.cf> or any of its parents")
        assert_unauthorized(session, f"REVOKE SELECT ON ks.cf FROM {NORMAL}",
                            f"User {NORMAL} has no AUTHORIZE permission on <table ks.cf> or any of its parents")

    with session_as(manager, server, NORMAL, "wrongpwd") as session:
        assert_unauthorized(session, "LIST USERS", ANONYMOUS_REFUSED)
    with session_as(manager, server, "nosuchuser", "nosuchpwd") as session:
        assert_unauthorized(session, "LIST USERS", ANONYMOUS_REFUSED)
    assert_login_fails(manager, server)

    await switch_auth(manager, server, ALLOW_ALL)

    # Everyone is anonymous now, whatever credentials they offer.
    for username, password in [("cassandra", "cassandra"), (NORMAL, NORMAL_PASSWORD)]:
        with session_as(manager, server, username, password) as session:
            assert_unauthorized(session, "LIST USERS", ANONYMOUS_REFUSED)

    await manager.server_stop_gracefully(server.server_id)
