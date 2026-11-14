#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""End-to-end tests of SaslauthdAuthenticator: CQL logins checked by saslauthd
against LDAP.

test.py starts slapd and saslauthd once per run (test/pylib/ldap_server.py)
and exports SASLAUTHD_MUX_PATH. The directory is shared by every test in the
run, so these tests only read it. Its fixed entries are (uid/password):
cassandra/cassandra, jsmith/joeisgreat (member of role1) and jdoe/pa55w0rd
(member of role3).

The authenticator's unit tests are in test/ldap/saslauthd_authenticator_test.cc;
these tests cover what only a running node exercises: logins over CQL,
switching authenticators across restarts, an unusable socket path, role
lifecycle and LDAP group roles.
"""

import logging
import os
import time
from contextlib import contextmanager

import pytest
from cassandra import InvalidRequest, Unauthorized
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable
from cassandra.policies import RoundRobinPolicy
from cassandra.protocol import BadCredentials

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.internal_types import ServerUpState
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.skip_types import skip_env
from test.pylib.util import unique_name, wait_for

logger = logging.getLogger(__name__)

SASLAUTHD_AUTHENTICATOR = "com.scylladb.auth.SaslauthdAuthenticator"

# Entries in the LDAP directory, see test/pylib/ldap_server.py.
JDOE, JDOE_PASSWORD = "jdoe", "pa55w0rd"
JSMITH, JSMITH_PASSWORD = "jsmith", "joeisgreat"
JDOE_GROUP = "role3"


def saslauthd_socket_path() -> str:
    path = os.environ.get("SASLAUTHD_MUX_PATH")
    if not path:
        skip_env("needs the saslauthd started by the test.py harness")
    return path


def saslauthd_config(**extra) -> dict:
    return {
        **auth_config,
        "authenticator": SASLAUTHD_AUTHENTICATOR,
        "saslauthd_socket_path": saslauthd_socket_path(),
        **extra,
    }


def ldap_role_manager_config() -> dict:
    """Map a user's LDAP groups to Scylla roles, as in test_ldap_service_levels.py."""
    host = os.environ.get("SEASTAR_LDAP_HOST")
    port = os.environ.get("SEASTAR_LDAP_PORT")
    if not host or not port:
        skip_env("needs the LDAP server started by the test.py harness")
    return {
        "role_manager": "com.scylladb.auth.LDAPRoleManager",
        "ldap_url_template":
            f"ldap://{host}:{port}/dc=example,dc=com?cn?sub"
            "?(uniqueMember=uid={USER},ou=People,dc=example,dc=com)",
        "ldap_attr_role": "cn",
        "ldap_bind_dn": "cn=root,dc=example,dc=com",
        "ldap_bind_passwd": "secret",
    }


@contextmanager
def session_as(manager: ScyllaClusterManager, server, username: str, password: str | None):
    """A CQL session on `server` logged in as `username`, closed on exit."""
    # A policy of its own: con_gen's default RoundRobinPolicy is one object shared
    # by every cluster it creates, so a failed login's cluster would take the
    # host away from sessions that are still open.
    cluster = manager.con_gen([server.ip_addr], manager.port, manager.use_ssl,
                              PlainTextAuthProvider(username=username, password=password or ""),
                              load_balancing_policy=RoundRobinPolicy())
    try:
        yield cluster.connect()
    finally:
        safe_driver_shutdown(cluster)


def assert_can_login(manager, server, username, password):
    with session_as(manager, server, username, password) as session:
        session.execute("SELECT key FROM system.local WHERE key = 'local'")


def login_errors(e: NoHostAvailable) -> list[str]:
    # str(NoHostAvailable) shows the per-host errors through repr(), which escapes
    # the quotes in server messages such as "doesn't exist"; read them directly.
    return [str(err) for err in e.errors.values()]


def assert_login_fails(manager, server, username, password, expected):
    """The login is refused, with `expected` in the server's error message."""
    with pytest.raises(NoHostAvailable) as exc_info:
        with session_as(manager, server, username, password):
            pass
    errors = login_errors(exc_info.value)
    assert any(expected in err for err in errors), errors


async def test_login(manager: ScyllaClusterManager):
    """A role logs in with the password LDAP holds for it, and only if the role
    exists in Scylla and may log in. Ported from dtest's TestLdapSaslAuth:
    test_authentication, test_wrong_user, test_user_login_only,
    test_soft_restart_scylla and test_hard_restart_scylla."""
    server = await manager.server_add(config=saslauthd_config())
    # The harness itself logs in as cassandra/cassandra, which is in LDAP.
    cql = manager.get_cql()
    await cql.run_async(f"CREATE ROLE {JDOE} WITH LOGIN = true")

    assert_can_login(manager, server, JDOE, JDOE_PASSWORD)
    assert_login_fails(manager, server, JDOE, "wrong-password", "Incorrect credentials")
    assert_login_fails(manager, server, JDOE, None, "Incorrect credentials")
    # In neither LDAP nor Scylla.
    assert_login_fails(manager, server, unique_name("nobody_"), JDOE_PASSWORD, "Incorrect credentials")
    # In LDAP but not in Scylla.
    assert_login_fails(manager, server, JSMITH, JSMITH_PASSWORD, f"User {JSMITH} doesn't exist")
    # In both, but without LOGIN.
    await cql.run_async(f"CREATE ROLE {JSMITH}")
    assert_login_fails(manager, server, JSMITH, JSMITH_PASSWORD, f"{JSMITH} is not permitted to log in")

    # Logins keep working after a graceful and after a hard restart.
    await manager.server_restart(server.server_id)
    assert_can_login(manager, server, JDOE, JDOE_PASSWORD)
    await manager.server_stop(server.server_id, convict=False)
    await manager.server_start(server.server_id)
    assert_can_login(manager, server, JDOE, JDOE_PASSWORD)


async def test_passwords_are_rejected(manager: ScyllaClusterManager):
    """Passwords live in LDAP, so CREATE and ALTER ROLE refuse one. From dtest's
    test_switch_with_password_auth."""
    await manager.server_add(config=saslauthd_config())
    cql = manager.get_cql()

    with pytest.raises(BadCredentials, match="Cannot create passwords with SaslauthdAuthenticator"):
        await cql.run_async(f"CREATE ROLE {JDOE} WITH PASSWORD = 'secret' AND LOGIN = true")

    await cql.run_async(f"CREATE ROLE {JDOE} WITH LOGIN = true")
    with pytest.raises(BadCredentials, match="Cannot modify passwords with SaslauthdAuthenticator"):
        await cql.run_async(f"ALTER ROLE {JDOE} WITH PASSWORD = 'secret'")
    # Options other than the password can still be altered.
    await cql.run_async(f"ALTER ROLE {JDOE} WITH LOGIN = false")
    await cql.run_async(f"ALTER ROLE {JDOE} WITH LOGIN = true")


async def test_switch_to_password_authenticator_and_back(manager: ScyllaClusterManager):
    """A role created under SaslauthdAuthenticator has no password in Scylla.
    After switching to PasswordAuthenticator it cannot log in until it is
    given one; after switching back, LDAP's password works again and Scylla's
    does not. Ported from dtest's test_switch_with_password_auth."""
    server = await manager.server_add(config=saslauthd_config())
    cql = manager.get_cql()
    await cql.run_async(f"CREATE ROLE {JDOE} WITH LOGIN = true")
    assert_can_login(manager, server, JDOE, JDOE_PASSWORD)

    await manager.server_stop_gracefully(server.server_id)
    await manager.server_update_config(server.server_id, "authenticator", "PasswordAuthenticator")
    await manager.server_start(server.server_id)
    await manager.driver_connect(server=server)
    cql = manager.get_cql()

    assert_login_fails(manager, server, JDOE, None, "Bad credentials")
    assert_login_fails(manager, server, JDOE, JDOE_PASSWORD, "Bad credentials")
    scylla_password = JDOE_PASSWORD + "-scylla"
    await cql.run_async(f"ALTER ROLE {JDOE} WITH PASSWORD = '{scylla_password}'")
    assert_can_login(manager, server, JDOE, scylla_password)

    await manager.server_stop_gracefully(server.server_id)
    await manager.server_update_config(server.server_id, "authenticator", SASLAUTHD_AUTHENTICATOR)
    await manager.server_start(server.server_id)

    assert_can_login(manager, server, JDOE, JDOE_PASSWORD)
    assert_login_fails(manager, server, JDOE, scylla_password, "Incorrect credentials")


@pytest.mark.parametrize("socket_path", [
    pytest.param("/nonexistent/saslauthd/mux", id="missing"),
    pytest.param("/dev/zero", id="not_a_socket"),
    pytest.param("/tmp", id="directory"),
])
async def test_unusable_socket_path(manager: ScyllaClusterManager, socket_path: str):
    """With saslauthd_socket_path pointing at something saslauthd does not listen
    on, the node still starts and every login is refused with a socket error.
    Once the path is fixed, logins work again. Ported from dtest's
    test_invalid_saslauthd_socket."""
    server = await manager.server_add(config=saslauthd_config())
    await manager.get_cql().run_async(f"CREATE ROLE {JDOE} WITH LOGIN = true")

    await manager.server_stop_gracefully(server.server_id)
    await manager.server_update_config(server.server_id, "saslauthd_socket_path", socket_path)
    # The harness's own cassandra login would fail too, so don't wait for CQL.
    await manager.server_start(server.server_id,
                               expected_server_up_state=ServerUpState.HOST_ID_QUERIED)

    async def login_refused_by_socket_error():
        # Until the CQL port is open the driver fails with a connection error
        # instead; keep trying until the refusal comes from the authenticator.
        try:
            with session_as(manager, server, JDOE, JDOE_PASSWORD):
                pass
        except NoHostAvailable as e:
            if any("saslauthd socket" in err for err in login_errors(e)):
                return True
            logger.info("login not refused by the authenticator yet: %s", e)
            return None
        pytest.fail(f"logged in as {JDOE} with saslauthd_socket_path={socket_path}")
    await wait_for(login_refused_by_socket_error, time.time() + 60)

    await manager.server_stop_gracefully(server.server_id)
    await manager.server_update_config(server.server_id, "saslauthd_socket_path", saslauthd_socket_path())
    await manager.server_start(server.server_id)
    assert_can_login(manager, server, JDOE, JDOE_PASSWORD)


async def test_drop_role(manager: ScyllaClusterManager):
    """DROP ROLE under SaslauthdAuthenticator, whose drop() has nothing to remove.
    Ported from dtest's scylladb/scylladb#25571 tests: test_block_not_existing_drop,
    test_block_current_role_drop, test_drop_cassandra_role,
    test_recreate_role_after_drop and test_existing_session_after_drop."""
    server = await manager.server_add(config=saslauthd_config())
    cql = manager.get_cql()

    with pytest.raises(InvalidRequest, match="Role not_existing_role doesn't exist."):
        await cql.run_async("DROP ROLE not_existing_role")

    # A role that is not in LDAP can be created and dropped.
    not_in_ldap = unique_name("no_ldap_")
    await cql.run_async(f"CREATE ROLE {not_in_ldap}")
    await cql.run_async(f"DROP ROLE {not_in_ldap}")

    # An open session stops working once its role is dropped.
    await cql.run_async(f"CREATE ROLE {JSMITH} WITH LOGIN = true")
    with session_as(manager, server, JSMITH, JSMITH_PASSWORD) as session:
        session.execute("LIST ROLES")
        await cql.run_async(f"DROP ROLE {JSMITH}")
        with pytest.raises(InvalidRequest, match=f"Role {JSMITH} doesn't exist."):
            session.execute("LIST ROLES")
    assert_login_fails(manager, server, JSMITH, JSMITH_PASSWORD, f"User {JSMITH} doesn't exist")
    # Recreated, it logs in again with its LDAP password.
    await cql.run_async(f"CREATE ROLE {JSMITH} WITH LOGIN = true")
    assert_can_login(manager, server, JSMITH, JSMITH_PASSWORD)

    await cql.run_async(f"CREATE ROLE {JDOE} WITH LOGIN = true AND SUPERUSER = true")
    with session_as(manager, server, JDOE, JDOE_PASSWORD) as jdoe:
        with pytest.raises(InvalidRequest, match="Cannot DROP primary role for current login."):
            jdoe.execute(f"DROP ROLE {JDOE}")
        # Another superuser can drop the default one, although it is still in LDAP.
        jdoe.execute("DROP ROLE cassandra")
        assert_login_fails(manager, server, "cassandra", "cassandra", "User cassandra doesn't exist")
        # Put it back: the harness keeps using it after the test.
        jdoe.execute("CREATE ROLE cassandra WITH SUPERUSER = true AND LOGIN = true")
    assert_can_login(manager, server, "cassandra", "cassandra")


async def test_ldap_group_permissions(manager: ScyllaClusterManager):
    """SaslauthdAuthenticator together with LDAPRoleManager: the user logs in
    with its LDAP password and gets the permissions of the roles named after
    its LDAP groups. A compact version of the permission tests that dtest's
    TestLdapSaslAuth inherits from TestLdap (test_partial_permissions,
    test_grant_role_permissions, test_revoke_role_permissions,
    test_multiple_roles_*)."""
    # jdoe gets role3's permissions through its LDAP group, so a GRANT or REVOKE
    # on role3 doesn't invalidate jdoe's cached permissions: LDAPRoleManager
    # reloads them on a timer, separately on each shard. With several shards,
    # a check that passed on one shard says nothing about the shard serving
    # the next statement. One shard means one cache.
    server = await manager.server_add(cmdline=["--smp", "1"],
                                      config=saslauthd_config(**ldap_role_manager_config()))
    cql = manager.get_cql()
    ks = unique_name("ks_")
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async(f"CREATE TABLE {ks}.t (k int PRIMARY KEY, v int)")
    await cql.run_async(f"CREATE ROLE {JDOE_GROUP}")
    await cql.run_async(f"CREATE ROLE {JDOE} WITH LOGIN = true")
    # jsmith is in role1, not in role3.
    await cql.run_async(f"CREATE ROLE {JSMITH} WITH LOGIN = true")

    select = f"SELECT * FROM {ks}.t"
    insert = f"INSERT INTO {ks}.t (k, v) VALUES (1, 1)"

    async def eventually(allowed: bool, session, statement):
        # Permission changes reach the authorizer's cache asynchronously.
        async def outcome_as_expected():
            try:
                session.execute(statement)
                succeeded = True
            except Unauthorized:
                succeeded = False
            return True if succeeded == allowed else None
        label = f"{statement!r} {'allowed' if allowed else 'refused'}"
        await wait_for(outcome_as_expected, time.time() + 60, label=label)

    with session_as(manager, server, JDOE, JDOE_PASSWORD) as jdoe, \
         session_as(manager, server, JSMITH, JSMITH_PASSWORD) as jsmith:
        await eventually(False, jdoe, select)

        await cql.run_async(f"GRANT SELECT ON KEYSPACE {ks} TO {JDOE_GROUP}")
        await eventually(True, jdoe, select)
        await eventually(False, jdoe, insert)
        await eventually(False, jsmith, select)

        await cql.run_async(f"GRANT MODIFY ON KEYSPACE {ks} TO {JDOE_GROUP}")
        await eventually(True, jdoe, insert)

        await cql.run_async(f"REVOKE SELECT ON KEYSPACE {ks} FROM {JDOE_GROUP}")
        await eventually(False, jdoe, select)
        await eventually(True, jdoe, insert)
