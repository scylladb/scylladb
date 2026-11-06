#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Users that get their roles from LDAP groups through LDAPRoleManager, and
# SaslauthdAuthenticator with an invalid saslauthd socket. Ported from
# scylla-dtest ldap_test.py. Like the dtests' node: one node with
# CassandraAuthorizer, permissions_validity_in_ms 0, batch commitlog sync and
# auth-cache debug logging, a keyspace ks with the dtests' table ks.cf, and
# the permissions checked with the dtests' statements. The LDAP server is the
# slapd that test.py runs, shared by the tests of a run, so the roles and
# LDAP groups get unique names; the dtests' "cassandra" group, which gives
# its members the superuser role, is a uniquely named superuser role. At the
# end of each test the node is stopped gracefully, as the dtests' fixture
# does before it stops the LDAP server.

import asyncio
import random
import time

import ldap3
import pytest
from cassandra import Unauthorized
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable

from test.cluster.auth_cluster.test_ldap import (  # noqa: F401 - the ldap fixture
    BASE_DN, assert_login_fails, ldap, ldap_config, saslauthd_config, session_as, user_dn)
from test.cluster.util import stop_servers_gracefully
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name

LDAP_PASSWORD = "cassandra"
NODE_CONFIG = {"authorizer": "org.apache.cassandra.auth.CassandraAuthorizer", "permissions_validity_in_ms": 0,
               "commitlog_sync": "batch"}
CMDLINE = ["--logger-log-level", "auth-cache=debug"]
ALL = ["create", "modify", "select"]


def create_cf(cql, name):
    """The dtests' create_cf(): a (key, c, v) table, read once to check it
    exists."""
    cql.execute(f"CREATE COLUMNFAMILY {name} (key varchar, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment='test cf' "
                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    cql.execute(f"SELECT * FROM {name} LIMIT 1")


async def prepare(manager: ScyllaClusterManager, config, ldap_user=None):
    """One node, the keyspace ks with ks.cf and a row, and, unless
    ldap_user is None, the login role ldap_user with the LDAP password."""
    server = await manager.server_add(config={**config, **NODE_CONFIG, "tablets_mode_for_new_keyspaces": "disabled"}, cmdline=CMDLINE)
    cql = manager.get_cql()
    if ldap_user:
        cql.execute(f"CREATE ROLE '{ldap_user}' WITH login=true AND password='{LDAP_PASSWORD}'")
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    create_cf(cql, "ks.cf")
    cql.execute("INSERT INTO ks.cf (key, c, v) VALUES ('key1', 'c1', 'v1')")
    return server, cql


def superuser_group(cql, ldap, members):
    """An LDAP group named after a superuser role, with the given members."""
    role = unique_name("su_")
    cql.execute(f"CREATE ROLE {role} WITH SUPERUSER = true")
    ldap.add_group(role, members)
    return role


def eventually_unauthorized(fun, user, permission, timeout=60):
    deadline = time.time() + timeout
    while True:
        try:
            fun()
        except Unauthorized as e:
            assert f"User {user} has no {permission.upper()} permission on " in str(e), e
            return
        assert time.time() < deadline, f"{user} still has {permission}"
        time.sleep(0.5)


def connect_patiently(manager, server, user, password, timeout=60):
    """A session of the user, retried until the login succeeds; the caller
    shuts down session.cluster."""
    deadline = time.time() + timeout
    while True:
        cluster = manager.con_gen([server.ip_addr], manager.port, manager.use_ssl,
                                  PlainTextAuthProvider(username=user, password=password))
        try:
            return cluster.connect()
        except NoHostAvailable:
            safe_driver_shutdown(cluster)
            if time.time() >= deadline:
                raise
            time.sleep(0.5)


def wait_login_fails(manager, server, user, password, timeout=60):
    """Waits until the node accepts connections again, then checks that the
    login fails with bad credentials."""
    deadline = time.time() + timeout
    while True:
        try:
            assert_login_fails(manager, server, user, password)
            return
        except AssertionError:
            if time.time() >= deadline:
                raise
            time.sleep(0.5)


def check_user_permissions(manager, server, user, password, permissions):
    """The dtests' check_user_permissions(): the user may CREATE, MODIFY and
    SELECT exactly as given; the Unauthorized checks are retried, as the
    role cache is eventually consistent."""
    session = connect_patiently(manager, server, user, password)

    def try_create_ks():
        session.execute(f"CREATE KEYSPACE {unique_name('ks_')} WITH replication="
                        "{'class':'NetworkTopologyStrategy', 'replication_factor':1}")

    def try_create_cf():
        create_cf(session, f"ks.{unique_name('table_')}")

    if "create" not in permissions:
        eventually_unauthorized(try_create_ks, user, "create")
        eventually_unauthorized(try_create_cf, user, "create")
    else:
        name = unique_name("ks_")
        session.execute(f"CREATE KEYSPACE {name} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':1}}")
        create_cf(session, f"{name}.{unique_name('table_')}")

    modifications = ["INSERT INTO ks.cf (key, c, v) VALUES ('key', 'c', 'v')",
                     "UPDATE ks.cf SET v = 'vv' WHERE key = 'key' and c = 'c'",
                     "DELETE from ks.cf WHERE key = 'key' and c = 'c'"]
    for stmt in modifications:
        if "modify" not in permissions:
            eventually_unauthorized(lambda: session.execute(stmt), user, "modify")
        else:
            session.execute(stmt)

    if "select" not in permissions:
        eventually_unauthorized(lambda: session.execute("SELECT * from ks.cf LIMIT 1"), user, "select")
    else:
        session.execute("SELECT * from ks.cf LIMIT 1")
    safe_driver_shutdown(session.cluster)


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_wrong_user(manager: ScyllaClusterManager, ldap):
    """A user that exists neither in Scylla nor in LDAP cannot log in."""
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config(), user)
    superuser_group(cql, ldap, [user, "qa-user", "cassandra"])
    with pytest.raises(NoHostAvailable):
        with session_as(manager, server, unique_name("abcd_"), LDAP_PASSWORD):
            pass


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_hard_restart_scylla(manager: ScyllaClusterManager, ldap):
    """A user's LDAP group roles still apply after the node is killed and
    restarted, replaying its commitlog."""
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config(), user)
    superuser_group(cql, ldap, [user, "qa-user", "cassandra"])
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)
    await manager.server_stop(server.server_id, convict=False)
    await manager.server_start(server.server_id)
    await manager.driver_connect()
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_multiple_users_superuser_role(manager: ScyllaClusterManager, ldap):
    """Each of ten members of the superuser group has all permissions."""
    server, cql = await prepare(manager, ldap_config(), unique_name("scylla-qa-"))
    users = [unique_name(f"user_{i}_") for i in range(10)]
    superuser_group(cql, ldap, users)
    for user in users:
        cql.execute(f"CREATE ROLE '{user}' WITH login=true AND password='{LDAP_PASSWORD}'")
        check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_multiple_users_with_modify_role(manager: ScyllaClusterManager, ldap):
    """Each of ten members of a group named after a role with MODIFY on all
    keyspaces has only that permission."""
    server, _ = await prepare(manager, ldap_config(), unique_name("scylla-qa-"))
    # The dtest's new session of cassandra (patient_cql_connection()): its
    # first statements, CREATE ROLE and GRANT, run on a fresh connection.
    cql = connect_patiently(manager, server, "cassandra", "cassandra")
    try:
        role = unique_name("modify_role_")
        cql.execute(f"CREATE ROLE '{role}'")
        cql.execute(f"GRANT modify ON ALL KEYSPACES TO '{role}'")
        users = [unique_name(f"user_{i}_") for i in range(10)]
        ldap.add_group(role, users)
        for user in users:
            cql.execute(f"CREATE ROLE '{user}' WITH login=true AND password='{LDAP_PASSWORD}'")
            check_user_permissions(manager, server, user, LDAP_PASSWORD, ["modify"])
    finally:
        safe_driver_shutdown(cql.cluster)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_modify_username_on_ldap(manager: ScyllaClusterManager, ldap):
    """Replacing the superuser group's members with another user takes the
    permissions away from the old members and gives them to the new one."""
    user, new_user = unique_name("scylla-qa-"), unique_name("qa-user-")
    server, cql = await prepare(manager, ldap_config(), user)
    group = superuser_group(cql, ldap, [user, "qa-user", "cassandra"])
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)
    assert ldap.conn.modify(f"cn={group},{BASE_DN}", {"uniqueMember": [(ldap3.MODIFY_REPLACE, [user_dn(new_user)])]}), ldap.conn.result
    try:
        check_user_permissions(manager, server, user, LDAP_PASSWORD, [])
    except Unauthorized:
        pass
    cql.execute(f"CREATE ROLE '{new_user}' WITH login=true AND password='{LDAP_PASSWORD}'")
    check_user_permissions(manager, server, new_user, LDAP_PASSWORD, ALL)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_invalid_saslauthd_socket(manager: ScyllaClusterManager, ldap):
    """
    With SaslauthdAuthenticator, a user with an LDAP password logs in; after
    saslauthd_socket_path is set to a path that is not saslauthd's socket
    and the node restarts, logins fail with bad credentials. Like the dtest,
    which sets "/tmp/" in each of its five rounds, the node restarts five
    times.
    """
    user = unique_name("scylla-qa-")
    server = await manager.server_add(config={**saslauthd_config(), **NODE_CONFIG, "tablets_mode_for_new_keyspaces": "disabled"}, cmdline=CMDLINE)
    cql = manager.get_cql()
    cql.execute(f"CREATE ROLE '{user}' WITH login=true")
    ldap.add_user(user, LDAP_PASSWORD)
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    create_cf(cql, "ks.cf")
    cql.execute("INSERT INTO ks.cf (key, c, v) VALUES ('key1', 'c1', 'v1')")
    safe_driver_shutdown(connect_patiently(manager, server, user, LDAP_PASSWORD).cluster)

    for _ in range(5):
        await manager.server_stop_gracefully(server.server_id)
        await manager.server_update_config(server.server_id, "saslauthd_socket_path", "/tmp/", reload=False)
        await manager.server_start(server.server_id, connect_driver=False)
        wait_login_fails(manager, server, user, LDAP_PASSWORD)


def create_role(cql, user, password):
    """TestLdap.create_role() with PasswordAuthenticator."""
    cql.execute(f"CREATE ROLE '{user}' WITH login=true AND password='{password}'")


def create_role_grant_permission(cql, role, permissions, resource):
    """The dtests' create_role_grant_permission()."""
    cql.execute(f"CREATE ROLE '{role}'")
    for permission in permissions:
        cql.execute(f"GRANT {permission} ON {resource} TO '{role}'")


def default_members(user):
    """The dtests' add_role_to_ldap() default members."""
    return [user, "qa-user", "cassandra"]


# Ported from scylla-dtest ldap_test.py::TestLdap::test_simple_ldap_connection.
# A member of the superuser LDAP group may create, modify and select.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_simple_ldap_connection(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config(), user)
    superuser_group(cql, ldap, default_members(user))
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)


# Ported from scylla-dtest ldap_test.py::TestLdap::test_user_login_only.
# A role created by an LDAP superuser, in no LDAP group, may only log in.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_user_login_only(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config(), user)
    superuser_group(cql, ldap, default_members(user))
    login_user = unique_name("login_user_")
    session = connect_patiently(manager, server, user, LDAP_PASSWORD)
    try:
        create_role(session, login_user, "test")
    finally:
        safe_driver_shutdown(session.cluster)
    check_user_permissions(manager, server, login_user, "test", [])


# Ported from scylla-dtest ldap_test.py::TestLdap::test_partial_permissions.
# Six users, each the only member of the LDAP group of a role with some of
# CREATE, MODIFY and SELECT on all keyspaces, have exactly those.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_partial_permissions(manager: ScyllaClusterManager, ldap):
    server, cql = await prepare(manager, ldap_config(), unique_name("scylla-qa-"))
    for permissions in [["create"], ["select"], ["modify"], ["create", "select"], ["modify", "select"], ["create", "modify", "select"]]:
        name = "_".join(permissions)
        user, role = unique_name(f"{name}_user_"), unique_name(f"{name}_role_")
        create_role_grant_permission(cql, role, permissions, "all keyspaces")
        create_role(cql, user, user)
        ldap.add_group(role, [user])
        check_user_permissions(manager, server, user, user, permissions)


# Ported from scylla-dtest ldap_test.py::TestLdap::test_soft_restart_scylla.
# A user's LDAP group roles still apply after the node is stopped gracefully
# and started again.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_soft_restart_scylla(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config(), user)
    superuser_group(cql, ldap, default_members(user))
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_start(server.server_id)
    await manager.driver_connect()
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)


# Ported from scylla-dtest ldap_test.py::TestLdap::test_multiple_roles_superuser.
# The user is added to the LDAP groups of five roles with all permissions on
# all keyspaces, one at a time, and then to the superuser group; it has all
# permissions after each.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_multiple_roles_superuser(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config(), user)
    for i in range(1, 6):
        role = unique_name(f"r{i}_")
        create_role_grant_permission(cql, role, ALL, "ALL KEYSPACES")
        ldap.add_group(role, default_members(user))
        check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)
    superuser_group(cql, ldap, default_members(user))
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)


# Ported from scylla-dtest ldap_test.py::TestLdap::test_multiple_roles_single_permission.
# The user is added to the LDAP groups of five roles with the same, randomly
# chosen permission; it has only that one.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_multiple_roles_single_permission(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config())
    permission = random.choice(ALL)
    create_role(cql, user, LDAP_PASSWORD)
    for i in range(1, 6):
        role = unique_name(f"r{i}_")
        create_role_grant_permission(cql, role, [permission], "ALL KEYSPACES")
        ldap.add_group(role, default_members(user))
        check_user_permissions(manager, server, user, LDAP_PASSWORD, [permission])


# Ported from scylla-dtest ldap_test.py::TestLdap::test_multiple_roles_permissions_combination.
# Six roles, each with one of CREATE, MODIFY and SELECT, all with the user
# in their LDAP groups: the user has all three.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_multiple_roles_permissions_combination(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config())
    create_role(cql, user, LDAP_PASSWORD)
    for i, action in zip(range(1, 7), ALL * 2):
        role = unique_name(f"r{i}_")
        create_role_grant_permission(cql, role, [action], "ALL KEYSPACES")
        ldap.add_group(role, default_members(user))
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)


# Ported from scylla-dtest ldap_test.py::TestLdap::test_add_ldap_after_regular_work.
# A node first works without LDAP: a role with SELECT and MODIFY granted to
# the user with CQL, and 100 rows. After LDAPRoleManager is configured and
# the node restarts gracefully, the user's membership in the superuser LDAP
# group gives it all permissions.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_add_ldap_after_regular_work(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    ldap_options = ldap_config()
    server, cql = await prepare(manager, {k: v for k, v in ldap_options.items() if not k.startswith(("role_manager", "ldap_"))})
    cql.execute(f"create role '{user}' with login=true and password='{LDAP_PASSWORD}'")
    role = unique_name("select_modify_")
    create_role_grant_permission(cql, role, ["select", "modify"], "ALL KEYSPACES")
    cql.execute(f"grant '{role}' to '{user}'")
    for i in range(100):
        cql.execute(f"INSERT INTO ks.cf (key, c, v) VALUES ('key{i}', 'c{i}', 'v{i}')")
    await manager.server_update_config(server.server_id, config_options=ldap_options, reload=False)
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_start(server.server_id)
    await manager.driver_connect()
    superuser_group(manager.get_cql(), ldap, default_members(user))
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)


# Ported from scylla-dtest ldap_test.py::TestLdap::test_grant_role_permissions.
# Permissions granted one by one to the role of the user's LDAP group apply
# to the user.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_grant_role_permissions(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config(), user)
    role = unique_name("test_role_")
    permissions = ["create"]
    create_role_grant_permission(cql, role, permissions, "all keyspaces")
    ldap.add_group(role, [user])
    check_user_permissions(manager, server, user, LDAP_PASSWORD, permissions)
    for permission in ["modify", "select"]:
        cql.execute(f"GRANT {permission} ON all keyspaces TO '{role}'")
        permissions.append(permission)
        check_user_permissions(manager, server, user, LDAP_PASSWORD, permissions)


# Ported from scylla-dtest ldap_test.py::TestLdap::test_revoke_role_permissions.
# Permissions revoked one by one from the role of the user's LDAP group no
# longer apply to the user.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_revoke_role_permissions(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config(), user)
    role = unique_name("test_role_")
    permissions = list(ALL)
    create_role_grant_permission(cql, role, permissions, "all keyspaces")
    ldap.add_group(role, [user])
    check_user_permissions(manager, server, user, LDAP_PASSWORD, permissions)
    for _ in range(2):
        revoked = permissions.pop(-1)
        cql.execute(f"REVOKE {revoked} ON all keyspaces FROM '{role}'")
        check_user_permissions(manager, server, user, LDAP_PASSWORD, permissions)


# Ported from scylla-dtest ldap_test.py::TestLdap::test_remove_user_from_ldap.
# Removed from the superuser LDAP group, the user loses all permissions.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_remove_user_from_ldap(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config(), user)
    group = superuser_group(cql, ldap, default_members(user))
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)
    ldap.remove_member(group, user)
    check_user_permissions(manager, server, user, LDAP_PASSWORD, [])


# Ported from scylla-dtest ldap_test.py::TestLdap::test_add_user_to_ldap.
# A user added to the superuser LDAP group before its role exists gets all
# permissions once the role is created; the old member keeps them.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_add_user_to_ldap(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    server, cql = await prepare(manager, ldap_config(), user)
    group = superuser_group(cql, ldap, default_members(user))
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)
    new_user = unique_name("qa-superuser-")
    ldap.add_member(group, new_user)
    check_user_permissions(manager, server, user, LDAP_PASSWORD, ALL)
    create_role(cql, new_user, LDAP_PASSWORD)
    check_user_permissions(manager, server, new_user, LDAP_PASSWORD, ALL)


def assert_bad_credentials(manager, server, user, password):
    """The dtests' pytest.raises(NoHostAvailable, match="Bad credentials")."""
    with pytest.raises(NoHostAvailable, match="Bad credentials"):
        with session_as(manager, server, user, password):
            pass


# Ported from scylla-dtest ldap_test.py::TestLdapSaslAuth::test_authentication.
# With SaslauthdAuthenticator a role logs in only with the password of its
# LDAP entry: not before the entry exists, nor with a wrong password.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_authentication(manager: ScyllaClusterManager, ldap):
    server = await manager.server_add(config={**saslauthd_config(), **NODE_CONFIG, "tablets_mode_for_new_keyspaces": "disabled"}, cmdline=CMDLINE)
    # The dtest's role is the default superuser "cassandra", absent from its
    # LDAP server at first; test.py's shared slapd has a cassandra entry, so a
    # uniquely named login role stands in for it until its entry is added.
    user = unique_name("cassandra-")
    manager.get_cql().execute(f"CREATE ROLE '{user}' WITH login=true AND superuser=true")
    assert_bad_credentials(manager, server, user, "cassandra")
    ldap.add_user(user, "cassandra")
    safe_driver_shutdown(connect_patiently(manager, server, user, "cassandra").cluster)
    safe_driver_shutdown(connect_patiently(manager, server, "cassandra", "cassandra").cluster)
    assert_bad_credentials(manager, server, "cassandra", "wrong-password")


# Ported from scylla-dtest ldap_test.py::TestLdapSaslAuth::test_switch_with_password_auth.
# On two nodes, a role created under SaslauthdAuthenticator has only an LDAP
# password: Scylla refuses to set one. After the cluster switches to
# PasswordAuthenticator the role cannot log in until it gets a Scylla
# password; after switching back, its LDAP password works again.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_switch_with_password_auth(manager: ScyllaClusterManager, ldap):
    user = unique_name("scylla-qa-")
    config = {**saslauthd_config(), **NODE_CONFIG, "tablets_mode_for_new_keyspaces": "disabled"}
    servers = await manager.servers_add(2, config=config, cmdline=CMDLINE)
    node1 = servers[0]
    ldap.add_user(user, LDAP_PASSWORD)
    cql = manager.get_cql()
    cql.execute(f"CREATE ROLE '{user}' WITH login=true")
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    create_cf(cql, "ks.cf")
    cql.execute("INSERT INTO ks.cf (key, c, v) VALUES ('key1', 'c1', 'v1')")
    session = connect_patiently(manager, node1, "cassandra", "cassandra")
    safe_driver_shutdown(connect_patiently(manager, node1, user, LDAP_PASSWORD).cluster)
    with pytest.raises(Exception, match="Cannot modify passwords with SaslauthdAuthenticator"):
        session.execute(f"ALTER ROLE '{user}' WITH PASSWORD = 'new_password'")
    safe_driver_shutdown(session.cluster)

    async def restart_cluster_with(authenticator):
        for s in servers:
            await manager.server_update_config(s.server_id, "authenticator", authenticator, reload=False)
        await asyncio.gather(*(manager.server_stop_gracefully(s.server_id) for s in servers))
        await asyncio.gather(*(manager.server_start(s.server_id) for s in servers))
        await manager.driver_connect()

    await restart_cluster_with("org.apache.cassandra.auth.PasswordAuthenticator")
    session = connect_patiently(manager, node1, "cassandra", "cassandra")
    assert_bad_credentials(manager, node1, user, "")
    assert_bad_credentials(manager, node1, user, LDAP_PASSWORD)
    session.execute(f"ALTER ROLE '{user}' WITH PASSWORD = '{LDAP_PASSWORD}-new'")
    safe_driver_shutdown(session.cluster)
    safe_driver_shutdown(connect_patiently(manager, node1, user, f"{LDAP_PASSWORD}-new").cluster)

    await restart_cluster_with("com.scylladb.auth.SaslauthdAuthenticator")
    safe_driver_shutdown(connect_patiently(manager, node1, user, LDAP_PASSWORD).cluster)
