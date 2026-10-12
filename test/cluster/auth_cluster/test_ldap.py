#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Helpers for the LDAP tests (test_ldap_role_manager_users.py): the slapd
# that test.py starts (test/pylib/ldap_server.py), shared by all tests of a
# run, so every test adds its own uniquely named entries and removes them,
# and node configs for LDAPRoleManager and SaslauthdAuthenticator.

import contextlib
import logging
import os

import ldap3
import pytest
from cassandra import AuthenticationFailed
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable

from test.cluster.auth_cluster.test_ldap_service_levels import ldap_config
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.skip_types import skip_env

logger = logging.getLogger(__name__)

BASE_DN = "dc=example,dc=com"
PEOPLE_DN = f"ou=People,{BASE_DN}"

# Permissions are not cached, and LDAP group membership is re-read every
# permissions_update_interval_in_ms, so changes show up quickly.
FAST_AUTH_REFRESH = {"permissions_validity_in_ms": 0, "permissions_update_interval_in_ms": 100}


def user_dn(user):
    return f"uid={user},{PEOPLE_DN}"


class LdapDirectory:
    """Adds entries to the shared slapd and removes them on close()."""

    def __init__(self):
        server = ldap3.Server(f"ldap://{os.environ['SEASTAR_LDAP_HOST']}:{os.environ['SEASTAR_LDAP_PORT']}")
        self.conn = ldap3.Connection(server, f"cn=root,{BASE_DN}", "secret", auto_bind=True)
        self.added = []

    def _check(self, ok, what):
        assert ok, f"LDAP {what} failed: {self.conn.result}"

    def add_group(self, name, members):
        dn = f"cn={name},{BASE_DN}"
        self._check(self.conn.add(dn, ["groupOfUniqueNames"],
                                  {"cn": name, "uniqueMember": [user_dn(m) for m in members]}),
                    f"add {dn}")
        self.added.append(dn)

    def add_member(self, group, user):
        self._check(self.conn.modify(f"cn={group},{BASE_DN}",
                                     {"uniqueMember": [(ldap3.MODIFY_ADD, [user_dn(user)])]}),
                    f"add {user} to {group}")

    def remove_member(self, group, user):
        self._check(self.conn.modify(f"cn={group},{BASE_DN}",
                                     {"uniqueMember": [(ldap3.MODIFY_DELETE, [user_dn(user)])]}),
                    f"remove {user} from {group}")

    def add_user(self, user, password):
        dn = user_dn(user)
        self._check(self.conn.add(dn, ["organizationalPerson", "uidObject"],
                                  {"cn": user, "sn": user, "uid": user, "userPassword": password}),
                    f"add {dn}")
        self.added.append(dn)

    def close(self):
        for dn in reversed(self.added):
            if not self.conn.delete(dn):
                logger.warning("Failed to delete %s: %s", dn, self.conn.result)
        self.conn.unbind()


@pytest.fixture
def ldap():
    if not os.environ.get("SEASTAR_LDAP_HOST"):
        skip_env("needs the LDAP server started by the test.py harness")
    directory = LdapDirectory()
    yield directory
    directory.close()


@contextlib.contextmanager
def session_as(manager: ScyllaClusterManager, server, user, password):
    cluster = manager.con_gen([server.ip_addr], manager.port, manager.use_ssl,
                              PlainTextAuthProvider(username=user, password=password))
    try:
        yield cluster.connect()
    finally:
        safe_driver_shutdown(cluster)


def assert_login_fails(manager, server, user, password):
    with pytest.raises(NoHostAvailable) as e:
        with session_as(manager, server, user, password):
            pass
    assert all(isinstance(err, AuthenticationFailed) for err in e.value.errors.values()), e.value


def saslauthd_config():
    """LDAPRoleManager with SaslauthdAuthenticator: passwords are checked by
    the saslauthd that test.py runs against its slapd."""
    return {**ldap_config(), **FAST_AUTH_REFRESH,
            "authenticator": "com.scylladb.auth.SaslauthdAuthenticator",
            "saslauthd_socket_path": os.environ["SASLAUTHD_MUX_PATH"]}
