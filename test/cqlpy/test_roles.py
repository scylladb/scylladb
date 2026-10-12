# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for managing roles: CREATE, ALTER and DROP ROLE, GRANT and REVOKE of
# roles, LIST ROLES, and which roles may log in.
# Permissions granted on resources are tested in test_permissions.py.
#############################################################################

import time
import pytest
from contextlib import contextmanager
from cassandra import AuthenticationFailed
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import InvalidRequest, Unauthorized
from .util import unique_name, new_user, new_session, cql_session, is_scylla
from .test_permissions import eventually_authorized, eventually_unauthorized

# Create a role with a unique name and the given options (e.g.,
# "LOGIN = true"), and drop it at the end if the test did not drop it.
@contextmanager
def new_role(cql, options=''):
    name = unique_name()
    cql.execute(f"CREATE ROLE {name}" + (f" WITH {options}" if options else ""))
    try:
        yield name
    finally:
        cql.execute(f"DROP ROLE IF EXISTS {name}")

# Run a LIST ROLES statement and return the listed roles as a dict mapping the
# role name to its (superuser, login) flags. If "only" is given, roles not in
# it are ignored - the other tests sharing the server create their own roles.
def list_roles(cql, stmt, only=None):
    return {r.role: (r.super, r.login) for r in cql.execute(stmt) if only is None or r.role in only}

@contextmanager
def login(cql, username, password):
    endpoint = cql.hosts[0].endpoint
    with cql_session(host=endpoint.address, port=endpoint.port, is_ssl=(cql.cluster.ssl_context is not None), username=username, password=password) as session:
        yield session

def can_login(cql, username, password):
    try:
        with login(cql, username, password):
            return True
    except NoHostAvailable as e:
        # Usually e.errors maps hosts to their errors, but while Cassandra's
        # caches catch up the driver may fail with just a list of hosts.
        if isinstance(e.errors, dict):
            assert all(isinstance(err, AuthenticationFailed) for err in e.errors.values()), e
        return False

# Cassandra caches roles and credentials for a couple of seconds, so a change
# to a role (ALTER ROLE, GRANT, ...) may take a while to affect logging in.
def eventually_can_login(cql, username, password, expected=True, timeout_s=10):
    deadline = time.time() + timeout_s
    while can_login(cql, username, password) != expected:
        assert time.time() < deadline, f"login of {username} is still {'refused' if expected else 'allowed'}"
        time.sleep(0.1)

# Test that CREATE ROLE and DROP ROLE add and remove the role in LIST ROLES.
# A role created without options is neither a superuser nor allowed to log in.
def test_create_drop_role(cql):
    role = unique_name()
    cql.execute(f"CREATE ROLE {role}")
    try:
        assert list_roles(cql, "LIST ROLES", [role]) == {role: (False, False)}
    finally:
        cql.execute(f"DROP ROLE {role}")
    assert list_roles(cql, "LIST ROLES", [role]) == {}

# Test that CREATE ROLE IF NOT EXISTS and DROP ROLE IF EXISTS are idempotent.
def test_conditional_create_drop_role(cql):
    role = unique_name()
    try:
        cql.execute(f"CREATE ROLE IF NOT EXISTS {role}")
        cql.execute(f"CREATE ROLE IF NOT EXISTS {role}")
        assert list_roles(cql, "LIST ROLES", [role]) == {role: (False, False)}
    finally:
        cql.execute(f"DROP ROLE IF EXISTS {role}")
        cql.execute(f"DROP ROLE IF EXISTS {role}")
    assert list_roles(cql, "LIST ROLES", [role]) == {}

# Test that CREATE ROLE of an existing role and DROP ROLE of a missing one
# fail (without IF [NOT] EXISTS).
def test_create_drop_role_errors(cql):
    with new_role(cql) as role:
        with pytest.raises(InvalidRequest, match=f"{role} already exists"):
            cql.execute(f"CREATE ROLE {role}")
        cql.execute(f"DROP ROLE {role}")
        with pytest.raises(InvalidRequest, match=f"{role} doesn't exist"):
            cql.execute(f"DROP ROLE {role}")

# Test that the permissions on ALL ROLES of a role (here "admin") are
# inherited by the roles it is granted to: they can create, alter and drop
# other roles, but only a superuser can change superuser status, and the
# permissions are gone after the admin role is revoked. Also check that a
# role created with LOGIN = false cannot log in (#4284) until it is altered.
def test_role_admin_permissions_are_inherited(cql):
    with new_role(cql, "SUPERUSER = false AND LOGIN = false") as admin, new_user(cql) as mike:
        cql.execute(f"GRANT ALL ON ALL ROLES TO {admin}")
        cql.execute(f"GRANT {admin} TO {mike}")
        with new_session(cql, mike) as mike_session:
            role1 = unique_name()
            eventually_authorized(lambda: mike_session.execute(f"CREATE ROLE IF NOT EXISTS {role1} WITH PASSWORD = '11111' AND LOGIN = false"))
            try:
                assert not can_login(cql, role1, '11111')
                mike_session.execute(f"ALTER ROLE {role1} WITH LOGIN = true")
                mike_session.execute(f"ALTER ROLE {role1} WITH PASSWORD = '22222'")
                eventually_can_login(cql, role1, '22222')

                # only superusers can set superuser status (Cassandra words
                # the error differently for a role granted to the user)
                for role in [role1, mike]:
                    with pytest.raises(Unauthorized, match="superuser status"):
                        mike_session.execute(f"ALTER ROLE {role} WITH SUPERUSER = true")

                # a role without permissions on roles can only alter itself
                with login(cql, role1, '22222') as role1_session:
                    with pytest.raises(Unauthorized):
                        role1_session.execute(f"CREATE ROLE {unique_name()} WITH LOGIN = false")
                    with pytest.raises(Unauthorized):
                        role1_session.execute(f"ALTER ROLE {mike} WITH LOGIN = false")
                    with pytest.raises(Unauthorized):
                        role1_session.execute(f"DROP ROLE {mike}")
                    role1_session.execute(f"ALTER ROLE {role1} WITH PASSWORD = '33333'")

                mike_session.execute(f"DROP ROLE {role1}")
                assert list_roles(cql, "LIST ROLES", [role1]) == {}
            finally:
                cql.execute(f"DROP ROLE IF EXISTS {role1}")

            # revoking the admin role takes away its permissions
            cql.execute(f"REVOKE {admin} FROM {mike}")
            role3 = unique_name()
            try:
                eventually_unauthorized(lambda: mike_session.execute(f"CREATE ROLE IF NOT EXISTS {role3} WITH LOGIN = false"))
            finally:
                cql.execute(f"DROP ROLE IF EXISTS {role3}")

# Test that a non-superuser with CREATE, DROP and AUTHORIZE on ALL ROLES can
# grant a superuser role to another role and revoke it, and drop that role,
# but cannot create a superuser role.
def test_grant_revoke_superuser_role_as_non_superuser(cql):
    with new_role(cql, "SUPERUSER = true AND LOGIN = false") as another_superuser, \
         new_role(cql, "SUPERUSER = false AND LOGIN = false") as non_superuser, \
         new_user(cql) as mike:
        for permission in ['CREATE', 'DROP', 'AUTHORIZE']:
            cql.execute(f"GRANT {permission} ON ALL ROLES TO {mike}")
        with new_session(cql, mike) as mike_session:
            role1 = unique_name()
            mike_session.execute(f"CREATE ROLE {role1} WITH SUPERUSER = false")
            try:
                mike_session.execute(f"GRANT {non_superuser} TO {role1}")
                mike_session.execute(f"GRANT {another_superuser} TO {role1}")
                with pytest.raises(Unauthorized, match="Only superusers can create a role with superuser status"):
                    mike_session.execute(f"CREATE ROLE {unique_name()} WITH SUPERUSER = true")
                assert list_roles(cql, f"LIST ROLES OF {role1}") == {
                    another_superuser: (True, False), non_superuser: (False, False), role1: (False, False)}

                mike_session.execute(f"REVOKE {another_superuser} FROM {role1}")
                mike_session.execute(f"REVOKE {non_superuser} FROM {role1}")
                assert list_roles(cql, f"LIST ROLES OF {role1}") == {role1: (False, False)}
                mike_session.execute(f"DROP ROLE {non_superuser}")
                mike_session.execute(f"DROP ROLE {role1}")
                assert list_roles(cql, "LIST ROLES", [role1, non_superuser]) == {}
            finally:
                cql.execute(f"DROP ROLE IF EXISTS {role1}")

# Test the errors of GRANT and REVOKE of roles that do not exist. When neither
# role exists, Scylla reports the grantee, Cassandra the granted role.
def test_grant_revoke_role_errors(cql):
    with new_role(cql) as role1:
        missing1 = unique_name()
        missing2 = unique_name()
        with pytest.raises(InvalidRequest, match=f"{missing1} doesn't exist"):
            cql.execute(f"GRANT {missing1} TO {role1}")
        with pytest.raises(InvalidRequest, match=f"{missing1} doesn't exist"):
            cql.execute(f"GRANT {role1} TO {missing1}")
        with pytest.raises(InvalidRequest, match=f"{missing1 if is_scylla(cql) else missing2} doesn't exist"):
            cql.execute(f"GRANT {missing2} TO {missing1}")
        with pytest.raises(InvalidRequest, match=f"{missing1} doesn't exist"):
            cql.execute(f"REVOKE {missing1} FROM {role1}")
        with pytest.raises(InvalidRequest, match=f"{missing1} doesn't exist"):
            cql.execute(f"REVOKE {role1} FROM {missing1}")

# Test LIST ROLES, LIST ROLES OF and LIST ROLES OF ... NORECURSIVE, as a
# superuser and as a user who sees only the roles granted to it unless it has
# DESCRIBE on ALL ROLES. Also check that REVOKE and DROP ROLE remove the
# role from the lists.
def test_list_roles(cql):
    with new_user(cql) as mike, new_role(cql) as role1, new_role(cql) as role2:
        all_roles = [mike, role1, role2]
        assert list_roles(cql, "LIST ROLES", all_roles) == {mike: (False, True), role1: (False, False), role2: (False, False)}

        cql.execute(f"GRANT {role1} TO {role2}")
        cql.execute(f"GRANT {role2} TO {mike}")
        assert list_roles(cql, f"LIST ROLES OF {role2}") == {role1: (False, False), role2: (False, False)}
        assert list_roles(cql, f"LIST ROLES OF {mike}") == {mike: (False, True), role1: (False, False), role2: (False, False)}
        assert list_roles(cql, f"LIST ROLES OF {mike} NORECURSIVE") == {mike: (False, True), role2: (False, False)}

        with new_session(cql, mike) as mike_session:
            with pytest.raises(Unauthorized):
                mike_session.execute("LIST ROLES OF cassandra")
            # without DESCRIBE on ALL ROLES, LIST ROLES shows only the roles
            # granted to the user
            assert list_roles(mike_session, "LIST ROLES") == {mike: (False, True), role1: (False, False), role2: (False, False)}
            assert list_roles(mike_session, f"LIST ROLES OF {mike}") == {mike: (False, True), role1: (False, False), role2: (False, False)}
            assert list_roles(mike_session, f"LIST ROLES OF {mike} NORECURSIVE") == {mike: (False, True), role2: (False, False)}
            assert list_roles(mike_session, f"LIST ROLES OF {role2}") == {role1: (False, False), role2: (False, False)}
            cql.execute(f"GRANT DESCRIBE ON ALL ROLES TO {mike}")
            eventually_authorized(lambda: mike_session.execute("LIST ROLES OF cassandra"))
            assert list_roles(mike_session, "LIST ROLES", all_roles + ['cassandra']) == {
                'cassandra': (True, True), mike: (False, True), role1: (False, False), role2: (False, False)}

        cql.execute(f"REVOKE {role1} FROM {role2}")
        assert list_roles(cql, f"LIST ROLES OF {role2}") == {role2: (False, False)}
        assert list_roles(cql, f"LIST ROLES OF {mike}") == {mike: (False, True), role2: (False, False)}
        cql.execute(f"REVOKE {role2} FROM {mike}")
        assert list_roles(cql, f"LIST ROLES OF {mike}") == {mike: (False, True)}
        # dropping a role removes it from the roles of its grantees
        cql.execute(f"GRANT {role2} TO {mike}")
        cql.execute(f"DROP ROLE {role2}")
        assert list_roles(cql, f"LIST ROLES OF {mike}") == {mike: (False, True)}

# Test that a GRANT which would create a cycle of roles is refused.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_prevent_circular_grants(cql):
    with new_role(cql) as mike, new_role(cql) as role1, new_role(cql) as role2:
        cql.execute(f"GRANT {role2} TO {role1}")
        cql.execute(f"GRANT {role1} TO {mike}")
        with pytest.raises(InvalidRequest, match=f"{mike} (already includes role|is a member of) {role1}"):
            cql.execute(f"GRANT {mike} TO {role1}")
        with pytest.raises(InvalidRequest, match=f"{mike} (already includes role|is a member of) {role2}"):
            cql.execute(f"GRANT {mike} TO {role2}")

# Test that the legacy CREATE USER creates a role that can log in, and its
# [NO]SUPERUSER option sets the superuser flag.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_create_user_as_alias_for_create_role(cql):
    for option, superuser in [("NOSUPERUSER", False), ("SUPERUSER", True)]:
        user = unique_name()
        cql.execute(f"CREATE USER {user} WITH PASSWORD '12345' {option}")
        try:
            assert list_roles(cql, f"LIST ROLES OF {user}") == {user: (superuser, True)}
        finally:
            cql.execute(f"DROP ROLE {user}")

# Test the case of role names (CASSANDRA-10394): CREATE ROLE lowercases
# unquoted names, including unreserved keywords, and keeps the case of quoted
# names and string literals. The legacy CREATE USER keeps the case even of
# unquoted names. Check this by logging in with each spelling.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_role_name_case(cql):
    def check_name(create, name, wrong_name):
        cql.execute(f"{create} WITH PASSWORD = '12345' AND LOGIN = true" if create.startswith("CREATE ROLE")
                    else f"{create} WITH PASSWORD '12345'")
        try:
            assert can_login(cql, name, '12345')
            assert not can_login(cql, wrong_name, '12345')
        finally:
            cql.execute(f"DROP ROLE IF EXISTS \"{name}\"")
    name = unique_name()
    upper = name.upper()
    mixed = name.capitalize()
    check_name(f"CREATE ROLE {upper}", name, upper)
    check_name(f"CREATE ROLE '{upper}'", upper, mixed)
    check_name(f"CREATE ROLE \"{upper}\"", upper, mixed)
    check_name(f"CREATE USER {upper}", upper, mixed)
    check_name(f"CREATE USER '{upper}'", upper, mixed)
    # "count" is an unreserved keyword, allowed as an unquoted role name
    cql.execute("DROP ROLE IF EXISTS count")
    check_name("CREATE ROLE COUNT", "count", "COUNT")


# Test that a role granted a superuser role is a superuser too: it can create
# roles without having been granted CREATE.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_superuser_status_is_inherited(cql):
    with new_user(cql) as mike, new_role(cql, "SUPERUSER = true") as db_admin:
        with new_session(cql, mike) as mike_session:
            another_role = unique_name()
            create = f"CREATE ROLE {another_role} WITH SUPERUSER = false AND LOGIN = false"
            with pytest.raises(Unauthorized):
                mike_session.execute(create)
            cql.execute(f"GRANT {db_admin} TO {mike}")
            try:
                eventually_authorized(lambda: mike_session.execute(create))
                assert list_roles(mike_session, "LIST ROLES", [another_role, db_admin, mike]) == {
                    another_role: (False, False), db_admin: (True, False), mike: (False, True)}
            finally:
                cql.execute(f"DROP ROLE IF EXISTS {another_role}")

# Test that LIST USERS shows a user that inherits superuser status from a
# granted role as a superuser.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_list_users_considers_inherited_superuser_status(cql):
    with new_role(cql, "SUPERUSER = true") as db_admin, new_user(cql) as mike:
        cql.execute(f"GRANT {db_admin} TO {mike}")
        assert {r.name: r.super for r in cql.execute("LIST USERS") if r.name in [mike, db_admin]} == {mike: True}

