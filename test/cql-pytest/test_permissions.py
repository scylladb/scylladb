# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for managing permissions
#############################################################################

import pytest
import time
import rest_api
from cassandra.protocol import SyntaxException, InvalidRequest, Unauthorized
from util import new_test_table, new_user, new_session, new_test_keyspace, unique_name

# Test that granting permissions to various resources works for the default user.
# This case does not include functions, because due to differences in implementation
# the tests will diverge between Scylla and Cassandra (e.g. there's no common language)
# to create a user-defined function in.
# Marked cassandra_bug, because Cassandra allows granting a DESCRIBE permission
# to a specific ROLE, in contradiction to its own documentation:
# https://cassandra.apache.org/doc/latest/cassandra/cql/security.html#cql-permissions
def test_grant_applicable_data_and_role_permissions(cql, test_keyspace, cassandra_bug):
    schema = "a int primary key"
    user = "cassandra"
    with new_test_table(cql, test_keyspace, schema) as table:
        # EXECUTE is not listed, as it only applies to functions, which aren't covered in this test case
        all_permissions = set(['create', 'alter', 'drop', 'select', 'modify', 'authorize', 'describe'])
        applicable_permissions = {
            'all keyspaces': ['create', 'alter', 'drop', 'select', 'modify', 'authorize'],
            f'keyspace {test_keyspace}': ['create', 'alter', 'drop', 'select', 'modify', 'authorize'],
            f'table {table}': ['alter', 'drop', 'select', 'modify', 'authorize'],
            'all roles': ['create', 'alter', 'drop', 'authorize', 'describe'],
            f'role {user}': ['alter', 'drop', 'authorize'],
        }
        for resource, permissions in applicable_permissions.items():
            # Applicable permissions can be legally granted
            for permission in permissions:
                cql.execute(f"GRANT {permission} ON {resource} TO {user}")
            # Only applicable permissions can be granted - nonsensical combinations
            # are refused with an error
            for permission in all_permissions.difference(set(permissions)):
                with pytest.raises((InvalidRequest, SyntaxException), match="support.*permissions"):
                    cql.execute(f"GRANT {permission} ON {resource} TO {user}")


def eventually_authorized(fun, timeout_s=10):
    for i in range(timeout_s * 10):
        try:
            return fun()
        except Unauthorized as e:
            time.sleep(0.1)
    return fun()

def eventually_unauthorized(fun, timeout_s=10):
    for i in range(timeout_s * 10):
        try:
            fun()
            time.sleep(0.1)
        except Unauthorized as e:
            return
    try:
        fun()
        pytest.fail(f"Function {fun} was not refused as unauthorized")
    except Unauthorized as e:
        return

def grant(cql, permission, resource, username):
    cql.execute(f"GRANT {permission} ON {resource} TO {username}")

def revoke(cql, permission, resource, username):
    cql.execute(f"REVOKE {permission} ON {resource} FROM {username}")

# Helper function for checking that given `function` can only be executed
# with given `permission` granted, and returns an unauthorized error
# with the `permission` revoked.
def check_enforced(cql, username, permission, resource, function):
    eventually_unauthorized(function)
    grant(cql, permission, resource, username)
    eventually_authorized(function)
    revoke(cql, permission, resource, username)
    eventually_unauthorized(function)

# Test that data permissions can be granted and revoked, and that they're effective
def test_grant_revoke_data_permissions(cql, test_keyspace):
    with new_user(cql) as username:
        with new_session(cql, username) as user_session:
            ks = unique_name()
            # Permissions on all keyspaces
            def create_keyspace_idempotent():
                user_session.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}")
                user_session.execute(f"DROP KEYSPACE IF EXISTS {ks}")
            check_enforced(cql, username, permission='CREATE', resource='ALL KEYSPACES', function=create_keyspace_idempotent)
            # Permissions for a specific keyspace
            with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }") as keyspace:
                t = unique_name()
                def create_table_idempotent():
                    user_session.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{t}(id int primary key)")
                    cql.execute(f"DROP TABLE IF EXISTS {keyspace}.{t}")
                eventually_unauthorized(create_table_idempotent)
                grant(cql, 'CREATE', f'KEYSPACE {keyspace}', username)
                eventually_authorized(create_table_idempotent)
                cql.execute(f"CREATE TABLE {keyspace}.{t}(id int primary key)")
                # Permissions for a specific table
                check_enforced(cql, username, permission='ALTER', resource=f'{keyspace}.{t}',
                        function=lambda: user_session.execute(f"ALTER TABLE {keyspace}.{t} WITH comment = 'hey'"))
                check_enforced(cql, username, permission='SELECT', resource=f'{keyspace}.{t}',
                        function=lambda: user_session.execute(f"SELECT * FROM {keyspace}.{t}"))
                check_enforced(cql, username, permission='MODIFY', resource=f'{keyspace}.{t}',
                        function=lambda: user_session.execute(f"INSERT INTO {keyspace}.{t}(id) VALUES (42)"))
                cql.execute(f"DROP TABLE {keyspace}.{t}")
                revoke(cql, 'CREATE', f'KEYSPACE {keyspace}', username)
                eventually_unauthorized(create_table_idempotent)

                def drop_table_idempotent():
                    user_session.execute(f"DROP TABLE IF EXISTS {keyspace}.{t}")
                    cql.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{t}(id int primary key)")

                cql.execute(f"CREATE TABLE {keyspace}.{t}(id int primary key)")
                check_enforced(cql, username, permission='DROP', resource=f'KEYSPACE {keyspace}', function=drop_table_idempotent)
                cql.execute(f"DROP TABLE {keyspace}.{t}")

                # CREATE permission on all keyspaces also implies creating any tables in any keyspace
                check_enforced(cql, username, permission='CREATE', resource='ALL KEYSPACES', function=create_table_idempotent)
                # Same for DROP
                cql.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{t}(id int primary key)")
                check_enforced(cql, username, permission='DROP', resource='ALL KEYSPACES', function=drop_table_idempotent)

# Test that permissions on a table are not granted to a user as a creator if the table already exists when the user
# tries to create it. Reproduces GHSA-ww5v-p45p-3vhq
def test_create_on_existing_table(cql):
    names = [row.table_name for row in cql.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'system'")]
    is_scylla = any('scylla' in name for name in names)
    def ensure_updated_permissions():
        if is_scylla:
            rest_api.post_request(cql, "authorization_cache/reset")
        else:
            time.sleep(4)
    schema = "a int primary key"
    with new_user(cql) as username:
        with new_session(cql, username) as user_session:
            with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }") as keyspace:
                grant(cql, 'CREATE', f"KEYSPACE {keyspace}", username)
                # Wait until the CREATE permission appears in the permissions cache
                ensure_updated_permissions()
                with new_test_table(cql, keyspace, schema) as table:
                    def ensure_all_table_permissions_unauthorized(user_session):
                        def ensure_unauthorized(fun):
                            try:
                                fun()
                                pytest.fail(f"Function {fun} was not refused as unauthorized")
                            except Unauthorized:
                                pass
                        ensure_unauthorized(lambda: user_session.execute(f"ALTER TABLE {table} WITH comment = 'hey'"))
                        ensure_unauthorized(lambda: user_session.execute(f"SELECT * FROM {table}"))
                        ensure_unauthorized(lambda: user_session.execute(f"INSERT INTO {table}(a) VALUES (42)"))
                        ensure_unauthorized(lambda: user_session.execute(f"DROP TABLE {table}"))
                        # Grant the SELECT permission to the user so that the GRANT SELECT TO cassandra below only requires
                        # the missing AUTHORIZE permission. Wait until the permissions cache registers the SELECT permission,
                        # and then revoke it after confirming the user doesn't have the AUTHORIZE permission.
                        grant(cql, 'SELECT', table, username)
                        ensure_updated_permissions()
                        rest_api.post_request(cql, "authorization_cache/reset")
                        ensure_unauthorized(lambda: user_session.execute(f"GRANT SELECT ON {table} TO cassandra"))
                        revoke(cql, 'SELECT', table, username)

                    ensure_all_table_permissions_unauthorized(user_session)
                    try:
                        user_session.execute(f"CREATE TABLE {table}(a int primary key)")
                    except:
                        pass
                    # As a result of the CREATE query, user could be granted invalid permissions but they may still be not
                    # visible in the permissions cache. Sleep until permissions cache is refreshed.
                    ensure_updated_permissions()
                    ensure_all_table_permissions_unauthorized(user_session)
                    user_session.execute(f"CREATE TABLE IF NOT EXISTS {table}(a int primary key)")
                    ensure_updated_permissions()
                    ensure_all_table_permissions_unauthorized(user_session)
