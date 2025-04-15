# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for managing permissions
#############################################################################

import pytest
import time
from . import rest_api
from cassandra.protocol import SyntaxException, InvalidRequest, Unauthorized, ConfigurationException
from .util import new_test_table, new_function, new_user, new_session, new_test_keyspace, unique_name, new_type
from contextlib import contextmanager

# Figure out which keyspace contains the roles table (its location changed
# across different releases of Scylla and Cassandra)
def get_roles_table(cql):
    for ks in ['system', 'system_auth_v2', 'system_auth']:
        try:
            t = ks + '.roles'
            cql.execute(f'SELECT * FROM {t}')
            # If we're still here, the SELECT didn't fail
            return t
        except:
            pass
    pytest.fail("Couldn't find roles table")

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

def test_user_displays_as_authenticated(cql):
    with new_session(cql, "cassandra") as user_session_1:
        with new_session(cql, "cassandra") as user_session_2:
            with new_session(cql, "cassandra") as user_session_3:
                # Every opened session should create around 5 connections,
                # so we'll have 3x5=15 opened connections,
                # and we expect that their status will be READY,
                # but, on rare occasions, it may happen that
                # the connection's status is still stuck at ESTABLISHED or AUTHENTICATING,
                # and we must handle this case as well,
                # so we simply sleep and retry later.
                for retry in range(0, 5):
                    res = user_session_1.execute("SELECT connection_stage FROM system.clients")
                    if all([r[0] == "READY" for r in res]):
                        return
                    else:
                        time.sleep(2)
                assert False

# Test that data permissions can be granted and revoked, and that they're effective
def test_grant_revoke_data_permissions(cql, test_keyspace):
    with new_user(cql) as username:
        with new_session(cql, username) as user_session:
            ks = unique_name()
            # Permissions on all keyspaces
            def create_keyspace_idempotent():
                user_session.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}")
                user_session.execute(f"DROP KEYSPACE IF EXISTS {ks}")
            check_enforced(cql, username, permission='CREATE', resource='ALL KEYSPACES', function=create_keyspace_idempotent)
            # Permissions for a specific keyspace
            with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }") as keyspace:
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

# Test that permissions for user-defined functions are serialized in a Cassandra-compatible way
def test_udf_permissions_serialization(cql):
    schema = "a int primary key"
    user = "cassandra"
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }") as keyspace, new_user(cql) as user:
        with new_test_table(cql, keyspace, schema) as table:
            # Creating a bilingual function makes this test case work for both Scylla and Cassandra
            div_body_lua = "(b bigint, i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return b//i'"
            div_body_java = "(b bigint, i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return b/i;'"
            div_body = div_body_lua
            try:
                with new_function(cql, keyspace, div_body) as div_fun:
                    pass
            except:
                div_body = div_body_java
            with new_function(cql, keyspace, div_body) as div_fun:
                applicable_permissions = {
                    'all functions': ['create', 'alter', 'drop', 'authorize', 'execute'],
                    f'all functions in keyspace {keyspace}': ['create', 'alter', 'drop', 'authorize', 'execute'],
                    f'function {keyspace}.{div_fun}(bigint, int)': ['alter', 'drop', 'authorize', 'execute'],
                }
                for resource, permissions in applicable_permissions.items():
                    # Applicable permissions can be legally granted
                    for permission in permissions:
                        cql.execute(f"GRANT {permission} ON {resource} TO {user}")

                permissions = {row.resource: row.permissions for row in cql.execute(f"SELECT * FROM system.role_permissions")}
                assert permissions['functions'] == set(['ALTER', 'AUTHORIZE', 'CREATE', 'DROP', 'EXECUTE'])
                assert permissions[f'functions/{keyspace}'] == set(['ALTER', 'AUTHORIZE', 'CREATE', 'DROP', 'EXECUTE'])
                assert permissions[f'functions/{keyspace}/{div_fun}[org.apache.cassandra.db.marshal.LongType^org.apache.cassandra.db.marshal.Int32Type]'] == set(['ALTER', 'AUTHORIZE', 'DROP', 'EXECUTE'])

                resources_with_execute = [row.resource for row in cql.execute(f"LIST EXECUTE OF {user}")]
                assert '<all functions>' in resources_with_execute
                assert f'<all functions in {keyspace}>' in resources_with_execute
                assert f'<function {keyspace}.{div_fun}(bigint, int)>' in resources_with_execute

# Test that names that require quoting (e.g. due to having nonorthodox characters)
# are properly handled, with right permissions granted.
# Cassandra doesn't quote names properly, so the test fails
def test_udf_permissions_quoted_names(cassandra_bug, cql):
    udt_name = f'"{unique_name()}weird_udt[t^t]a^b^[]"'
    schema = f"a frozen<{udt_name}> primary key"
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }") as keyspace:
        with new_type(cql, keyspace, "(a text, b int)", udt_name) as udt, new_test_table(cql, keyspace, schema) as table:
            fun_body_lua = f"(i {udt}) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 42;'"
            fun_body_java = f"(i {udt}) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 42;'"
            fun_body = fun_body_lua
            try:
                with new_function(cql, keyspace, fun_body):
                    pass
            except:
                fun_body = fun_body_java
            with new_function(cql, keyspace, fun_body, f'"{unique_name()}weird[name1^name2]x^y"') as weird_fun:
                with new_user(cql) as username:
                    with new_session(cql, username) as user_session:
                        grant(cql, 'EXECUTE', f'FUNCTION {keyspace}.{weird_fun}({udt})', username)
                        grant(cql, 'SELECT', table, username)
                        cql.execute(f"INSERT INTO {table}(a) VALUES ({{a:'hello', b:42}})")

                        assert list([r[0] for r in user_session.execute(f"SELECT {keyspace}.{weird_fun}(a) FROM {table}")]) == [42]

                        resources_with_execute = [row.resource for row in cql.execute(f"LIST EXECUTE OF {username}")]
                        assert f'<function {keyspace}.{weird_fun}(frozen<{udt_name}>)>' in resources_with_execute

                        revoke(cql, 'EXECUTE', f'FUNCTION {keyspace}.{weird_fun}({udt})', username)
                        check_enforced(cql, username, 'EXECUTE', f'FUNCTION {keyspace}.{weird_fun}({udt})',
                                lambda: user_session.execute(f"SELECT {keyspace}.{weird_fun}(a) FROM {table}"))

# Test that dropping a function without specifying its signature works with the DROP permission if there's only
# one function with the given name, and that it fails if there are multiple functions with the same name,
# regardless of the permissions of the user.
# If the signature is specified, test that the permission check is performed as usual.
# This test is marked cassandra_bug because it exposes a small error-path
# bug in Cassandra: When trying to drop an overloaded function without
# specifying which overload is intended, this error should be reported,
# regardless of whether some of the overloads have or don't have drop
# permissions. Cassandra erroneously reports the unrelated missing permissions.
# Reported to Cassandra as CASSANDRA-19005.
def test_drop_udf_with_same_name(cql, cassandra_bug):
    schema = "a int primary key"
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }") as keyspace:
        body1_lua = "(i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 42;'"
        body1_java = "(i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 42L;'"
        body2_lua = "(i int, j int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 42;'"
        body2_java = "(i int, j int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 42L;'"
        body1 = body1_lua
        body2 = body2_lua
        try:
            with new_function(cql, keyspace, body1):
                pass
        except:
            body1 = body1_java
            body2 = body2_java
        fun = "fun43"
        cql.execute(f"CREATE FUNCTION {keyspace}.{fun}{body1}")
        cql.execute(f"CREATE FUNCTION {keyspace}.{fun}{body2}")
        with new_user(cql) as username:
            with new_session(cql, username) as user_session:
                grant(cql, 'DROP', f'FUNCTION {keyspace}.{fun}(int)', username)
                # When trying to drop a function without a signature but
                # there is more than one function with that name, we should
                # get an InvalidRequest, as is confirmed by the test
                # test_udf.py::test_drop_udf_with_same_name.
                # Cassandra fails here because it has a bug: It decided to
                # complain about the true (but irrelevant) fact that one of
                # the two functions doesn't have drop permissions and throws
                # Unauthorized instead of InvalidRequest.
                with pytest.raises(InvalidRequest):
                    user_session.execute(f"DROP FUNCTION {keyspace}.{fun}")
                eventually_unauthorized(lambda: user_session.execute(f"DROP FUNCTION {keyspace}.{fun}(int, int)"))
                grant(cql, 'DROP', f'FUNCTION {keyspace}.{fun}(int, int)', username)
                with pytest.raises(InvalidRequest):
                    user_session.execute(f"DROP FUNCTION {keyspace}.{fun}")
                eventually_authorized(lambda: user_session.execute(f"DROP FUNCTION {keyspace}.{fun}(int)"))
                eventually_authorized(lambda: user_session.execute(f"DROP FUNCTION {keyspace}.{fun}"))

# Test that permissions set for user-defined functions are enforced
# Tests for ALTER are separate, because they are qualified as cassandra_bug
def test_grant_revoke_udf_permissions(cql):
    schema = "a int primary key, b list<int>"
    user = "cassandra"
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }") as keyspace:
        with new_test_table(cql, keyspace, schema) as table:
            fun_body_lua = "(i int, l list<int>) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 42;'"
            fun_body_java = "(i int, l list<int>) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return 42;'"
            fun_body = fun_body_lua
            try:
                with new_function(cql, keyspace, fun_body) as fun:
                    pass
            except:
                fun_body = fun_body_java
            with new_user(cql) as username:
                with new_session(cql, username) as user_session:
                    fun = "fun42"

                    def create_function_idempotent():
                        user_session.execute(f"CREATE FUNCTION IF NOT EXISTS {keyspace}.{fun} {fun_body}")
                        cql.execute(f"DROP FUNCTION IF EXISTS {keyspace}.{fun}(int, list<int>)")
                    check_enforced(cql, username, permission='CREATE', resource=f'all functions in keyspace {keyspace}',
                            function=create_function_idempotent)
                    check_enforced(cql, username, permission='CREATE', resource='all functions',
                            function=create_function_idempotent)

                    def drop_function_idempotent():
                        user_session.execute(f"DROP FUNCTION IF EXISTS {keyspace}.{fun}(int, list<int>)")
                        cql.execute(f"CREATE FUNCTION IF NOT EXISTS {keyspace}.{fun} {fun_body}")
                    for resource in [f'function {keyspace}.{fun}(int, list<int>)', f'all functions in keyspace {keyspace}', 'all functions']:
                        check_enforced(cql, username, permission='DROP', resource=resource, function=drop_function_idempotent)

                    grant(cql, 'SELECT', table, username)
                    for resource in [f'function {keyspace}.{fun}(int, list<int>)', f'all functions in keyspace {keyspace}', 'all functions']:
                        check_enforced(cql, username, permission='EXECUTE', resource=resource,
                                function=lambda: user_session.execute(f"SELECT {keyspace}.{fun}(a, b) FROM {table}"))

                    grant(cql, 'EXECUTE', 'ALL FUNCTIONS', username)
                    def grant_idempotent():
                        grant(user_session, 'EXECUTE', f'function {keyspace}.{fun}(int, list<int>)', 'cassandra')
                        revoke(cql, 'EXECUTE', f'function {keyspace}.{fun}(int, list<int>)', 'cassandra')
                    for resource in [f'function {keyspace}.{fun}(int, list<int>)', f'all functions in keyspace {keyspace}', 'all functions']:
                        check_enforced(cql, username, permission='AUTHORIZE', resource=resource, function=grant_idempotent)

# This test case is artificially extracted from the one above,
# because it's qualified as cassandra_bug - the documentation quotes that ALTER is needed on
# functions if the definition is replaced (CREATE OR REPLACE FUNCTION (...)),
# and yet it's not enforced
def test_grant_revoke_alter_udf_permissions(cassandra_bug, cql):
    schema = "a int primary key"
    user = "cassandra"
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }") as keyspace:
        with new_test_table(cql, keyspace, schema) as table:
            fun_body_lua = "(i int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 42;'"
            fun_body_java = "(i int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return 42;'"
            fun_body = fun_body_lua
            try:
                with new_function(cql, keyspace, fun_body) as fun:
                    pass
            except:
                fun_body = fun_body_java
            with new_user(cql) as username:
                with new_session(cql, username) as user_session:
                    fun = "fun42"

                    grant(cql, 'ALTER', 'ALL FUNCTIONS', username)
                    check_enforced(cql, username, permission='CREATE', resource=f'all functions in keyspace {keyspace}',
                            function=lambda: user_session.execute(f"CREATE OR REPLACE FUNCTION {keyspace}.{fun} {fun_body}"))
                    check_enforced(cql, username, permission='CREATE', resource='all functions',
                            function=lambda: user_session.execute(f"CREATE OR REPLACE FUNCTION {keyspace}.{fun} {fun_body}"))
                    revoke(cql, 'ALTER', 'ALL FUNCTIONS', username)

                    grant(cql, 'CREATE', 'ALL FUNCTIONS', username)
                    check_enforced(cql, username, permission='ALTER', resource=f'all functions in keyspace {keyspace}',
                            function=lambda: user_session.execute(f"CREATE OR REPLACE FUNCTION {keyspace}.{fun} {fun_body}"))
                    check_enforced(cql, username, permission='ALTER', resource='all functions',
                            function=lambda: user_session.execute(f"CREATE OR REPLACE FUNCTION {keyspace}.{fun} {fun_body}")) 
                    check_enforced(cql, username, permission='ALTER', resource=f'FUNCTION {keyspace}.{fun}(int)',
                            function=lambda: user_session.execute(f"CREATE OR REPLACE FUNCTION {keyspace}.{fun} {fun_body}"))

# Test that granting permissions on non-existent UDFs fails
# This test is marked cassandra_bug because it exposes a small error-path
# bug in their parser CASSANDRA-19006 (see comment below).
def test_grant_perms_on_nonexistent_udf(cql, cassandra_bug):
    keyspace = "ks"
    fun_name = "fun42"
    with new_user(cql) as username:
        grant(cql, 'EXECUTE', 'ALL FUNCTIONS', username)
        revoke(cql, 'EXECUTE', 'ALL FUNCTIONS', username)
        with pytest.raises(InvalidRequest):
            grant(cql, 'EXECUTE', f'ALL FUNCTIONS IN KEYSPACE {keyspace}', username)
        # When the keyspace is non-existent, Scylla returns InvalidRequest
        # and Cassandra returns ConfigurationException. Let's allow both.
        with pytest.raises((InvalidRequest, ConfigurationException)):
            grant(cql, 'EXECUTE', f'FUNCTION {keyspace}.{fun_name}(int)', username)
        cql.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}")
        grant(cql, 'EXECUTE', f'ALL FUNCTIONS IN KEYSPACE {keyspace}', username)
        revoke(cql, 'EXECUTE', f'ALL FUNCTIONS IN KEYSPACE {keyspace}', username)
        # When the keyspace exists, but the function doesn't, we should
        # return an InvalidRequest, complaining that "function ks.fun42(int)>
        # doesn't exist.". Cassandra has a bug here - CASSANDRA-19006 - it
        # returns a SyntaxException with the message "NoSuchElementException
        # No value present", suggesting a bug in their parser.
        with pytest.raises(InvalidRequest):
            grant(cql, 'EXECUTE', f'FUNCTION {keyspace}.{fun_name}(int)', username)

        fun_body_lua = "(i int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 42;'"
        fun_body_java = "(i int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return 42;'"
        fun_body = fun_body_lua
        try:
            with new_function(cql, keyspace, fun_body) as fun:
                pass
        except:
            fun_body = fun_body_java
        with new_function(cql, keyspace, fun_body, fun_name):
            grant(cql, 'EXECUTE', f'FUNCTION {keyspace}.{fun_name}(int)', username)
        cql.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")

# Test that permissions for user-defined aggregates are also enforced.
# scylla_only, because Lua is used as the target language
def test_grant_revoke_uda_permissions(scylla_only, cql):
    schema = 'id bigint primary key'
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }") as keyspace:
        with new_test_table(cql, keyspace, schema) as table:
            for i in range(8):
                cql.execute(f"INSERT INTO {table} (id) VALUES ({10**i})")
            avg_partial_body = "(state tuple<bigint, bigint>, val bigint) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + val, state[2] + 1}'"
            div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
            with new_function(cql, keyspace, avg_partial_body) as avg_partial, new_function(cql, keyspace, div_body) as div_fun:
                custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
                with new_user(cql) as username:
                    with new_session(cql, username) as user_session:
                        custom_avg = "custom_avg"
                        def create_aggr_idempotent():
                            user_session.execute(f"CREATE AGGREGATE IF NOT EXISTS {keyspace}.{custom_avg} {custom_avg_body}")
                            cql.execute(f"DROP AGGREGATE IF EXISTS {keyspace}.{custom_avg}(bigint)")
                        grant(cql, 'EXECUTE', f'function {keyspace}.{avg_partial}(tuple<bigint, bigint>, bigint)', username)
                        grant(cql, 'EXECUTE', f'function {keyspace}.{div_fun}(tuple<bigint, bigint>)', username)
                        check_enforced(cql, username, permission='CREATE', resource=f'all functions in keyspace {keyspace}',
                                function=create_aggr_idempotent)
                        check_enforced(cql, username, permission='CREATE', resource='all functions',
                                function=create_aggr_idempotent)

                        grant(cql, 'CREATE', 'ALL FUNCTIONS', username)
                        check_enforced(cql, username, permission='ALTER', resource=f'all functions in keyspace {keyspace}',
                                function=lambda: user_session.execute(f"CREATE OR REPLACE AGGREGATE {keyspace}.{custom_avg} {custom_avg_body}"))
                        check_enforced(cql, username, permission='ALTER', resource='all functions',
                                function=lambda: user_session.execute(f"CREATE OR REPLACE AGGREGATE {keyspace}.{custom_avg} {custom_avg_body}"))

                        grant(cql, 'SELECT', table, username)
                        for resource in [f'function {keyspace}.{custom_avg}(bigint)', f'all functions in keyspace {keyspace}', 'all functions']:
                            check_enforced(cql, username, permission='EXECUTE', resource=resource,
                                    function=lambda: user_session.execute(f"SELECT {keyspace}.{custom_avg}(id) FROM {table}"))

                        def drop_aggr_idempotent():
                            user_session.execute(f"DROP AGGREGATE IF EXISTS {keyspace}.{custom_avg}(bigint)")
                            cql.execute(f"CREATE AGGREGATE IF NOT EXISTS {keyspace}.{custom_avg} {custom_avg_body}")
                        for resource in [f'function {keyspace}.{custom_avg}(bigint)', f'all functions in keyspace {keyspace}', 'all functions']:
                            check_enforced(cql, username, permission='DROP', resource=resource, function=drop_aggr_idempotent)

                        grant(cql, 'EXECUTE', 'ALL FUNCTIONS', username)
                        def grant_idempotent():
                            grant(user_session, 'EXECUTE', f'function {keyspace}.{custom_avg}(bigint)', 'cassandra')
                            revoke(cql, 'EXECUTE', f'function {keyspace}.{custom_avg}(bigint)', 'cassandra')
                        for resource in [f'function {keyspace}.{custom_avg}(bigint)', f'all functions in keyspace {keyspace}', 'all functions']:
                            check_enforced(cql, username, permission='AUTHORIZE', resource=resource, function=grant_idempotent)

                        cql.execute(f"DROP AGGREGATE IF EXISTS {keyspace}.{custom_avg}(bigint)")

# Test that permissions for user-defined functions created on top of user-defined types work
def test_udf_permissions_with_udt(cql):
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }") as keyspace:
        with new_type(cql, keyspace, '(v int)') as udt:
            schema = f"a frozen<{udt}> primary key"
            with new_test_table(cql, keyspace, schema) as table:
                fun_body_lua = f"(i {udt}) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 42;'"
                fun_body_java = f"(i {udt}) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return 42;'"
                fun_body = fun_body_lua
                try:
                    with new_function(cql, keyspace, fun_body) as fun:
                        pass
                except:
                    fun_body = fun_body_java
                with new_user(cql) as username:
                    with new_session(cql, username) as user_session:
                        with new_function(cql, keyspace, fun_body) as fun:
                            cql.execute(f"INSERT INTO {table}(a) VALUES ((7))")
                            grant(cql, 'SELECT', table, username)
                            grant(cql, 'EXECUTE', f'FUNCTION {keyspace}.{fun}({udt})', username)
                            user_session.execute(f'SELECT {keyspace}.{fun}(a) FROM {table}')

# Test that permissions on user-defined functions with no arguments work
def test_udf_permissions_no_args(cql):
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }") as keyspace:
        with new_test_table(cql, keyspace, schema="a int primary key") as table, new_user(cql) as username:
            with new_session(cql, username) as user_session:
                fun_body_lua = f"() CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 42;'"
                fun_body_java = f"() CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return 42;'"
                fun_body = fun_body_lua
                try:
                    with new_function(cql, keyspace, fun_body):
                        pass
                except:
                    fun_body = fun_body_java
                with new_function(cql, keyspace, fun_body) as fun:
                    grant(cql, 'SELECT', table, username)
                    check_enforced(cql, username, permission='EXECUTE', resource=f'function {keyspace}.{fun}()',
                            function=lambda: user_session.execute(f'SELECT {keyspace}.{fun}() FROM {table}'))
                    with pytest.raises(SyntaxException):
                        nonexistent_func = unique_name()
                        user_session.execute(f'GRANT SELECT ON FUNCTION {keyspace}.{nonexistent_func}() TO cassandra')

# Test that the create permission can't be granted on a single function. Similarly to how we handle Roles and Tables,
# we do not allow permissions on non-existent Functions, so the CREATE permission on a specific function is meaningless,
# because granting it would only be allowed if the function was already created before.
# Reproduces #13822
def test_create_on_single_function(cql):
    schema = "a int primary key"
    user = "cassandra"
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }") as keyspace:
        with new_test_table(cql, keyspace, schema) as table:
            fun_body_lua = f"(a int, b int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return a + b;'"
            fun_body_java = f"(a int, b int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return a + b;'"
            fun_body = fun_body_lua
            try:
                with new_function(cql, keyspace, fun_body):
                    pass
            except:
                fun_body = fun_body_java
            with new_function(cql, keyspace, fun_body) as fun:
                with pytest.raises(SyntaxException):
                    grant(cql, 'CREATE', f'FUNCTION {keyspace}.{fun}(int, int)', user)

# Test that user can be granted permissions to change the password for a superuser. This behavior matches Cassandra's.
# Reproduces ScyllaDB OSS #14277
def test_grant_all_allows_superuser_password_change(cql):
    with new_user(cql) as username:
        with new_user(cql, with_superuser_privileges=True) as superuser:
            grant(cql, 'ALTER', 'ALL ROLES', username)
            with new_session(cql, username) as user_session:
                user_session.execute(f"ALTER ROLE {superuser} WITH PASSWORD = '{superuser}2'")

# Reproduces https://github.com/scylladb/scylla-enterprise/issues/5210,
# i.e. a CVE reported for Cassandra https://www.cve.org/CVERecord?id=CVE-2025-23015,
def test_non_superuser_with_modify_all_keyspaces_permissions_cannot_modify_system_keyspaces(cql, test_keyspace):
    roles_table = get_roles_table(cql)
    with new_user(cql) as username:
        with new_session(cql, username) as user_session:
            with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as ks_cf:
                # a non-superuser CANNOT modify any random table in any random keyspace
                eventually_unauthorized(lambda: user_session.execute(f"INSERT INTO {ks_cf} (a) VALUES (1)"))
                # unless we grant him a MODIFY permission to ALL KEYSPACES
                grant(cql, 'MODIFY', 'ALL KEYSPACES', username)
                # with which he can now MODIFY it
                eventually_authorized(lambda: user_session.execute(f"INSERT INTO {ks_cf} (a) VALUES (1)"))

                # but even an authenticated non-superuser with MODIFY on ALL KEYSPACES permissions
                # cannot update system tables, especially elevate itself to a superuser
                eventually_unauthorized(lambda: user_session.execute(f"UPDATE {roles_table} SET is_superuser = True WHERE role = '{username}'"))

                # but an authenticated superuser user can update system tables
                # (note that `cql` is a session for the already authenticated "cassandra" superuser)
                cql.execute(f"UPDATE {roles_table} SET is_superuser = True WHERE role = '{username}'")

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

# Test that native functions permissions are always implicitly granted.
# every user with SELECT permission for a table should be able to use
# the native functions (non UDF/UDA functions)
# ref: https://github.com/scylladb/scylladb/issues/16526
def test_native_functions_always_executable(cql):
    schema = "a int primary key"
    with new_test_keyspace(cql,"WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }") as keyspace:
        with new_test_table(cql,keyspace,schema) as table:
            cql.execute(f'INSERT INTO {table}(a) VALUES(15)')
            cql.execute(f'INSERT INTO {table}(a) VALUES(3)')
            cql.execute(f'INSERT INTO {table}(a) VALUES(84)')
            with new_user(cql) as username:
                grant(cql, 'SELECT', table, username)
                with new_session(cql,username) as user_session:
                    assert list(user_session.execute(f"SELECT count(*) FROM {table}")) == [(3,)]
                    assert list(user_session.execute(f"SELECT max(a) FROM {table}")) == [(84,)]
                    assert list(user_session.execute(f"SELECT min(a) FROM {table}")) == [(3,)]
                    assert list(user_session.execute(f"SELECT sum(a) FROM {table}")) == [(102,)]

@contextmanager
def eventually_new_named_table(cql, table, schema, extra=""):
    eventually_authorized(lambda: cql.execute(f'CREATE TABLE {table} ({schema}) {extra}'))
    try:
        yield table
    finally:
        eventually_authorized(lambda: cql.execute(f'DROP TABLE {table}'))

# When a role has permissions to create a table, the role is given full
# access to the table it just created (we call this feature "auto-grant").
# The following tests check the auto-grant feature. The first test checks
# that the base table created by CREATE TABLE is usable by the role creating
# it. The second test is about the view created by CREATE MATERIALIZED VIEW
# being accessible. The third test is about newly created CDC being accessible.
def test_auto_grant_base(cql, test_keyspace):
    with new_user(cql) as username:
        # Grant username CREATE permissions on test_keyspace, but no other
        # permissions:
        grant(cql, 'CREATE', 'KEYSPACE ' + test_keyspace, username)
        with new_session(cql, username) as user_session:
            schema = "p int primary key"
            table = f'{test_keyspace}.{unique_name()}'
            with eventually_new_named_table(user_session, table, schema):
                # Check that the username was auto-granted SELECT permissions
                eventually_authorized(lambda: user_session.execute(f'SELECT * FROM {table}'))
                # Check that the username was auto-granted MODIFY permissions
                eventually_authorized(lambda: user_session.execute(f'INSERT INTO {table}(p) VALUES(42)'))
                # Check that the username was auto-granted ALTER permissions
                eventually_authorized(lambda: user_session.execute(f"ALTER TABLE {table} WITH comment = 'hey'"))
                # When this scope ends, table is dropped in user_session,
                # so we check that DROP permissions were auto-granted as well.

# Test that a role creating a materialized view is automatically granted
# permissions to read it.
def test_auto_grant_view(cql, test_keyspace):
    with new_user(cql) as username:
        # Grant username CREATE permissions on test_keyspace, but no other
        # permissions:
        grant(cql, 'CREATE', 'KEYSPACE ' + test_keyspace, username)
        with new_session(cql, username) as user_session:
            schema = "p int primary key, v int"
            table = f'{test_keyspace}.{unique_name()}'
            with eventually_new_named_table(user_session, table, schema):
                # Because username has CREATE permissions, it can also use
                # CREATE MATERIALIZED VIEW
                mv = f'{test_keyspace}.{unique_name()}'
                eventually_authorized(lambda: user_session.execute(f'CREATE MATERIALIZED VIEW {mv} AS SELECT * FROM {table} WHERE p IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, p)'))
                # Check that the username was auto-granted SELECT permissions
                # on the newly created materialized view:
                eventually_authorized(lambda: user_session.execute(f'SELECT * FROM {mv}'))
                # Check that the username was auto-granted ALTER permissions
                # on the materialized view
                eventually_authorized(lambda: user_session.execute(f"ALTER MATERIALIZED VIEW {mv} WITH comment = 'hey'"))
                # Check that the username was auto-granted DROP permissions
                # on the materialized view
                eventually_authorized(lambda: user_session.execute(f'DROP MATERIALIZED VIEW {mv}'))

# Test that a role creating CDC is automatically granted permissions to access
# the CDC log table.
# This test has two variants - cdc_on_create=True creates the table immediately
# with CDC enabled, while cdc_on_create=False creates a table first and only
# then enables CDC. Both should work.
# This is a scylla_only test because it tests the Scylla-only CDC feature.
# Reproduces #19798:
@pytest.mark.xfail(reason="issue #19798")
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
@pytest.mark.parametrize("cdc_on_create", [True, False])
def test_auto_grant_cdc(cql, test_keyspace, cdc_on_create, scylla_only):
    with new_user(cql) as username:
        # Grant username CREATE permissions on test_keyspace, but no other
        # permissions:
        grant(cql, 'CREATE', 'KEYSPACE ' + test_keyspace, username)
        with new_session(cql, username) as user_session:
            schema = "p int primary key"
            extra = "with cdc = {'enabled': true}"
            table = f'{test_keyspace}.{unique_name()}'
            with eventually_new_named_table(user_session, table, schema, extra if cdc_on_create else ''):
                # If cdc_on_create=False, we created the table without CDC
                # enabled and we'll enable it now. username should be allowed
                # to do it (it is auto-granted ALTER permissions on the table
                # it created)
                if not cdc_on_create:
                    eventually_authorized(lambda: user_session.execute(f'ALTER TABLE {table} {extra}'))
                # Check that the username was auto-granted SELECT permissions
                # on the newly created CDC log
                # Note: While this test is xfailing, we use a lower timeout
                # here to avoid a very slow failure. When the test begins
                # to pass, the timeout_s doesn't matter and can be removed.
                eventually_authorized(lambda: user_session.execute(f'SELECT * FROM {table}_scylla_cdc_log'), timeout_s=1)

# In the above tests we checked the "auto-grant" feature - where a user that
# creates table is automatically granted permissions on the new table, views
# and CDC logs it created. If auto-grant is implemented, it is also critical
# that auto-revoke is implemented - i.e., permissions are revoked when a
# table, view or CDC log is deleted. If we forget to do that, it is possible
# that a first user creates a table/view/cdc, deletes it, and when a second
# user reuses the same name, the first user wrongly gets permission to access
# the second user's data.
# The goal of the following tests is the check this auto-revoke feature.

def test_auto_revoke_base(cql, test_keyspace):
    with new_user(cql) as user1, new_user(cql) as user2:
        # Grant user1 and user2 CREATE permissions on test_keyspace, but no
        # other permissions:
        grant(cql, 'CREATE', 'KEYSPACE ' + test_keyspace, user1)
        grant(cql, 'CREATE', 'KEYSPACE ' + test_keyspace, user2)
        with new_session(cql, user1) as user1_session, new_session(cql, user2) as user2_session:
            schema = "p int primary key"
            table = f'{test_keyspace}.{unique_name()}'
            with eventually_new_named_table(user1_session, table, schema):
                # A minimal check that auto-grant actually worked for user1,
                # user2 can't read the table.
                eventually_authorized(lambda: user1_session.execute(f'SELECT * FROM {table}'))
                eventually_unauthorized(lambda: user2_session.execute(f'SELECT * FROM {table}'))
            # At this point, table was deleted by user1, so user2 can create
            # a table with the same name "table".
            with eventually_new_named_table(user2_session, table, schema):
                # Now, permissions were auto-granted for user2, but user1
                # no longer has permissions on this table.
                eventually_authorized(lambda: user2_session.execute(f'SELECT * FROM {table}'))
                eventually_unauthorized(lambda: user1_session.execute(f'SELECT * FROM {table}'))

def test_auto_revoke_view(cql, test_keyspace):
    with new_user(cql) as user1, new_user(cql) as user2:
        # Grant user1 and user2 CREATE permissions on test_keyspace, but no
        # other permissions:
        grant(cql, 'CREATE', 'KEYSPACE ' + test_keyspace, user1)
        grant(cql, 'CREATE', 'KEYSPACE ' + test_keyspace, user2)
        with new_session(cql, user1) as user1_session, new_session(cql, user2) as user2_session:
            schema = "p int primary key, v int"
            mv = f'{test_keyspace}.{unique_name()}'
            table = f'{test_keyspace}.{unique_name()}'
            with eventually_new_named_table(user1_session, table, schema):
                # Create a view with a pre-chosen name mv as user1:
                eventually_authorized(lambda: user1_session.execute(f'CREATE MATERIALIZED VIEW {mv} AS SELECT * FROM {table} WHERE p IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, p)'))
                # user1 was auto-granted permissions to read this view, user2
                # was not:
                eventually_authorized(lambda: user1_session.execute(f'SELECT * FROM {mv}'))
                eventually_unauthorized(lambda: user2_session.execute(f'SELECT * FROM {mv}'))
                eventually_authorized(lambda: user1_session.execute(f'DROP MATERIALIZED VIEW {mv}'))
                # After user1 dropped the view with name mv, we let user2
                # create a different table but reuse the same view name mv.
                # We want to check that user1 no longer has permissions on
                # this view:
                table2 = f'{test_keyspace}.{unique_name()}'
                with eventually_new_named_table(user2_session, table2, schema):
                    eventually_authorized(lambda: user2_session.execute(f'CREATE MATERIALIZED VIEW {mv} AS SELECT * FROM {table2} WHERE p IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, p)'))
                    eventually_authorized(lambda: user2_session.execute(f'SELECT * FROM {mv}'))
                    eventually_unauthorized(lambda: user1_session.execute(f'SELECT * FROM {mv}'))
                    eventually_authorized(lambda: user2_session.execute(f'DROP MATERIALIZED VIEW {mv}'))

# This is a scylla_only test because it tests the Scylla-only CDC feature.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_auto_revoke_cdc(cql, test_keyspace, scylla_only):
    with new_user(cql) as user1, new_user(cql) as user2:
        # Grant user1 and user2 CREATE permissions on test_keyspace, but no
        # other permissions:
        grant(cql, 'CREATE', 'KEYSPACE ' + test_keyspace, user1)
        grant(cql, 'CREATE', 'KEYSPACE ' + test_keyspace, user2)
        with new_session(cql, user1) as user1_session, new_session(cql, user2) as user2_session:
            schema = "p int primary key"
            extra = "with cdc = {'enabled': true}"
            table = f'{test_keyspace}.{unique_name()}'
            with eventually_new_named_table(user1_session, table, schema, extra):
                pass
            # At this point, table was deleted by user1, so user2 can create
            # a table with the same name "table" (and a CDC log for it)
            with eventually_new_named_table(user2_session, table, schema, extra):
                # Now, permissions were auto-granted for user2, but user1
                # no longer has permissions on this table.
                #eventually_authorized(lambda: user2_session.execute(f'SELECT * FROM {table}_scylla_cdc_log')) # Reproduces #19798
                eventually_unauthorized(lambda: user1_session.execute(f'SELECT * FROM {table}_scylla_cdc_log'))

# Test that an unprivileged user can read from *some* system tables, such
# as system_schema.tables, but cannot read from *other* system tables - most
# notably the system.roles table. Allowing unprivileged users to read from
# the roles table could have allowed them to read other users' salted hash
# keys (which can be used to brute-force the other users' passwords, and in
# Alternator - to directly log as those users!)
def test_select_system_table(cql):
    roles_table = get_roles_table(cql)

    with new_user(cql) as user1:
        with new_session(cql, user1) as user1_session:
            # user1 was not explicitly granted permissions for
            # system_schema.tables, but can still read it!
            # This white-listing of certain system tables that even an
            # unprivileged user can read happens in the function
            # service::client_state::has_access().
            eventually_authorized(lambda: user1_session.execute('SELECT * FROM system_schema.tables'))
            # But the system.roles table is NOT white-listed - it should be
            # unreadable to the unprivileged user, until explicitly granted
            # the permission to read it:
            eventually_unauthorized(lambda: user1_session.execute(f'SELECT * FROM {roles_table}'))
            grant(cql, 'SELECT', roles_table, user1)
            eventually_authorized(lambda: user1_session.execute(f'SELECT * FROM {roles_table}'))
