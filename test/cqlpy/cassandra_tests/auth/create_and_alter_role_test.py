# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of CreateAndAlterRoleTest.java from Cassandra's
# test/unit/org/apache/cassandra/auth directory.

from contextlib import contextmanager
import time
from ..porting import *
from ...util import unique_name, cql_session
from cassandra.protocol import InvalidRequest, SyntaxException

# The Java test computes bcrypt hashes of the passwords with
# hashpw(password, gensalt(4)). We don't have a bcrypt library, so we use
# hashes of the same passwords computed in advance, with the same cost (4).
plainTextPwd = "super_secret_thing"
plainTextPwd2 = "much_safer_password"
hashedPassword = "$2a$04$PoiUnv/9OZ5nYjlN087uIe5KLMV6yJfDkgKAAHO0pRVTEvB3UQU/i"
hashedPassword2 = "$2a$04$NjgM520/tzxN/X79x2R5Su0zU9jUPdyT7B56bYxNOEI6xQArNK5rO"

# Cassandra's useUser(): a new session logged in as the given user, with
# the given password.
@contextmanager
def use_user(cql, user, password):
    endpoint = cql.hosts[0].endpoint
    with cql_session(host=endpoint.address, port=endpoint.port, is_ssl=(cql.cluster.ssl_context is not None), username=user, password=password) as session:
        yield session

# Roles with the given names, which are dropped (if they exist) at the end.
@contextmanager
def dropped_roles(cql, *roles):
    try:
        yield
    finally:
        for role in roles:
            cql.execute(f"DROP ROLE IF EXISTS {role}")

# The cql fixture is logged in as the superuser "cassandra", so it serves
# as the Java test's useSuperUser().

# Cassandra's assertInvalidMessage(), which accepts any exception type -
# Cassandra (and Scylla) report some of these errors as syntax errors.
def assertInvalidMessage(cql, message, cmd):
    with pytest.raises((InvalidRequest, SyntaxException), match=message):
        cql.execute(cmd)

# Cassandra and Scylla word this error differently.
MUTUALLY_EXCLUSIVE = "Options 'password' and 'hashed password' are mutually exclusive|Only one of the options: PASSWORD, HASHED PASSWORD can be provided"

# Cassandra refuses to change a role's password more often than every 5
# seconds. The Java test disables this limit through an internal API
# (CassandraRoleManager.updatePasswordUpdateMinInterval(0)), which we can't
# do, so we retry a password change which fails because of it. Scylla has no
# such limit, so this doesn't slow down the test on Scylla.
def change_password(cql, cmd):
    deadline = time.time() + 10
    while True:
        try:
            cql.execute(cmd)
            return
        except Exception as e:
            if 'can only be changed every' not in str(e) or time.time() > deadline:
                raise
            time.sleep(0.5)

# Reproduces SCYLLADB-5229: Scylla doesn't validate the hashed password,
# and ALTER ROLE with a hashed password fails.
@pytest.mark.xfail(reason="SCYLLADB-5229")
def testcreateAlterRoleWithHashedPassword(cql):
    user1 = "hashed_pw_role_" + unique_name()
    user2 = "pw_role_" + unique_name()
    with dropped_roles(cql, user1, user2):
        assertInvalidMessage(cql, "Invalid hashed password value",
                               "CREATE ROLE %s WITH login=true AND hashed password='%s'" %
                                   (user1, "this_is_an_invalid_hash"))
        assertInvalidMessage(cql, MUTUALLY_EXCLUSIVE,
                               "CREATE ROLE %s WITH login=true AND password='%s' AND hashed password='%s'" %
                                   (user1, plainTextPwd, hashedPassword))
        cql.execute("CREATE ROLE %s WITH login=true AND hashed password='%s'" % (user1, hashedPassword))
        cql.execute("CREATE ROLE %s WITH login=true AND password='%s'" % (user2, plainTextPwd))

        with use_user(cql, user1, plainTextPwd) as session:
            session.execute("SELECT key FROM system.local")

        with use_user(cql, user2, plainTextPwd) as session:
            session.execute("SELECT key FROM system.local")

        assertInvalidMessage(cql, MUTUALLY_EXCLUSIVE,
                               "ALTER ROLE %s WITH password='%s' AND hashed password='%s'" %
                                   (user1, plainTextPwd2, hashedPassword2))
        change_password(cql, "ALTER ROLE %s WITH password='%s'" % (user1, plainTextPwd2))
        change_password(cql, "ALTER ROLE %s WITH hashed password='%s'" % (user2, hashedPassword2))

        with use_user(cql, user1, plainTextPwd2) as session:
            session.execute("SELECT key FROM system.local")

        with use_user(cql, user2, plainTextPwd2) as session:
            session.execute("SELECT key FROM system.local")

# Reproduces SCYLLADB-5229: Scylla doesn't validate the hashed password,
# and CREATE USER and ALTER USER don't support HASHED PASSWORD.
@pytest.mark.xfail(reason="SCYLLADB-5229")
def testcreateAlterUserWithHashedPassword(cql):
    user1 = "hashed_pw_user_" + unique_name()
    user2 = "pw_user_" + unique_name()
    with dropped_roles(cql, user1, user2):
        assertInvalidMessage(cql, "Invalid hashed password value",
                               "CREATE USER %s WITH hashed password '%s'" %
                                   (user1, "this_is_an_invalid_hash"))
        cql.execute("CREATE USER %s WITH hashed password '%s'" % (user1, hashedPassword))
        cql.execute("CREATE USER %s WITH password '%s'" % (user2, plainTextPwd))

        with use_user(cql, user1, plainTextPwd) as session:
            session.execute("SELECT key FROM system.local")

        with use_user(cql, user2, plainTextPwd) as session:
            session.execute("SELECT key FROM system.local")

        change_password(cql, "ALTER USER %s WITH password '%s'" % (user1, plainTextPwd2))
        change_password(cql, "ALTER USER %s WITH hashed password '%s'" % (user2, hashedPassword2))

        with use_user(cql, user1, plainTextPwd2) as session:
            session.execute("SELECT key FROM system.local")

        with use_user(cql, user2, plainTextPwd2) as session:
            session.execute("SELECT key FROM system.local")

# The Java test reads the roles from the system_auth.roles table. Scylla
# keeps its roles in a different table, system.roles, so we use the
# LIST ROLES statement instead, which works on both.
def getAllRoles(cql):
    return set(r.role for r in cql.execute("LIST ROLES"))

# Reproduces SCYLLADB-5144: ALTER ROLE IF EXISTS isn't supported.
@pytest.mark.xfail(reason="SCYLLADB-5144")
def testcreateAlterRoleIfExists(cql):
    does_not_exist_yet = "does_not_exist_yet_" + unique_name()
    also_does_not_exist_yet = "also_does_not_exist_yet_" + unique_name()
    with dropped_roles(cql, does_not_exist_yet, also_does_not_exist_yet):
        cql.execute(f"CREATE ROLE IF NOT EXISTS {does_not_exist_yet}")
        assert does_not_exist_yet in getAllRoles(cql)

        # execute one more time
        cql.execute(f"CREATE ROLE IF NOT EXISTS {does_not_exist_yet}")

        with pytest.raises(InvalidRequest, match=f"{does_not_exist_yet} already exists"):
            cql.execute(f"CREATE ROLE {does_not_exist_yet}")

        # alter non-existing is no-op when "if exists" is specified
        cql.execute(f"ALTER ROLE IF EXISTS {also_does_not_exist_yet} WITH LOGIN = true")
        roles = getAllRoles(cql)
        assert does_not_exist_yet in roles
        # not created - CASSANDRA-19749
        assert also_does_not_exist_yet not in roles
