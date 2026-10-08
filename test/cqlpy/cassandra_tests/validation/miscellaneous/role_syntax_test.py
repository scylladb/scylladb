# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ...porting import *

# Cassandra's assertValidSyntax() only parses the statement, without
# executing it. The closest we can do through CQL is to prepare the
# statement - which parses it but doesn't execute it. Like the original,
# we only check that there is no syntax error - other errors are fine.
def assertValidSyntax(cql, query):
    try:
        cql.prepare(query)
    except SyntaxException as e:
        pytest.fail(f"Expected query syntax to be valid but was invalid. Query is: {query}; Error is {e}")
    except InvalidRequest:
        pass

def assertInvalidSyntax(cql, query):
    with pytest.raises(SyntaxException):
        cql.execute(query)

def teststandardOptionsSyntaxTest(cql):
    assertValidSyntax(cql, "CREATE ROLE r WITH LOGIN = true AND SUPERUSER = false AND PASSWORD = 'foo'")
    assertValidSyntax(cql, "CREATE ROLE r WITH PASSWORD = 'foo' AND LOGIN = true AND SUPERUSER = false")
    assertValidSyntax(cql, "CREATE ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false")
    assertValidSyntax(cql, "CREATE ROLE r WITH LOGIN = true AND PASSWORD = 'foo' AND SUPERUSER = false")
    assertValidSyntax(cql, "CREATE ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false")

    assertValidSyntax(cql, "ALTER ROLE r WITH LOGIN = true AND SUPERUSER = false AND PASSWORD = 'foo'")
    assertValidSyntax(cql, "ALTER ROLE r WITH PASSWORD = 'foo' AND LOGIN = true AND SUPERUSER = false")
    assertValidSyntax(cql, "ALTER ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false")
    assertValidSyntax(cql, "ALTER ROLE r WITH LOGIN = true AND PASSWORD = 'foo' AND SUPERUSER = false")
    assertValidSyntax(cql, "ALTER ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false")

def testcustomOptionsSyntaxTest(cql):
    assertValidSyntax(cql, "CREATE ROLE r WITH OPTIONS = {'a':'b', 'b':1}")
    assertInvalidSyntax(cql, "CREATE ROLE r WITH OPTIONS = 'term'")
    assertInvalidSyntax(cql, "CREATE ROLE r WITH OPTIONS = 99")

    assertValidSyntax(cql, "ALTER ROLE r WITH OPTIONS = {'a':'b', 'b':1}")
    assertInvalidSyntax(cql, "ALTER ROLE r WITH OPTIONS = 'term'")
    assertInvalidSyntax(cql, "ALTER ROLE r WITH OPTIONS = 99")

def testcreateSyntaxTest(cql):
    assertValidSyntax(cql, "CREATE ROLE r1")
    assertValidSyntax(cql, "CREATE ROLE 'r1'")
    assertValidSyntax(cql, "CREATE ROLE \"r1\"")
    assertValidSyntax(cql, "CREATE ROLE $$r1$$")
    assertValidSyntax(cql, "CREATE ROLE $$ r1 ' x $ x ' $$")
    assertValidSyntax(cql, "CREATE USER u1")
    assertValidSyntax(cql, "CREATE USER 'u1'")
    assertValidSyntax(cql, "CREATE USER $$u1$$")
    assertValidSyntax(cql, "CREATE USER $$ u1 ' x $ x ' $$")
    # user names may not be quoted names
    assertInvalidSyntax(cql, "CREATE USER \"u1\"")

def testdropSyntaxTest(cql):
    assertValidSyntax(cql, "DROP ROLE r1")
    assertValidSyntax(cql, "DROP ROLE 'r1'")
    assertValidSyntax(cql, "DROP ROLE \"r1\"")
    assertValidSyntax(cql, "DROP ROLE $$r1$$")
    assertValidSyntax(cql, "DROP ROLE $$ r1 ' x $ x ' $$")
    assertValidSyntax(cql, "DROP USER u1")
    assertValidSyntax(cql, "DROP USER 'u1'")
    assertValidSyntax(cql, "DROP USER $$u1$$")
    assertValidSyntax(cql, "DROP USER $$ u1 ' x $ x ' $$")
    # user names may not be quoted names
    assertInvalidSyntax(cql, "DROP USER \"u1\"")

# Reproduces SCYLLADB-5144 (ALTER ROLE/USER IF EXISTS).
@pytest.mark.xfail(reason="SCYLLADB-5144")
def testalterSyntaxTest(cql):
    assertValidSyntax(cql, "ALTER ROLE r1 WITH PASSWORD = 'password'")
    assertValidSyntax(cql, "ALTER ROLE 'r1' WITH PASSWORD = 'password'")
    assertValidSyntax(cql, "ALTER ROLE \"r1\" WITH PASSWORD = 'password'")
    assertValidSyntax(cql, "ALTER ROLE $$r1$$ WITH PASSWORD = 'password'")
    assertValidSyntax(cql, "ALTER ROLE $$ r1 ' x $ x ' $$ WITH PASSWORD = 'password'")
    # ALTER has slightly different form for USER (no =)
    assertValidSyntax(cql, "ALTER USER u1 WITH PASSWORD 'password'")
    assertValidSyntax(cql, "ALTER USER 'u1' WITH PASSWORD 'password'")
    assertValidSyntax(cql, "ALTER USER $$u1$$ WITH PASSWORD 'password'")
    assertValidSyntax(cql, "ALTER USER $$ u1 ' x $ x ' $$ WITH PASSWORD 'password'")
    # ALTER with IF EXISTS syntax
    assertValidSyntax(cql, "ALTER ROLE IF EXISTS r1 WITH PASSWORD = 'password'")
    assertValidSyntax(cql, "ALTER USER IF EXISTS u1 WITH PASSWORD 'password'")
    # user names may not be quoted names
    assertInvalidSyntax(cql, "ALTER USER \"u1\" WITH PASSWORD 'password'")
