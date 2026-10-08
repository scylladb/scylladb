# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# The original Java test sets the logged-in user and grants permissions
# through Cassandra's internal APIs, and only checks the authorization of
# each statement, without executing it. In the translation, we create a new
# role, grant it permissions with GRANT and REVOKE statements, and execute
# the statements in a session logged in as this role.

from ...porting import *
from ....util import new_user, new_session
from cassandra.protocol import Unauthorized

REPLICATION = "replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"

# Permissions are cached for a short time (permissions_validity_in_ms), so
# after GRANT or REVOKE, a statement may still see the old permissions for a
# short while. So these functions retry for a while.
def eventually_authorized(fun, timeout_s=10):
    deadline = time.time() + timeout_s
    while True:
        try:
            return fun()
        except Unauthorized:
            if time.time() > deadline:
                raise
            time.sleep(0.01)

# A statement may succeed despite a missing permission only for a short
# while after that permission was revoked (cached_success=True); otherwise
# it should be refused immediately.
def eventually_unauthorized(fun, message, cached_success=False, timeout_s=10):
    deadline = time.time() + timeout_s
    while True:
        try:
            fun()
        except Unauthorized as e:
            # An Unauthorized error about a different function may be the
            # result of a recently-granted permission not yet being seen.
            if re.search(message, str(e)):
                return
            if time.time() > deadline:
                assert re.search(message, str(e)), f"'{message}' not found in '{e}'"
        else:
            if not cached_success or time.time() > deadline:
                pytest.fail("Expected an Unauthorized error, but none was thrown")
        time.sleep(0.01)

# A test's keyspace, table and role, and a session logged in as the role.
class AuthTest:
    def __init__(self, cql, keyspace, table, role, session):
        self.cql = cql
        self.keyspace = keyspace
        self.table = table
        self.role = role
        self.session = session
        self.revoked = False

    # Equivalent of the Java test's createFunction() with KEYSPACE
    def createFunction(self, query):
        name = self.keyspace + "." + unique_name()
        self.cql.execute(query.replace("%s", name, 1))
        return name

    def createSimpleFunction(self):
        return self.createFunction("CREATE FUNCTION %s() " +
                                   "  CALLED ON NULL INPUT " +
                                   "  RETURNS int " +
                                   "  " + java_or_lua(self.cql, "return Integer.valueOf(0);", "return 0"))

    def createSimpleStateFunction(self):
        return self.createFunction("CREATE FUNCTION %s(a int, b int) " +
                                   "CALLED ON NULL INPUT " +
                                   "RETURNS int " +
                                   java_or_lua(self.cql, "return Integer.valueOf( (a != null ? a.intValue() : 0 ) + b.intValue());",
                                               "if a == nil then a = 0 end return a + b"))

    def createSimpleFinalFunction(self):
        return self.createFunction("CREATE FUNCTION %s(a int) " +
                                   "CALLED ON NULL INPUT " +
                                   "RETURNS int " +
                                   java_or_lua(self.cql, "return a;", "return a"))

    def createAggregate(self, query):
        return self.createFunction(query)

    def grantExecuteOnFunction(self, functionName, argTypes=""):
        self.cql.execute(f"GRANT EXECUTE ON FUNCTION {functionName}({argTypes}) TO {self.role}")

    def revokeExecuteOnFunction(self, functionName, argTypes=""):
        self.cql.execute(f"REVOKE EXECUTE ON FUNCTION {functionName}({argTypes}) FROM {self.role}")
        self.revoked = True

    # Equivalent of the Java test's getStatement(cql).authorize(clientState)
    def assertAuthorized(self, cql):
        eventually_authorized(lambda: self.session.execute(cql))

    def assertUnauthorized(self, cql, functionName, argTypes):
        eventually_unauthorized(lambda: self.session.execute(cql),
                                re.escape(f"User {self.role} has no EXECUTE permission on <function {functionName}({argTypes})> or any of its parents"),
                                cached_success=self.revoked)
        self.revoked = False

    def assertPermissionsOnFunction(self, cql, functionName, argTypes=""):
        self.assertUnauthorized(cql, functionName, argTypes)
        self.grantExecuteOnFunction(functionName, argTypes)
        self.assertAuthorized(cql)

    def assertPermissionsOnNestedFunctions(self, innerFunction, outerFunction):
        cql = f"SELECT k, {outerFunction}({innerFunction}()) FROM {self.table} WHERE k=0"
        # fail fast with an UAE on the first function
        self.assertUnauthorized(cql, outerFunction, "int")
        self.grantExecuteOnFunction(outerFunction, "int")

        # after granting execute on the first function, still fail due to the inner function
        self.assertUnauthorized(cql, innerFunction, "")
        self.grantExecuteOnFunction(innerFunction)

        # now execution of both is permitted
        self.assertAuthorized(cql)

    # Create a new table, and grant the test user SELECT and MODIFY on it
    @contextmanager
    def setupTable(self, tableDef):
        with create_table(self.cql, self.keyspace, tableDef) as table:
            # test user needs SELECT & MODIFY on the table regardless of permissions on any function
            self.cql.execute(f"GRANT SELECT ON {table} TO {self.role}")
            self.cql.execute(f"GRANT MODIFY ON {table} TO {self.role}")
            old_table = self.table
            self.table = table
            try:
                yield table
            finally:
                self.table = old_table

# Equivalent of the Java test's setup(): a new keyspace (instead of KEYSPACE)
# with a table, and a new role (instead of "test_role") with SELECT and
# MODIFY permissions on the table, and a session logged in as this role.
@pytest.fixture
def t(cql):
    with create_keyspace(cql, REPLICATION) as keyspace, new_user(cql) as role, new_session(cql, role) as session:
        test = AuthTest(cql, keyspace, None, role, session)
        with test.setupTable("(k int, v1 int, v2 int, PRIMARY KEY (k, v1))"):
            yield test

def functionCall(functionName, *args):
    return f"{functionName}({','.join(args)})"

def aggregateCql(sFunc, fFunc):
    return ("CREATE AGGREGATE %s(int) " +
            "SFUNC " + sFunc.split(".")[1] + " " +
            "STYPE int " +
            "FINALFUNC " + fFunc.split(".")[1] + " " +
            "INITCOND 0")

def testfunctionInSelection(t):
    functionName = t.createSimpleFunction()
    cql = f"SELECT k, {functionCall(functionName)} FROM {t.table} WHERE k = 1;"
    t.assertPermissionsOnFunction(cql, functionName)

# Reproduces #13746 (user-defined functions can only be used in SELECT's
# selection clause)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testfunctionInSelectPKRestriction(t):
    functionName = t.createSimpleFunction()
    cql = f"SELECT * FROM {t.table} WHERE k = {functionCall(functionName)}"
    t.assertPermissionsOnFunction(cql, functionName)

# Reproduces #13746 (user-defined functions can only be used in SELECT's
# selection clause)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testfunctionInSelectClusteringRestriction(t):
    functionName = t.createSimpleFunction()
    cql = f"SELECT * FROM {t.table} WHERE k = 0 AND v1 = {functionCall(functionName)}"
    t.assertPermissionsOnFunction(cql, functionName)

# Reproduces #13746 (user-defined functions can only be used in SELECT's
# selection clause)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testfunctionInSelectInRestriction(t):
    functionName = t.createSimpleFunction()
    cql = f"SELECT * FROM {t.table} WHERE k IN ({functionCall(functionName)}, {functionCall(functionName)})"
    t.assertPermissionsOnFunction(cql, functionName)

# Reproduces #13746 (user-defined functions can only be used in SELECT's
# selection clause)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testfunctionInSelectMultiColumnInRestriction(t):
    with t.setupTable("(k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2))"):
        functionName = t.createSimpleFunction()
        cql = f"SELECT * FROM {t.table} WHERE k=0 AND (v1, v2) IN (({functionCall(functionName)}, {functionCall(functionName)}))"
        t.assertPermissionsOnFunction(cql, functionName)

# Reproduces #13746 (user-defined functions can only be used in SELECT's
# selection clause)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testfunctionInSelectMultiColumnEQRestriction(t):
    with t.setupTable("(k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2))"):
        functionName = t.createSimpleFunction()
        cql = f"SELECT * FROM {t.table} WHERE k=0 AND (v1, v2) = ({functionCall(functionName)}, {functionCall(functionName)})"
        t.assertPermissionsOnFunction(cql, functionName)

# Reproduces #13746 (user-defined functions can only be used in SELECT's
# selection clause)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testfunctionInSelectMultiColumnSliceRestriction(t):
    with t.setupTable("(k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2))"):
        functionName = t.createSimpleFunction()
        cql = f"SELECT * FROM {t.table} WHERE k=0 AND (v1, v2) < ({functionCall(functionName)}, {functionCall(functionName)})"
        t.assertPermissionsOnFunction(cql, functionName)

# Reproduces #13746 (user-defined functions can only be used in SELECT's
# selection clause)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testfunctionInSelectTokenEQRestriction(t):
    functionName = t.createSimpleFunction()
    cql = f"SELECT * FROM {t.table} WHERE token(k) = token({functionCall(functionName)})"
    t.assertPermissionsOnFunction(cql, functionName)

# Reproduces #13746 (user-defined functions can only be used in SELECT's
# selection clause)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testfunctionInSelectTokenSliceRestriction(t):
    functionName = t.createSimpleFunction()
    cql = f"SELECT * FROM {t.table} WHERE token(k) < token({functionCall(functionName)})"
    t.assertPermissionsOnFunction(cql, functionName)
