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
from cassandra.protocol import Unauthorized

# Functions are created in a new keyspace (KEYSPACE_PER_TEST in the original
# Java test), which is dropped at the end of the test.
REPLICATION = "replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"

# Equivalent of the Java test's createFunction(): replaces the first "%s" in
# the query by a new unique function name in the given keyspace, creates the
# function and returns its name.
def createFunction(cql, keyspace, query):
    name = keyspace + "." + unique_name()
    cql.execute(query.replace("%s", name, 1))
    return name

def createFunctionOverload(cql, name, query):
    cql.execute(query.replace("%s", name, 1))

def shortFunctionName(name):
    return name.split(".")[1]

# Scylla's error messages for dropping a non-existent function are different
# from Cassandra's: "No function named ks.f found" when no argument types are
# given, and "User function ks.f(int, text) doesn't exist" when they are. So
# we accept either.
def doesntExistMessage(name):
    return (re.escape(f"Function '{name}' doesn't exist") + "|" +
            re.escape(f"No function named {name} found") + "|" +
            re.escape(f"User function {name} doesn't exist"))

# Cassandra rejects modifying functions in the system keyspace with an
# InvalidRequest error "System keyspace 'system' is not user-modifiable",
# while Scylla returns an Unauthorized error "system keyspace is not
# user-modifiable.", so we accept either.
def assertSystemKeyspaceNotModifiable(session, cmd):
    with pytest.raises((InvalidRequest, Unauthorized), match="(?i)system keyspace.* is not user-modifiable"):
        session.execute(cmd)

def testNonExistingOnes(cql, test_keyspace):
    KEYSPACE = test_keyspace
    assert_invalid_throw_message_re(cql, KEYSPACE, doesntExistMessage(f"{KEYSPACE}.func_does_not_exist"),
                                    InvalidRequest,
                                    "DROP FUNCTION " + KEYSPACE + ".func_does_not_exist")

    assert_invalid_throw_message_re(cql, KEYSPACE, doesntExistMessage(f"{KEYSPACE}.func_does_not_exist(int, text)"),
                                    InvalidRequest,
                                    "DROP FUNCTION " + KEYSPACE + ".func_does_not_exist(int, text)")

    assert_invalid_throw_message_re(cql, KEYSPACE, doesntExistMessage("keyspace_does_not_exist.func_does_not_exist"),
                                    InvalidRequest,
                                    "DROP FUNCTION keyspace_does_not_exist.func_does_not_exist")

    assert_invalid_throw_message_re(cql, KEYSPACE, doesntExistMessage("keyspace_does_not_exist.func_does_not_exist(int, text)"),
                                    InvalidRequest,
                                    "DROP FUNCTION keyspace_does_not_exist.func_does_not_exist(int, text)")

    execute(cql, KEYSPACE, "DROP FUNCTION IF EXISTS " + KEYSPACE + ".func_does_not_exist")
    execute(cql, KEYSPACE, "DROP FUNCTION IF EXISTS " + KEYSPACE + ".func_does_not_exist(int,text)")
    execute(cql, KEYSPACE, "DROP FUNCTION IF EXISTS keyspace_does_not_exist.func_does_not_exist")
    execute(cql, KEYSPACE, "DROP FUNCTION IF EXISTS keyspace_does_not_exist.func_does_not_exist(int,text)")

def testFunctionDropOnKeyspaceDrop(cql):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST:
        fSin = createFunction(cql, KEYSPACE_PER_TEST,
                              "CREATE FUNCTION %s ( input double ) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS double " +
                              java_or_lua(cql, "return Double.valueOf(Math.sin(input.doubleValue()));",
                                          "return input"))

        # The Java test also checks Cassandra's internal schema object
        # (Schema.instance.getUserFunctions()), which we can't do. But
        # system_schema.functions is checked through CQL.
        assert_rows(execute(cql, KEYSPACE_PER_TEST, "SELECT function_name, language FROM system_schema.functions WHERE keyspace_name=?", KEYSPACE_PER_TEST),
                    row(shortFunctionName(fSin), "lua" if is_scylla(cql) else "java"))

    assert_empty(execute(cql, KEYSPACE_PER_TEST, "SELECT function_name, language FROM system_schema.functions WHERE keyspace_name=?", KEYSPACE_PER_TEST))

# The tests testFunctionDropPreparedStatement,
# testDropFunctionDropsPreparedStatementsWithDelayedValues and
# testDropKeyspaceContainingFunctionDropsPreparedStatementsWithDelayedValues
# were not translated, because they check Cassandra's internal cache of
# prepared statements.

# Reproduces #13746 (a function in WHERE or in INSERT values)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testFunctionExecution(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_table(cql, test_keyspace, "(v text PRIMARY KEY)") as table:
        execute(cql, table, "INSERT INTO %s(v) VALUES (?)", "aaa")

        fRepeat = createFunction(cql, KEYSPACE_PER_TEST,
                                 "CREATE FUNCTION %s(v text, n int) " +
                                 "RETURNS NULL ON NULL INPUT " +
                                 "RETURNS text " +
                                 java_or_lua(cql,
                                             "StringBuilder sb = new StringBuilder();\n" +
                                             "    for (int i = 0; i < n; i++)\n" +
                                             "        sb.append(v);\n" +
                                             "    return sb.toString();",
                                             "local s = \"\"\n" +
                                             "    for i = 1, n do\n" +
                                             "        s = s .. v\n" +
                                             "    end\n" +
                                             "    return s"))

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE v=" + fRepeat + "(?, ?)", "a", 3), row("aaa"))
        assert_empty(execute(cql, table, "SELECT v FROM %s WHERE v=" + fRepeat + "(?, ?)", "a", 2))

# Reproduces #13746 (a function in WHERE or in INSERT values)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testFunctionExecutionWithReversedTypeAsOutput(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_table(cql, test_keyspace, "(k int, v text, PRIMARY KEY(k, v)) WITH CLUSTERING ORDER BY (v DESC)") as table:
        fRepeat = createFunction(cql, KEYSPACE_PER_TEST,
                                 "CREATE FUNCTION %s(v text) " +
                                 "RETURNS NULL ON NULL INPUT " +
                                 "RETURNS text " +
                                 java_or_lua(cql, "return v + v;", "return v .. v"))

        execute(cql, table, "INSERT INTO %s(k, v) VALUES (?, " + fRepeat + "(?))", 1, "a")
