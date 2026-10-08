# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Although this file tests Java user-defined functions, most of its tests
# check CQL features (types, schema changes, etc.) through such functions, so
# they are translated: like other translated tests of user-defined functions,
# the functions are written in Java when running on Cassandra and in Lua when
# running on Scylla, with equivalent bodies. Tests of the Java language itself
# are not translated.

from ...porting import *
from cassandra.protocol import FunctionFailure

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

def shortFunctionName(name):
    return name.split(".")[1]

# Like java_or_lua(), but quotes the function body with $$ instead of single
# quotes, like many of the original Java tests do.
def java_or_lua_dollar(cql, java_body, lua_body):
    if is_scylla(cql):
        return "LANGUAGE lua AS $$" + lua_body + "$$"
    return "LANGUAGE java AS $$" + java_body + "$$"

def language(cql):
    return "lua" if is_scylla(cql) else "java"

def testJavaFunctionNoParameters(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key int primary key, val double)") as table:
        functionBody = "\n  return 1L;\n" if not is_scylla(cql) else "\n  return 1\n"

        fName = createFunction(cql, test_keyspace,
                               "CREATE OR REPLACE FUNCTION %s() " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS bigint " +
                               "LANGUAGE " + language(cql) + "\n" +
                               "AS '" + functionBody + "';")
        try:
            assert_rows(execute(cql, table, "SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                                test_keyspace, shortFunctionName(fName)),
                        row(language(cql), functionBody))

            execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1.0)
            execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2.0)
            execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3.0)
            assert_rows(execute(cql, table, "SELECT key, val, " + fName + "() FROM %s"),
                        row(1, 1.0, 1),
                        row(2, 2.0, 1),
                        row(3, 3.0, 1))
        finally:
            cql.execute("DROP FUNCTION " + fName)

# The tests testJavaFunctionInvalidBodies and testJavaFunctionInvalidReturn
# were not translated, because they check errors from compiling the Java
# function body.

def testJavaFunctionArgumentTypeMismatch(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE, create_table(cql, test_keyspace, "(key int primary key, val bigint)") as table:
        fName = createFunction(cql, KEYSPACE,
                               "CREATE OR REPLACE FUNCTION %s(val double)" +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS double " +
                               java_or_lua(cql, "return Double.valueOf(val);", "return val") + ";")

        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3)
        assert_invalid_message(cql, table, "val cannot be passed as argument 0 of function",
                               "SELECT key, val, " + fName + "(val) FROM %s")

# The original Java functions in this file use Math.sin(), but Scylla's Lua
# has no math library. The function's value isn't important for these tests,
# so we use a simpler function in both languages.
def sin(x):
    return x * 2

def testJavaFunction(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE, create_table(cql, test_keyspace, "(key int primary key, val double)") as table:
        if is_scylla(cql):
            functionBody = ("\n" +
                            "  -- parameter val is a Lua number\n" +
                            "  --[[ return type is a Lua number ]]\n" +
                            "  if val == nil then\n" +
                            "    return nil\n" +
                            "  end\n" +
                            "  return val * 2\n")
        else:
            functionBody = ("\n" +
                            "  // parameter val is of type java.lang.Double\n" +
                            "  /* return type is of type java.lang.Double */\n" +
                            "  if (val == null) {\n" +
                            "    return null;\n" +
                            "  }\n" +
                            "  return val * 2;\n")

        fName = createFunction(cql, KEYSPACE,
                               "CREATE OR REPLACE FUNCTION %s(val double) " +
                               "CALLED ON NULL INPUT " +
                               "RETURNS double " +
                               "LANGUAGE " + language(cql) + " " +
                               "AS '" + functionBody + "';")

        assert_rows(execute(cql, table, "SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                            KEYSPACE, shortFunctionName(fName)),
                    row(language(cql), functionBody))

        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1.0)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2.0)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3.0)
        assert_rows(execute(cql, table, "SELECT key, val, " + fName + "(val) FROM %s"),
                    row(1, 1.0, sin(1.0)),
                    row(2, 2.0, sin(2.0)),
                    row(3, 3.0, sin(3.0)))

def testJavaFunctionCounter(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE, create_table(cql, test_keyspace, "(key int primary key, val counter)") as table:
        fName = createFunction(cql, KEYSPACE,
                               "CREATE OR REPLACE FUNCTION %s(val counter) " +
                               "CALLED ON NULL INPUT " +
                               "RETURNS bigint " +
                               java_or_lua(cql, "return val + 1;", "return val + 1") + ";")

        execute(cql, table, "UPDATE %s SET val = val + 1 WHERE key = 1")
        assert_rows(execute(cql, table, "SELECT key, val, " + fName + "(val) FROM %s"),
                    row(1, 1, 2))
        execute(cql, table, "UPDATE %s SET val = val + 1 WHERE key = 1")
        assert_rows(execute(cql, table, "SELECT key, val, " + fName + "(val) FROM %s"),
                    row(1, 2, 3))
        execute(cql, table, "UPDATE %s SET val = val + 2 WHERE key = 1")
        assert_rows(execute(cql, table, "SELECT key, val, " + fName + "(val) FROM %s"),
                    row(1, 4, 5))
        execute(cql, table, "UPDATE %s SET val = val - 2 WHERE key = 1")
        assert_rows(execute(cql, table, "SELECT key, val, " + fName + "(val) FROM %s"),
                    row(1, 2, 3))

def testJavaKeyspaceFunction(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_table(cql, test_keyspace, "(key int primary key, val double)") as table:
        if is_scylla(cql):
            functionBody = ("\n" +
                            "  -- parameter val is a Lua number\n" +
                            "  --[[ return type is a Lua number ]]\n" +
                            "  if val == nil then\n" +
                            "    return nil\n" +
                            "  end\n" +
                            "  return val * 2\n")
        else:
            functionBody = ("\n" +
                            "  // parameter val is of type java.lang.Double\n" +
                            "  /* return type is of type java.lang.Double */\n" +
                            "  if (val == null) {\n" +
                            "    return null;\n" +
                            "  }\n" +
                            "  return val * 2;\n")

        fName = createFunction(cql, KEYSPACE_PER_TEST,
                               "CREATE OR REPLACE FUNCTION %s(val double) " +
                               "CALLED ON NULL INPUT " +
                               "RETURNS double " +
                               "LANGUAGE " + language(cql) + " " +
                               "AS '" + functionBody + "';")

        assert_rows(execute(cql, table, "SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                            KEYSPACE_PER_TEST, shortFunctionName(fName)),
                    row(language(cql), functionBody))

        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1.0)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2.0)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3.0)
        assert_rows(execute(cql, table, "SELECT key, val, " + fName + "(val) FROM %s"),
                    row(1, 1.0, sin(1.0)),
                    row(2, 2.0, sin(2.0)),
                    row(3, 3.0, sin(3.0)))

# Reproduces SCYLLADB-5159 (wrong error code for a failing function)
@pytest.mark.xfail(reason="SCYLLADB-5159")
def testJavaRuntimeException(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_table(cql, test_keyspace, "(key int primary key, val double)") as table:
        if is_scylla(cql):
            functionBody = "\n  error(\"oh no!\")\n"
        else:
            functionBody = "\n  throw new RuntimeException(\"oh no!\");\n"

        fName = createFunction(cql, KEYSPACE_PER_TEST,
                               "CREATE OR REPLACE FUNCTION %s(val double) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS double " +
                               "LANGUAGE " + language(cql) + "\n" +
                               "AS '" + functionBody + "';")

        assert_rows(execute(cql, table, "SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                            KEYSPACE_PER_TEST, shortFunctionName(fName)),
                    row(language(cql), functionBody))

        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1.0)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2.0)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3.0)

        # function throws a RuntimeException which is wrapped by FunctionExecutionException
        # (On Scylla, the Lua function raises an error, and the error message
        # doesn't mention java.lang.RuntimeException.)
        assert_invalid_throw_message_re(cql, table, "java.lang.RuntimeException: oh no|oh no!", FunctionFailure,
                                        "SELECT key, val, " + fName + "(val) FROM %s")

def testJavaDollarQuotedFunction(cql):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST:
        if is_scylla(cql):
            functionBody = ("\n" +
                            "  -- parameter val is a Lua number\n" +
                            "  --[[ return type is a Lua string ]]\n" +
                            "  if input == nil then\n" +
                            "    return nil\n" +
                            "  end\n" +
                            "  return \"'\"..tostring(input * 2)..'\\''\n")
        else:
            functionBody = ("\n" +
                            "  // parameter val is of type java.lang.Double\n" +
                            "  /* return type is of type java.lang.Double */\n" +
                            "  if (input == null) {\n" +
                            "    return null;\n" +
                            "  }\n" +
                            "  return \"'\"+(input * 2)+'\\'';\n")

        fName = createFunction(cql, KEYSPACE_PER_TEST,
                               "CREATE FUNCTION %s( input double ) " +
                               "CALLED ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE " + language(cql) + "\n" +
                               "AS $$" + functionBody + "$$;")

        assert_rows(execute(cql, KEYSPACE_PER_TEST, "SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                            KEYSPACE_PER_TEST, shortFunctionName(fName)),
                    row(language(cql), functionBody))
