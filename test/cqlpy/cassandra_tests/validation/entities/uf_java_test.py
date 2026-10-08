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
