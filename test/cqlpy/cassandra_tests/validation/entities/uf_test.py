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
from ....util import new_cql
from cassandra.protocol import FunctionFailure, ResultMessage, Unauthorized

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

# The Java test's assertSchemaChange() checks the SCHEMA_CHANGE result of a
# schema-changing statement. The Python driver doesn't expose this result,
# so we use a new session whose protocol handler records it.
def assertSchemaChange(cql, query, change, target, keyspace, name, *argTypes):
    with new_cql(cql) as session:
        events = []
        class recording_handler(session.client_protocol_handler):
            @classmethod
            def decode_message(cls, *args, **kwargs):
                msg = super().decode_message(*args, **kwargs)
                if isinstance(msg, ResultMessage) and msg.schema_change_event:
                    events.append(msg.schema_change_event)
                return msg
        session.client_protocol_handler = recording_handler
        session.execute(query)
    assert len(events) == 1
    event = events[0]
    assert event['change_type'] == change
    assert event['target_type'] == target
    assert event['keyspace'] == keyspace
    assert event['function'].name == name
    # Scylla reports a tuple nested in a collection as frozen<tuple<...>>,
    # while Cassandra omits the (implied) frozen<>.
    assert [t.replace("frozen<tuple<int, int>>", "tuple<int, int>") for t in event['function'].argument_types] == list(argTypes)

# Reproduces SCYLLADB-5168 (CREATE OR REPLACE FUNCTION of an existing function
# reports CREATED instead of UPDATED)
@pytest.mark.xfail(reason="SCYLLADB-5168")
def testSchemaChange(cql):
    with create_keyspace(cql, REPLICATION) as KEYSPACE:
        functionName = unique_name()
        f = KEYSPACE + "." + functionName

        assertSchemaChange(cql, "CREATE OR REPLACE FUNCTION " + f + "(state double, val double)" +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS double " +
                           java_or_lua(cql, "return Double.valueOf(Math.max(state, val));",
                                       "if state > val then return state end return val"),
                           "CREATED",
                           "FUNCTION",
                           KEYSPACE, functionName,
                           "double", "double")

        assertSchemaChange(cql, "CREATE OR REPLACE FUNCTION " + f + "(state int, val int) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS int " +
                           java_or_lua(cql, "return Integer.valueOf(Math.max(state, val));",
                                       "if state > val then return state end return val"),
                           "CREATED",
                           "FUNCTION",
                           KEYSPACE, functionName,
                           "int", "int")

        assertSchemaChange(cql, "CREATE OR REPLACE FUNCTION " + f + "(state int, val int) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS int " +
                           java_or_lua(cql, "return Integer.valueOf(Math.min(state, val));",
                                       "if state < val then return state end return val"),
                           "UPDATED",
                           "FUNCTION",
                           KEYSPACE, functionName,
                           "int", "int")

        assertSchemaChange(cql, "DROP FUNCTION " + f + "(double, double)",
                           "DROPPED", "FUNCTION",
                           KEYSPACE, functionName,
                           "double", "double")

        # The function with nested tuple should be created without throwing InvalidRequestException. See CASSANDRA-15857
        flName = unique_name()
        fl = KEYSPACE + "." + flName

        assertSchemaChange(cql, "CREATE OR REPLACE FUNCTION " + fl + "(state list<tuple<int, int>>, val double) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS double " +
                           java_or_lua(cql, "return val;", "return val"),
                           "CREATED", "FUNCTION",
                           KEYSPACE, flName,
                           "list<tuple<int, int>>", "double")

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

# The original test uses Java's Math.sin() as the function, but Scylla's Lua
# has no math library. The function's value isn't important for this test,
# so we use a simpler function in both languages.
SIN_JAVA = "return input * 2;"
SIN_LUA = "return input * 2"
def sin(x):
    return x * 2

# Reproduces SCYLLADB-5169 (CREATE OR REPLACE FUNCTION should not change the
# return type or null-input behavior)
@pytest.mark.xfail(reason="SCYLLADB-5169")
def testFunctionCreationAndDrop(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_table(cql, test_keyspace, "(key int PRIMARY KEY, d double)") as table:
        execute(cql, table, "INSERT INTO %s(key, d) VALUES (?, ?)", 1, 1.0)
        execute(cql, table, "INSERT INTO %s(key, d) VALUES (?, ?)", 2, 2.0)
        execute(cql, table, "INSERT INTO %s(key, d) VALUES (?, ?)", 3, 3.0)

        # simple creation
        fSin = createFunction(cql, KEYSPACE_PER_TEST,
                              "CREATE FUNCTION %s ( input double ) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS double " +
                              java_or_lua(cql, SIN_JAVA, SIN_LUA))
        # check we can't recreate the same function
        assert_invalid_message(cql, table, "already exists",
                               "CREATE FUNCTION " + fSin + " ( input double ) " +
                               "CALLED ON NULL INPUT " +
                               "RETURNS double " +
                               java_or_lua(cql, SIN_JAVA, SIN_LUA))

        # but that it doesn't comply with "IF NOT EXISTS"
        execute(cql, table, "CREATE FUNCTION IF NOT EXISTS " + fSin + " ( input double ) " +
                "CALLED ON NULL INPUT " +
                "RETURNS double " +
                java_or_lua(cql, SIN_JAVA, SIN_LUA))

        # Validate that it works as expected
        assert_rows(execute(cql, table, "SELECT key, " + fSin + "(d) FROM %s"),
                    row(1, sin(1.0)),
                    row(2, sin(2.0)),
                    row(3, sin(3.0)))

        # Replace the method with incompatible return type
        assert_invalid_message(cql, table, "the new return type text is not compatible with the return type double of existing function",
                               "CREATE OR REPLACE FUNCTION " + fSin + " ( input double ) " +
                               "CALLED ON NULL INPUT " +
                               "RETURNS text " +
                               java_or_lua(cql, 'return "42d";', 'return "42d"'))

        # proper replacement
        execute(cql, table, "CREATE OR REPLACE FUNCTION " + fSin + " ( input double ) " +
                "CALLED ON NULL INPUT " +
                "RETURNS double " +
                java_or_lua(cql, "return Double.valueOf(42d);", "return 42"))

        # Validate the method as been replaced
        assert_rows(execute(cql, table, "SELECT key, " + fSin + "(d) FROM %s"),
                    row(1, 42.0),
                    row(2, 42.0),
                    row(3, 42.0))

        # same function but other keyspace
        fSin2 = createFunction(cql, test_keyspace,
                               "CREATE FUNCTION %s ( input double ) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS double " +
                               java_or_lua(cql, SIN_JAVA, SIN_LUA))
        assert_rows(execute(cql, table, "SELECT key, " + fSin2 + "(d) FROM %s"),
                    row(1, sin(1.0)),
                    row(2, sin(2.0)),
                    row(3, sin(3.0)))

        # Drop
        execute(cql, table, "DROP FUNCTION " + fSin)
        execute(cql, table, "DROP FUNCTION " + fSin2)

        # Drop unexisting function
        assert_invalid_message_re(cql, table, doesntExistMessage(fSin), "DROP FUNCTION " + fSin)
        # but don't complain with "IF EXISTS"
        execute(cql, table, "DROP FUNCTION IF EXISTS " + fSin)

        # can't drop native functions
        # Our session has no keyspace, and Scylla doesn't look for native
        # functions in DROP FUNCTION, so it fails with "No keyspace has been
        # specified" instead.
        assert_invalid_message_re(cql, table, "(?i)system keyspace.* is not user-modifiable|No keyspace has been specified", "DROP FUNCTION to_timestamp")
        assert_invalid_message_re(cql, table, "(?i)system keyspace.* is not user-modifiable|No keyspace has been specified", "DROP FUNCTION uuid")

        # sin() no longer exists
        assert_invalid_message(cql, table, "Unknown function", "SELECT key, sin(d) FROM %s")

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

# Reproduces #13746 (a function in WHERE or in INSERT values)
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testFunctionOverloading(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_table(cql, test_keyspace, "(k text PRIMARY KEY, v int)") as table:
        execute(cql, table, "INSERT INTO %s(k, v) VALUES (?, ?)", "f2", 1)

        fOverload = createFunction(cql, KEYSPACE_PER_TEST,
                                   "CREATE FUNCTION %s ( input varchar ) " +
                                   "RETURNS NULL ON NULL INPUT " +
                                   "RETURNS text " +
                                   java_or_lua(cql, 'return "f1";', 'return "f1"'))
        createFunctionOverload(cql, fOverload,
                               "CREATE OR REPLACE FUNCTION %s(i int) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               java_or_lua(cql, 'return "f2";', 'return "f2"'))
        createFunctionOverload(cql, fOverload,
                               "CREATE OR REPLACE FUNCTION %s(v1 text, v2 text) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               java_or_lua(cql, 'return "f3";', 'return "f3"'))
        createFunctionOverload(cql, fOverload,
                               "CREATE OR REPLACE FUNCTION %s(v ascii) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               java_or_lua(cql, 'return "f1";', 'return "f1"'))

        # text == varchar, so this should be considered as a duplicate
        assert_invalid_message(cql, table, "already exists",
                               "CREATE FUNCTION " + fOverload + "(v varchar) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               java_or_lua(cql, 'return "f1";', 'return "f1"'))

        assert_rows(execute(cql, table, "SELECT " + fOverload + "(k), " + fOverload + "(v), " + fOverload + "(k, k) FROM %s"),
                    row("f1", "f2", "f3"))

        # This shouldn't work if we use preparation since there no way to know which overload to use
        assert_invalid_message(cql, table, "Ambiguous call to function", "SELECT v FROM %s WHERE k = " + fOverload + "(?)", "foo")

        # but those should since we specifically cast
        assert_empty(execute(cql, table, "SELECT v FROM %s WHERE k = " + fOverload + "((text)?)", "foo"))
        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = " + fOverload + "((int)?)", 3), row(1))
        assert_empty(execute(cql, table, "SELECT v FROM %s WHERE k = " + fOverload + "((ascii)?)", "foo"))
        # And since varchar == text, this should work too
        assert_empty(execute(cql, table, "SELECT v FROM %s WHERE k = " + fOverload + "((varchar)?)", "foo"))

        # no such functions exist...
        assert_invalid_message_re(cql, table, doesntExistMessage(f"{fOverload}(boolean)"), "DROP FUNCTION " + fOverload + "(boolean)")
        assert_invalid_message_re(cql, table, doesntExistMessage(f"{fOverload}(bigint)"), "DROP FUNCTION " + fOverload + "(bigint)")

        # 'overloaded' has multiple overloads - so it has to fail (CASSANDRA-7812)
        # Scylla's error message is "There are multiple functions named ..."
        assert_invalid_message_re(cql, table, "matches multiple function definitions|There are multiple functions named", "DROP FUNCTION " + fOverload)
        execute(cql, table, "DROP FUNCTION " + fOverload + "(varchar)")
        assert_invalid_message(cql, table, "none of its type signatures match", "SELECT v FROM %s WHERE k = " + fOverload + "((text)?)", "foo")
        execute(cql, table, "DROP FUNCTION " + fOverload + "(text, text)")
        assert_invalid_message(cql, table, "none of its type signatures match", "SELECT v FROM %s WHERE k = " + fOverload + "((text)?,(text)?)", "foo", "bar")
        execute(cql, table, "DROP FUNCTION " + fOverload + "(ascii)")
        assert_invalid_message(cql, table, "cannot be passed as argument 0 of function", "SELECT v FROM %s WHERE k = " + fOverload + "((ascii)?)", "foo")
        # single-int-overload must still work
        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = " + fOverload + "((int)?)", 3), row(1))
        # overloaded has just one overload now - so the following DROP FUNCTION is not ambigious (CASSANDRA-7812)
        execute(cql, table, "DROP FUNCTION " + fOverload)

# The Java function in this test has Java comments in its body. The Lua
# function has the equivalent Lua comments.
def testCreateOrReplaceJavaFunction(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_table(cql, test_keyspace, "(key int primary key, val double)") as table:
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1.0)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2.0)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3.0)

        fName = createFunction(cql, KEYSPACE_PER_TEST,
                               "CREATE FUNCTION %s( input double ) " +
                               "CALLED ON NULL INPUT " +
                               "RETURNS double " +
                               java_or_lua(cql,
                                           "\n" +
                                           "  // parameter val is of type java.lang.Double\n" +
                                           "  /* return type is of type java.lang.Double */\n" +
                                           "  if (input == null) {\n" +
                                           "    return null;\n" +
                                           "  }\n" +
                                           "  return input * 2;\n",
                                           "\n" +
                                           "  -- parameter val is a Lua number\n" +
                                           "  --[[ return type is a Lua number ]]\n" +
                                           "  if input == nil then\n" +
                                           "    return nil\n" +
                                           "  end\n" +
                                           "  return input * 2\n") + ";")

        # just check created function
        assert_rows(execute(cql, table, "SELECT key, val, " + fName + "(val) FROM %s"),
                    row(1, 1.0, sin(1.0)),
                    row(2, 2.0, sin(2.0)),
                    row(3, 3.0, sin(3.0)))

        execute(cql, table, "CREATE OR REPLACE FUNCTION " + fName + "( input double ) " +
                "CALLED ON NULL INPUT " +
                "RETURNS double " +
                java_or_lua(cql, "\n  return input;\n", "\n  return input\n") + ";")

        # check if replaced function returns correct result
        assert_rows(execute(cql, table, "SELECT key, val, " + fName + "(val) FROM %s"),
                    row(1, 1.0, 1.0),
                    row(2, 2.0, 2.0),
                    row(3, 3.0, 3.0))

def testFunctionInTargetKeyspace(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_table(cql, test_keyspace, "(key int primary key, val double)") as table:
        execute(cql, table, "CREATE TABLE " + KEYSPACE_PER_TEST + ".second_tab (key int primary key, val double)")

        fName = createFunction(cql, KEYSPACE_PER_TEST,
                               "CREATE OR REPLACE FUNCTION %s(val double) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS double " +
                               java_or_lua(cql, "return Double.valueOf(val);", "return val") + ";")

        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1.0)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2.0)
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3.0)
        assert_invalid_message(cql, table, "Unknown function",
                               "SELECT key, val, " + shortFunctionName(fName) + "(val) FROM %s")

        execute(cql, table, "INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 1, 1.0)
        execute(cql, table, "INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 2, 2.0)
        execute(cql, table, "INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 3, 3.0)
        assert_rows(execute(cql, table, "SELECT key, val, " + fName + "(val) FROM " + KEYSPACE_PER_TEST + ".second_tab"),
                    row(1, 1.0, 1.0),
                    row(2, 2.0, 2.0),
                    row(3, 3.0, 3.0))

def testFunctionWithReservedName(cql):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST:
        execute(cql, KEYSPACE_PER_TEST, "CREATE TABLE " + KEYSPACE_PER_TEST + ".second_tab (key int primary key, val double)")

        fName = createFunction(cql, KEYSPACE_PER_TEST,
                               "CREATE OR REPLACE FUNCTION %s() " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS timestamp " +
                               java_or_lua(cql, "return null;", "return nil") + ";")

        execute(cql, KEYSPACE_PER_TEST, "INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 1, 1.0)
        execute(cql, KEYSPACE_PER_TEST, "INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 2, 2.0)
        execute(cql, KEYSPACE_PER_TEST, "INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 3, 3.0)

        # ensure that system now() is executed
        rows = list(execute(cql, KEYSPACE_PER_TEST, "SELECT key, val, now() FROM " + KEYSPACE_PER_TEST + ".second_tab"))
        assert len(rows) == 3
        assert rows[0][2] is not None

        # ensure that KEYSPACE_PER_TEST's now() is executed
        rows = list(execute(cql, KEYSPACE_PER_TEST, "SELECT key, val, " + fName + "() FROM " + KEYSPACE_PER_TEST + ".second_tab"))
        assert len(rows) == 3
        assert rows[0][2] is None

def testFunctionInSystemKS(cql, test_keyspace):
    KEYSPACE = test_keyspace
    try:
        execute(cql, KEYSPACE, "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".to_timestamp(val timeuuid) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS timestamp " +
                java_or_lua(cql, "return null;", "return nil") + ";")

        assertSystemKeyspaceNotModifiable(cql, "CREATE OR REPLACE FUNCTION system.jnft(val double) " +
                                               "RETURNS NULL ON NULL INPUT " +
                                               "RETURNS double " +
                                               java_or_lua(cql, "return null;", "return nil") + ";")
        assertSystemKeyspaceNotModifiable(cql, "CREATE OR REPLACE FUNCTION system.to_timestamp(val timeuuid) " +
                                               "RETURNS NULL ON NULL INPUT " +
                                               "RETURNS timestamp " +
                                               java_or_lua(cql, "return null;", "return nil") + ";")
        assertSystemKeyspaceNotModifiable(cql, "DROP FUNCTION system.now")

        # KS for executeLocally() is system
        # (The Java test runs these statements without a keyspace, using
        # executeLocally(), whose keyspace is "system". We use a new session
        # with "USE system" instead.)
        with new_cql(cql) as session:
            session.execute("USE system")
            assertSystemKeyspaceNotModifiable(session, "CREATE OR REPLACE FUNCTION jnft(val double) " +
                                                       "RETURNS NULL ON NULL INPUT " +
                                                       "RETURNS double " +
                                                       java_or_lua(cql, "return null;", "return nil") + ";")
            assertSystemKeyspaceNotModifiable(session, "CREATE OR REPLACE FUNCTION to_timestamp(val timeuuid) " +
                                                       "RETURNS NULL ON NULL INPUT " +
                                                       "RETURNS timestamp " +
                                                       java_or_lua(cql, "return null;", "return nil") + ";")
            assertSystemKeyspaceNotModifiable(session, "DROP FUNCTION now")
    finally:
        execute(cql, KEYSPACE, "DROP FUNCTION IF EXISTS " + KEYSPACE + ".to_timestamp")

# Reproduces SCYLLADB-5170 (CREATE FUNCTION in a non-existent keyspace fails
# with a server error)
@pytest.mark.xfail(reason="SCYLLADB-5170")
def testFunctionNonExistingKeyspace(cql):
    assert_invalid_message(cql, "", "Keyspace 'this_ks_does_not_exist' doesn't exist",
                           "CREATE OR REPLACE FUNCTION this_ks_does_not_exist.jnft(val double) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS double " +
                           java_or_lua(cql, "return null;", "return nil") + ";")

# Reproduces SCYLLADB-5170 (CREATE FUNCTION in a non-existent keyspace fails
# with a server error)
@pytest.mark.xfail(reason="SCYLLADB-5170")
def testFunctionAfterOnDropKeyspace(cql):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST:
        pass

    assert_invalid_message(cql, "", "Keyspace '" + KEYSPACE_PER_TEST + "' doesn't exist",
                           "CREATE OR REPLACE FUNCTION " + KEYSPACE_PER_TEST + ".jnft(val double) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS double " +
                           java_or_lua(cql, "return null;", "return nil") + ";")

def testWrongKeyspace(cql, test_keyspace):
    KEYSPACE = test_keyspace
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_type(cql, KEYSPACE, "(txt text, i int)") as type:
        assert_invalid_message(cql, KEYSPACE, f"Statement on keyspace {KEYSPACE_PER_TEST} cannot refer to a user type in keyspace {KEYSPACE}; user types can only be used in the keyspace they are defined in",
                               "CREATE FUNCTION " + KEYSPACE_PER_TEST + ".test_wrong_ks( val int ) " +
                               "CALLED ON NULL INPUT " +
                               "RETURNS " + type + " " +
                               java_or_lua(cql, "return val;", "return val") + ";")

        assert_invalid_message(cql, KEYSPACE, f"Statement on keyspace {KEYSPACE_PER_TEST} cannot refer to a user type in keyspace {KEYSPACE}; user types can only be used in the keyspace they are defined in",
                               "CREATE FUNCTION " + KEYSPACE_PER_TEST + ".test_wrong_ks( val " + type + " ) " +
                               "CALLED ON NULL INPUT " +
                               "RETURNS int " +
                               java_or_lua(cql, "return val;", "return val") + ";")

def testUserTypeDrop(cql):
    # The type, table and function are created in a new keyspace instead of
    # KEYSPACE, so that they will all be dropped at the end of the test.
    with create_keyspace(cql, REPLICATION) as KEYSPACE:
        type = KEYSPACE + "." + unique_name()
        execute(cql, KEYSPACE, "CREATE TYPE " + type + " (txt text, i int)")

        with create_table(cql, KEYSPACE, "(key int primary key, udt frozen<" + type + ">)") as table:
            fName = createFunction(cql, KEYSPACE,
                                   "CREATE FUNCTION %s( udt " + type + " ) " +
                                   "CALLED ON NULL INPUT " +
                                   "RETURNS int " +
                                   java_or_lua(cql, 'return Integer.valueOf(udt.getInt("i"));', "return udt.i") + ";")

            # The Java test also checks Cassandra's internal schema object
            # and its cache of prepared statements, which we can't do.

            # UT still referenced by table
            assert_invalid_message(cql, table, "Cannot drop user type", "DROP TYPE " + type)

        # UT still referenced by UDF
        assert_invalid_message(cql, KEYSPACE, "as it is still used by function", "DROP TYPE " + type)

# Reproduces SCYLLADB-5171 (duplicate argument names in CREATE FUNCTION)
def testDuplicateArgNames(cql, test_keyspace):
    KEYSPACE = test_keyspace
    try:
        assert_invalid_message(cql, KEYSPACE, "Duplicate argument names for given function",
                               "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".scrinv(input double, input int) " +
                               "CALLED ON NULL INPUT " +
                               "RETURNS double " +
                               java_or_lua(cql, "return Math.max(input, input)", "return input") + ";")
    finally:
        # If the CREATE FUNCTION wrongly succeeded, don't leave the function
        # behind in the shared test_keyspace, where it breaks other tests.
        cql.execute("DROP FUNCTION IF EXISTS " + KEYSPACE + ".scrinv")

# Reproduces SCYLLADB-5169 (CREATE OR REPLACE FUNCTION should not change the
# return type or null-input behavior)
@pytest.mark.xfail(reason="SCYLLADB-5169")
def testReplaceAllowNulls(cql):
    with create_keyspace(cql, REPLICATION) as KEYSPACE:
        fNulls = createFunction(cql, KEYSPACE,
                                "CREATE OR REPLACE FUNCTION %s(val int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS text " +
                                java_or_lua(cql, 'return "foo bar";', 'return "foo bar"') + ";")
        fNoNulls = createFunction(cql, KEYSPACE,
                                  "CREATE OR REPLACE FUNCTION %s(val int) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS text " +
                                  java_or_lua(cql, 'return "foo bar";', 'return "foo bar"') + ";")

        assert_invalid(cql, KEYSPACE, "CREATE OR REPLACE FUNCTION " + fNulls + "(val int) " +
                       "RETURNS NULL ON NULL INPUT " +
                       "RETURNS text " +
                       java_or_lua(cql, 'return "foo bar";', 'return "foo bar"') + ";")
        assert_invalid(cql, KEYSPACE, "CREATE OR REPLACE FUNCTION " + fNoNulls + "(val int) " +
                       "CALLED ON NULL INPUT " +
                       "RETURNS text " +
                       java_or_lua(cql, 'return "foo bar";', 'return "foo bar"') + ";")

        execute(cql, KEYSPACE, "CREATE OR REPLACE FUNCTION " + fNulls + "(val int) " +
                "CALLED ON NULL INPUT " +
                "RETURNS text " +
                java_or_lua(cql, 'return "foo bar";', 'return "foo bar"') + ";")
        execute(cql, KEYSPACE, "CREATE OR REPLACE FUNCTION " + fNoNulls + "(val int) " +
                "RETURNS NULL ON NULL INPUT " +
                "RETURNS text " +
                java_or_lua(cql, 'return "foo bar";', 'return "foo bar"') + ";")

# The test testBrokenFunction was not translated, because it replaces a
# function in Cassandra's internal schema by a "broken" one, which can't be
# done through CQL.

# The Java test checks the error with each of the protocol versions. We only
# check the protocol version used by the driver, where the error should be a
# FunctionFailure.
# Reproduces SCYLLADB-5159 (wrong error code for a failing function)
@pytest.mark.xfail(reason="SCYLLADB-5159")
def testFunctionExecutionExceptionNet(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_table(cql, test_keyspace, "(key int primary key, dval double)") as table:
        execute(cql, table, "INSERT INTO %s (key, dval) VALUES (?, ?)", 1, 1.0)

        fName = createFunction(cql, KEYSPACE_PER_TEST,
                               "CREATE OR REPLACE FUNCTION %s(val double) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS double " +
                               java_or_lua(cql, "throw new RuntimeException();", 'error("thrown to unit test - not a bug")'))

        with pytest.raises(FunctionFailure):
            execute(cql, table, "SELECT " + fName + "(dval) FROM %s WHERE key = 1")

# Reproduces SCYLLADB-5141 (snake_case names of native functions) and
# SCYLLADB-5183 (an empty int value passed to a function causes a server error)
@pytest.mark.xfail(reason="SCYLLADB-5141, SCYLLADB-5183")
def testEmptyString(cql, test_keyspace):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST, create_table(cql, test_keyspace, "(key int primary key, sval text, aval ascii, bval blob, empty_int int)") as table:
        execute(cql, table, "INSERT INTO %s (key, sval, aval, bval, empty_int) VALUES (?, ?, ?, ?, blob_as_int(0x))", 1, "", "", b"")

        fNameSRC = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val text) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS text " +
                                  java_or_lua(cql, "return val;", "return val"))

        fNameSCC = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val text) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS text " +
                                  java_or_lua(cql, 'return "";', 'return ""'))

        fNameSRN = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val text) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS text " +
                                  java_or_lua(cql, "return val;", "return val"))

        fNameSCN = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val text) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS text " +
                                  java_or_lua(cql, 'return "";', 'return ""'))

        fNameBRC = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS blob " +
                                  java_or_lua(cql, "return val;", "return val"))

        fNameBCC = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS blob " +
                                  java_or_lua(cql, "return ByteBuffer.allocate(0);", 'return ""'))

        fNameBRN = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS blob " +
                                  java_or_lua(cql, "return val;", "return val"))

        fNameBCN = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS blob " +
                                  java_or_lua(cql, "return ByteBuffer.allocate(0);", 'return ""'))

        fNameIRC = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val int) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS int " +
                                  java_or_lua(cql, "return val;", "return val"))

        fNameICC = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val int) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS int " +
                                  java_or_lua(cql, "return 0;", "return 0"))

        fNameIRN = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val int) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS int " +
                                  java_or_lua(cql, "return val;", "return val"))

        fNameICN = createFunction(cql, KEYSPACE_PER_TEST,
                                  "CREATE OR REPLACE FUNCTION %s(val int) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS int " +
                                  java_or_lua(cql, "return 0;", "return 0"))

        assert_rows(execute(cql, table, "SELECT " + fNameSRC + "(sval) FROM %s"), row(""))
        assert_rows(execute(cql, table, "SELECT " + fNameSRN + "(sval) FROM %s"), row(""))
        assert_rows(execute(cql, table, "SELECT " + fNameSCC + "(sval) FROM %s"), row(""))
        assert_rows(execute(cql, table, "SELECT " + fNameSCN + "(sval) FROM %s"), row(""))
        assert_rows(execute(cql, table, "SELECT " + fNameSRC + "(aval) FROM %s"), row(""))
        assert_rows(execute(cql, table, "SELECT " + fNameSRN + "(aval) FROM %s"), row(""))
        assert_rows(execute(cql, table, "SELECT " + fNameSCC + "(aval) FROM %s"), row(""))
        assert_rows(execute(cql, table, "SELECT " + fNameSCN + "(aval) FROM %s"), row(""))
        assert_rows(execute(cql, table, "SELECT " + fNameBRC + "(bval) FROM %s"), row(b""))
        assert_rows(execute(cql, table, "SELECT " + fNameBRN + "(bval) FROM %s"), row(b""))
        assert_rows(execute(cql, table, "SELECT " + fNameBCC + "(bval) FROM %s"), row(b""))
        assert_rows(execute(cql, table, "SELECT " + fNameBCN + "(bval) FROM %s"), row(b""))
        assert_rows(execute(cql, table, "SELECT " + fNameIRC + "(empty_int) FROM %s"), row(None))
        assert_rows(execute(cql, table, "SELECT " + fNameIRN + "(empty_int) FROM %s"), row(None))
        assert_rows(execute(cql, table, "SELECT " + fNameICC + "(empty_int) FROM %s"), row(0))
        assert_rows(execute(cql, table, "SELECT " + fNameICN + "(empty_int) FROM %s"), row(None))

# Reproduces SCYLLADB-5165 (function names with '/', '[' or ']' should be
# rejected)
@pytest.mark.xfail(reason="SCYLLADB-5165")
def testRejectInvalidFunctionNamesOnCreation(cql):
    with create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST:
        for funcName in ["my/fancy/func", "my_other[fancy]func"]:
            assert_invalid_message(cql, KEYSPACE_PER_TEST, f"Function name '{funcName}' is invalid",
                                   f'CREATE OR REPLACE FUNCTION {KEYSPACE_PER_TEST}."{funcName}"(val int) ' +
                                   "RETURNS NULL ON NULL INPUT " +
                                   "RETURNS int " +
                                   java_or_lua(cql, "return val;", "return val"))
