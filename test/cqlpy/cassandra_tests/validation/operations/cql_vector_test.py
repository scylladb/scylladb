# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 54e46880690bd5effb31116986292c1bdc9e891e
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from ...porting import *
from cassandra.connection import DRIVER_NAME, DRIVER_VERSION

# Some of the lines in these tests are commented out because Scylla doesn't support arithmetic operations in literals (issue #2693).

@pytest.fixture(scope="function")
def skip_if_driver_doesnt_support_variable_width_types():
    scylla_driver = 'Scylla' in DRIVER_NAME
    driver_version = tuple(int(x) for x in DRIVER_VERSION.split('.'))
    if scylla_driver or (not scylla_driver and driver_version < (3, 29, 2)):
        pytest.skip("The driver doesn't support variable width types in vectors.")

def test_select(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk vector<int, 2> primary key)") as table:
        execute(cql, table, "INSERT INTO %s (pk) VALUES ([1, 2])")

        vector = [1, 2]
        row = [vector]
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = [1, 2]"), row)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", vector), row)

        # assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = [1, 1 + 1]"), row)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = [1, ?]", 2), row)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = [1, (int) ?]", 2), row)
        # assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = [1, 1 + (int) ?]", 1), row)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk IN ([1, 2])"), row)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk IN ([1, 2], [1, 2])"), row)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk IN (?)", vector), row)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk IN ?", [vector]), row)
        # assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk IN ([1, 1 + 1])"), row)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk IN ([1, ?])", 2), row)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk IN ([1, (int) ?])", 2), row)
        # assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk IN ([1, 1 + (int) ?])", 1), row)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk > [0, 0] AND pk < [1, 3] ALLOW FILTERING"), row)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE token(pk) = token([1, 2])"), row)

        assertRows(execute(cql, table, "SELECT * FROM %s"), row)


def test_selectNonPk(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int primary key, value vector<int, 2>)") as table:
        execute(cql, table, "INSERT INTO %s (pk, value) VALUES (0, [1, 2])")
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE value=[1, 2] ALLOW FILTERING"), [0, [1, 2]])

def test_insert(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk vector<int, 2> primary key)") as table:
        def test():
            assertRows(execute(cql, table, "SELECT * FROM %s"), [[1, 2]])
            execute(cql, table, "TRUNCATE %s")
            assertRows(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (pk) VALUES ([1, 2])")
        test()

        execute(cql, table, "INSERT INTO %s (pk) VALUES (?)", [1, 2])
        test()

        # execute(cql, table, "INSERT INTO %s (pk) VALUES ([1, 1 + 1])")
        # test()

        execute(cql, table, "INSERT INTO %s (pk) VALUES ([1, ?])", 2)
        test()

        execute(cql, table, "INSERT INTO %s (pk) VALUES ([1, (int) ?])", 2)
        test()

        # execute(cql, table, "INSERT INTO %s (pk) VALUES ([1, 1 + (int) ?])", 1)
        # test()

def test_insertNonPK(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int primary key, value vector<int, 2>)") as table:
        def test():
            assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, [1, 2]))
            execute(cql, table, "TRUNCATE %s")
            assertRows(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (pk, value) VALUES (0, [1, 2])")
        test()

        execute(cql, table, "INSERT INTO %s (pk, value) VALUES (0, ?)", [1, 2])
        test()

        # execute(cql, table, "INSERT INTO %s (pk, value) VALUES (0, [1, 1 + 1])")
        # test()

        execute(cql, table, "INSERT INTO %s (pk, value) VALUES (0, [1, ?])", 2)
        test()

        execute(cql, table, "INSERT INTO %s (pk, value) VALUES (0, [1, (int) ?])", 2)
        test()

        # execute(cql, table, "INSERT INTO %s (pk, value) VALUES (0, [1, 1 + (int) ?])", 1);
        # test()
    
def test_invalidNumberOfDimensionsFixedWidth(cql, test_keyspace):
    with create_table(cql, test_keyspace,"(pk int primary key, value vector<int, 2>)") as table:

        # fewer values than expected, with literals and bind markers
        assertInvalidThrowMessage(cql, table, "Invalid vector literal",InvalidRequest,
                                    "INSERT INTO %s (pk, value) VALUES (0, [1])")
        # Not translated because the Python driver refuses to send incorrect
        # parameters for prepared statements
        # assertInvalidThrowMessage("Not enough bytes to read a vector<int, 2>",
        #                   InvalidRequestException.class,
        #                   "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1));

        # more values than expected, with literals and bind markers
        assertInvalidThrowMessage(cql, table,  "Invalid vector literal",InvalidRequest,
                                    "INSERT INTO %s (pk, value) VALUES (0, [1, 2, 3])")

        # Not translated because the Python driver refuses to send incorrect
        # parameters for prepared statements
        # assertInvalidThrowMessage("Unexpected 4 extraneous bytes after vector<int, 2> value",
        #                           InvalidRequestException.class,
        #                           "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2, 3));


def test_invalidNumberOfDimensionsVariableWidth(cql, test_keyspace, skip_if_driver_doesnt_support_variable_width_types):
    with create_table(cql, test_keyspace, "(pk int primary key, value vector<text, 2>)") as table:

        # fewer values than expected, with literals and bind markers
        assertInvalidThrowMessage(cql, table, "Invalid vector literal", InvalidRequest,
                                    "INSERT INTO %s (pk, value) VALUES (0, ['a'])")

        # Not translated because the Python driver refuses to send incorrect
        # parameters for prepared statements
        # assertInvalidThrowMessage("Not enough bytes to read a vector<text, 2>",
        #                           InvalidRequestException.class,
        #                           "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a"));

        # more values than expected, with literals and bind markers
        assertInvalidThrowMessage(cql, table,"Invalid vector literal", InvalidRequest,
                                    "INSERT INTO %s (pk, value) VALUES (0, ['a', 'b', 'c'])")

        # Not translated because the Python driver refuses to send incorrect
        # parameters for prepared statements
        # assertInvalidThrowMessage(cql, table,"bytes", InvalidRequest,
        #                             "INSERT INTO %s (pk, value) VALUES (0, ?)", ["a", "b", "c"])

def test_sandwichBetweenUDTs(cql, test_keyspace, skip_if_driver_doesnt_support_variable_width_types):
    with create_type(cql, test_keyspace, "(y int)") as b:
        with create_type(cql, test_keyspace, f"(z vector<frozen<{b}>, 2>)") as a:
            with create_table(cql, test_keyspace, f"(pk int primary key, value {a})") as table:

                execute(cql, table, "INSERT INTO %s (pk, value) VALUES (0, ?)", user_type("z", [user_type("y", 1), user_type("y", 2)]))
                assertRows(execute(cql, table, "SELECT * FROM %s"),
                            [0, user_type("z", [user_type("y", 1), user_type("y", 2)])])

def test_invalidElementTypeFixedWidth(cql,test_keyspace):
    with create_table(cql, test_keyspace, "(pk int primary key, value vector<int, 2>)") as table:

        # fixed-length bigint instead of int, with literals and bind markers
        assertInvalidThrowMessage(cql, table,"Invalid vector literal", InvalidRequest,
                                    "INSERT INTO %s (pk, value) VALUES (0, [(bigint) 1, (bigint) 2])")

        # Not translated because the Python driver refuses to send incorrect
        # parameters for prepared statements
        # assertInvalidThrowMessage("Unexpected 8 extraneous bytes after vector<int, 2> value",
        #                           InvalidRequestException.class,
        #                           "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1L, Long.MAX_VALUE));

        #  variable-length text instead of int, with literals and bind markers
        assertInvalidThrowMessage(cql, table,"Invalid vector literal", InvalidRequest,
                                    "INSERT INTO %s (pk, value) VALUES (0, ['a', 'b'])")

        # Not translated because the Python driver refuses to send incorrect
        # parameters for prepared statements
        # assertInvalidThrowMessage("Not enough bytes to read a vector<int, 2>",
        #                         InvalidRequestException.class,
        #                         "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a", "b"));


def test_invalidElementTypeVariableWidth(cql, test_keyspace,skip_if_driver_doesnt_support_variable_width_types):
    with create_table(cql, test_keyspace, "(pk int primary key, value vector<text, 2>)") as table:

        # fixed-length int instead of text, with literals and bind markers
        assertInvalidThrowMessage(cql, table, "Invalid vector literal",
                                  InvalidRequest,
                                  "INSERT INTO %s (pk, value) VALUES (0, [1, 2])")
        # Not translated because the Python driver refuses to send incorrect
        # parameters for prepared statements
        # assertInvalidThrowMessage("Unexpected 6 extraneous bytes after vector<text, 2> value",
        #                           InvalidRequestException.class,
        #                           "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2));

        # variable-length varint instead of text, with literals and bind markers
        assertInvalidThrowMessage(cql, table, "Invalid vector literal",
                                  InvalidRequest,
                                  "INSERT INTO %s (pk, value) VALUES (0, [(varint) 1, (varint) 2])")

        # Not translated because the Python driver refuses to send incorrect
        # parameters for prepared statements
        # assertInvalidThrowMessage("String didn't validate.",
        #                           InvalidRequestException.class,
        #                           "INSERT INTO %s (pk, value) VALUES (0, ?)",
        #                           vector(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), BigInteger.ONE));
def test_update(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int primary key, value vector<int, 2>)") as table:
        def test():
            assertRows(execute(cql, table, "SELECT * FROM %s"), [0, [1, 2]])
            execute(cql, table, "TRUNCATE %s")
            assertRows(execute(cql, table, "SELECT * FROM %s"))

        
        execute(cql, table, "UPDATE %s SET value = [1, 2] WHERE pk = 0")
        test()

        execute(cql, table, "UPDATE %s SET value = ? WHERE pk = 0", [1, 2])
        test()

        # execute(cql, table, "UPDATE %s SET value = [1, 1 + 1] WHERE pk = 0")
        # test()

        execute(cql, table, "UPDATE %s SET value = [1, ?] WHERE pk = 0", 2)
        test()

        execute(cql, table, "UPDATE %s SET value = [1, (int) ?] WHERE pk = 0", 2)
        test()

        # execute(cql, table, "UPDATE %s SET value = [1, 1 + (int) ?] WHERE pk = 0", 1)
        # test()

def test_nullValues(cql, test_keyspace):
    def assertAcceptsNullValues(t):
        with create_table(cql, test_keyspace, f"(k int primary key, v vector<{t}, 2>)") as table:
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, null)")
            assertRows(execute(cql, table, "SELECT * FROM %s"), [0, None])

            execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", None)
            assertRows(execute(cql, table, "SELECT * FROM %s"), [0, None])

    assertAcceptsNullValues("int")  # fixed length
    assertAcceptsNullValues("float")  # fixed length with special/optimized treatment

    # The Python driver doesn't support variable width types in vectors yet.
    # assertAcceptsNullValues("text")  # variable length


def test_emptyValues(cql, test_keyspace):
    def assertRejectsEmptyValues(t):
        with create_table(cql, test_keyspace, f"(k int primary key, v vector<{t}, 2>)") as table:
            assertInvalidThrowMessage(cql, table, f"Invalid HEX constant (0x) for \"v\" of type vector<{t}, 2>",
                                      InvalidRequest,
                                      "INSERT INTO %s (k, v) VALUES (0, 0x)")

            # Not translated because the Python driver refuses to send incorrect
            # parameters for prepared statements
            # assertInvalidThrowMessage(cql, table, f"Not enough bytes to read a vector",
            #                           InvalidRequest,
            #                           "INSERT INTO %s (k, v) VALUES (0, ?)",
            #                           [])

    assertRejectsEmptyValues("int")  # fixed length
    assertRejectsEmptyValues("float")  # fixed length with special/optimized treatment
    
    # The Python driver doesn't support variable width types in vectors yet.
    # assertRejectsEmptyValues("text")  # variable length
    

# Test commented out because it uses internal Cassandra Java APIs, not CQL
#     @Test
#     public void functions()
#     {
#         VectorType<Integer> type = VectorType.getInstance(Int32Type.instance, 2);
#         Vector<Integer> vector = vector(1, 2);

#         NativeFunctions.instance.add(new NativeScalarFunction("f", type, type)
#         {
#             @Override
#             public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
#             {
#                 return arguments.get(0);
#             }

#             @Override
#             public Arguments newArguments(ProtocolVersion version)
#             {
#                 return FunctionArguments.newNoopInstance(version, 1);
#             }
#         });

#         createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");
#         execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector);

#         assertRows(execute("SELECT f(value) FROM %s WHERE pk=0"), row(vector));
#         assertRows(execute("SELECT f([1, 2]) FROM %s WHERE pk=0"), row(vector));
#     }

# Test commented out because it uses internal Cassandra Java APIs, not CQL
#     @Test
#     public void specializedFunctions()
#     {
#         VectorType<Float> type = VectorType.getInstance(FloatType.instance, 2);
#         Vector<Float> vector = vector(1.0f, 2.0f);

#         NativeFunctions.instance.add(new NativeScalarFunction("f", type, type, type)
#         {
#             @Override
#             public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
#             {
#                 float[] left = arguments.get(0);
#                 float[] right = arguments.get(1);
#                 int size = Math.min(left.length, right.length);
#                 float[] sum = new float[size];
#                 for (int i = 0; i < size; i++)
#                     sum[i] = left[i] + right[i];
#                 return type.decomposeAsFloat(sum);
#             }

#             @Override
#             public Arguments newArguments(ProtocolVersion version)
#             {
#                 return new FunctionArguments(version,
#                                              (v, b) -> type.composeAsFloat(b),
#                                              (v, b) -> type.composeAsFloat(b));
#             }
#         });

#         createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<float, 2>)");
#         execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector);
#         execute("INSERT INTO %s (pk, value) VALUES (1, ?)", vector);

#         Object[][] expected = { row(vector(2f, 4f)), row(vector(2f, 4f)) };
#         assertRows(execute("SELECT f(value, [1.0, 2.0]) FROM %s"), expected);
#         assertRows(execute("SELECT f([1.0, 2.0], value) FROM %s"), expected);
#     }

@pytest.mark.xfail(reason="Issue #5411")
def test_token(cql , test_keyspace):
    with create_table(cql, test_keyspace, "(pk vector<int, 2> primary key)") as table:

        execute(cql, table,"INSERT INTO %s (pk) VALUES (?)", "abcd")
        tokenColumn = execute(cql, table, "SELECT token(pk) as t FROM %s")
        tokenTerminal = execute(cql, table, "SELECT token([1, 2]) as t FROM %s")
        assertRows(tokenColumn, tokenTerminal)

# FIXME Scylla doesn't support java as a language for UDFs, so this test is commented out.
# @Test
# public void udf() throws Throwable
# {
#     createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");
#     Vector<Integer> vector = vector(1, 2);
#     execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector);

#     // identity function
#     String f = createFunction(KEYSPACE,
#                                 "",
#                                 "CREATE FUNCTION %s (x vector<int, 2>) " +
#                                 "CALLED ON NULL INPUT " +
#                                 "RETURNS vector<int, 2> " +
#                                 "LANGUAGE java " +
#                                 "AS 'return x;'");
#     assertRows(execute(format("SELECT %s(value) FROM %%s", f)), row(vector));
#     assertRows(execute(format("SELECT %s([2, 3]) FROM %%s", f)), row(vector(2, 3)));
#     assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Vector<Integer>) null));

#     // identitiy function with nested type
#     f = createFunction(KEYSPACE,
#                         "",
#                         "CREATE FUNCTION %s (x list<vector<int, 2>>) " +
#                         "CALLED ON NULL INPUT " +
#                         "RETURNS list<vector<int, 2>> " +
#                         "LANGUAGE java " +
#                         "AS 'return x;'");
#     assertRows(execute(format("SELECT %s([value]) FROM %%s", f)), row(list(vector)));
#     assertRows(execute(format("SELECT %s([[2, 3]]) FROM %%s", f)), row(list(vector(2, 3))));
#     assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Vector<Integer>) null));

#     // identitiy function with elements of variable length
#     f = createFunction(KEYSPACE,
#                         "",
#                         "CREATE FUNCTION %s (x vector<text, 2>) " +
#                         "CALLED ON NULL INPUT " +
#                         "RETURNS vector<text, 2> " +
#                         "LANGUAGE java " +
#                         "AS 'return x;'");
#     assertRows(execute(format("SELECT %s(['abc', 'defghij']) FROM %%s", f)), row(vector("abc", "defghij")));
#     assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Vector<Integer>) null));

#     // function accessing vector argument elements
#     f = createFunction(KEYSPACE,
#                         "",
#                         "CREATE FUNCTION %s (x vector<int, 2>, i int) " +
#                         "CALLED ON NULL INPUT " +
#                         "RETURNS int " +
#                         "LANGUAGE java " +
#                         "AS 'return x == null ? null : x.get(i);'");
#     assertRows(execute(format("SELECT %s(value, 0), %<s(value, 1) FROM %%s", f)), row(1, 2));
#     assertRows(execute(format("SELECT %s([2, 3], 0), %<s([2, 3], 1) FROM %%s", f)), row(2, 3));
#     assertRows(execute(format("SELECT %s(null, 0) FROM %%s", f)), row((Integer) null));

#     // function accessing vector argument dimensions
#     f = createFunction(KEYSPACE,
#                         "",
#                         "CREATE FUNCTION %s (x vector<int, 2>) " +
#                         "CALLED ON NULL INPUT " +
#                         "RETURNS int " +
#                         "LANGUAGE java " +
#                         "AS 'return x == null ? 0 : x.size();'");
#     assertRows(execute(format("SELECT %s(value) FROM %%s", f)), row(2));
#     assertRows(execute(format("SELECT %s([2, 3]) FROM %%s", f)), row(2));
#     assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row(0));

#     // build vector with elements of fixed length
#     f = createFunction(KEYSPACE,
#                         "",
#                         "CREATE FUNCTION %s () " +
#                         "CALLED ON NULL INPUT " +
#                         "RETURNS vector<double, 3> " +
#                         "LANGUAGE java " +
#                         "AS 'return Arrays.asList(1.3, 2.2, 3.1);'");
#     assertRows(execute(format("SELECT %s() FROM %%s", f)), row(vector(1.3, 2.2, 3.1)));

#     // build vector with elements of variable length
#     f = createFunction(KEYSPACE,
#                         "",
#                         "CREATE FUNCTION %s () " +
#                         "CALLED ON NULL INPUT " +
#                         "RETURNS vector<text, 3> " +
#                         "LANGUAGE java " +
#                         "AS 'return Arrays.asList(\"a\", \"bc\", \"def\");'");
#     assertRows(execute(format("SELECT %s() FROM %%s", f)), row(vector("a", "bc", "def")));

#     // concat vectors, just to put it all together
#     f = createFunction(KEYSPACE,
#                         "",
#                         "CREATE FUNCTION %s (x vector<int, 2>, y vector<int, 2>) " +
#                         "CALLED ON NULL INPUT " +
#                         "RETURNS vector<int, 4> " +
#                         "LANGUAGE java " +
#                         "AS '" +
#                         "if (x == null || y == null) return null;" +
#                         "List<Integer> l = new ArrayList<Integer>(x); " +
#                         "l.addAll(y); " +
#                         "return l;'");
#     assertRows(execute(format("SELECT %s(value, [3, 4]) FROM %%s", f)), row(vector(1, 2, 3, 4)));
#     assertRows(execute(format("SELECT %s([2, 3], value) FROM %%s", f)), row(vector(2, 3, 1, 2)));
#     assertRows(execute(format("SELECT %s(null, null) FROM %%s", f)), row((Vector<Integer>) null));

#     // Test wrong arguments on function call
#     assertInvalidThrowMessage("cannot be passed as argument 0 of function " + f,
#                                 InvalidRequestException.class,
#                                 format("SELECT %s((int) 0, [3, 4]) FROM %%s", f));
#     assertInvalidThrowMessage("cannot be passed as argument 1 of function " + f,
#                                 InvalidRequestException.class,
#                                 format("SELECT %s([1, 2], (int) 0) FROM %%s", f));
#     assertInvalidThrowMessage("Invalid number of arguments in call to function " + f,
#                                 InvalidRequestException.class,
#                                 format("SELECT %s([1, 2]) FROM %%s", f));
#     assertInvalidThrowMessage("Invalid number of arguments in call to function " + f,
#                                 InvalidRequestException.class,
#                                 format("SELECT %s([1, 2], [3, 4], [5, 6]) FROM %%s", f));
#     assertInvalidThrowMessage("Unable to create a vector selector of type vector<int, 2> from 3 elements",
#                                 InvalidRequestException.class,
#                                 format("SELECT %s([1, 2, 3], [4, 5, 6]) FROM %%s", f));

#     // Test wrong types on function creation
#     assertInvalidThrowMessage("vectors may only have positive dimensions; given 0",
#                                 InvalidRequestException.class,
#                                 "CREATE FUNCTION %s (x vector<int, 0>) " +
#                                 "CALLED ON NULL INPUT " +
#                                 "RETURNS vector<int, 2> " +
#                                 "LANGUAGE java " +
#                                 "AS 'return x;'");
#     assertInvalidThrowMessage("vectors may only have positive dimensions; given 0",
#                                 InvalidRequestException.class,
#                                 "CREATE FUNCTION %s (x vector<int, 2>) " +
#                                 "CALLED ON NULL INPUT " +
#                                 "RETURNS vector<int, 0> " +
#                                 "LANGUAGE java " +
#                                 "AS 'return x;'");

#     // function reading and writing a udt vector field
#     String udt = createType("CREATE TYPE %s (v vector<int,2>)");
#     alterTable("ALTER TABLE %s ADD udt " + udt);
#     execute("INSERT INTO %s (pk, udt) VALUES (0, ?)", userType("v", vector));
#     f = createFunction(KEYSPACE,
#                         "",
#                         "CREATE FUNCTION %s (udt " + udt + ") " +
#                         "CALLED ON NULL INPUT " +
#                         "RETURNS " + udt + ' ' +
#                         "LANGUAGE java " +
#                         "AS '" +
#                         "if (udt == null) return null;" +
#                         "List<Integer> v = new ArrayList<Integer>(udt.getVector(\"v\", Integer.class));" +
#                         "v.set(0, 7);" +
#                         "return udt.setVector(\"v\", v);'");
#     assertRows(execute(format("SELECT %s(udt) FROM %%s", f)), row(userType("v", vector(7, 2))));
#     assertRows(execute(format("SELECT %s({v: [10, 20]}) FROM %%s", f)), row(userType("v", vector(7, 20))));
#     assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Object) null));

#     // make sure the function referencing the UDT is dropped before dropping the UDT at cleanup
#     execute("DROP FUNCTION " + f);
# }
