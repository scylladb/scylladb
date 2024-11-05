# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 8d91b469afd3fcafef7ef85c10c8acc11703ba2d
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *
from decimal import Decimal
from ctypes import c_float
from datetime import datetime, timezone
from cassandra.util import Date, Time, Duration, uuid_from_time, ms_timestamp_from_datetime

def testInvalidQueries(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int primary key, b text, c double)") as table:
        assertInvalidMessage(cql, table, "cannot be cast to", "SELECT CAST(a AS boolean) FROM %s")

def testNumericCastsInSelectionClause(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(a tinyint primary key, b smallint, c int, d bigint, e float, f double, g decimal, h varint, i int)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f, g, h) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                1, 2, 3, 4, 5.2, 6.3, Decimal("6.3"), 4)

        # Reproduces #14508:
        assertColumnNames(execute(cql, table, "SELECT CAST(b AS int), CAST(c AS int), CAST(d AS double) FROM %s"),
                          # The original names unmanipulated by the Python
                          # driver are cast(b as int).
                          "cast_b_as_int",
                          "c",
                          "cast_d_as_double")

        assertRows(execute(cql, table, "SELECT CAST(a AS tinyint), " +
                "CAST(b AS tinyint), " +
                "CAST(c AS tinyint), " +
                "CAST(d AS tinyint), " +
                "CAST(e AS tinyint), " +
                "CAST(f AS tinyint), " +
                "CAST(g AS tinyint), " +
                "CAST(h AS tinyint), " +
                "CAST(i AS tinyint) FROM %s"),
                   row(1, 2, 3, 4, 5, 6, 6, 4, None))

        assertRows(execute(cql, table, "SELECT CAST(a AS smallint), " +
                "CAST(b AS smallint), " +
                "CAST(c AS smallint), " +
                "CAST(d AS smallint), " +
                "CAST(e AS smallint), " +
                "CAST(f AS smallint), " +
                "CAST(g AS smallint), " +
                "CAST(h AS smallint), " +
                "CAST(i AS smallint) FROM %s"),
                   row(1, 2, 3, 4, 5, 6, 6, 4, None))

        assertRows(execute(cql, table, "SELECT CAST(a AS int), " +
                "CAST(b AS int), " +
                "CAST(c AS int), " +
                "CAST(d AS int), " +
                "CAST(e AS int), " +
                "CAST(f AS int), " +
                "CAST(g AS int), " +
                "CAST(h AS int), " +
                "CAST(i AS int) FROM %s"),
                   row(1, 2, 3, 4, 5, 6, 6, 4, None))

        assertRows(execute(cql, table, "SELECT CAST(a AS bigint), " +
                "CAST(b AS bigint), " +
                "CAST(c AS bigint), " +
                "CAST(d AS bigint), " +
                "CAST(e AS bigint), " +
                "CAST(f AS bigint), " +
                "CAST(g AS bigint), " +
                "CAST(h AS bigint), " +
                "CAST(i AS bigint) FROM %s"),
                   row(1, 2, 3, 4, 5, 6, 6, 4, None))

        assertRows(execute(cql, table, "SELECT CAST(a AS float), " +
                "CAST(b AS float), " +
                "CAST(c AS float), " +
                "CAST(d AS float), " +
                "CAST(e AS float), " +
                "CAST(f AS float), " +
                "CAST(g AS float), " +
                "CAST(h AS float), " +
                "CAST(i AS float) FROM %s"),
                   row(1.0, 2.0, 3.0, 4.0, c_float(5.2).value, c_float(6.3).value, c_float(6.3).value, 4.0, None))

        assertRows(execute(cql, table, "SELECT CAST(a AS double), " +
                "CAST(b AS double), " +
                "CAST(c AS double), " +
                "CAST(d AS double), " +
                "CAST(e AS double), " +
                "CAST(f AS double), " +
                "CAST(g AS double), " +
                "CAST(h AS double), " +
                "CAST(i AS double) FROM %s"),
                   row(1.0, 2.0, 3.0, 4.0, c_float(5.2).value, 6.3, 6.3, 4.0, None))

        # Cassandra has a bug here (CASSANDRA-18647), so this test was modified
        # from the original and fails on Cassandra and marked cassandra_bug:
        # When the "float" (32-bit) number 5.2 is converted to "decimal",
        # Cassandra wrongly expands it to double and becomes 5.199999809265137,
        # and only then converted to decimal with all those silly extra digits.
        # Scylla, correctly, only keeps the relevant digits for the original
        # float - 5.2.
        assertRows(execute(cql, table, "SELECT CAST(a AS decimal), " +
                "CAST(b AS decimal), " +
                "CAST(c AS decimal), " +
                "CAST(d AS decimal), " +
                "CAST(e AS decimal), " +
                "CAST(f AS decimal), " +
                "CAST(g AS decimal), " +
                "CAST(h AS decimal), " +
                "CAST(i AS decimal) FROM %s"),
                   row(Decimal("1"),
                       Decimal("2"),
                       Decimal("3"),
                       Decimal("4"),
                       # this fails in Cassandra. In Cassandra, we get
                       # Decimal(str(c_float(5.2).value))
                       Decimal("5.2"),
                       Decimal("6.3"),
                       Decimal("6.3"),
                       Decimal("4"),
                       None))

        assertRows(execute(cql, table, "SELECT CAST(a AS ascii), " +
                "CAST(b AS ascii), " +
                "CAST(c AS ascii), " +
                "CAST(d AS ascii), " +
                "CAST(e AS ascii), " +
                "CAST(f AS ascii), " +
                "CAST(g AS ascii), " +
                "CAST(h AS ascii), " +
                "CAST(i AS ascii) FROM %s"),
                   row("1",
                       "2",
                       "3",
                       "4",
                       "5.2",
                       "6.3",
                       "6.3",
                       "4",
                       None))

        assertRows(execute(cql, table, "SELECT CAST(a AS text), " +
                "CAST(b AS text), " +
                "CAST(c AS text), " +
                "CAST(d AS text), " +
                "CAST(e AS text), " +
                "CAST(f AS text), " +
                "CAST(g AS text), " +
                "CAST(h AS text), " +
                "CAST(i AS text) FROM %s"),
                   row("1",
                       "2",
                       "3",
                       "4",
                       "5.2",
                       "6.3",
                       "6.3",
                       "4",
                       None))

def testNoLossOfPrecisionForCastToDecimal(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, bigint_clmn bigint, varint_clmn varint)") as table:
        execute(cql, table, "INSERT INTO %s(k, bigint_clmn, varint_clmn) VALUES(2, 9223372036854775807, 1234567890123456789)")

        assertRows(execute(cql, table, "SELECT CAST(bigint_clmn AS decimal), CAST(varint_clmn AS decimal) FROM %s"),
                   row(Decimal("9223372036854775807"), Decimal("1234567890123456789")))

def testTimeCastsInSelectionClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a timeuuid primary key, b timestamp, c date, d time)") as table:
        yearMonthDay = "2015-05-21"
        dateNoTime = datetime(2015, 5, 21, 0, 0, 0)
        dateTime = datetime(2015, 5, 21, 11, 3, 2)

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, '" + yearMonthDay + " 11:03:02+00', '2015-05-21', '11:03:02')",
                uuid_from_time(dateTime))

        assertRows(execute(cql, table, "SELECT CAST(a AS timestamp), " +
                           "CAST(b AS timestamp), " +
                           "CAST(c AS timestamp) FROM %s"),
                   row(dateTime, dateTime, dateNoTime))

        date = Date(yearMonthDay)
        assertRows(execute(cql, table, "SELECT CAST(a AS date), " +
                           "CAST(b AS date), " +
                           "CAST(c AS date) FROM %s"),
                   row(date, date, date))

        # Reproduces #14518:
        assertRows(execute(cql, table, "SELECT CAST(b AS text), " +
                           "CAST(c AS text), " +
                           "CAST(d AS text) FROM %s"),
                   row(yearMonthDay + "T11:03:02.000Z", yearMonthDay, "11:03:02.000000000"))

def testOtherTypeCastsInSelectionClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a ascii primary key, b inet, c boolean)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, '127.0.0.1', ?)",
                "test", True)

        assertRows(execute(cql, table, "SELECT CAST(a AS text), " +
                "CAST(b AS text), " +
                "CAST(c AS text) FROM %s"),
                   row("test", "127.0.0.1", "true"))

def testCastsWithReverseOrder(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b smallint, c double, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)",
                1, 2, 6.3)

        assertRows(execute(cql, table, "SELECT CAST(a AS tinyint), " +
                "CAST(b AS tinyint), " +
                "CAST(c AS tinyint) FROM %s"),
                   row(1, 2, 6))

        assertRows(execute(cql, table, "SELECT CAST(CAST(a AS tinyint) AS smallint), " +
                "CAST(CAST(b AS tinyint) AS smallint), " +
                "CAST(CAST(c AS tinyint) AS smallint) FROM %s"),
                   row(1, 2, 6))

        # In Cassandra, doubles cast to text always get a decimal point
        # (e.g., "1.0") but Scylla may yield just "1". Both are fine.
        assert_rows2(execute(cql, table, "SELECT CAST(CAST(CAST(a AS tinyint) AS double) AS text), " +
                "CAST(CAST(CAST(b AS tinyint) AS double) AS text), " +
                "CAST(CAST(CAST(c AS tinyint) AS double) AS text) FROM %s"),
                   [row("1.0", "2.0", "6.0")],
                   [row("1", "2", "6")])

        # FIXME: I commented out this test for UDF because of laziness to
        # translate it to Lua for Scylla :-( Bring it back.
        #String f = createFunction(KEYSPACE, "int",
        #                          "CREATE FUNCTION %s(val int) " +
        #                                  "RETURNS NULL ON NULL INPUT " +
        #                                  "RETURNS double " +
        #                                  "LANGUAGE java " +
        #                                  "AS 'return (double)val;'")
        #
        #assertRows(execute(cql, table, "SELECT " + f + "(CAST(b AS int)) FROM %s"),
        #           row((double) 2))
        #
                #assertRows(execute(cql, table, "SELECT CAST(" + f + "(CAST(b AS int)) AS text) FROM %s"),
        #           row("2.0"))

# Reproduces #14501:
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCounterCastsInSelectionClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int primary key, b counter)") as table:
        execute(cql, table, "UPDATE %s SET b = b + 2 WHERE a = 1")

        assertRows(execute(cql, table, "SELECT CAST(b AS tinyint), " +
                "CAST(b AS smallint), " +
                "CAST(b AS int), " +
                "CAST(b AS bigint), " +
                "CAST(b AS float), " +
                "CAST(b AS double), " +
                "CAST(b AS decimal), " +
                "CAST(b AS ascii), " +
                "CAST(b AS text) FROM %s"),
                   row(2, 2, 2, 2, 2.0, 2.0, Decimal("2"), "2", "2"))

# Verifies that the {@code CAST} function can be used in the values of {@code INSERT INTO} statements.
@pytest.mark.xfail(reason="issue #14522")
def testCastsInInsertIntoValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, v int)") as table:
        # Simple cast
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, CAST(1.3 AS int))")
        assertRows(execute(cql, table, "SELECT v FROM %s"), row(1))

        # Nested casts
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, CAST(CAST(CAST(2.3 AS int) AS float) AS int))")
        assertRows(execute(cql, table, "SELECT v FROM %s"), row(2))

        # Cast of placeholder with type hint
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, CAST((float) ? AS int))", 3.4)
        assertRows(execute(cql, table, "SELECT v FROM %s"), row(3))

        # Cast of placeholder without type hint
        assertInvalidMessage(cql, table, "Ambiguous call to function system.castAsInt",
                                    "INSERT INTO %s (k, v) VALUES (1, CAST(? AS int))", 3.4)

        # Type hint of cast
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, (int) CAST(4.9 AS int))")
        assertRows(execute(cql, table, "SELECT v FROM %s"), row(4))

        # FIXME: I commented out these tests for UDF because of laziness to
        # translate it to Lua for Scylla :-( Bring it back.

        # Function of cast
        #execute(cql, table, String.format("INSERT INTO %%s (k, v) VALUES (1, %s(CAST(5 AS float)))", floatToInt()))
        #assertRows(execute(cql, table, "SELECT v FROM %s"), row(5))

        # Cast of function
        #execute(cql, table, String.format("INSERT INTO %%s (k, v) VALUES (1, CAST(%s(6) AS int))", intToFloat()))
        #assertRows(execute(cql, table, "SELECT v FROM %s"), row(6))

# Verifies that the {@code CAST} function can be used in the values of {@code UPDATE} statements.
@pytest.mark.xfail(reason="issue #14522")
def testCastsInUpdateValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, v int)") as table:
        # Simple cast
        execute(cql, table, "UPDATE %s SET v = CAST(1.3 AS int) WHERE k = 1")
        assertRows(execute(cql, table, "SELECT v FROM %s"), row(1))

        # Nested casts
        execute(cql, table, "UPDATE %s SET v = CAST(CAST(CAST(2.3 AS int) AS float) AS int) WHERE k = 1")
        assertRows(execute(cql, table, "SELECT v FROM %s"), row(2))

        # Cast of placeholder with type hint
        execute(cql, table, "UPDATE %s SET v = CAST((float) ? AS int) WHERE k = 1", 3.4)
        assertRows(execute(cql, table, "SELECT v FROM %s"), row(3))

        # Cast of placeholder without type hint
        assertInvalidMessage(cql, table, "Ambiguous call to function system.castAsInt",
                                    "UPDATE %s SET v = CAST(? AS int) WHERE k = 1", 3.4)

        # Type hint of cast
        execute(cql, table, "UPDATE %s SET v = (int) CAST(4.9 AS int) WHERE k = 1")
        assertRows(execute(cql, table, "SELECT v FROM %s"), row(4))

        # FIXME: I commented out these tests for UDF because of laziness to
        # translate it to Lua for Scylla :-( Bring it back.

        #// Function of cast
        #execute(cql, table, String.format("UPDATE %%s SET v = %s(CAST(5 AS float)) WHERE k = 1", floatToInt()))
        #assertRows(execute(cql, table, "SELECT v FROM %s"), row(5))

        #// Cast of function
        #execute(cql, table, String.format("UPDATE %%s SET v = CAST(%s(6) AS int) WHERE k = 1", intToFloat()))
        #assertRows(execute(cql, table, "SELECT v FROM %s"), row(6))

# Verifies that the {@code CAST} function can be used in the {@code WHERE} clause of {@code UPDATE} statements.
@pytest.mark.xfail(reason="issue #14522")
def testCastsInUpdateWhereClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, v int)") as table:
        for i in range(1,7):
            execute(cql, table, "INSERT INTO %s (k) VALUES (?)", i)

        # Simple cast
        execute(cql, table, "UPDATE %s SET v = ? WHERE k = CAST(1.3 AS int)", 1)
        assertRows(execute(cql, table, "SELECT v FROM %s WHERE k = ?", 1), row(1))

        # Nested casts
        execute(cql, table, "UPDATE %s SET v = ? WHERE k = CAST(CAST(CAST(2.3 AS int) AS float) AS int)", 2)
        assertRows(execute(cql, table, "SELECT v FROM %s WHERE k = ?", 2), row(2))

        # Cast of placeholder with type hint
        execute(cql, table, "UPDATE %s SET v = ? WHERE k = CAST((float) ? AS int)", 3, 3.4)
        assertRows(execute(cql, table, "SELECT v FROM %s WHERE k = ?", 3), row(3))

        # Cast of placeholder without type hint
        assertInvalidMessage(cql, table, "Ambiguous call to function system.castAsInt",
                                    "UPDATE %s SET v = ? WHERE k = CAST(? AS int)", 3, 3.4)

        # Type hint of cast
        execute(cql, table, "UPDATE %s SET v = ? WHERE k = (int) CAST(4.9 AS int)", 4)
        assertRows(execute(cql, table, "SELECT v FROM %s WHERE k = ?", 4), row(4))

        # FIXME: I commented out these tests for UDF because of laziness to
        # translate it to Lua for Scylla :-( Bring it back.

        #// Function of cast
        #execute(cql, table, String.format("UPDATE %%s SET v = ? WHERE k = %s(CAST(5 AS float))", floatToInt()), 5)
        #assertRows(execute(cql, table, "SELECT v FROM %s WHERE k = ?", 5), row(5))

        #// Cast of function
        #execute(cql, table, String.format("UPDATE %%s SET v = ? WHERE k = CAST(%s(6) AS int)", intToFloat()), 6)
        #assertRows(execute(cql, table, "SELECT v FROM %s WHERE k = ?", 6), row(6))

# Verifies that the {@code CAST} function can be used in the {@code WHERE} clause of {@code SELECT} statements.
@pytest.mark.xfail(reason="issue #14522")
def testCastsInSelectWhereClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key)") as table:
        for i in range(1,7):
            execute(cql, table, "INSERT INTO %s (k) VALUES (?)", i)

        # Simple cast
        assertRows(execute(cql, table, "SELECT k FROM %s WHERE k = CAST(1.3 AS int)"), row(1))

        # Nested casts
        assertRows(execute(cql, table, "SELECT k FROM %s WHERE k = CAST(CAST(CAST(2.3 AS int) AS float) AS int)"), row(2))

        # Cast of placeholder with type hint
        assertRows(execute(cql, table, "SELECT k FROM %s WHERE k = CAST((float) ? AS int)", 3.4), row(3))

        # Cast of placeholder without type hint
        assertInvalidMessage(cql, table, "Ambiguous call to function system.castAsInt",
                                    "SELECT k FROM %s WHERE k = CAST(? AS int)", 3.4)

        # Type hint of cast
        assertRows(execute(cql, table, "SELECT k FROM %s WHERE k = (int) CAST(4.9 AS int)"), row(4))

        # FIXME: I commented out these tests for UDF because of laziness to
        # translate it to Lua for Scylla :-( Bring it back.

        #// Function of cast
        #assertRows(execute(cql, table, String.format("SELECT k FROM %%s WHERE k = %s(CAST(5 AS float))", floatToInt())), row(5))

        #// Cast of function
        #assertRows(execute(cql, table, String.format("SELECT k FROM %%s WHERE k = CAST(%s(6) AS int)", intToFloat())), row(6))

# Verifies that the {@code CAST} function can be used in the {@code WHERE} clause of {@code DELETE} statements.
@pytest.mark.xfail(reason="issue #14522")
def testCastsInDeleteWhereClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key)") as table:
        for i in range(1, 7):
            execute(cql, table, "INSERT INTO %s (k) VALUES (?)", i)

        # Simple cast
        execute(cql, table, "DELETE FROM %s WHERE k = CAST(1.3 AS int)")
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 1))

        # Nested casts
        execute(cql, table, "DELETE FROM %s WHERE k = CAST(CAST(CAST(2.3 AS int) AS float) AS int)")
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 2))

        # Cast of placeholder with type hint
        execute(cql, table, "DELETE FROM %s WHERE k = CAST((float) ? AS int)", 3.4)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 3))

        # Cast of placeholder without type hint
        assertInvalidMessage(cql, table, "Ambiguous call to function system.castAsInt",
                                    "DELETE FROM %s WHERE k = CAST(? AS int)", 3.4)

        # Type hint of cast
        execute(cql, table, "DELETE FROM %s WHERE k = (int) CAST(4.9 AS int)")
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 4))

        # FIXME: I commented out these tests for UDF because of laziness to
        # translate it to Lua for Scylla :-( Bring it back.

        #// Function of cast
        #execute(cql, table, String.format("DELETE FROM %%s WHERE k = %s(CAST(5 AS float))", floatToInt()))
        #assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 5))

        #// Cast of function
        #execute(cql, table, String.format("DELETE FROM %%s WHERE k = CAST(%s(6) AS int)", intToFloat()))
        #assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 6))

# FIXME: I commented out these utility functions for UDF because of laziness to
# translate it to Lua for Scylla :-( Bring it back.
#    /**
#     * Creates a CQL function that casts an {@code int} argument into a {@code float}.
#     *
#     * @return the name of the created function
#     */
#    private String floatToInt() throws Throwable
#    {
#        return createFunction(KEYSPACE,
#                              "int, int",
#                              "CREATE FUNCTION IF NOT EXISTS %s (x float) " +
#                              "CALLED ON NULL INPUT " +
#                              "RETURNS int " +
#                              "LANGUAGE java " +
#                              "AS 'return Float.valueOf(x).intValue();'")
#    }
#
#    /**
#     * Creates a CQL function that casts a {@code float} argument into an {@code int}.
#     *
#     * @return the name of the created function
#     */
#    private String intToFloat() throws Throwable
#    {
#        return createFunction(KEYSPACE,
#                              "int, int",
#                              "CREATE FUNCTION IF NOT EXISTS %s (x int) " +
#                              "CALLED ON NULL INPUT " +
#                              "RETURNS float " +
#                              "LANGUAGE java " +
#                              "AS 'return (float) x;'")
#    }

# Verifies that the {@code CAST} function can be used in the {@code WHERE} clause of {@code CREATE MATERIALIZED
@pytest.mark.xfail(reason="issue #14522")
def testCastsInCreateViewWhereClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, v int)") as table:
        viewName = test_keyspace + ".mv_with_cast"
        execute(cql, table, f"CREATE MATERIALIZED VIEW {viewName} AS SELECT * FROM {table} WHERE k < CAST(3.14 AS int) AND v IS NOT NULL PRIMARY KEY (v, k)")

        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, 10)")
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (2, 20)")
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (3, 30)")

        assertRows(execute(cql, table, f"SELECT * FROM {viewName}"), row(10, 1), row(20, 2))

        execute(cql, table, "DROP MATERIALIZED VIEW " + viewName)
