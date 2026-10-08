# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Any tests that do not fit in any other category,
# migrated from python dtests, CASSANDRA-9160

from ...porting import *
import math

# Test support for nulls
# migrated from cql_tests.py:TestCQL.null_support_test()
def testNullSupport(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, v1 int, v2 set<text>, PRIMARY KEY (k, c))") as table:
        execute(cql, table, "INSERT INTO %s (k, c, v1, v2) VALUES (0, 0, null, {'1', '2'})")
        execute(cql, table, "INSERT INTO %s (k, c, v1) VALUES (0, 1, 1)")

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 0, null, {"1", "2"}),
                   row(0, 1, 1, null))

        execute(cql, table, "INSERT INTO %s (k, c, v1) VALUES (0, 1, null)")
        execute(cql, table, "INSERT INTO %s (k, c, v2) VALUES (0, 0, null)")

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 0, null, null),
                   row(0, 1, null, null))

        assert_invalid(cql, table, "INSERT INTO %s (k, c, v2) VALUES (0, 2, {1, null})")
        # Scylla deliberately allows "WHERE k = null", and it just matches
        # nothing - see test_null.py::test_filtering_eq_null. So this check
        # is commented out:
        #assert_invalid(cql, table, "SELECT * FROM %s WHERE k = null")
        assert_invalid(cql, table, "INSERT INTO %s (k, c, v2) VALUES (0, 0, { 'foo', 'bar', null })")

# Test reserved keywords
# migrated from cql_tests.py:TestCQL.reserved_keyword_test()
def testReservedKeywords(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key text PRIMARY KEY, count counter)") as table:
        table_name = unique_name()
        assert_invalid_throw(cql, table, SyntaxException, f"CREATE TABLE {test_keyspace}.{table_name} (select text PRIMARY KEY, x int)")

# Test identifiers
# migrated from cql_tests.py:TestCQL.identifier_test()
def testIdentifiers(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key_23 int PRIMARY KEY, CoLuMn int)") as table:
        execute(cql, table, "INSERT INTO %s (Key_23, Column) VALUES (0, 0)")
        execute(cql, table, "INSERT INTO %s (KEY_23, COLUMN) VALUES (0, 0)")

        assert_invalid(cql, table, "INSERT INTO %s (key_23, column, column) VALUES (0, 0, 0)")
        assert_invalid(cql, table, "INSERT INTO %s (key_23, column, COLUMN) VALUES (0, 0, 0)")
        assert_invalid(cql, table, "INSERT INTO %s (key_23, key_23, column) VALUES (0, 0, 0)")
        assert_invalid(cql, table, "INSERT INTO %s (key_23, KEY_23, column) VALUES (0, 0, 0)")

        table_name = unique_name()
        assert_invalid_throw(cql, table, SyntaxException, f"CREATE TABLE {test_keyspace}.{table_name} (select int PRIMARY KEY, column int)")

# Migrated from cql_tests.py:TestCQL.unescaped_string_test()
def testUnescapedString(cql, test_keyspace):
    with create_table(cql, test_keyspace, "( k text PRIMARY KEY, c text, )") as table:
        #The \ in this query string is not forwarded to cassandra.
        #The ' is being escaped in python, but only ' is forwarded
        #over the wire instead of \'.
        assert_invalid_throw(cql, table, SyntaxException, "INSERT INTO %s (k, c) VALUES ('foo', 'CQL is cassandra\'s best friend')")

# Migrated from cql_tests.py:TestCQL.boolean_test()
def testBoolean(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k boolean PRIMARY KEY, b boolean)") as table:
        execute(cql, table, "INSERT INTO %s (k, b) VALUES (true, false)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = true"),
                   row(true, false))

# Migrated from cql_tests.py:TestCQL.float_with_exponent_test()
def testFloatWithExponent(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, d double, f float)") as table:
        execute(cql, table, "INSERT INTO %s (k, d, f) VALUES (0, 3E+10, 3.4E3)")
        execute(cql, table, "INSERT INTO %s (k, d, f) VALUES (1, 3.E10, -23.44E-3)")
        execute(cql, table, "INSERT INTO %s (k, d, f) VALUES (2, 3, -2)")

# Migrated from cql_tests.py:TestCQL.conversion_functions_test()
# Reproduces SCYLLADB-5141 (snake_case names of native functions).
@pytest.mark.xfail(reason="SCYLLADB-5141")
def testConversionFunctions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, i varint, b blob)") as table:
        execute(cql, table, "INSERT INTO %s (k, i, b) VALUES (0, blob_as_varint(bigint_as_blob(3)), text_as_blob('foobar'))")
        assert_rows(execute(cql, table, "SELECT i, blob_as_text(b) FROM %s WHERE k = 0"),
                   row(3, "foobar"))

# Migrated from cql_tests.py:TestCQL.empty_blob_test()
def testEmptyBlob(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, b blob)") as table:
        execute(cql, table, "INSERT INTO %s (k, b) VALUES (0, 0x)")

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, b""))

def fill(cql, table):
    for i in range(2):
        for j in range(2):
            execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", i, j, i + j)

    return getRows(execute(cql, table, "SELECT * FROM %s"))

# Migrated from cql_tests.py:TestCQL.empty_in_test()
def testEmpty(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, k2 int, v int, PRIMARY KEY (k1, k2))") as table:
        # Inserts a few rows to make sure we don 't actually query something
        rows = fill(cql, table)

        # Test empty IN() in SELECT
        assert_empty(execute(cql, table, "SELECT v FROM %s WHERE k1 IN ()"))
        assert_empty(execute(cql, table, "SELECT v FROM %s WHERE k1 = 0 AND k2 IN ()"))

        # Test empty IN() in DELETE
        execute(cql, table, "DELETE FROM %s WHERE k1 IN ()")
        assertArrayEquals(rows, getRows(execute(cql, table, "SELECT * FROM %s")))

        # Test empty IN() in UPDATE
        execute(cql, table, "UPDATE %s SET v = 3 WHERE k1 IN () AND k2 = 2")
        assertArrayEquals(rows, getRows(execute(cql, table, "SELECT * FROM %s")))

# Migrated from cql_tests.py:TestCQL.function_with_null_test()
# Reproduces SCYLLADB-5141 (snake_case names of native functions).
@pytest.mark.xfail(reason="SCYLLADB-5141")
def testFunctionWithNull(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t timeuuid,)") as table:
        execute(cql, table, "INSERT INTO %s (k) VALUES (0)")
        rows = getRows(execute(cql, table, "SELECT to_timestamp(t) FROM %s WHERE k=0"))
        assert rows[0][0] is None

# Migrated from cql_tests.py:TestCQL.column_name_validation_test()
def testColumnNameValidation(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, c int, v timeuuid, PRIMARY KEY (k, c))") as table:
        assert_invalid(cql, table, "INSERT INTO %s (k, c) VALUES ('', 0)")

        # Insert a value that don't fit 'int'
        assert_invalid(cql, table, "INSERT INTO %s (k, c) VALUES (0, 10000000000)")

        # Insert a non-version 1 uuid
        assert_invalid(cql, table, "INSERT INTO %s (k, c, v) VALUES (0, 0, 550e8400-e29b-41d4-a716-446655440000)")

# Migrated from cql_tests.py:TestCQL.nan_infinity_test()
def testNanInfinityValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(f float PRIMARY KEY)") as table:
        execute(cql, table, "INSERT INTO %s (f) VALUES (NaN)")
        execute(cql, table, "INSERT INTO %s (f) VALUES (-NaN)")
        execute(cql, table, "INSERT INTO %s (f) VALUES (Infinity)")
        execute(cql, table, "INSERT INTO %s (f) VALUES (-Infinity)")

        selected = getRows(execute(cql, table, "SELECT * FROM %s"))

        # selected should be[[nan],[inf],[-inf]],
        # but assert element - wise because NaN!=NaN
        assert len(selected) == 3
        assert len(selected[0]) == 1
        assert math.isnan(selected[0][0])

        assert math.isinf(selected[1][0]) #inf
        assert selected[1][0] > 0

        assert math.isinf(selected[2][0]) #-inf
        assert selected[2][0] < 0
