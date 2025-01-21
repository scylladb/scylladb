# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 2a4cd36475de3eb47207cd88d2d472b876c6816d
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from ...cassandra_tests.porting import *

from decimal import Decimal
from ctypes import c_float
from cassandra.protocol import FunctionFailure
from cassandra.util import Date
from datetime import datetime

@pytest.mark.xfail(reason="#2693")
def testStringConcatenation(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a text, b ascii, c text, PRIMARY KEY(a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('जॉन', 'Doe', 'जॉन Doe')")

        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a + a, a + b, b + a, b + b FROM %s WHERE a = 'जॉन' AND b = 'Doe' AND c = 'जॉन Doe'"),
                "a + a", "a + b", "b + a", "b + b")

        assertRows(execute(cql, table, "SELECT a + ' ' + a, a + ' ' + b, b + ' ' + a, b + ' ' + b FROM %s WHERE a = 'जॉन' AND b = 'Doe' AND c = 'जॉन Doe'"),
            row("जॉन जॉन", "जॉन Doe", "Doe जॉन", "Doe Doe"))

@pytest.mark.xfail(reason="#2693")
def testSingleOperations(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a tinyint, b smallint, c int, d bigint, e float, f double, g varint, h decimal, PRIMARY KEY(a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f, g, h) VALUES (1, 2, 3, 4, 5.5, 6.5, 7, 8.5)")

        # Test additions
        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a + a, b + a, c + a, d + a, e + a, f + a, g + a, h + a FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                          "a + a", "b + a", "c + a", "d + a", "e + a", "f + a", "g + a", "h + a")

        assertRows(execute(cql, table, "SELECT a + a, b + a, c + a, d + a, e + a, f + a, g + a, h + a FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(2, 3, 4, 5, 6.5, 7.5, 8, Decimal("9.5")))

        assertRows(execute(cql, table, "SELECT a + b, b + b, c + b, d + b, e + b, f + b, g + b, h + b FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(3, 4, 5, 6, c_float(7.5).value, 8.5, 9, Decimal("10.5")))

        assertRows(execute(cql, table, "SELECT a + c, b + c, c + c, d + c, e + c, f + c, g + c, h + c FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(4, 5, 6, 7, c_float(8.5).value, 9.5, 10, Decimal("11.5")))

        assertRows(execute(cql, table, "SELECT a + d, b + d, c + d, d + d, e + d, f + d, g + d, h + d FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(5, 6, 7, 8, 9.5, 10.5, 11, Decimal("12.5")))

        assertRows(execute(cql, table, "SELECT a + e, b + e, c + e, d + e, e + e, f + e, g + e, h + e FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(c_float(6.5).value, c_float(7.5).value, c_float(8.5).value, 9.5, c_float(11.0).value, 12.0, Decimal("12.5"), Decimal("14.0")))

        assertRows(execute(cql, table, "SELECT a + f, b + f, c + f, d + f, e + f, f + f, g + f, h + f FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(7.5, 8.5, 9.5, 10.5, 12.0, 13.0, Decimal("13.5"), Decimal("15.0")))

        assertRows(execute(cql, table, "SELECT a + g, b + g, c + g, d + g, e + g, f + g, g + g, h + g FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(8, 9, 10, 11, Decimal("12.5"), Decimal("13.5"), 14, Decimal("15.5")))

        assertRows(execute(cql, table, "SELECT a + h, b + h, c + h, d + h, e + h, f + h, g + h, h + h FROM %s WHERE a = 1 AND b = 2 AND c = 1 + 2"),
                   row(Decimal("9.5"),
                       Decimal("10.5"),
                       Decimal("11.5"),
                       Decimal("12.5"),
                       Decimal("14.0"),
                       Decimal("15.0"),
                       Decimal("15.5"),
                       Decimal("17.0")))

        # Test substractions

        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a - a, b - a, c - a, d - a, e - a, f - a, g - a, h - a FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                          "a - a", "b - a", "c - a", "d - a", "e - a", "f - a", "g - a", "h - a")

        assertRows(execute(cql, table, "SELECT a - a, b - a, c - a, d - a, e - a, f - a, g - a, h - a FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(0, 1, 2, 3, c_float(4.5).value, 5.5, 6, Decimal("7.5")))

        assertRows(execute(cql, table, "SELECT a - b, b - b, c - b, d - b, e - b, f - b, g - b, h - b FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(-1, 0, 1, 2, c_float(3.5).value, 4.5, 5, Decimal("6.5")))

        assertRows(execute(cql, table, "SELECT a - c, b - c, c - c, d - c, e - c, f - c, g - c, h - c FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(-2, -1, 0, 1, c_float(2.5).value, 3.5, 4, Decimal("5.5")))

        assertRows(execute(cql, table, "SELECT a - d, b - d, c - d, d - d, e - d, f - d, g - d, h - d FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(-3, -2, -1, 0, 1.5, 2.5, 3, Decimal("4.5")))

        assertRows(execute(cql, table, "SELECT a - e, b - e, c - e, d - e, e - e, f - e, g - e, h - e FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(c_float(-4.5).value, c_float(-3.5).value, c_float(-2.5).value, -1.5, c_float(0.0).value, 1.0, Decimal("1.5"), Decimal("3.0")))

        assertRows(execute(cql, table, "SELECT a - f, b - f, c - f, d - f, e - f, f - f, g - f, h - f FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(-5.5, -4.5, -3.5, -2.5, -1.0, 0.0, Decimal("0.5"), Decimal("2.0")))

        assertRows(execute(cql, table, "SELECT a - g, b - g, c - g, d - g, e - g, f - g, g - g, h - g FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(-6, -5, -4, -3, Decimal("-1.5"), Decimal("-0.5"), 0, Decimal("1.5")))

        assertRows(execute(cql, table, "SELECT a - h, b - h, c - h, d - h, e - h, f - h, g - h, h - h FROM %s WHERE a = 1 AND b = 2 AND c = 4 - 1"),
                   row(Decimal("-7.5"),
                       Decimal("-6.5"),
                       Decimal("-5.5"),
                       Decimal("-4.5"),
                       Decimal("-3.0"),
                       Decimal("-2.0"),
                       Decimal("-1.5"),
                       Decimal("0.0")))

        # Test multiplications

        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a * a, b * a, c * a, d * a, e * a, f * a, g * a, h * a FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                          "a * a", "b * a", "c * a", "d * a", "e * a", "f * a", "g * a", "h * a")

        assertRows(execute(cql, table, "SELECT a * a, b * a, c * a, d * a, e * a, f * a, g * a, h * a FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(1, 2, 3, 4, c_float(5.5).value, 6.5, 7, Decimal("8.50")))

        assertRows(execute(cql, table, "SELECT a * b, b * b, c * b, d * b, e * b, f * b, g * b, h * b FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(2, 4, 6, 8, c_float(11.0).value, 13.0, 14, Decimal("17.00")))

        assertRows(execute(cql, table, "SELECT a * c, b * c, c * c, d * c, e * c, f * c, g * c, h * c FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(3, 6, 9, 12, c_float(16.5).value, 19.5, 21, Decimal("25.50")))

        assertRows(execute(cql, table, "SELECT a * d, b * d, c * d, d * d, e * d, f * d, g * d, h * d FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(4, 8, 12, 16, 22.0, 26.0, 28, Decimal("34.00")))

        assertRows(execute(cql, table, "SELECT a * e, b * e, c * e, d * e, e * e, f * e, g * e, h * e FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(c_float(5.5).value, c_float(11.0).value, c_float(16.5).value, 22.0, c_float(30.25).value, 35.75, Decimal("38.5"), Decimal("46.75")))

        assertRows(execute(cql, table, "SELECT a * f, b * f, c * f, d * f, e * f, f * f, g * f, h * f FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(6.5, 13.0, 19.5, 26.0, 35.75, 42.25, Decimal("45.5"), Decimal("55.25")))

        assertRows(execute(cql, table, "SELECT a * g, b * g, c * g, d * g, e * g, f * g, g * g, h * g FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(7, 14, 21, 28, Decimal("38.5"), Decimal("45.5"), 49, Decimal("59.5")))

        assertRows(execute(cql, table, "SELECT a * h, b * h, c * h, d * h, e * h, f * h, g * h, h * h FROM %s WHERE a = 1 AND b = 2 AND c = 3 * 1"),
                   row(Decimal("8.50"),
                       Decimal("17.00"),
                       Decimal("25.50"),
                       Decimal("34.00"),
                       Decimal("46.75"),
                       Decimal("55.25"),
                       Decimal("59.5"),
                       Decimal("72.25")))

        # Test divisions

        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a / a, b / a, c / a, d / a, e / a, f / a, g / a, h / a FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                          "a / a", "b / a", "c / a", "d / a", "e / a", "f / a", "g / a", "h / a")

        assertRows(execute(cql, table, "SELECT a / a, b / a, c / a, d / a, e / a, f / a, g / a, h / a FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(1, 2, 3, 4, c_float(5.5).value, 6.5, 7, Decimal("8.5")))

        assertRows(execute(cql, table, "SELECT a / b, b / b, c / b, d / b, e / b, f / b, g / b, h / b FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(0, 1, 1, 2, c_float(2.75).value, 3.25, 3, Decimal("4.25")))

        assertRows(execute(cql, table, "SELECT a / c, b / c, c / c, d / c, e / c, f / c, g / c, h / c FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(0, 0, 1, 1, c_float(1.8333334).value, 2.1666666666666665, 2, Decimal("2.83333333333333333333333333333333")))

        assertRows(execute(cql, table, "SELECT a / d, b / d, c / d, d / d, e / d, f / d, g / d, h / d FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(0, 0, 0, 1, 1.375, 1.625, 1, Decimal("2.125")))

        assertRows(execute(cql, table, "SELECT a / e, b / e, c / e, d / e, e / e, f / e, g / e, h / e FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(c_float(0.18181819).value, c_float(0.36363637).value, c_float(0.54545456).value, 0.7272727272727273, c_float(1.0).value, 1.1818181818181819, Decimal("1.27272727272727272727272727272727"), Decimal("1.54545454545454545454545454545455")))

        assertRows(execute(cql, table, "SELECT a / f, b / f, c / f, d / f, e / f, f / f, g / f, h / f FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(0.15384615384615385, 0.3076923076923077, 0.46153846153846156, 0.6153846153846154, 0.8461538461538461, 1.0, Decimal("1.07692307692307692307692307692308"), Decimal("1.30769230769230769230769230769231")))

        assertRows(execute(cql, table, "SELECT a / g, b / g, c / g, d / g, e / g, f / g, g / g, h / g FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(0, 0, 0, 0,
                       Decimal("0.78571428571428571428571428571429"),
                       Decimal("0.92857142857142857142857142857143"),
                       1,
                       Decimal("1.21428571428571428571428571428571")))

        assertRows(execute(cql, table, "SELECT a / h, b / h, c / h, d / h, e / h, f / h, g / h, h / h FROM %s WHERE a = 1 AND b = 2 AND c = 3 / 1"),
                   row(Decimal("0.11764705882352941176470588235294"),
                       Decimal("0.23529411764705882352941176470588"),
                       Decimal("0.35294117647058823529411764705882"),
                       Decimal("0.47058823529411764705882352941176"),
                       Decimal("0.64705882352941176470588235294118"),
                       Decimal("0.76470588235294117647058823529412"),
                       Decimal("0.82352941176470588235294117647059"),
                       Decimal("1")))

        # Test modulo operations

        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a % a, b % a, c % a, d % a, e % a, f % a, g % a, h % a FROM %s WHERE a = 1 AND b = 2 AND c = 23 % 5"),
                          "a % a", "b % a", "c % a", "d % a", "e % a", "f % a", "g % a", "h % a")

        assertRows(execute(cql, table, "SELECT a % a, b % a, c % a, d % a, e % a, f % a, g % a, h % a FROM %s WHERE a = 1 AND b = 2 AND c = 23 % 5"),
                   row(0, 0, 0, 0, c_float(0.5).value, 0.5, 0, Decimal("0.5")))

        assertRows(execute(cql, table, "SELECT a % b, b % b, c % b, d % b, e % b, f % b, g % b, h % b FROM %s WHERE a = 1 AND b = 2 AND c = 23 % 5"),
                   row(1, 0, 1, 0, c_float(1.5).value, 0.5, 1, Decimal("0.5")))

        assertRows(execute(cql, table, "SELECT a % c, b % c, c % c, d % c, e % c, f % c, g % c, h % c FROM %s WHERE a = 1 AND b = 2 AND c = 23 % 5"),
                   row(1, 2, 0, 1, c_float(2.5).value, 0.5, 1, Decimal("2.5")))

        assertRows(execute(cql, table, "SELECT a % d, b % d, c % d, d % d, e % d, f % d, g % d, h % d FROM %s WHERE a = 1 AND b = 2 AND c = 23 % 5"),
                   row(1, 2, 3, 0, 1.5, 2.5, 3, Decimal("0.5")))

        assertRows(execute(cql, table, "SELECT a % e, b % e, c % e, d % e, e % e, f % e, g % e, h % e FROM %s WHERE a = 1 AND b = 2 AND c = 23 % 5"),
                   row(c_float(1.0).value, c_float(2.0).value, c_float(3.0).value, 4.0, c_float(0.0).value, 1.0, Decimal("1.5"), Decimal("3.0")))

        assertRows(execute(cql, table, "SELECT a % f, b % f, c % f, d % f, e % f, f % f, g % f, h % f FROM %s WHERE a = 1 AND b = 2 AND c = 23 % 5"),
                   row(1.0, 2.0, 3.0, 4.0, 5.5, 0.0, Decimal("0.5"), Decimal("2.0")))

        assertRows(execute(cql, table, "SELECT a % g, b % g, c % g, d % g, e % g, f % g, g % g, h % g FROM %s WHERE a = 1 AND b = 2 AND c = 23 % 5"),
                   row(1,2,3,4, Decimal("5.5"), Decimal("6.5"), 0, Decimal("1.5")))

        assertRows(execute(cql, table, "SELECT a % h, b % h, c % h, d % h, e % h, f % h, g % h, h % h FROM %s WHERE a = 1 AND b = 2 AND c = 23 % 5"),
                   row(Decimal("1.0"),
                       Decimal("2.0"),
                       Decimal("3.0"),
                       Decimal("4.0"),
                       Decimal("5.5"),
                       Decimal("6.5"),
                       Decimal("7"),
                       Decimal("0.0")))

        # Test negation

        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT -a, -b, -c, -d, -e, -f, -g, -h FROM %s WHERE a = 1 AND b = 2"),
                          "-a", "-b", "-c", "-d", "-e", "-f", "-g", "-h")

        assertRows(execute(cql, table, "SELECT -a, -b, -c, -d, -e, -f, -g, -h FROM %s WHERE a = 1 AND b = 2"),
                   row(-1, -2, -3, -4, c_float(-5.5).value, -6.5, -7, Decimal("-8.5")))

        # Test with null
        execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", None, 1, 2, 3)
        assertRows(execute(cql, table, "SELECT a + d, b + d, c + d, d + d, e + d, f + d, g + d, h + d FROM %s WHERE a = 1 AND b = 2"),
                   row(None, None, None, None, None, None, None, None))

@pytest.mark.xfail(reason="#2693")
def testModuloWithDecimals(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(numerator decimal, dec_mod decimal, int_mod int, bigint_mod bigint, PRIMARY KEY(numerator, dec_mod))") as table:
        execute(cql, table, "INSERT INTO %s (numerator, dec_mod, int_mod, bigint_mod) VALUES (123456789112345678921234567893123456, 2, 2, 2)")

        assertRows(execute(cql, table, "SELECT numerator % dec_mod, numerator % int_mod, numerator % bigint_mod from %s"),
                   row(Decimal("0"), Decimal("0.0"), Decimal("0.0")))

@pytest.mark.xfail(reason="#2693")
def testSingleOperationsWithLiterals(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c1 tinyint, c2 smallint, v text, PRIMARY KEY(pk, c1, c2))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (2, 2, 2, 'test')")

        # There is only one function outputing tinyint
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 2 AND c1 = 1 + 1"),
                   row(2, 2, 2, "test"))

        # As the operation can only be a sum between tinyints the expected type is tinyint
        #(cannot be reproduced in Python which sends the right type)
        #assertInvalidMessage(cql, table, "Expected 1 byte for a tinyint (4)",
        #                     "SELECT * FROM %s WHERE pk = 2 AND c1 = 1 + ?", 1)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 2 AND c1 = 1 + ?", 1),
                   row(2, 2, 2, "test"))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 + 1 AND c1 = 2"),
                   row(2, 2, 2, "test"))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 2 AND c1 = 2 AND c2 = 1 + 1"),
                   row(2, 2, 2, "test"))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 2 AND c1 = 2 AND c2 = 1 * (1 + 1)"),
                   row(2, 2, 2, "test"))

        # tinyint, smallint and int could be used there so we need to disambiguate
        assertInvalidMessage(cql, table, "Ambiguous '+' operation with args ? and 1: use type hint to disambiguate, example '(int) ?'",
                             "SELECT * FROM %s WHERE pk = ? + 1 AND c1 = 2", 1)

        assertInvalidMessage(cql, table, "Ambiguous '+' operation with args ? and 1: use type hint to disambiguate, example '(int) ?'",
                             "SELECT * FROM %s WHERE pk = 2 AND c1 = 2 AND c2 = 1 * (? + 1)", 1)

        assertRows(execute(cql, table, "SELECT 1 + 1, v FROM %s WHERE pk = 2 AND c1 = 2"),
                   row(2, "test"))

        # As the output type is unknown the ? type cannot be determined
        assertInvalidMessage(cql, table, "Ambiguous '+' operation with args 1 and ?: use type hint to disambiguate, example '(int) ?'",
                             "SELECT 1 + ?, v FROM %s WHERE pk = 2 AND c1 = 2", 1)

        # As the prefered type for the constants is int, the returned type will be int
        assertRows(execute(cql, table, "SELECT 100 + 50, v FROM %s WHERE pk = 2 AND c1 = 2"),
                   row(150, "test"))

        # As the output type is unknown the ? type cannot be determined
        assertInvalidMessage(cql, table, "Ambiguous '+' operation with args ? and 50: use type hint to disambiguate, example '(int) ?'",
                             "SELECT ? + 50, v FROM %s WHERE pk = 2 AND c1 = 2", 100)

    with create_table(cql, test_keyspace, "(a tinyint, b smallint, c int, d bigint, e float, f double, g varint, h decimal, PRIMARY KEY(a, b)) WITH CLUSTERING ORDER BY (b DESC)") as table: #// Make sure we test with ReversedTypes
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f, g, h) VALUES (1, 2, 3, 4, 5.5, 6.5, 7, 8.5)")

        # Test additions
        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a + 1, b + 1, c + 1, d + 1, e + 1, f + 1, g + 1, h + 1 FROM %s WHERE a = 1 AND b = 2"),
                          "a + 1", "b + 1", "c + 1", "d + 1", "e + 1", "f + 1", "g + 1", "h + 1")

        assertRows(execute(cql, table, "SELECT a + 1, b + 1, c + 1, d + 1, e + 1, f + 1, g + 1, h + 1 FROM %s WHERE a = 1 AND b = 2"),
                   row(2, 3, 4, 5, c_float(6.5).value, 7.5, 8, Decimal("9.5")))

        assertRows(execute(cql, table, "SELECT 2 + a, 2 + b, 2 + c, 2 + d, 2 + e, 2 + f, 2 + g, 2 + h FROM %s WHERE a = 1 AND b = 2"),
                   row(3, 4, 5, 6, c_float(7.5).value, 8.5, 9, Decimal("10.5")))

        bigInt = 2147483647 + 10

        assertRows(execute(cql, table, "SELECT a + " + str(bigInt) + ","
                               + " b + " + str(bigInt) + ","
                               + " c + " + str(bigInt) + ","
                               + " d + " + str(bigInt) + ","
                               + " e + " + str(bigInt) + ","
                               + " f + " + str(bigInt) + ","
                               + " g + " + str(bigInt) + ","
                               + " h + " + str(bigInt) + " FROM %s WHERE a = 1 AND b = 2"),
                   row(1 + bigInt,
                       2 + bigInt,
                       3 + bigInt,
                       4 + bigInt,
                       5.5 + bigInt,
                       6.5 + bigInt,
                       bigInt + 7,
                       Decimal(bigInt + 8.5)))

        assertRows(execute(cql, table, "SELECT a + 5.5, b + 5.5, c + 5.5, d + 5.5, e + 5.5, f + 5.5, g + 5.5, h + 5.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(6.5, 7.5, 8.5, 9.5, 11.0, 12.0, Decimal("12.5"), Decimal("14.0")))

        assertRows(execute(cql, table, "SELECT a + 6.5, b + 6.5, c + 6.5, d + 6.5, e + 6.5, f + 6.5, g + 6.5, h + 6.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(7.5, 8.5, 9.5, 10.5, 12.0, 13.0, Decimal("13.5"), Decimal("15.0")))

        # Test substractions

        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a - 1, b - 1, c - 1, d - 1, e - 1, f - 1, g - 1, h - 1 FROM %s WHERE a = 1 AND b = 2"),
                          "a - 1", "b - 1", "c - 1", "d - 1", "e - 1", "f - 1", "g - 1", "h - 1")

        assertRows(execute(cql, table, "SELECT a - 1, b - 1, c - 1, d - 1, e - 1, f - 1, g - 1, h - 1 FROM %s WHERE a = 1 AND b = 2"),
                   row(0, 1, 2, 3, c_float(4.5).value, 5.5, 6, Decimal("7.5")))

        assertRows(execute(cql, table, "SELECT a - 2, b - 2, c - 2, d - 2, e - 2, f - 2, g - 2, h - 2 FROM %s WHERE a = 1 AND b = 2"),
                   row(-1, 0, 1, 2, c_float(3.5).value, 4.5, 5, Decimal("6.5")))

        assertRows(execute(cql, table, "SELECT a - 3, b - 3, 3 - 3, d - 3, e - 3, f - 3, g - 3, h - 3 FROM %s WHERE a = 1 AND b = 2"),
                   row(-2, -1, 0, 1, c_float(2.5).value, 3.5, 4, Decimal("5.5")))

        assertRows(execute(cql, table, "SELECT a - " + str(bigInt) + ","
                               + " b - " + str(bigInt) + ","
                               + " c - " + str(bigInt) + ","
                               + " d - " + str(bigInt) + ","
                               + " e - " + str(bigInt) + ","
                               + " f - " + str(bigInt) + ","
                               + " g - " + str(bigInt) + ","
                               + " h - " + str(bigInt) + " FROM %s WHERE a = 1 AND b = 2"),
                   row(1 - bigInt,
                       2 - bigInt,
                       3 - bigInt,
                       4 - bigInt,
                       5.5 - bigInt,
                       6.5 - bigInt,
                       7 - bigInt,
                       Decimal(8.5 - bigInt)))

        assertRows(execute(cql, table, "SELECT a - 5.5, b - 5.5, c - 5.5, d - 5.5, e - 5.5, f - 5.5, g - 5.5, h - 5.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(-4.5, -3.5, -2.5, -1.5, 0.0, 1.0, Decimal("1.5"), Decimal("3.0")))

        assertRows(execute(cql, table, "SELECT a - 6.5, b - 6.5, c - 6.5, d - 6.5, e - 6.5, f - 6.5, g - 6.5, h - 6.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(-5.5, -4.5, -3.5, -2.5, -1.0, 0.0, Decimal("0.5"), Decimal("2.0")))

        # Test multiplications

        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a * 1, b * 1, c * 1, d * 1, e * 1, f * 1, g * 1, h * 1 FROM %s WHERE a = 1 AND b = 2"),
                          "a * 1", "b * 1", "c * 1", "d * 1", "e * 1", "f * 1", "g * 1", "h * 1")

        assertRows(execute(cql, table, "SELECT a * 1, b * 1, c * 1, d * 1, e * 1, f * 1, g * 1, h * 1 FROM %s WHERE a = 1 AND b = 2"),
                   row(1, 2, 3, 4, c_float(5.5).value, 6.5, 7, Decimal("8.50")))

        assertRows(execute(cql, table, "SELECT a * 2, b * 2, c * 2, d * 2, e * 2, f * 2, g * 2, h * 2 FROM %s WHERE a = 1 AND b = 2"),
                   row(2, 4, 6, 8, c_float(11.0).value, 13.0, 14, Decimal("17.00")))

        assertRows(execute(cql, table, "SELECT a * 3, b * 3, c * 3, d * 3, e * 3, f * 3, g * 3, h * 3 FROM %s WHERE a = 1 AND b = 2"),
                   row(3, 6, 9, 12, c_float(16.5).value, 19.5, 21, Decimal("25.50")))

        assertRows(execute(cql, table, "SELECT a * " + str(bigInt) + ","
                            + " b * " + str(bigInt) + ","
                            + " c * " + str(bigInt) + ","
                            + " d * " + str(bigInt) + ","
                            + " e * " + str(bigInt) + ","
                            + " f * " + str(bigInt) + ","
                            + " g * " + str(bigInt) + ","
                            + " h * " + str(bigInt) + " FROM %s WHERE a = 1 AND b = 2"),
                               row(1 * bigInt,
                                   2 * bigInt,
                                   3 * bigInt,
                                   4 * bigInt,
                                   5.5 * bigInt,
                                   6.5 * bigInt,
                                   7 * bigInt,
                                   Decimal(8.5 * bigInt)))

        assertRows(execute(cql, table, "SELECT a * 5.5, b * 5.5, c * 5.5, d * 5.5, e * 5.5, f * 5.5, g * 5.5, h * 5.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(5.5, 11.0, 16.5, 22.0, 30.25, 35.75, Decimal("38.5"), Decimal("46.75")))

        assertRows(execute(cql, table, "SELECT a * 6.5, b * 6.5, c * 6.5, d * 6.5, e * 6.5, 6.5 * f, g * 6.5, h * 6.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(6.5, 13.0, 19.5, 26.0, 35.75, 42.25, Decimal("45.5"), Decimal("55.25")))

        # Test divisions

        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a / 1, b / 1, c / 1, d / 1, e / 1, f / 1, g / 1, h / 1 FROM %s WHERE a = 1 AND b = 2"),
                          "a / 1", "b / 1", "c / 1", "d / 1", "e / 1", "f / 1", "g / 1", "h / 1")

        assertRows(execute(cql, table, "SELECT a / 1, b / 1, c / 1, d / 1, e / 1, f / 1, g / 1, h / 1 FROM %s WHERE a = 1 AND b = 2"),
                   row(1, 2, 3, 4, c_float(5.5).value, 6.5, 7, Decimal("8.5")))

        assertRows(execute(cql, table, "SELECT a / 2, b / 2, c / 2, d / 2, e / 2, f / 2, g / 2, h / 2 FROM %s WHERE a = 1 AND b = 2"),
                   row(0, 1, 1, 2, c_float(2.75).value, 3.25, 3, Decimal("4.25")))

        assertRows(execute(cql, table, "SELECT a / 3, b / 3, c / 3, d / 3, e / 3, f / 3, g / 3, h / 3 FROM %s WHERE a = 1 AND b = 2"),
                   row(0, 0, 1, 1, c_float(1.8333334).value, 2.1666666666666665, 2, Decimal("2.83333333333333333333333333333333")))

        assertRows(execute(cql, table, "SELECT a / " + str(bigInt) + ","
                + " b / " + str(bigInt) + ","
                + " c / " + str(bigInt) + ","
                + " d / " + str(bigInt) + ","
                + " e / " + str(bigInt) + ","
                + " f / " + str(bigInt) + ","
                + " g / " + str(bigInt) + " FROM %s WHERE a = 1 AND b = 2"),
                   row(1 // bigInt,
                       2 // bigInt,
                       3 // bigInt,
                       4 // bigInt,
                       5.5 / bigInt,
                       6.5 / bigInt,
                       7 // bigInt))

        assertRows(execute(cql, table, "SELECT a / 5.5, b / 5.5, c / 5.5, d / 5.5, e / 5.5, f / 5.5, g / 5.5, h / 5.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(0.18181818181818182, 0.36363636363636365, 0.5454545454545454, 0.7272727272727273, 1.0, 1.1818181818181819, Decimal("1.27272727272727272727272727272727"), Decimal("1.54545454545454545454545454545455")))

        assertRows(execute(cql, table, "SELECT a / 6.5, b / 6.5, c / 6.5, d / 6.5, e / 6.5, f / 6.5, g / 6.5, h / 6.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(0.15384615384615385, 0.3076923076923077, 0.46153846153846156, 0.6153846153846154, 0.8461538461538461, 1.0, Decimal("1.07692307692307692307692307692308"), Decimal("1.30769230769230769230769230769231")))

        # Test modulo operations

        with original_column_names(cql) as cql:
            assertColumnNames(execute(cql, table, "SELECT a % 1, b % 1, c % 1, d % 1, e % 1, f % 1, g % 1, h % 1 FROM %s WHERE a = 1 AND b = 2"),
                          "a % 1", "b % 1", "c % 1", "d % 1", "e % 1", "f % 1", "g % 1", "h % 1")

        assertRows(execute(cql, table, "SELECT a % 1, b % 1, c % 1, d % 1, e % 1, f % 1, g % 1, h % 1 FROM %s WHERE a = 1 AND b = 2"),
                   row(0, 0, 0, 0, c_float(0.5).value, 0.5, 0, Decimal("0.5")))

        assertRows(execute(cql, table, "SELECT a % 2, b % 2, c % 2, d % 2, e % 2, f % 2, g % 2, h % 2 FROM %s WHERE a = 1 AND b = 2"),
                   row(1, 0, 1, 0, c_float(1.5).value, 0.5, 1, Decimal("0.5")))

        assertRows(execute(cql, table, "SELECT a % 3, b % 3, c % 3, d % 3, e % 3, f % 3, g % 3, h % 3 FROM %s WHERE a = 1 AND b = 2"),
                   row(1, 2, 0, 1, c_float(2.5).value, 0.5, 1, Decimal("2.5")))

        assertRows(execute(cql, table, "SELECT a % " + str(bigInt) + ","
                            + " b % " + str(bigInt) + ","
                            + " c % " + str(bigInt) + ","
                            + " d % " + str(bigInt) + ","
                            + " e % " + str(bigInt) + ","
                            + " f % " + str(bigInt) + ","
                            + " g % " + str(bigInt) + ","
                            + " h % " + str(bigInt) + " FROM %s WHERE a = 1 AND b = 2"),
                               row(1 % bigInt,
                                   2 % bigInt,
                                   3 % bigInt,
                                   4 % bigInt,
                                   5.5 % bigInt,
                                   6.5 % bigInt,
                                   7 % bigInt,
                                   Decimal(8.5 % bigInt)))

        assertRows(execute(cql, table, "SELECT a % 5.5, b % 5.5, c % 5.5, d % 5.5, e % 5.5, f % 5.5, g % 5.5, h % 5.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(1.0, 2.0, 3.0, 4.0, 0.0, 1.0, Decimal("1.5"), Decimal("3.0")))

        assertRows(execute(cql, table, "SELECT a % 6.5, b % 6.5, c % 6.5, d % 6.5, e % 6.5, f % 6.5, g % 6.5, h % 6.5 FROM %s WHERE a = 1 AND b = 2"),
                   row(1.0, 2.0, 3.0, 4.0, 5.5, 0.0, Decimal("0.5"), Decimal("2.0")))

        assertRows(execute(cql, table, "SELECT a, b, 1 + 1, 2 - 1, 2 * 2, 2 / 1 , 2 % 1, (int) -1 FROM %s WHERE a = 1 AND b = 2"),
                   row(1, 2, 2, 1, 4, 2, 0, -1))

@pytest.mark.xfail(reason="#2693")
def testDivisionWithDecimals(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(numerator decimal, denominator decimal, PRIMARY KEY ((numerator, denominator)))") as table:
        execute(cql, table, "INSERT INTO %s (numerator, denominator) VALUES (8.5, 200000000000000000000000000000000000)")
        execute(cql, table, "INSERT INTO %s (numerator, denominator) VALUES (10000, 3)")

        assertRows(execute(cql, table, "SELECT numerator / denominator from %s"),
                   row(Decimal("0.0000000000000000000000000000000000425")),
                   row(Decimal("3333.33333333333333333333333333333333")))

@pytest.mark.xfail(reason="#2693")
def testWithCounters(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b counter)") as table:
        execute(cql, table, "UPDATE %s SET b = b + 1 WHERE a = 1")
        execute(cql, table, "UPDATE %s SET b = b + 1 WHERE a = 1")
        assertRows(execute(cql, table, "SELECT b FROM %s WHERE a = 1"), row(2))

        assertRows(execute(cql, table, "SELECT b + (tinyint) 1,"
                + " b + (smallint) 1,"
                + " b + 1,"
                + " b + (bigint) 1,"
                + " b + (float) 1.5,"
                + " b + 1.5,"
                + " b + (varint) 1,"
                + " b + (decimal) 1.5,"
                + " b + b FROM %s WHERE a = 1"),
                   row(3, 3, 3, 3, 3.5, 3.5, 3, Decimal("3.5"), 4))

        assertRows(execute(cql, table, "SELECT b - (tinyint) 1,"
                + " b - (smallint) 1,"
                + " b - 1,"
                + " b - (bigint) 1,"
                + " b - (float) 1.5,"
                + " b - 1.5,"
                + " b - (varint) 1,"
                + " b - (decimal) 1.5,"
                + " b - b FROM %s WHERE a = 1"),
                   row(1, 1, 1, 1, 0.5, 0.5, 1, Decimal("0.5"), 0))

        assertRows(execute(cql, table, "SELECT b * (tinyint) 1,"
                + " b * (smallint) 1,"
                + " b * 1,"
                + " b * (bigint) 1,"
                + " b * (float) 1.5,"
                + " b * 1.5,"
                + " b * (varint) 1,"
                + " b * (decimal) 1.5,"
                + " b * b FROM %s WHERE a = 1"),
                   row(2, 2, 2, 2, 3.0, 3.0, 2, Decimal("3.00"), 4))

        assertRows(execute(cql, table, "SELECT b / (tinyint) 1,"
                + " b / (smallint) 1,"
                + " b / 1,"
                + " b / (bigint) 1,"
                + " b / (float) 0.5,"
                + " b / 0.5,"
                + " b / (varint) 1,"
                + " b / (decimal) 0.5,"
                + " b / b FROM %s WHERE a = 1"),
                   row(2, 2, 2, 2, 4.0, 4.0, 2, Decimal("4"), 1))

        assertRows(execute(cql, table, "SELECT b % (tinyint) 1,"
                + " b % (smallint) 1,"
                + " b % 1,"
                + " b % (bigint) 1,"
                + " b % (float) 0.5,"
                + " b % 0.5,"
                + " b % (varint) 1,"
                + " b % (decimal) 0.5,"
                + " b % b FROM %s WHERE a = 1"),
                   row(0, 0, 0, 0, 0.0, 0.0, 0, Decimal("0.0"), 0))

        assertRows(execute(cql, table, "SELECT -b FROM %s WHERE a = 1"), row(-2))

@pytest.mark.xfail(reason="#2693")
def testPrecedenceAndParentheses(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 5, 25, 4)")

        with original_column_names(cql) as cql:
            rs = execute(cql, table, "SELECT a - c / b + d FROM %s")
            assertColumnNames(rs, "a - c / b + d")
            assertRows(rs, row(1))

            rs = execute(cql, table, "SELECT (c - b) / a + d FROM %s")
            assertColumnNames(rs, "(c - b) / a + d")
            assertRows(rs, row(14))

            rs = execute(cql, table, "SELECT c / a / b FROM %s")
            assertColumnNames(rs, "c / a / b")
            assertRows(rs, row(2))

            rs = execute(cql, table, "SELECT c / b / d FROM %s")
            assertColumnNames(rs, "c / b / d")
            assertRows(rs, row(1))

            rs = execute(cql, table, "SELECT (c - a) % d / a FROM %s")
            assertColumnNames(rs, "(c - a) % d / a")
            assertRows(rs, row(1))

            rs = execute(cql, table, "SELECT (c - a) % d / a + d FROM %s")
            assertColumnNames(rs, "(c - a) % d / a + d")
            assertRows(rs, row(5))

            rs = execute(cql, table, "SELECT -(c - a) % d / a + d FROM %s")
            assertColumnNames(rs, "-(c - a) % d / a + d")
            assertRows(rs, row(3))

            rs = execute(cql, table, "SELECT (-c - a) % d / a + d FROM %s")
            assertColumnNames(rs, "(-c - a) % d / a + d")
            assertRows(rs, row(3))

            rs = execute(cql, table, "SELECT c - a % d / a + d FROM %s")
            assertColumnNames(rs, "c - a % d / a + d")
            assertRows(rs, row(28))

            rs = execute(cql, table, "SELECT (int)((c - a) % d / (a + d)) FROM %s")
            assertColumnNames(rs, "(int)((c - a) % d / (a + d))")
            assertRows(rs, row(0))

            # test with aliases
            rs = execute(cql, table, "SELECT (int)((c - a) % d / (a + d)) as result FROM %s")
            assertColumnNames(rs, "result")
            assertRows(rs, row(0))

            rs = execute(cql, table, "SELECT c / a / b as divisions FROM %s")
            assertColumnNames(rs, "divisions")
            assertRows(rs, row(2))

            assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = (int) ? / 2 - 5", 2, 20),
                   row(2, 5, 25, 4))

            assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = (int) ? / (2 + 2)", 2, 20),
                   row(2, 5, 25, 4))

@pytest.mark.xfail(reason="#2693")
def testWithDivisionByZero(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a tinyint, b smallint, c int, d bigint, e float, f double, g varint, h decimal, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f, g, h) VALUES (0, 2, 3, 4, 5.5, 6.5, 7, 8.5)")

        OperationExecutionException = FunctionFailure
        assertInvalidThrowMessage(cql, table, "the operation 'tinyint / tinyint' failed: / by zero",
                                  OperationExecutionException,
                                  "SELECT a / a FROM %s WHERE a = 0 AND b = 2")

        assertInvalidThrowMessage(cql, table, "the operation 'smallint / tinyint' failed: / by zero",
                                  OperationExecutionException,
                                  "SELECT b / a FROM %s WHERE a = 0 AND b = 2")

        assertInvalidThrowMessage(cql, table, "the operation 'int / tinyint' failed: / by zero",
                                  OperationExecutionException,
                                  "SELECT c / a FROM %s WHERE a = 0 AND b = 2")

        assertInvalidThrowMessage(cql, table, "the operation 'bigint / tinyint' failed: / by zero",
                                  OperationExecutionException,
                                  "SELECT d / a FROM %s WHERE a = 0 AND b = 2")

        assertInvalidThrowMessage(cql, table, "the operation 'smallint / smallint' failed: / by zero",
                                  OperationExecutionException,
                                  "SELECT a FROM %s WHERE a = 0 AND b = 10/0")

        assertRows(execute(cql, table, "SELECT e / a FROM %s WHERE a = 0 AND b = 2"), row(float('inf')))
        assertRows(execute(cql, table, "SELECT f / a FROM %s WHERE a = 0 AND b = 2"), row(float('inf')))

        assertInvalidThrowMessage(cql, table, "the operation 'varint / tinyint' failed: BigInteger divide by zero",
                                  OperationExecutionException,
                                  "SELECT g / a FROM %s WHERE a = 0 AND b = 2")

        assertInvalidThrowMessage(cql, table, "the operation 'decimal / tinyint' failed: BigInteger divide by zero",
                                  OperationExecutionException,
                                  "SELECT h / a FROM %s WHERE a = 0 AND b = 2")

@pytest.mark.xfail(reason="#2693")
def testWithNanAndInfinity(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b double, c decimal)") as table:
        assertInvalidMessage(cql, table, "Ambiguous '+' operation with args ? and 1: use type hint to disambiguate, example '(int) ?'",
                             "INSERT INTO %s (a, b, c) VALUES (? + 1, ?, ?)", 0, float('nan'), Decimal("1"))

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ((int) ? + 1, -?, ?)", 0, float('nan'), Decimal("1"))

        assertRows(execute(cql, table, "SELECT * FROM %s"), row(1, result_cleanup(float('nan')), Decimal("1")))

        assertRows(execute(cql, table, "SELECT a + NAN, b + 1 FROM %s"), row(result_cleanup(float('nan')), result_cleanup(float('nan'))))
        OperationExecutionException = FunctionFailure
        assertInvalidThrowMessage(cql, table, "the operation 'decimal + double' failed: A NaN cannot be converted into a decimal",
                                  OperationExecutionException,
                                  "SELECT c + NAN FROM %s")

        assertRows(execute(cql, table, "SELECT a + (float) NAN, b + 1 FROM %s"), row(result_cleanup(float('nan')), result_cleanup(float('nan'))))
        assertInvalidThrowMessage(cql, table, "the operation 'decimal + float' failed: A NaN cannot be converted into a decimal",
                                  OperationExecutionException,
                                  "SELECT c + (float) NAN FROM %s")

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, float('inf'), Decimal("1"))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(1, float('inf'), Decimal("1")))

        assertRows(execute(cql, table, "SELECT a + INFINITY, b + 1 FROM %s"), row(float('inf'), float('inf')))
        assertInvalidThrowMessage(cql, table, "the operation 'decimal + double' failed: An infinite number cannot be converted into a decimal",
                                  OperationExecutionException,
                                  "SELECT c + INFINITY FROM %s")

        assertRows(execute(cql, table, "SELECT a + (float) INFINITY, b + 1 FROM %s"), row(float('inf'), float('inf')))
        assertInvalidThrowMessage(cql, table, "the operation 'decimal + float' failed: An infinite number cannot be converted into a decimal",
                                  OperationExecutionException,
                                  "SELECT c + (float) INFINITY FROM %s")

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, float('-inf'), Decimal("1"))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(1, float('-inf'), Decimal("1")))

        assertRows(execute(cql, table, "SELECT a + -INFINITY, b + 1 FROM %s"), row(float('-inf'), float('-inf')))
        assertInvalidThrowMessage(cql, table, "the operation 'decimal + double' failed: An infinite number cannot be converted into a decimal",
                                  OperationExecutionException,
                                  "SELECT c + -INFINITY FROM %s")

        assertRows(execute(cql, table, "SELECT a + (float) -INFINITY, b + 1 FROM %s"), row(float('-inf'), float('-inf')))
        assertInvalidThrowMessage(cql, table, "the operation 'decimal + float' failed: An infinite number cannot be converted into a decimal",
                                  OperationExecutionException,
                                  "SELECT c + (float) -INFINITY FROM %s")

@pytest.mark.xfail(reason="#2693")
def testInvalidTypes(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b boolean, c text)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, True, "test")

        assertInvalidMessage(cql, table, "the '+' operation is not supported between a and b", "SELECT a + b FROM %s")
        assertInvalidMessage(cql, table, "the '+' operation is not supported between b and c", "SELECT b + c FROM %s")
        assertInvalidMessage(cql, table, "the '+' operation is not supported between b and 1", "SELECT b + 1 FROM %s")
        assertInvalidMessage(cql, table, "the '+' operation is not supported between 1 and b", "SELECT 1 + b FROM %s")
        assertInvalidMessage(cql, table, "the '+' operation is not supported between b and NaN", "SELECT b + NaN FROM %s")
        assertInvalidMessage(cql, table, "the '/' operation is not supported between a and b", "SELECT a / b FROM %s")
        assertInvalidMessage(cql, table, "the '/' operation is not supported between b and c", "SELECT b / c FROM %s")
        assertInvalidMessage(cql, table, "the '/' operation is not supported between b and 1", "SELECT b / 1 FROM %s")
        assertInvalidMessage(cql, table, "the '/' operation is not supported between NaN and b", "SELECT NaN / b FROM %s")
        assertInvalidMessage(cql, table, "the '/' operation is not supported between -Infinity and b", "SELECT -Infinity / b FROM %s")

@pytest.mark.xfail(reason="#2693")
def testOverflow(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b tinyint, c smallint)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 1)
        assertRows(execute(cql, table, "SELECT a + (int) ?, b + (tinyint) ?, c + (smallint) ? FROM %s", 1, 1, 1),
                   row(2, 2, 2))
        assertRows(execute(cql, table, "SELECT a + (int) ?, b + (tinyint) ?, c + (smallint) ? FROM %s", 2147483647, 127, 32767),
                   row(-2147483648, -128, -32768))

@pytest.mark.xfail(reason="#2693")
def testOperationsWithDuration(cql, test_keyspace):
    # Test with timestamp type.
    with create_table(cql, test_keyspace, "(pk int, time timestamp, v int, primary key (pk, time))") as table:
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:10:00 UTC', 1)")
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:12:00 UTC', 2)")
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:14:00 UTC', 3)")
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:15:00 UTC', 4)")
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:21:00 UTC', 5)")
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:22:00 UTC', 6)")

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND time > ? - 5m", datetime(2016,9,27,16,20,0)),
                   row(1, datetime(2016,9,27,16,21,0), 5),
                   row(1, datetime(2016,9,27,16,22,0), 6))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND time >= ? - 10m", datetime(2016,9,27,16,25,0)),
                   row(1, datetime(2016,9,27,16,15,0), 4),
                   row(1, datetime(2016,9,27,16,21,0), 5),
                   row(1, datetime(2016,9,27,16,22,0), 6))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND time >= ? + 5m", datetime(2016,9,27,16,15,0)),
                   row(1, datetime(2016,9,27,16,21,0), 5),
                   row(1, datetime(2016,9,27,16,22,0), 6))

        assertRows(execute(cql, table, "SELECT time - 10m FROM %s WHERE pk = 1"),
                   row(datetime(2016,9,27,16,0,0)),
                   row(datetime(2016,9,27,16,2,0)),
                   row(datetime(2016,9,27,16,4,0)),
                   row(datetime(2016,9,27,16,5,0)),
                   row(datetime(2016,9,27,16,11,0)),
                   row(datetime(2016,9,27,16,12,0)))

        assertInvalidMessage(cql, table, "the '%' operation is not supported between time and 10m",
                             "SELECT time % 10m FROM %s WHERE pk = 1")
        assertInvalidMessage(cql, table, "the '*' operation is not supported between time and 10m",
                             "SELECT time * 10m FROM %s WHERE pk = 1")
        assertInvalidMessage(cql, table, "the '/' operation is not supported between time and 10m",
                             "SELECT time / 10m FROM %s WHERE pk = 1")
        assertInvalidThrowMessage(cql, table, "the operation 'timestamp - duration' failed: The duration must have a millisecond precision. Was: 10us",
                             FunctionFailure,
                             "SELECT * FROM %s WHERE pk = 1 AND time > ? - 10us", datetime(2016,9,27,16,15,0))

    # Test with date type.
    with create_table(cql, test_keyspace, "(pk int, time date, v int, primary key (pk, time))") as table:

        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27', 1)")
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-28', 2)")
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-29', 3)")
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-30', 4)")
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-10-01', 5)")
        execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-10-04', 6)")

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND time > ? - 5d", Date("2016-10-04")),
                   row(1, Date("2016-09-30"), 4),
                   row(1, Date("2016-10-01"), 5),
                   row(1, Date("2016-10-04"), 6))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND time > ? - 6d", Date("2016-10-04")),
                   row(1, Date("2016-09-29"), 3),
                   row(1, Date("2016-09-30"), 4),
                   row(1, Date("2016-10-01"), 5),
                   row(1, Date("2016-10-04"), 6))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND time >= ? + 1d",  Date("2016-10-01")),
                   row(1, Date("2016-10-04"), 6))

        assertRows(execute(cql, table, "SELECT time - 2d FROM %s WHERE pk = 1"),
                   row(Date("2016-09-25")),
                   row(Date("2016-09-26")),
                   row(Date("2016-09-27")),
                   row(Date("2016-09-28")),
                   row(Date("2016-09-29")),
                   row(Date("2016-10-02")))

        assertInvalidMessage(cql, table, "the '%' operation is not supported between time and 10m",
                             "SELECT time % 10m FROM %s WHERE pk = 1")
        assertInvalidMessage(cql, table, "the '*' operation is not supported between time and 10m",
                             "SELECT time * 10m FROM %s WHERE pk = 1")
        assertInvalidMessage(cql, table, "the '/' operation is not supported between time and 10m",
                             "SELECT time / 10m FROM %s WHERE pk = 1")
        assertInvalidThrowMessage(cql, table, "the operation 'date - duration' failed: The duration must have a day precision. Was: 10m",
                             FunctionFailure,
                             "SELECT * FROM %s WHERE pk = 1 AND time > ? - 10m", Date("2016-10-04"))

@pytest.mark.xfail(reason="#2693")
def testFunctionException(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c1 int, c2 int, v text, primary key (pk, c1, c2))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (1, 0, 2, 'test')")

        assertInvalidThrowMessage(cql, table,
                                  "the operation 'int / int' failed: / by zero",
                                  FunctionFailure,
                                  "SELECT c2 / c1 FROM %s WHERE pk = 1")
