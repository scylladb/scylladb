# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ..porting import *
from .abstract_format_fct import create_format_table, create_default_format_table, format_double
from cassandra.protocol import InvalidRequest
import random

# The function format_bytes() was added in Cassandra 6 (CASSANDRA-19546), so
# these tests are marked new_to_cassandra_6.

INT = "int"
TINYINT = "tinyint"
SMALLINT = "smallint"
BIGINT = "bigint"
VARINT = "varint"
ASCII = "ascii"
TEXT = "text"

INTEGER_MAX_VALUE = 2**31 - 1
SHORT_MAX_VALUE = 2**15 - 1
BYTE_MAX_VALUE = 2**7 - 1
LONG_MAX_VALUE = 2**63 - 1

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testOneValueArgumentExact(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 1073741825],
                                                         [2, 1073741823],
                                                         [3, 0]]) as table: # 0 B
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 1"), row("1 GiB"))
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 2"), row("1024 MiB"))
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 3"), row("0 B"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testOneValueArgumentDecimalRoundup(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 1563401650],
                                                         [2, 1072441589],
                                                         [3, 102775],
                                                         [4, 102]]) as table:
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 1"), row("1.46 GiB")) # 1.4560
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 2"), row("1022.76 MiB")) # 1022.7599
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 3"), row("100.37 KiB")) # 100.3662
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 4"), row("102 B"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testOneValueArgumentDecimalRoundDown(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 1557999386],
                                                         [2, 1072433201],
                                                         [3, 102769],
                                                         [4, 102]]) as table:
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 1"), row("1.45 GiB")) # 1.451
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 2"), row("1022.75 MiB")) # 1022.752
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 3"), row("100.36 KiB")) # 100.3613
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 4"), row("102 B"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testValueAndUnitArgumentsExact(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 1073741825],
                                                         [2, 0]]) as table:
        assertRows(execute(cql, table, "select format_bytes(col1, 'B') from %s where pk = 1"), row("1073741825 B"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'KiB') from %s where pk = 1"), row("1048576 KiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'MiB') from %s where pk = 1"), row("1024 MiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB') from %s where pk = 1"), row("1 GiB"))

        assertRows(execute(cql, table, "select format_bytes(col1, 'B') from %s where pk = 2"), row("0 B"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'KiB') from %s where pk = 2"), row("0 KiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'MiB') from %s where pk = 2"), row("0 MiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB') from %s where pk = 2"), row("0 GiB"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testValueAndUnitArgumentsDecimal(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 1563401650],
                                                         [2, 1557999336]]) as table:
        assertRows(execute(cql, table, "select format_bytes(col1, 'B') from %s where pk = 1"), row("1563401650 B"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'KiB') from %s where pk = 1"), row("1526759.42 KiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'MiB') from %s where pk = 1"), row("1490.98 MiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB') from %s where pk = 1"), row("1.46 GiB"))

        assertRows(execute(cql, table, "select format_bytes(col1, 'B') from %s where pk = 2"), row("1557999336 B"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'KiB') from %s where pk = 2"), row("1521483.73 KiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'MiB') from %s where pk = 2"), row("1485.82 MiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB') from %s where pk = 2"), row("1.45 GiB"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testValueWithSourceAndTargetArgumentExact(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 1073741825],
                                                         [2, 1],
                                                         [3, 0]]) as table:
        assertRows(execute(cql, table, "select format_bytes(col1, 'B',   'B') from %s where pk = 1"), row("1073741825 B"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'KiB') from %s where pk = 1"), row("1048576 KiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'MiB') from %s where pk = 1"), row("1024 MiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'GiB') from %s where pk = 1"), row("1 GiB"))

        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB', 'GiB') from %s where pk = 2"), row("1 GiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB', 'MiB') from %s where pk = 2"), row("1024 MiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB', 'KiB') from %s where pk = 2"), row("1048576 KiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB',   'B') from %s where pk = 2"), row("1073741824 B"))

        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB', 'GiB') from %s where pk = 3"), row("0 GiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB', 'MiB') from %s where pk = 3"), row("0 MiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB', 'KiB') from %s where pk = 3"), row("0 KiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB',   'B') from %s where pk = 3"), row("0 B"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testValueWithSourceAndTargetArgumentDecimal(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 1563401650],
                                                         [2, 1557999336]]) as table:
        assertRows(execute(cql, table, "select format_bytes(col1, 'B',   'B') from %s where pk = 1"), row("1563401650 B"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'KiB') from %s where pk = 1"), row("1526759.42 KiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'MiB') from %s where pk = 1"), row("1490.98 MiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'GiB') from %s where pk = 1"), row("1.46 GiB"))

        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'B') from %s where pk = 2"), row("1557999336 B"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'KiB') from %s where pk = 2"), row("1521483.73 KiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'MiB') from %s where pk = 2"), row("1485.82 MiB"))
        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'GiB') from %s where pk = 2"), row("1.45 GiB"))

# The Java test uses QuickTheories to check 1024 random positive integers.
# We check fewer values, to keep the test fast.
# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testFuzzNumberGenerators(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int primary key, col1 int)") as table:
        rand = random.Random(0)
        for _ in range(100):
            randInt = rand.randint(1, INTEGER_MAX_VALUE)
            execute(cql, table, "INSERT INTO %s (pk, col1) VALUES (?, ?)", 1, randInt)

            assertRows(execute(cql, table, "select format_bytes(col1, 'MiB') from %s where pk = 1"), row(format_double(randInt / 1024.0 / 1024.0) + " MiB"))
            assertRows(execute(cql, table, "select format_bytes(col1, 'KiB', 'GiB') from %s where pk = 1"), row(format_double(randInt / 1024.0 / 1024.0) + " GiB"))
            assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'GiB') from %s where pk = 1"), row(format_double(randInt / 1024.0 / 1024.0 / 1024.0 ) + " GiB"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testOverflow(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [BIGINT, INT, SMALLINT, TINYINT],
                    [[1,
                      1073741825 * 1024 + 1,
                      INTEGER_MAX_VALUE - 1,
                      SHORT_MAX_VALUE - 1,
                      BYTE_MAX_VALUE - 1],
                     [2,
                      1073741825 * 1024 + 1,
                      INTEGER_MAX_VALUE,
                      SHORT_MAX_VALUE,
                      BYTE_MAX_VALUE]]) as table:

        # this will stop at Long.MAX_VALUE
        assertRows(execute(cql, table, "select format_bytes(col1, 'GiB', 'B') from %s where pk = 1"), row("9223372036854776000 B"))
        assertRows(execute(cql, table, "select format_bytes(col2, 'GiB', 'B') from %s where pk = 1"), row("2305843007066210300 B"))
        assertRows(execute(cql, table, "select format_bytes(col3, 'GiB', 'B') from %s where pk = 1"), row("35182224605184 B"))
        assertRows(execute(cql, table, "select format_bytes(col4, 'GiB', 'B') from %s where pk = 1"), row("135291469824 B"))

        assertRows(execute(cql, table, "select format_bytes(col2, 'GiB', 'B') from %s where pk = 2"), row("2305843008139952130 B"))
        assertRows(execute(cql, table, "select format_bytes(col3, 'GiB', 'B') from %s where pk = 2"), row("35183298347008 B"))
        assertRows(execute(cql, table, "select format_bytes(col4, 'GiB', 'B') from %s where pk = 2"), row("136365211648 B"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testAllSupportedColumnTypes(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT, TINYINT, SMALLINT, BIGINT, VARINT, ASCII, TEXT],
                    [[1,
                      INTEGER_MAX_VALUE,
                      BYTE_MAX_VALUE,
                      SHORT_MAX_VALUE,
                      LONG_MAX_VALUE,
                      INTEGER_MAX_VALUE,
                      "'" + str(INTEGER_MAX_VALUE) + "'",
                      "'" + str(INTEGER_MAX_VALUE) + "'",
                      ]]) as table:

        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 1"), row("2 GiB"))
        assertRows(execute(cql, table, "select format_bytes(col2) from %s where pk = 1"), row("127 B"))
        assertRows(execute(cql, table, "select format_bytes(col3) from %s where pk = 1"), row("32 KiB"))
        assertRows(execute(cql, table, "select format_bytes(col4) from %s where pk = 1"), row("8589934592 GiB"))
        assertRows(execute(cql, table, "select format_bytes(col5) from %s where pk = 1"), row("2 GiB"))
        assertRows(execute(cql, table, "select format_bytes(col6) from %s where pk = 1"), row("2 GiB"))
        assertRows(execute(cql, table, "select format_bytes(col7) from %s where pk = 1"), row("2 GiB"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testNegativeValueIsInvalid(cql, test_keyspace, new_to_cassandra_6):
    with create_default_format_table(cql, test_keyspace, [["1", "-1", "-2"]]) as table:
        assertInvalidThrowMessage(cql, table, "value must be non-negative", InvalidRequest,
                                  "select format_bytes(col1) from %s where pk = 1")

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testUnparsableTextIsInvalid(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [TEXT], [[1, "'abc'"], [2, "'-1'"]]) as table:
        assertInvalidThrowMessage(cql, table, "unable to convert string 'abc' to a value of type long", InvalidRequest,
                                  "select format_bytes(col1) from %s where pk = 1")

        assertInvalidThrowMessage(cql, table, "value must be non-negative", InvalidRequest,
                                  "select format_bytes(col1) from %s where pk = 2")

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testInvalidUnits(cql, test_keyspace, new_to_cassandra_6):
    with create_default_format_table(cql, test_keyspace, [["1", "1", "2"]]) as table:
        for functionCall in ["format_bytes(col1, 'abc')",
                             "format_bytes(col1, 'B', 'abc')",
                             "format_bytes(col1, 'abc', 'B')",
                             "format_bytes(col1, 'abc', 'abc')"]:
            assertInvalidThrowMessage(cql, table, "Unsupported data storage unit: abc. Supported units are: B, KiB, MiB, GiB", InvalidRequest,
                                      "select " + functionCall + " from %s where pk = 1")

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testInvalidArgumentsSize(cql, test_keyspace, new_to_cassandra_6):
    with create_default_format_table(cql, test_keyspace, [["1", "1", "2"]]) as table:
        # Test arguemnt size = 0
        assertInvalidThrowMessage(cql, table, "Invalid number of arguments for function system.format_bytes([int|tinyint|smallint|bigint|varint|ascii|text], [ascii], [ascii])", InvalidRequest,
                                  "select format_bytes() from %s where pk = 1")

        # Test argument size > 3
        assertInvalidThrowMessage(cql, table, "Invalid number of arguments for function system.format_bytes([int|tinyint|smallint|bigint|varint|ascii|text], [ascii], [ascii])", InvalidRequest,
                                  "select format_bytes(col1, 'B', 'KiB', 'GiB') from %s where pk = 1")

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testHandlingNullValues(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [TEXT, ASCII, INT],
                             [[1, None, None, None]]) as table:

        assertRows(execute(cql, table, "select format_bytes(col1), format_bytes(col2), format_bytes(col3) from %s where pk = 1"),
                   row(None, None, None))

        assertRows(execute(cql, table, "select format_bytes(col1, 'B') from %s where pk = 1"), row(None))
        assertRows(execute(cql, table, "select format_bytes(col1, 'B', 'KiB') from %s where pk = 1"), row(None))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testHandlingNullArguments(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [TEXT, ASCII, INT],
                             [[1, None, None, None],
                              [2, "'1'", "'2'", 3]]) as table:

        assertRows(execute(cql, table, "select format_bytes(col1, null) from %s where pk = 1"), row(None))

        for functionCall in ["format_bytes(col3, null)",
                             "format_bytes(col3, null, null)",
                             "format_bytes(col3, null, 'KiB')",
                             "format_bytes(col3, 'KiB', null)"]:
            assertInvalidThrowMessage(cql, table, "none of the arguments may be null", InvalidRequest,
                                      "select " + functionCall + " from %s where pk = 2")

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testSizeSmallerThan1KibiByte(cql, test_keyspace, new_to_cassandra_6):
    with create_default_format_table(cql, test_keyspace, [["1", "900", "2000"]]) as table:
        assertRows(execute(cql, table, "select format_bytes(col1) from %s where pk = 1"), row("900 B"))
