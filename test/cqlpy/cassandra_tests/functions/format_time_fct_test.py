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

# The function format_time() was added in Cassandra 6 (CASSANDRA-19546), so
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
def testOneValueArgument(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 7200001], # 2h + 1ms
                                                         [2, 7199999], # 2h - 1ms
                                                         [3, 0]]) as table: # 0 B
        assertRows(execute(cql, table, "select format_time(col1) from %s where pk = 1"), row("2 h"))
        assertRows(execute(cql, table, "select format_time(col1) from %s where pk = 2"), row("2 h"))
        assertRows(execute(cql, table, "select format_time(col1) from %s where pk = 3"), row("0 ms"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testOneValueArgumentDecimal(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 9000000], # 2.5h
                                                         [2, 7704000], # 2.14h
                                                         [3, 7848000]]) as table: # 2.18h
        assertRows(execute(cql, table, "select format_time(col1) from %s where pk = 1"), row("2.5 h"))
        assertRows(execute(cql, table, "select format_time(col1) from %s where pk = 2"), row("2.14 h"))
        assertRows(execute(cql, table, "select format_time(col1) from %s where pk = 3"), row("2.18 h"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testValueAndUnitArguments(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 1073741826],
                                                         [2, 0]]) as table:
        assertRows(execute(cql, table, "select format_time(col1, 's') from %s where pk = 1"), row("1073741.83 s"))
        assertRows(execute(cql, table, "select format_time(col1, 'm') from %s where pk = 1"), row("17895.7 m"))
        assertRows(execute(cql, table, "select format_time(col1, 'h') from %s where pk = 1"), row("298.26 h"))
        assertRows(execute(cql, table, "select format_time(col1, 'd') from %s where pk = 1"), row("12.43 d"))

        assertRows(execute(cql, table, "select format_time(col1, 's') from %s where pk = 2"), row("0 s"))
        assertRows(execute(cql, table, "select format_time(col1, 'm') from %s where pk = 2"), row("0 m"))
        assertRows(execute(cql, table, "select format_time(col1, 'h') from %s where pk = 2"), row("0 h"))
        assertRows(execute(cql, table, "select format_time(col1, 'd') from %s where pk = 2"), row("0 d"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testValueWithSourceAndTargetArgument(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [INT], [[1, 1073741826],
                                                         [2, 1],
                                                         [3, 0]]) as table:
        assertRows(execute(cql, table, "select format_time(col1, 'ns', 'us') from %s where pk = 1"), row("1073741.83 us"))
        assertRows(execute(cql, table, "select format_time(col1, 'ns', 'ms') from %s where pk = 1"), row("1073.74 ms"))
        assertRows(execute(cql, table, "select format_time(col1, 'ns', 's') from %s where pk = 1"), row("1.07 s"))
        assertRows(execute(cql, table, "select format_time(col1, 'ns', 'm') from %s where pk = 1"), row("0.02 m"))

        assertRows(execute(cql, table, "select format_time(col1, 'us', 'ns') from %s where pk = 1"), row("1073741826000 ns"))
        assertRows(execute(cql, table, "select format_time(col1, 'us', 'ms') from %s where pk = 1"), row("1073741.83 ms"))
        assertRows(execute(cql, table, "select format_time(col1, 'us', 's') from %s where pk = 1"), row("1073.74 s"))
        assertRows(execute(cql, table, "select format_time(col1, 'us', 'm') from %s where pk = 1"), row("17.9 m"))
        assertRows(execute(cql, table, "select format_time(col1, 'us', 'h') from %s where pk = 1"), row("0.3 h"))
        assertRows(execute(cql, table, "select format_time(col1, 'us', 'd') from %s where pk = 1"), row("0.01 d"))

        assertRows(execute(cql, table, "select format_time(col1, 'ms', 'ms') from %s where pk = 1"), row("1073741826 ms"))
        assertRows(execute(cql, table, "select format_time(col1, 'ms', 's') from %s where pk = 1"), row("1073741.83 s"))
        assertRows(execute(cql, table, "select format_time(col1, 'ms', 'm') from %s where pk = 1"), row("17895.7 m"))
        assertRows(execute(cql, table, "select format_time(col1, 'ms', 'h') from %s where pk = 1"), row("298.26 h"))
        assertRows(execute(cql, table, "select format_time(col1, 'ms', 'd') from %s where pk = 1"), row("12.43 d"))

        assertRows(execute(cql, table, "select format_time(col1, 'd', 'd') from %s where pk = 2"), row("1 d"))
        assertRows(execute(cql, table, "select format_time(col1, 'd', 'h') from %s where pk = 2"), row("24 h"))
        assertRows(execute(cql, table, "select format_time(col1, 'd', 'm') from %s where pk = 2"), row("1440 m"))
        assertRows(execute(cql, table, "select format_time(col1, 'd', 's') from %s where pk = 2"), row("86400 s"))

        assertRows(execute(cql, table, "select format_time(col1, 'd', 'd') from %s where pk = 3"), row("0 d"))
        assertRows(execute(cql, table, "select format_time(col1, 'd', 'h') from %s where pk = 3"), row("0 h"))
        assertRows(execute(cql, table, "select format_time(col1, 'd', 'm') from %s where pk = 3"), row("0 m"))
        assertRows(execute(cql, table, "select format_time(col1, 'd', 's') from %s where pk = 3"), row("0 s"))
        assertRows(execute(cql, table, "select format_time(col1, 'd', 'ms') from %s where pk = 3"), row("0 ms"))
        assertRows(execute(cql, table, "select format_time(col1, 'd', 'us') from %s where pk = 3"), row("0 us"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testNoOverflow(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [BIGINT, INT, SMALLINT, TINYINT],
                    [[1,
                      LONG_MAX_VALUE - 1,
                      INTEGER_MAX_VALUE - 1,
                      SHORT_MAX_VALUE - 1,
                      BYTE_MAX_VALUE - 1],
                     [2,
                      LONG_MAX_VALUE,
                      INTEGER_MAX_VALUE,
                      SHORT_MAX_VALUE,
                      BYTE_MAX_VALUE]]) as table:

        # Won't overlfow because the value is one less than the Double.MAX_VALUE
        assertRows(execute(cql, table, "select format_time(col1, 'd', 'ns') from %s where pk = 1"), row("796899343984252600000000000000000 ns"))
        assertRows(execute(cql, table, "select format_time(col2, 'd', 'ns') from %s where pk = 1"), row("185542587014400000000000 ns"))
        assertRows(execute(cql, table, "select format_time(col3, 'd', 'ns') from %s where pk = 1"), row("2830982400000000000 ns"))
        assertRows(execute(cql, table, "select format_time(col4, 'd', 'ns') from %s where pk = 1"), row("10886400000000000 ns"))

        assertRows(execute(cql, table, "select format_time(col1, 'd', 'ns') from %s where pk = 2"), row("796899343984252600000000000000000 ns"))
        assertRows(execute(cql, table, "select format_time(col2, 'd', 'ns') from %s where pk = 2"), row("185542587100800000000000 ns"))
        assertRows(execute(cql, table, "select format_time(col3, 'd', 'ns') from %s where pk = 2"), row("2831068800000000000 ns"))
        assertRows(execute(cql, table, "select format_time(col4, 'd', 'ns') from %s where pk = 2"), row("10972800000000000 ns"))

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

        assertRows(execute(cql, table, "select format_time(col1) from %s where pk = 1"), row("24.86 d"))
        assertRows(execute(cql, table, "select format_time(col2) from %s where pk = 1"), row("127 ms"))
        assertRows(execute(cql, table, "select format_time(col3) from %s where pk = 1"), row("32.77 s"))
        assertRows(execute(cql, table, "select format_time(col4) from %s where pk = 1"), row("106751991167.3 d"))
        assertRows(execute(cql, table, "select format_time(col5) from %s where pk = 1"), row("24.86 d"))
        assertRows(execute(cql, table, "select format_time(col6) from %s where pk = 1"), row("24.86 d"))
        assertRows(execute(cql, table, "select format_time(col7) from %s where pk = 1"), row("24.86 d"))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testNegativeValueIsInvalid(cql, test_keyspace, new_to_cassandra_6):
    with create_default_format_table(cql, test_keyspace, [["1", "-1", "-2"]]) as table:
        assertInvalidThrowMessage(cql, table, "value must be non-negative", InvalidRequest,
                                  "select format_time(col1) from %s where pk = 1")

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testUnparsableTextIsInvalid(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [TEXT], [[1, "'abc'"], [2, "'-1'"]]) as table:
        assertInvalidThrowMessage(cql, table, "unable to convert string 'abc' to a value of type long", InvalidRequest,
                                  "select format_time(col1) from %s where pk = 1")

        assertInvalidThrowMessage(cql, table, "value must be non-negative", InvalidRequest,
                                  "select format_time(col1) from %s where pk = 2")

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testInvalidUnits(cql, test_keyspace, new_to_cassandra_6):
    with create_default_format_table(cql, test_keyspace, [["1", "1", "2"]]) as table:
        for functionCall in ["format_time(col1, 'abc')",
                             "format_time(col1, 'd', 'abc')",
                             "format_time(col1, 'abc', 'd')",
                             "format_time(col1, 'abc', 'abc')"]:
            assertInvalidThrowMessage(cql, table, "Unsupported time unit: abc. Supported units are: ns, us, ms, s, m, h, d", InvalidRequest,
                                      "select " + functionCall + " from %s where pk = 1")

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testInvalidArgumentsSize(cql, test_keyspace, new_to_cassandra_6):
    with create_default_format_table(cql, test_keyspace, [["1", "1", "2"]]) as table:
        # test arguemnt size = 0
        assertInvalidThrowMessage(cql, table, "Invalid number of arguments for function system.format_time([int|tinyint|smallint|bigint|varint|ascii|text], [ascii], [ascii])", InvalidRequest,
                                  "select format_time() from %s where pk = 1")

        # Test argument size > 3
        assertInvalidThrowMessage(cql, table, "Invalid number of arguments for function system.format_time([int|tinyint|smallint|bigint|varint|ascii|text], [ascii], [ascii])", InvalidRequest,
                                  "select format_time(col1, 'ms', 's', 'h') from %s where pk = 1")

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testHandlingNullValues(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [TEXT, ASCII, INT],
                             [[1, None, None, None]]) as table:

        assertRows(execute(cql, table, "select format_time(col1), format_time(col2), format_time(col3) from %s where pk = 1"),
                   row(None, None, None))

        assertRows(execute(cql, table, "select format_time(col1, 's') from %s where pk = 1"), row(None))
        assertRows(execute(cql, table, "select format_time(col1, 's', 'd') from %s where pk = 1"), row(None))

# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testHandlingNullArguments(cql, test_keyspace, new_to_cassandra_6):
    with create_format_table(cql, test_keyspace, [TEXT, ASCII, INT],
                             [[1, None, None, None],
                              [2, "'1'", "'2'", 3]]) as table:

        assertRows(execute(cql, table, "select format_time(col1, null) from %s where pk = 1"), row(None))

        for functionCall in ["format_time(col3, null)",
                             "format_time(col3, null, null)",
                             "format_time(col3, null, 'd')",
                             "format_time(col3, 'd', null)"]:
            assertInvalidThrowMessage(cql, table, "none of the arguments may be null", InvalidRequest,
                                      "select " + functionCall + " from %s where pk = 2")

# The Java test uses QuickTheories to check 1024 random positive integers.
# We check fewer values, to keep the test fast.
# Reproduces SCYLLADB-5219 (format_bytes() and format_time() functions)
@pytest.mark.xfail(reason="SCYLLADB-5219")
def testFuzzRandomGenerators(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int primary key, col1 int)") as table:
        rand = random.Random(0)
        for _ in range(100):
            randInt = rand.randint(1, INTEGER_MAX_VALUE)
            execute(cql, table, "INSERT INTO %s (pk, col1) VALUES (?, ?)", 1, randInt)
            assertRows(execute(cql, table, "select format_time(col1, 's', 'm') from %s where pk = 1"), row(format_double(randInt * (1 / 60.0)) + " m"))
            assertRows(execute(cql, table, "select format_time(col1, 's', 'h') from %s where pk = 1"), row(format_double(randInt * (1 / 3600.0)) + " h"))
            assertRows(execute(cql, table, "select format_time(col1, 's', 'd') from %s where pk = 1"), row(format_double(randInt * (1 / 86400.0)) + " d"))
            assertRows(execute(cql, table, "select format_time(col1, 'ms', 'm') from %s where pk = 1"), row(format_double(randInt * (1 / (60 * 1000.0))) + " m"))
            assertRows(execute(cql, table, "select format_time(col1, 'ms', 'h') from %s where pk = 1"), row(format_double(randInt * (1 / (3600 * 1000.0))) + " h"))
            assertRows(execute(cql, table, "select format_time(col1, 'ms', 'd') from %s where pk = 1"), row(format_double(randInt * (1 / (86400 * 1000.0))) + " d"))
