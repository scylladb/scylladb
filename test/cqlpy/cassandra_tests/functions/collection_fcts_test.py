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
from contextlib import contextmanager
from decimal import Decimal, ROUND_HALF_EVEN
from cassandra.protocol import InvalidRequest

# Tests for the functions defined on CollectionFcts: map_keys(), map_values(),
# collection_count(), collection_min(), collection_max(), collection_sum() and
# collection_avg(). They were added in Cassandra 5.0 (CASSANDRA-18060).
# Scylla doesn't support these functions yet (issue #24273), so all the
# tests below are xfail.

bigint1 = 12345678901234567890
bigint2 = 23456789012345678901
bigdecimal1 = Decimal("1234567890.1234567890")
bigdecimal2 = Decimal("2345678901.2345678901")

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testNotNumericCollection(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v uuid, l list<text>, s set<boolean>, fl frozen<list<text>>, fs frozen<set<boolean>>)") as table:
        # sum
        assertInvalidThrowMessage(cql, table, "Function system.collection_sum requires a numeric set/list argument, " +
                                  "but found argument v of type uuid",
                                  InvalidRequest,
                                  "SELECT collection_sum(v) FROM %s")
        assertInvalidThrowMessage(cql, table, "Function system.collection_sum requires a numeric set/list argument, " +
                                  "but found argument l of type list<text>",
                                  InvalidRequest,
                                  "SELECT collection_sum(l) FROM %s")
        assertInvalidThrowMessage(cql, table, "Function system.collection_sum requires a numeric set/list argument, " +
                                  "but found argument s of type set<boolean>",
                                  InvalidRequest,
                                  "SELECT collection_sum(s) FROM %s")
        assertInvalidThrowMessage(cql, table, "Function system.collection_sum requires a numeric set/list argument, " +
                                  "but found argument fl of type frozen<list<text>>",
                                  InvalidRequest,
                                  "SELECT collection_sum(fl) FROM %s")
        assertInvalidThrowMessage(cql, table, "Function system.collection_sum requires a numeric set/list argument, " +
                                  "but found argument fs of type frozen<set<boolean>>",
                                  InvalidRequest,
                                  "SELECT collection_sum(fs) FROM %s")

        # avg
        assertInvalidThrowMessage(cql, table, "Function system.collection_avg requires a numeric set/list argument, " +
                                  "but found argument v of type uuid",
                                  InvalidRequest,
                                  "SELECT collection_avg(v) FROM %s")
        assertInvalidThrowMessage(cql, table, "Function system.collection_avg requires a numeric set/list argument, " +
                                  "but found argument l of type list<text>",
                                  InvalidRequest,
                                  "SELECT collection_avg(l) FROM %s")
        assertInvalidThrowMessage(cql, table, "Function system.collection_avg requires a numeric set/list argument, " +
                                  "but found argument s of type set<boolean>",
                                  InvalidRequest,
                                  "SELECT collection_avg(s) FROM %s")
        assertInvalidThrowMessage(cql, table, "Function system.collection_avg requires a numeric set/list argument, " +
                                  "but found argument fl of type frozen<list<text>>",
                                  InvalidRequest,
                                  "SELECT collection_avg(fl) FROM %s")
        assertInvalidThrowMessage(cql, table, "Function system.collection_avg requires a numeric set/list argument, " +
                                  "but found argument fs of type frozen<set<boolean>>",
                                  InvalidRequest,
                                  "SELECT collection_avg(fs) FROM %s")

# The Java test's createTable(CQL3Type.Native type). "numeric" says whether
# the type is a NumberType.
@contextmanager
def create_collections_table(cql, test_keyspace, typ, numeric):
    with create_table(cql, test_keyspace, f"(" +
                                  f" k int PRIMARY KEY, " +
                                  f" l list<{typ}>, " +
                                  f" s set<{typ}>, " +
                                  f" m map<{typ}, {typ}>, " +
                                  f" fl frozen<list<{typ}>>, " +
                                  f" fs frozen<set<{typ}>>, " +
                                  f" fm frozen<map<{typ}, {typ}>>)") as table:

        # test functions with an empty table
        assertEmpty(execute(cql, table, "SELECT map_keys(m), map_keys(fm), map_values(m), map_values(fm) FROM %s"))
        assertEmpty(execute(cql, table, "SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"))
        assertEmpty(execute(cql, table, "SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"))
        assertEmpty(execute(cql, table, "SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"))
        assertEmpty(execute(cql, table, "SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"))

        errorMsg = "requires a numeric set/list argument"
        if numeric:
            assertEmpty(execute(cql, table, "SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"))
            assertEmpty(execute(cql, table, "SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"))
        else:
            assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_sum(l) FROM %s")
            assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_avg(l) FROM %s")
            assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_sum(s) FROM %s")
            assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_avg(s) FROM %s")
            assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_sum(fl) FROM %s")
            assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_avg(fl) FROM %s")
            assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_sum(fs) FROM %s")
            assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_avg(fs) FROM %s")
        assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_sum(m) FROM %s")
        assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_avg(m) FROM %s")
        assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_sum(fm) FROM %s")
        assertInvalidThrowMessage(cql, table, errorMsg, InvalidRequest, "SELECT collection_avg(fm) FROM %s")

        # prepare empty collections
        execute(cql, table, "INSERT INTO %s (k, l, fl, s, fs, m, fm) VALUES (1, ?, ?, ?, ?, ?, ?)",
                [], [], set(), set(), {}, {})

        yield table

# The tests for the different numeric types (testTinyInt, testSmallInt,
# testInt, testBigInt, testFloat, testDouble, testVarInt, testDecimal) are
# identical except for the type and values, and so are the tests for the
# other types (testAscii, testText, testBoolean). The Python translations
# share these two functions.
def do_test_numeric(cql, test_keyspace, typ, v1, v2, v3, v4, zero, sum, avg):
    with create_collections_table(cql, test_keyspace, typ, True) as table:
        # empty collections
        assertRows(execute(cql, table, "SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(None, None, set(), []))
        assertRows(execute(cql, table, "SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(None, None, None, 0, 0, 0))
        assertRows(execute(cql, table, "SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(None, None, None, None))
        assertRows(execute(cql, table, "SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(None, None, None, None))
        assertRows(execute(cql, table, "SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(None, None, zero, zero))
        assertRows(execute(cql, table, "SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(None, None, zero, zero))

        # not empty collections
        execute(cql, table, "INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                [v1, v2], [v1, v2],
                {v1, v2}, {v1, v2},
                {v1: v2, v3: v4}, {v1: v2, v3: v4})

        assertRows(execute(cql, table, "SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row({v1, v3}, {v1, v3}))
        assertRows(execute(cql, table, "SELECT map_values(m), map_values(fm) FROM %s"),
                   row([v2, v4], [v2, v4]))
        assertRows(execute(cql, table, "SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2))
        assertRows(execute(cql, table, "SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2))
        assertRows(execute(cql, table, "SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(v1, v1, v1, v1))
        assertRows(execute(cql, table, "SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(v2, v2, v2, v2))
        assertRows(execute(cql, table, "SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(sum, sum, sum, sum))
        assertRows(execute(cql, table, "SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(avg, avg, avg, avg))

def do_test_not_numeric(cql, test_keyspace, typ, v1, v2, v3, v4, values, min, max):
    with create_collections_table(cql, test_keyspace, typ, False) as table:
        # empty collections
        assertRows(execute(cql, table, "SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(None, None, set(), []))
        assertRows(execute(cql, table, "SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(None, None, None, 0, 0, 0))

        # not empty collections
        execute(cql, table, "INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                [v1, v2], [v1, v2],
                {v1, v2}, {v1, v2},
                {v1: v2, v3: v4}, {v1: v2, v3: v4})

        assertRows(execute(cql, table, "SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row({v1, v3}, {v1, v3}))
        assertRows(execute(cql, table, "SELECT map_values(m), map_values(fm) FROM %s"),
                   row(values, values))
        assertRows(execute(cql, table, "SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2))
        assertRows(execute(cql, table, "SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2))
        assertRows(execute(cql, table, "SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(min, min, min, min))
        assertRows(execute(cql, table, "SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(max, max, max, max))

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testTinyInt(cql, test_keyspace):
    do_test_numeric(cql, test_keyspace, "tinyint", 1, 2, 3, 4, 0, 3, 1)

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testSmallInt(cql, test_keyspace):
    do_test_numeric(cql, test_keyspace, "smallint", 1, 2, 3, 4, 0, 3, 1)

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testInt(cql, test_keyspace):
    do_test_numeric(cql, test_keyspace, "int", 1, 2, 3, 4, 0, 3, 1)

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testBigInt(cql, test_keyspace):
    do_test_numeric(cql, test_keyspace, "bigint", 1, 2, 3, 4, 0, 3, 1)

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testFloat(cql, test_keyspace):
    # The sum and average are computed with 32-bit float arithmetic.
    sum = to_float(to_float(1.23) + to_float(2.34))
    avg = to_float(to_float(to_float(1.23) + to_float(2.34)) / 2)
    do_test_numeric(cql, test_keyspace, "float", to_float(1.23), to_float(2.34), to_float(3.45), to_float(4.56), 0.0, sum, avg)

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testDouble(cql, test_keyspace):
    do_test_numeric(cql, test_keyspace, "double", 1.23, 2.34, 3.45, 4.56, 0.0, 3.57, 1.785)

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testVarInt(cql, test_keyspace):
    sum = bigint1 + bigint2
    avg = (bigint1 + bigint2) // 2
    # The map in the Java test is {bigint1: bigint2, bigint2: bigint1}
    do_test_numeric(cql, test_keyspace, "varint", bigint1, bigint2, bigint2, bigint1, 0, sum, avg)

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testDecimal(cql, test_keyspace):
    sum = bigdecimal1 + bigdecimal2
    # Like Java's BigDecimal.divide(divisor, RoundingMode.HALF_EVEN), which
    # keeps the scale of the dividend.
    avg = ((bigdecimal1 + bigdecimal2) / 2).quantize(bigdecimal1 + bigdecimal2, rounding=ROUND_HALF_EVEN)
    # The map in the Java test is {bigdecimal1: bigdecimal2, bigdecimal2: bigdecimal1}
    do_test_numeric(cql, test_keyspace, "decimal", bigdecimal1, bigdecimal2, bigdecimal2, bigdecimal1, Decimal(0), sum, avg)

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testAscii(cql, test_keyspace):
    do_test_not_numeric(cql, test_keyspace, "ascii", "abc", "bcd", "cde", "def", ["bcd", "def"], "abc", "bcd")

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testText(cql, test_keyspace):
    do_test_not_numeric(cql, test_keyspace, "text", "ábc", "bcd", "cdé", "déf", ["déf", "bcd"], "bcd", "ábc")

# Reproduces #24273 (collection functions)
@pytest.mark.xfail(reason="#24273")
def testBoolean(cql, test_keyspace):
    do_test_not_numeric(cql, test_keyspace, "boolean", True, False, False, True, [True, False], False, True)
