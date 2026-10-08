# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for CQL's WRITETIME, MAXWRITETIME and TTL selection functions.

from ...porting import *
import cassandra.protocol

# WRITETIME() and TTL() of a multi-cell column return a list, which may
# contain null elements (e.g., an element written without a TTL). The Python
# driver's Cython deserializer fails to decode a list with null elements
# ("Length must be positive", see
# https://github.com/scylladb/python-driver/issues/1119), so these tests use
# the driver's pure-Python deserializer, which handles them correctly.
@pytest.fixture(autouse=True)
def python_deserializer(cql):
    original = cql.client_protocol_handler
    cql.client_protocol_handler = cassandra.protocol._ProtocolHandler
    yield
    cql.client_protocol_handler = original

TIMESTAMP_1 = 1
TIMESTAMP_2 = 2
NO_TIMESTAMP = None

TTL_1 = 10000
TTL_2 = 20000
NO_TTL = None

# The Java test has two variants of assertWritetimeAndTTL(), one for a single
# timestamp and TTL and one for lists of them (for multi-cell columns). In
# Python we use one function which checks the type of the expected values.
def assertWritetimeAndTTL(cql, table, column, timestamp, ttl, where=None):
    where = "" if where is None else " WHERE " + where

    if isinstance(timestamp, list):
        maxTimestamp = max((t for t in timestamp if t is not None), default=None)
    else:
        maxTimestamp = timestamp

    # Verify write time
    assert_rows(execute(cql, table, f"SELECT WRITETIME({column}) FROM %s{where}"), row(timestamp))

    # Verify max write time
    assert_rows(execute(cql, table, f"SELECT MAXWRITETIME({column}) FROM %s{where}"), row(maxTimestamp))

    # Verify write time and max write time together
    assert_rows(execute(cql, table, f"SELECT WRITETIME({column}), MAXWRITETIME({column}) FROM %s{where}"),
                row(timestamp, maxTimestamp))

    # Verify ttl
    rs = list(execute(cql, table, f"SELECT TTL({column}) FROM %s{where}"))
    assert len(rs) == 1
    actual = rs[0][0]
    if isinstance(ttl, list):
        assert actual is not None
        assert len(ttl) == len(actual)
        for expectedTTL, actualTTL in zip(ttl, actual):
            assertTTL(expectedTTL, actualTTL)
    else:
        assertTTL(ttl, actual)

# Since the returned TTL is the remaining seconds since last update, it could
# be lower than the specified TTL depending on the test execution time, se we
# allow up to one-minute difference
def assertTTL(expected, actual):
    if expected is None:
        assert actual is None
    else:
        assert actual is not None
        assert actual > expected - 60
        assert actual <= expected

# Scylla's error messages are different: "WRITETIME is not legal on partition
# key component pk", and "WRITETIME on a subscript is only valid for
# non-frozen map or set columns".
def primaryKeySelectionMessage(function, column):
    return (re.escape(f"Cannot use selection function {function} on PRIMARY KEY part {column}") + "|" +
            re.escape(f"{function.upper()} is not legal on ") + ".* " + column)

def assertInvalidPrimaryKeySelection(cql, table, column):
    assert_invalid_throw_message_re(cql, table, primaryKeySelectionMessage("writetime", column),
                                    InvalidRequest,
                                    f"SELECT WRITETIME({column}) FROM %s")
    assert_invalid_throw_message_re(cql, table, primaryKeySelectionMessage("maxwritetime", column),
                                    InvalidRequest,
                                    f"SELECT MAXWRITETIME({column}) FROM %s")
    assert_invalid_throw_message_re(cql, table, primaryKeySelectionMessage("ttl", column),
                                    InvalidRequest,
                                    f"SELECT TTL({column}) FROM %s")

def assertInvalidListElementSelection(cql, table, column, lst):
    message = (re.escape(f"Element selection is only allowed on sets and maps, but {lst} is a list") +
               "|on a subscript is only valid for non-frozen map or set columns")
    assert_invalid_throw_message_re(cql, table, message, InvalidRequest, f"SELECT WRITETIME({column}) FROM %s")
    assert_invalid_throw_message_re(cql, table, message, InvalidRequest, f"SELECT MAXWRITETIME({column}) FROM %s")
    assert_invalid_throw_message_re(cql, table, message, InvalidRequest, f"SELECT TTL({column}) FROM %s")

def assertInvalidListSliceSelection(cql, table, column, lst):
    message = f"Slice selection is only allowed on sets and maps, but {lst} is a list"
    assert_invalid_throw_message(cql, table, message, InvalidRequest, f"SELECT WRITETIME({column}) FROM %s")
    assert_invalid_throw_message(cql, table, message, InvalidRequest, f"SELECT MAXWRITETIME({column}) FROM %s")
    assert_invalid_throw_message(cql, table, message, InvalidRequest, f"SELECT TTL({column}) FROM %s")

# Reproduces #10953 (MAXWRITETIME)
@pytest.mark.xfail(reason="#10953")
def testSimple(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck int, v int,  PRIMARY KEY(pk, ck))") as table:
        # Primary key columns should be rejected
        assertInvalidPrimaryKeySelection(cql, table, "pk")
        assertInvalidPrimaryKeySelection(cql, table, "ck")

        # No rows
        assert_empty(execute(cql, table, "SELECT WRITETIME(v) FROM %s"))
        assert_empty(execute(cql, table, "SELECT TTL(v) FROM %s"))

        # Insert row without TTL
        execute(cql, table, "INSERT INTO %s (pk, ck, v) VALUES (1, 2, 3) USING TIMESTAMP ?", TIMESTAMP_1)
        assertWritetimeAndTTL(cql, table, "v", TIMESTAMP_1, NO_TTL)

        # Update the row with TTL and a new timestamp
        execute(cql, table, "UPDATE %s USING TIMESTAMP ? AND TTL ? SET v=8 WHERE pk=1 AND ck=2", TIMESTAMP_2, TTL_1)
        assertWritetimeAndTTL(cql, table, "v", TIMESTAMP_2, TTL_1)

        # Combine with other columns
        assert_rows(execute(cql, table, "SELECT pk, WRITETIME(v) FROM %s"), row(1, TIMESTAMP_2))
        assert_rows(execute(cql, table, "SELECT WRITETIME(v), pk FROM %s"), row(TIMESTAMP_2, 1))
        assert_rows(execute(cql, table, "SELECT pk, WRITETIME(v), v, ck FROM %s"), row(1, TIMESTAMP_2, 8, 2))

# Reproduces #10953 (MAXWRITETIME), #22075 (slice selection) and SCYLLADB-5166
# (WRITETIME and TTL of a whole non-frozen collection or UDT)
@pytest.mark.xfail(reason="#10953, #22075, SCYLLADB-5166")
def testList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<int>)") as table:
        # Null column
        execute(cql, table, "INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
        assertWritetimeAndTTL(cql, table, "l", NO_TIMESTAMP, NO_TTL)

        # Create empty
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (1, []) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
        assertWritetimeAndTTL(cql, table, "l", NO_TIMESTAMP, NO_TTL)

        # Create with a single element without TTL
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (1, [1]) USING TIMESTAMP ?", TIMESTAMP_1)
        assertWritetimeAndTTL(cql, table, "l", [TIMESTAMP_1], [NO_TTL])

        # Add a new element to the list with a new timestamp and a TTL
        execute(cql, table, "UPDATE %s USING TIMESTAMP ? AND TTL ? SET l=l+[2] WHERE k=1", TIMESTAMP_2, TTL_2)
        assertWritetimeAndTTL(cql, table, "l", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])

        assertInvalidListElementSelection(cql, table, "l[0]", "l")
        assertInvalidListSliceSelection(cql, table, "l[..0]", "l")
        assertInvalidListSliceSelection(cql, table, "l[0..]", "l")
        assertInvalidListSliceSelection(cql, table, "l[1..1]", "l")
        assertInvalidListSliceSelection(cql, table, "l[1..2]", "l")

        # Read multiple rows to verify selector reset
        execute(cql, table, "TRUNCATE TABLE %s")
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (1, [1, 2, 3]) USING TIMESTAMP ?", TIMESTAMP_1)
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (2, [1, 2]) USING TIMESTAMP ?", TIMESTAMP_2)
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (3, [1]) USING TIMESTAMP ?", TIMESTAMP_1)
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (4, []) USING TIMESTAMP ?", TIMESTAMP_2)
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (5, null) USING TIMESTAMP ?", TIMESTAMP_2)
        assert_rows(execute(cql, table, "SELECT k, WRITETIME(l) FROM %s"),
                    row(5, NO_TIMESTAMP),
                    row(1, [TIMESTAMP_1, TIMESTAMP_1, TIMESTAMP_1]),
                    row(2, [TIMESTAMP_2, TIMESTAMP_2]),
                    row(4, NO_TIMESTAMP),
                    row(3, [TIMESTAMP_1]))

# Reproduces #10953 (MAXWRITETIME) and #22075 (slice selection)
@pytest.mark.xfail(reason="#10953, #22075")
def testFrozenList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v frozen<list<int>>)") as table:
        # Null column
        execute(cql, table, "INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
        assertWritetimeAndTTL(cql, table, "v", NO_TIMESTAMP, NO_TTL)

        # Create empty
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, []) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
        assertWritetimeAndTTL(cql, table, "v", TIMESTAMP_1, TTL_1)

        # truncate, since previous columns would win on reconcilliation because of their TTL (CASSANDRA-14592)
        execute(cql, table, "TRUNCATE TABLE %s")

        # Update with a single element without TTL
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, [1]) USING TIMESTAMP ?", TIMESTAMP_1)
        assertWritetimeAndTTL(cql, table, "v", TIMESTAMP_1, NO_TTL)

        # Add a new element to the list with a new timestamp and a TTL
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, [1, 2, 3]) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_2, TTL_2)
        assertWritetimeAndTTL(cql, table, "v", TIMESTAMP_2, TTL_2)

        assertInvalidListElementSelection(cql, table, "v[1]", "v")
        assertInvalidListSliceSelection(cql, table, "v[..0]", "v")
        assertInvalidListSliceSelection(cql, table, "v[0..]", "v")
        assertInvalidListSliceSelection(cql, table, "v[1..1]", "v")
        assertInvalidListSliceSelection(cql, table, "v[1..2]", "v")

        # Read multiple rows to verify selector reset
        execute(cql, table, "TRUNCATE TABLE %s")
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, [1, 2, 3]) USING TIMESTAMP ?", TIMESTAMP_1)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (2, [1, 2]) USING TIMESTAMP ?", TIMESTAMP_2)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (3, [1]) USING TIMESTAMP ?", TIMESTAMP_1)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (4, []) USING TIMESTAMP ?", TIMESTAMP_2)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (5, null) USING TIMESTAMP ?", TIMESTAMP_2)
        assert_rows(execute(cql, table, "SELECT k, WRITETIME(v) FROM %s"),
                    row(5, NO_TIMESTAMP),
                    row(1, TIMESTAMP_1),
                    row(2, TIMESTAMP_2),
                    row(4, TIMESTAMP_2),
                    row(3, TIMESTAMP_1))

# Reproduces #10953 (MAXWRITETIME), #22075 (slice selection) and SCYLLADB-5166
# (WRITETIME and TTL of a whole non-frozen collection or UDT)
@pytest.mark.xfail(reason="#10953, #22075, SCYLLADB-5166")
def testSet(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s set<int>)") as table:
        # Null column
        execute(cql, table, "INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
        assertWritetimeAndTTL(cql, table, "s", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[0..]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[0..0]", NO_TIMESTAMP, NO_TTL)

        # Create empty
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (1, {}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
        assertWritetimeAndTTL(cql, table, "s", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[0..]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[0..0]", NO_TIMESTAMP, NO_TTL)

        # Update with a single element without TTL
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (1, {1}) USING TIMESTAMP ?", TIMESTAMP_1)
        assertWritetimeAndTTL(cql, table, "s", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[1]", TIMESTAMP_1, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[2]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[..2]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[0..]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[1..]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[2..]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[0..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[0..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[1..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[1..2]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[2..2]", NO_TIMESTAMP, NO_TTL)

        # Add a new element to the set with a new timestamp and a TTL
        execute(cql, table, "UPDATE %s USING TIMESTAMP ? AND TTL ? SET s=s+{2} WHERE k=1", TIMESTAMP_2, TTL_2)
        assertWritetimeAndTTL(cql, table, "s", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "s[0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[1]", TIMESTAMP_1, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[2]", TIMESTAMP_2, TTL_2)
        assertWritetimeAndTTL(cql, table, "s[3]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[..2]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "s[..3]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "s[0..]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "s[1..]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "s[2..]", [TIMESTAMP_2], [TTL_2])
        assertWritetimeAndTTL(cql, table, "s[3..]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[0..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[0..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[0..2]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "s[0..3]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "s[1..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "s[1..2]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "s[1..3]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "s[2..2]", [TIMESTAMP_2], [TTL_2])
        assertWritetimeAndTTL(cql, table, "s[2..3]", [TIMESTAMP_2], [TTL_2])
        assertWritetimeAndTTL(cql, table, "s[3..3]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "s[3..4]", NO_TIMESTAMP, NO_TTL)

        # Combine timestamp selection with other selections and orders
        assert_rows(execute(cql, table, "SELECT k, WRITETIME(s[1]) FROM %s"), row(1, TIMESTAMP_1))
        assert_rows(execute(cql, table, "SELECT WRITETIME(s[1]), k FROM %s"), row(TIMESTAMP_1, 1))
        assert_rows(execute(cql, table, "SELECT WRITETIME(s[1]), WRITETIME(s[2]) FROM %s"), row(TIMESTAMP_1, TIMESTAMP_2))
        assert_rows(execute(cql, table, "SELECT WRITETIME(s[2]), WRITETIME(s[1]) FROM %s"), row(TIMESTAMP_2, TIMESTAMP_1))

        # Read multiple rows to verify selector reset
        execute(cql, table, "TRUNCATE TABLE %s")
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (1, {1, 2, 3}) USING TIMESTAMP ?", TIMESTAMP_1)
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (2, {1, 2}) USING TIMESTAMP ?", TIMESTAMP_2)
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (3, {1}) USING TIMESTAMP ?", TIMESTAMP_1)
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (4, {}) USING TIMESTAMP ?", TIMESTAMP_2)
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (5, null) USING TIMESTAMP ?", TIMESTAMP_2)
        assert_rows(execute(cql, table, "SELECT k, WRITETIME(s) FROM %s"),
                    row(5, NO_TIMESTAMP),
                    row(1, [TIMESTAMP_1, TIMESTAMP_1, TIMESTAMP_1]),
                    row(2, [TIMESTAMP_2, TIMESTAMP_2]),
                    row(4, NO_TIMESTAMP),
                    row(3, [TIMESTAMP_1]))
        assert_rows(execute(cql, table, "SELECT k, WRITETIME(s[1]) FROM %s"),
                    row(5, NO_TIMESTAMP),
                    row(1, TIMESTAMP_1),
                    row(2, TIMESTAMP_2),
                    row(4, NO_TIMESTAMP),
                    row(3, TIMESTAMP_1))

# Reproduces #10953 (MAXWRITETIME), #22075 (slice selection) and SCYLLADB-5166
# (WRITETIME and TTL of a whole non-frozen collection or UDT)
@pytest.mark.xfail(reason="#10953, #22075, SCYLLADB-5166")
def testMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<int, int>)") as table:
        # Null column
        execute(cql, table, "INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
        assertWritetimeAndTTL(cql, table, "m", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[0..]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[0..0]", NO_TIMESTAMP, NO_TTL)

        # Create empty
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (1, {}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
        assertWritetimeAndTTL(cql, table, "m", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[0..]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[0..0]", NO_TIMESTAMP, NO_TTL)

        # Update with a single element without TTL
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (1, {1:10}) USING TIMESTAMP ?", TIMESTAMP_1)
        assertWritetimeAndTTL(cql, table, "m", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[1]", TIMESTAMP_1, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[2]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[..2]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[0..]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[1..]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[2..]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[0..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[0..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[0..2]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[1..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[1..2]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[2..2]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[2..3]", NO_TIMESTAMP, NO_TTL)

        # Add a new element to the map with a new timestamp and a TTL
        execute(cql, table, "UPDATE %s USING TIMESTAMP ? AND TTL ? SET m=m+{2:20} WHERE k=1", TIMESTAMP_2, TTL_2)
        assertWritetimeAndTTL(cql, table, "m", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "m[0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[1]", TIMESTAMP_1, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[2]", TIMESTAMP_2, TTL_2)
        assertWritetimeAndTTL(cql, table, "m[3]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[..2]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "m[..3]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "m[0..]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "m[1..]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "m[2..]", [TIMESTAMP_2], [TTL_2])
        assertWritetimeAndTTL(cql, table, "m[3..]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[0..0]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[0..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[0..2]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "m[0..3]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "m[1..1]", [TIMESTAMP_1], [NO_TTL])
        assertWritetimeAndTTL(cql, table, "m[1..2]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "m[1..3]", [TIMESTAMP_1, TIMESTAMP_2], [NO_TTL, TTL_2])
        assertWritetimeAndTTL(cql, table, "m[2..2]", [TIMESTAMP_2], [TTL_2])
        assertWritetimeAndTTL(cql, table, "m[2..3]", [TIMESTAMP_2], [TTL_2])
        assertWritetimeAndTTL(cql, table, "m[3..3]", NO_TIMESTAMP, NO_TTL)
        assertWritetimeAndTTL(cql, table, "m[3..4]", NO_TIMESTAMP, NO_TTL)

        # Combine timestamp selection with other selections and orders
        assert_rows(execute(cql, table, "SELECT k, WRITETIME(m[1]) FROM %s"), row(1, TIMESTAMP_1))
        assert_rows(execute(cql, table, "SELECT WRITETIME(m[1]), k FROM %s"), row(TIMESTAMP_1, 1))
        assert_rows(execute(cql, table, "SELECT WRITETIME(m[1]), WRITETIME(m[2]) FROM %s"), row(TIMESTAMP_1, TIMESTAMP_2))
        assert_rows(execute(cql, table, "SELECT WRITETIME(m[2]), WRITETIME(m[1]) FROM %s"), row(TIMESTAMP_2, TIMESTAMP_1))

        # Read multiple rows to verify selector reset
        execute(cql, table, "TRUNCATE TABLE %s")
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (1, {1:10, 2:20, 3:30}) USING TIMESTAMP ?", TIMESTAMP_1)
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (2, {1:10, 2:20}) USING TIMESTAMP ?", TIMESTAMP_2)
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (3, {1:10}) USING TIMESTAMP ?", TIMESTAMP_1)
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (4, {}) USING TIMESTAMP ?", TIMESTAMP_2)
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (5, null) USING TIMESTAMP ?", TIMESTAMP_2)
        assert_rows(execute(cql, table, "SELECT k, WRITETIME(m) FROM %s"),
                    row(5, NO_TIMESTAMP),
                    row(1, [TIMESTAMP_1, TIMESTAMP_1, TIMESTAMP_1]),
                    row(2, [TIMESTAMP_2, TIMESTAMP_2]),
                    row(4, NO_TIMESTAMP),
                    row(3, [TIMESTAMP_1]))
        assert_rows(execute(cql, table, "SELECT k, WRITETIME(m[1]) FROM %s"),
                    row(5, NO_TIMESTAMP),
                    row(1, TIMESTAMP_1),
                    row(2, TIMESTAMP_2),
                    row(4, NO_TIMESTAMP),
                    row(3, TIMESTAMP_1))

# Reproduces #10953 (MAXWRITETIME) and SCYLLADB-5166 (WRITETIME and TTL of a
# whole non-frozen collection or UDT)
@pytest.mark.xfail(reason="#10953, SCYLLADB-5166")
def testUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(f1 int, f2 int)") as type:
        with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t " + type + ")") as table:
            # Null column
            execute(cql, table, "INSERT INTO %s (k) VALUES (0) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
            assertWritetimeAndTTL(cql, table, "t", NO_TIMESTAMP, NO_TTL)
            assertWritetimeAndTTL(cql, table, "t.f1", NO_TIMESTAMP, NO_TTL)
            assertWritetimeAndTTL(cql, table, "t.f2", NO_TIMESTAMP, NO_TTL)

            # Both fields are empty
            execute(cql, table, "INSERT INTO %s (k, t) VALUES (0, {f1:null, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
            assertWritetimeAndTTL(cql, table, "t", NO_TIMESTAMP, NO_TTL, "k=0")
            assertWritetimeAndTTL(cql, table, "t.f1", NO_TIMESTAMP, NO_TTL, "k=0")
            assertWritetimeAndTTL(cql, table, "t.f2", NO_TIMESTAMP, NO_TTL, "k=0")
            assert_rows(execute(cql, table, "SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=0"), row(0, NO_TIMESTAMP, NO_TIMESTAMP))

            # Only the first field is set
            execute(cql, table, "INSERT INTO %s (k, t) VALUES (1, {f1:1, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
            assertWritetimeAndTTL(cql, table, "t", [TIMESTAMP_1, NO_TIMESTAMP], [TTL_1, NO_TTL], "k=1")
            assertWritetimeAndTTL(cql, table, "t.f1", TIMESTAMP_1, TTL_1, "k=1")
            assertWritetimeAndTTL(cql, table, "t.f2", NO_TIMESTAMP, NO_TTL, "k=1")
            assert_rows(execute(cql, table, "SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=1"), row(1, TIMESTAMP_1, NO_TIMESTAMP))
            assert_rows(execute(cql, table, "SELECT k, WRITETIME(t.f2), WRITETIME(t.f1) FROM %s WHERE k=1"), row(1, NO_TIMESTAMP, TIMESTAMP_1))

            # Only the second field is set
            execute(cql, table, "INSERT INTO %s (k, t) VALUES (2, {f1:null, f2:2}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
            assertWritetimeAndTTL(cql, table, "t", [NO_TIMESTAMP, TIMESTAMP_1], [NO_TTL, TTL_1], "k=2")
            assertWritetimeAndTTL(cql, table, "t.f1", NO_TIMESTAMP, NO_TTL, "k=2")
            assertWritetimeAndTTL(cql, table, "t.f2", TIMESTAMP_1, TTL_1, "k=2")
            assert_rows(execute(cql, table, "SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=2"), row(2, NO_TIMESTAMP, TIMESTAMP_1))
            assert_rows(execute(cql, table, "SELECT k, WRITETIME(t.f2), WRITETIME(t.f1) FROM %s WHERE k=2"), row(2, TIMESTAMP_1, NO_TIMESTAMP))

            # Both fields are set
            execute(cql, table, "INSERT INTO %s (k, t) VALUES (3, {f1:1, f2:2}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1)
            assertWritetimeAndTTL(cql, table, "t", [TIMESTAMP_1, TIMESTAMP_1], [TTL_1, TTL_1], "k=3")
            assertWritetimeAndTTL(cql, table, "t.f1", TIMESTAMP_1, TTL_1, "k=3")
            assertWritetimeAndTTL(cql, table, "t.f2", TIMESTAMP_1, TTL_1, "k=3")
            assert_rows(execute(cql, table, "SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=3"), row(3, TIMESTAMP_1, TIMESTAMP_1))

            # Having only the first field set, update the second field
            execute(cql, table, "UPDATE %s USING TIMESTAMP ? AND TTL ? SET t.f2=2 WHERE k=1", TIMESTAMP_2, TTL_2)
            assertWritetimeAndTTL(cql, table, "t", [TIMESTAMP_1, TIMESTAMP_2], [TTL_1, TTL_2], "k=1")
            assertWritetimeAndTTL(cql, table, "t.f1", TIMESTAMP_1, TTL_1, "k=1")
            assertWritetimeAndTTL(cql, table, "t.f2", TIMESTAMP_2, TTL_2, "k=1")
            assert_rows(execute(cql, table, "SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=1"), row(1, TIMESTAMP_1, TIMESTAMP_2))
            assert_rows(execute(cql, table, "SELECT k, WRITETIME(t.f2), WRITETIME(t.f1) FROM %s WHERE k=1"), row(1, TIMESTAMP_2, TIMESTAMP_1))

            # Having only the second field set, update the second field
            execute(cql, table, "UPDATE %s USING TIMESTAMP ? AND TTL ? SET t.f1=1 WHERE k=2", TIMESTAMP_2, TTL_2)
            assertWritetimeAndTTL(cql, table, "t", [TIMESTAMP_2, TIMESTAMP_1], [TTL_2, TTL_1], "k=2")
            assertWritetimeAndTTL(cql, table, "t.f1", TIMESTAMP_2, TTL_2, "k=2")
            assertWritetimeAndTTL(cql, table, "t.f2", TIMESTAMP_1, TTL_1, "k=2")
            assert_rows(execute(cql, table, "SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=2"), row(2, TIMESTAMP_2, TIMESTAMP_1))
            assert_rows(execute(cql, table, "SELECT k, WRITETIME(t.f2), WRITETIME(t.f1) FROM %s WHERE k=2"), row(2, TIMESTAMP_1, TIMESTAMP_2))
