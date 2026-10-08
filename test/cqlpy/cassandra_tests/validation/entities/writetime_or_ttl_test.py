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
