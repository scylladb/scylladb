# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

import time
from cassandra.query import UNSET_VALUE


def testNegativeTimestamps(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v int)") as table:
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP ?", 1, 1, -42)
        assert_rows(execute(cql, table, "SELECT writetime(v) FROM %s WHERE k = ?", 1), [-42])

        assert_invalid(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP ?", 2, 2, -2**63)

# Test timestmp and ttl
# migrated from cql_tests.py:TestCQL.timestamp_and_ttl_test()
def testTimestampTTL(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, c text, d text)") as table:
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (1, 'test')")
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (2, 'test') USING TTL 400")

        res = list(execute(cql, table, "SELECT k, c, writetime(c), ttl(c) FROM %s"))
        assert len(res) == 2
        for r in res:
            if r[0] == 1:
                assert r[3] == None
            else:
                assert r[2] != None and r[3] != None

        # wrap writetime(), ttl() in other functions (test for CASSANDRA-8451)
        res = list(execute(cql, table, "SELECT k, c, blobAsBigint(bigintAsBlob(writetime(c))), ttl(c) FROM %s"))
        assert len(res) == 2
        for r in res:
            assert r[2] != None
            if r[0] == 1:
                assert r[3] == None
            else:
                assert r[3] != None

        res = list(execute(cql, table, "SELECT k, c, writetime(c), blobAsInt(intAsBlob(ttl(c))) FROM %s"))
        assert len(res) == 2
        for r in res:
            assert r[2] != None
            if r[0] == 1:
                assert r[3] == None
            else:
                assert r[3] != None

        assert_invalid(cql, table, "SELECT k, c, writetime(k) FROM %s")

        assert_rows(execute(cql, table, "SELECT k, d, writetime(d) FROM %s WHERE k = 1"),
                   [1, None, None])

# Migrated from cql_tests.py:TestCQL.invalid_custom_timestamp_test()
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testInvalidCustomTimestamp(cql, test_keyspace):
    # Conditional updates
    with create_table(cql, test_keyspace, "(k int, v int, PRIMARY KEY (k, v))") as table:
        execute(cql, table, "BEGIN BATCH " +
                "INSERT INTO %s (k, v) VALUES(0, 0) IF NOT EXISTS; " +
                "INSERT INTO %s (k, v) VALUES(0, 1) IF NOT EXISTS; " +
                "APPLY BATCH")
        assert_invalid(cql, table, "BEGIN BATCH " +
                      "INSERT INTO %s (k, v) VALUES(0, 2) IF NOT EXISTS USING TIMESTAMP 1; " +
                      "INSERT INTO %s (k, v) VALUES(0, 3) IF NOT EXISTS; " +
                      "APPLY BATCH")
        assert_invalid(cql, table, "BEGIN BATCH " +
                      "USING TIMESTAMP 1 INSERT INTO %s (k, v) VALUES(0, 4) IF NOT EXISTS; " +
                      "INSERT INTO %s (k, v) VALUES(0, 1) IF NOT EXISTS; " +
                      "APPLY BATCH")

        execute(cql, table, "INSERT INTO %s (k, v) VALUES(1, 0) IF NOT EXISTS")
        assert_invalid(cql, table, "INSERT INTO %s (k, v) VALUES(1, 1) IF NOT EXISTS USING TIMESTAMP 5")
    # Counters
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, c counter)") as table:
        execute(cql, table, "UPDATE %s SET c = c + 1 WHERE k = 0")
        assert_invalid(cql, table, "UPDATE %s USING TIMESTAMP 10 SET c = c + 1 WHERE k = 0")
        execute(cql, table, "BEGIN COUNTER BATCH " +
                "UPDATE %s SET c = c + 1 WHERE k = 0; " +
                "UPDATE %s SET c = c + 1 WHERE k = 0; " +
                "APPLY BATCH")

        assert_invalid(cql, table, "BEGIN COUNTER BATCH " +
                      "UPDATE %s USING TIMESTAMP 3 SET c = c + 1 WHERE k = 0; " +
                      "UPDATE %s SET c = c + 1 WHERE k = 0; " +
                      "APPLY BATCH")

        assert_invalid(cql, table, "BEGIN COUNTER BATCH " +
                      "USING TIMESTAMP 3 UPDATE %s SET c = c + 1 WHERE k = 0; " +
                      "UPDATE %s SET c = c + 1 WHERE k = 0; " +
                      "APPLY BATCH")

def testInsertTimestampWithUnset(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, i int)") as table:
        execute(cql, table, "INSERT INTO %s (k, i) VALUES (1, 1) USING TIMESTAMP ?", UNSET_VALUE) # treat as 'now'

def testTimestampsOnUnsetColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, i int)") as table:
        execute(cql, table, "INSERT INTO %s (k, i) VALUES (1, 1) USING TIMESTAMP 1;")
        execute(cql, table, "INSERT INTO %s (k) VALUES (2) USING TIMESTAMP 2;")
        execute(cql, table, "INSERT INTO %s (k, i) VALUES (3, 3) USING TIMESTAMP 1;")
        assert_rows_ignoring_order(execute(cql, table, "SELECT k, i, writetime(i) FROM %s "),
                   [1, 1, 1],
                   [2, None, None],
                   [3, 3, 1])

def testTimestampsOnUnsetColumnsWide(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, i int, PRIMARY KEY (k, c))") as table:
        execute(cql, table, "INSERT INTO %s (k, c, i) VALUES (1, 1, 1) USING TIMESTAMP 1;")
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (1, 2) USING TIMESTAMP 1;")
        execute(cql, table, "INSERT INTO %s (k, c, i) VALUES (1, 3, 1) USING TIMESTAMP 1;")
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (2, 2) USING TIMESTAMP 2;")
        execute(cql, table, "INSERT INTO %s (k, c, i) VALUES (3, 3, 3) USING TIMESTAMP 1;")
        assert_rows_ignoring_order(execute(cql, table, "SELECT k, c, i, writetime(i) FROM %s "),
                   [1, 1, 1, 1],
                   [1, 2, None, None],
                   [1, 3, 1, 1],
                   [2, 2, None, None],
                   [3, 3, 3, 1])

@pytest.mark.skip(reason="a very slow test (6 seconds), skipping it")
def testTimestampAndTTLPrepared(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, i int, PRIMARY KEY (k, c))") as table:
        execute(cql, table, "INSERT INTO %s (k, c, i) VALUES (1, 1, 1) USING TIMESTAMP ? AND TTL ?;", 1, 5)
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (1, 2) USING TIMESTAMP ? AND TTL ? ;", 1, 5)
        execute(cql, table, "INSERT INTO %s (k, c, i) VALUES (1, 3, 1) USING TIMESTAMP ? AND TTL ?;", 1, 5)
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (2, 2) USING TIMESTAMP ? AND TTL ?;", 2, 5)
        execute(cql, table, "INSERT INTO %s (k, c, i) VALUES (3, 3, 3) USING TIMESTAMP ? AND TTL ?;", 1, 5)
        assert_rows_ignoring_order(execute(cql, table, "SELECT k, c, i, writetime(i) FROM %s "),
                [1, 1, 1, 1],
                [1, 2, None, None],
                [1, 3, 1, 1],
                [2, 2, None, None],
                [3, 3, 3, 1])
        time.sleep(6)
        assert_empty(execute(cql, table, "SELECT k, c, i, writetime(i) FROM %s "))

@pytest.mark.skip(reason="a very slow test (6 seconds), skipping it")
def testTimestampAndTTLUpdatePrepared(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, i int, PRIMARY KEY (k, c))") as table:
        execute(cql, table, "UPDATE %s USING TIMESTAMP ? AND TTL ? SET i=1 WHERE k=1 AND c = 1 ;", 1, 5)
        execute(cql, table, "UPDATE %s USING TIMESTAMP ? AND TTL ? SET i=1 WHERE k=1 AND c = 3 ;", 1, 5)
        execute(cql, table, "UPDATE %s USING TIMESTAMP ? AND TTL ? SET i=1 WHERE k=2 AND c = 2 ;", 2, 5)
        execute(cql, table, "UPDATE %s USING TIMESTAMP ? AND TTL ? SET i=3 WHERE k=3 AND c = 3 ;", 1, 5)
        assert_rows_ignoring_order(execute(cql, table, "SELECT k, c, i, writetime(i) FROM %s "),
                [1, 1, 1, 1],
                [1, 3, 1, 1],
                [2, 2, 1, 2],
                [3, 3, 3, 1])
        time.sleep(6)
        assert_empty(execute(cql, table, "SELECT k, c, i, writetime(i) FROM %s "))
