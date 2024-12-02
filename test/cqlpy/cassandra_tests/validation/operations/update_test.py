# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 8d91b469afd3fcafef7ef85c10c8acc11703ba2d
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *
import re

def testTypeCasts(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t text, a ascii, d double, i int)") as table:
        # The following is fine
        execute(cql, table, "UPDATE %s SET t = 'foo' WHERE k = ?", 0)
        execute(cql, table, "UPDATE %s SET t = (ascii)'foo' WHERE k = ?", 0)
        execute(cql, table, "UPDATE %s SET t = (text)(ascii)'foo' WHERE k = ?", 0)
        execute(cql, table, "UPDATE %s SET a = 'foo' WHERE k = ?", 0)
        execute(cql, table, "UPDATE %s SET a = (ascii)'foo' WHERE k = ?", 0)

        # But trying to put some explicitly type-casted text into an ascii
        # column should be rejected (even though the text is actually ascci)
        assertInvalid(cql, table, "UPDATE %s SET a = (text)'foo' WHERE k = ?", 0)

        # This is also fine because integer constants works for both integer and float types
        execute(cql, table, "UPDATE %s SET i = 3 WHERE k = ?", 0)
        execute(cql, table, "UPDATE %s SET i = (int)3 WHERE k = ?", 0)
        execute(cql, table, "UPDATE %s SET d = 3 WHERE k = ?", 0)
        execute(cql, table, "UPDATE %s SET d = (double)3 WHERE k = ?", 0)

        # But values for ints and doubles are not truly compatible (their binary representation differs)
        assertInvalid(cql, table, "UPDATE %s SET d = (int)3 WHERE k = ?", 0)
        assertInvalid(cql, table, "UPDATE %s SET i = (double)3 WHERE k = ?", 0)

@pytest.mark.parametrize("forceFlush", [False, True])
def testUpdate(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, value int, PRIMARY KEY (partitionKey, clustering_1))") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 2, 2)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 3, 3)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (1, 0, 4)")

        if forceFlush:
            flush(cql, table)

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ?",
                           0, 1),
                   row(7))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1) = (?)", 8, 0, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ?",
                           0, 2),
                   row(8))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey IN (?, ?) AND clustering_1 = ?", 9, 0, 1, 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ?",
                           0, 1, 0),
                   row(0, 0, 9),
                   row(1, 0, 9))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey IN ? AND clustering_1 = ?", 19, [0, 1], 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN ? AND clustering_1 = ?",
                           [0, 1], 0),
                   row(0, 0, 19),
                   row(1, 0, 19))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 10, 0, 1, 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)",
                           0, 1, 0),
                   row(0, 0, 10),
                   row(0, 1, 10))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))", 20, 0, 0, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                           0, 0, 1),
                   row(0, 0, 20),
                   row(0, 1, 20))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", null, 0, 0)
        if forceFlush:
            flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                           0, 0, 1),
                   row(0, 0, null),
                   row(0, 1, 20))

        # test invalid queries

        # missing primary key element
        assertInvalidMessage(cql, table, "partitionkey",
                             "UPDATE %s SET value = ? WHERE clustering_1 = ? ", 7, 1)

        assertInvalidMessage(cql, table, "clustering_1",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0)

        assertInvalidMessage(cql, table, "clustering_1",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0)

        # token function
        assertInvalidMessage(cql, table, "The token function cannot be used in WHERE clauses for UPDATE",
                             "UPDATE %s SET value = ? WHERE token(partitionKey) = token(?) AND clustering_1 = ?",
                             7, 0, 1)

        # multiple time the same value
        # Cassandra throws syntax error here, Scylla throws the more
        # reasonable invalid request error.
        assertInvalidThrow(cql, table, (SyntaxException, InvalidRequest), "UPDATE %s SET value = ?, value = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1)

        # multiple time same primary key element in WHERE clause
        # This *is* accepted by Scylla - see issue #12472
        #assertInvalidMessage(cql, table, "clustering_1 cannot be restricted by more than one relation if it includes an Equal",
        #                     "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_1 = ?", 7, 0, 1, 1)

        # unknown identifiers
        assertInvalidMessage(cql, table, "value1",
                             "UPDATE %s SET value1 = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1)

        assertInvalidMessage(cql, table, "partitionkey1",
                             "UPDATE %s SET value = ? WHERE partitionKey1 = ? AND clustering_1 = ?", 7, 0, 1)

        assertInvalidMessage(cql, table, "clustering_3",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_3 = ?", 7, 0, 1)

        # Invalid operator in the where clause
        assertInvalidMessage(cql, table, "Only EQ and IN relation are supported on the partition key",
                             "UPDATE %s SET value = ? WHERE partitionKey > ? AND clustering_1 = ?", 7, 0, 1)

        # Cassandra complains about "Cannot use UPDATE with CONTAINS, but
        # Scylla complains about "Cannot use CONTAINS on non-collection".
        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey CONTAINS ? AND clustering_1 = ?", 7, 0, 1)

        # Reproduces #12474:
        assertInvalidMessage(cql, table, "where clause",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND value = ?", 7, 0, 1, 3)

        # Scylla and Cassandra print different error messages in this case:
        # Cassandra says "Slice restrictions are not supported on the
        # clustering columns in UPDATE statements", and Scylla says
        # "Invalid operator in where clause (clustering_1 > ?)"
        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 > ?", 7, 0, 1)

def testUpdateWithContainsAndContainsKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b frozen<map<int, int>>, c int, PRIMARY KEY (a, b))") as table:
        row = [1, {1: 1, 2: 2}, 3]
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", *row)

        assertRows(execute(cql, table, "SELECT * FROM %s"), row)

        # Scylla and Cassandra print different error messages in this case:
        # Cassandra says "Cannot use UPDATE with CONTAINS", Scylla says
        # "Cannot restrict clustering columns by a CONTAINS relation without
        # a secondary index or filtering"'.
        assertInvalid(cql, table,
                             "UPDATE %s SET c=3 WHERE a=1 AND b CONTAINS 1")

        assertRows(execute(cql, table, "SELECT * FROM %s"), row)

        assertInvalid(cql, table,
                             "UPDATE %s SET c=3 WHERE a=1 AND b CONTAINS KEY 1")

        assertRows(execute(cql, table, "SELECT * FROM %s"), row)

@pytest.mark.parametrize("forceFlush", [False, True])
def testUpdateWithSecondaryIndices(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, value int, values set<int>, PRIMARY KEY (partitionKey, clustering_1))") as table:
        execute(cql, table, "CREATE INDEX ON %s (value)")
        execute(cql, table, "CREATE INDEX ON %s (clustering_1)")
        execute(cql, table, "CREATE INDEX ON %s (values)")

        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 0, 0, {0})")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 1, 1, {0, 1})")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 2, 2, {0, 1, 2})")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 3, 3, {0, 1, 2, 3})")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (1, 0, 4, {0, 1, 2, 3, 4})")

        if forceFlush:
            flush(cql, table)

        # In most errors below, the error message from Scylla and Cassandra
        # are different:
        assertInvalidMessage(cql, table, "PRIMARY KEY column",
                             "UPDATE %s SET values= {6} WHERE partitionKey = ? AND clustering_1 = ? AND value = ?", 3, 3, 3)
        assertInvalid(cql, table,
                             "UPDATE %s SET value= ? WHERE partitionKey = ? AND clustering_1 = ? AND values CONTAINS ?", 6, 3, 3, 3)
        assertInvalid(cql, table,
                             "UPDATE %s SET values= {6} WHERE partitionKey = ? AND value = ?", 3, 3)
        assertInvalid(cql, table,
                             "UPDATE %s SET value= ? WHERE partitionKey = ? AND values CONTAINS ?", 6, 3, 3)
        assertInvalidMessage(cql, table, "partitionkey",
                             "UPDATE %s SET values= {6} WHERE clustering_1 = ?", 3)
        assertInvalid(cql, table,
                             "UPDATE %s SET values= {6} WHERE value = ?", 3)
        assertInvalid(cql, table,
                             "UPDATE %s SET value= ? WHERE values CONTAINS ?", 6, 3)

@pytest.mark.parametrize("forceFlush", [False, True])
def testUpdateWithTwoClusteringColumns(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, clustering_2 int, value int, PRIMARY KEY (partitionKey, clustering_1, clustering_2))") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 2, 2)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 3, 3)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 1, 1, 4)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 1, 2, 5)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (1, 0, 0, 6)")
        if forceFlush:
            flush(cql, table)

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 1),
                   row(7))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) = (?, ?)", 8, 0, 1, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 2),
                   row(8))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 9, 0, 1, 0, 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 0, 0),
                   row(0, 0, 0, 9),
                   row(1, 0, 0, 9))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey IN ? AND clustering_1 = ? AND clustering_2 = ?", 9, [0, 1], 0, 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN ? AND clustering_1 = ? AND clustering_2 = ?",
                           [0, 1], 0, 0),
                   row(0, 0, 0, 9),
                   row(1, 0, 0, 9))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)", 12, 0, 1, 1, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)",
                           0, 1, 1, 2),
                   row(0, 1, 1, 12),
                   row(0, 1, 2, 12))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND clustering_2 IN (?, ?)", 10, 0, 1, 0, 1, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND clustering_2 IN (?, ?)",
                           0, 1, 0, 1, 2),
                   row(0, 0, 1, 10),
                   row(0, 0, 2, 10),
                   row(0, 1, 1, 10),
                   row(0, 1, 2, 10))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))", 20, 0, 0, 2, 1, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                           0, 0, 2, 1, 2),
                   row(0, 0, 2, 20),
                   row(0, 1, 2, 20))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", null, 0, 0, 2)
        if forceFlush:
            flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                           0, 0, 2, 1, 2),
                   row(0, 0, 2, null),
                   row(0, 1, 2, 20))

        # test invalid queries

        # missing primary key element
        assertInvalidMessage(cql, table, "partitionkey",
                             "UPDATE %s SET value = ? WHERE clustering_1 = ? AND clustering_2 = ?", 7, 1, 1)

        assertInvalidMessage(cql, table, "clustering_1",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_2 = ?", 7, 0, 1)

        assertInvalidMessage(cql, table, "clustering_1",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0)

        # token function
        assertInvalidMessage(cql, table, "The token function cannot be used in WHERE clauses for UPDATE",
                             "UPDATE %s SET value = ? WHERE token(partitionKey) = token(?) AND clustering_1 = ? AND clustering_2 = ?",
                             7, 0, 1, 1)

        # multiple time the same value
        # Cassandra throws syntax error here, Scylla throws the more
        # reasonable invalid request error.
        assertInvalidThrow(cql, table, (SyntaxException, InvalidRequest), "UPDATE %s SET value = ?, value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)

        # multiple time same primary key element in WHERE clause
        # This *is* accepted by Scylla - see issue #12472
        #assertInvalidMessage(cql, table, "clustering_1 cannot be restricted by more than one relation if it includes an Equal",
        #                     "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND clustering_1 = ?", 7, 0, 1, 1, 1)

        # unknown identifiers
        assertInvalidMessage(cql, table, "value1",
                             "UPDATE %s SET value1 = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)

        assertInvalidMessage(cql, table, "partitionkey1",
                             "UPDATE %s SET value = ? WHERE partitionKey1 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)

        assertInvalidMessage(cql, table, "clustering_3",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_3 = ?", 7, 0, 1, 1)

        # Invalid operator in the where clause
        assertInvalidMessage(cql, table, "Only EQ and IN relation are supported on the partition key",
                             "UPDATE %s SET value = ? WHERE partitionKey > ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)

        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey CONTAINS ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)
        # Reproduces #12474:
        assertInvalidMessage(cql, table, "where clause",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND value = ?", 7, 0, 1, 1, 3)

        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 > ?", 7, 0, 1)

        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) > (?, ?)", 7, 0, 1, 1)

@pytest.mark.parametrize("forceFlush", [False, True])
def testUpdateWithMultiplePartitionKeyComponents(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey_1 int, partitionKey_2 int, clustering_1 int, clustering_2 int, value int, PRIMARY KEY ((partitionKey_1, partitionKey_2), clustering_1, clustering_2))") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 1, 0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 1, 1, 1, 2)")
        execute(cql, table, "INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (1, 0, 0, 1, 3)")
        execute(cql, table, "INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (1, 1, 0, 1, 3)")
        if forceFlush:
            flush(cql, table)

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey_1 = ? AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 0, 0, 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT value FROM %s WHERE partitionKey_1 = ? AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 0, 0, 0),
                   row(7))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?", 9, 0, 1, 1, 0, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 1, 0, 1),
                   row(0, 1, 0, 1, 9),
                   row(1, 1, 0, 1, 9))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 10, 0, 1, 0, 1, 0, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 0, 0, 0, 7),
                   row(0, 0, 0, 1, 10),
                   row(0, 1, 0, 1, 10),
                   row(0, 1, 1, 1, 2),
                   row(1, 0, 0, 1, 10),
                   row(1, 1, 0, 1, 10))

        # missing primary key element
        # Reproduces #12474:
        # Different error message returned, changed to assertInvalid instead of assertInvalidMessage
        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey_1 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 1, 1)

@pytest.mark.parametrize("forceFlush", [False, True])
def testUpdateWithAStaticColumn(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, clustering_2 int, value int, staticValue text static, PRIMARY KEY (partitionKey, clustering_1, clustering_2))") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value, staticValue) VALUES (0, 0, 0, 0, 'A')")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value, staticValue) VALUES (1, 0, 0, 6, 'B')")
        if forceFlush:
            flush(cql, table)

        execute(cql, table, "UPDATE %s SET staticValue = ? WHERE partitionKey = ?", "A2", 0)
        if forceFlush:
            flush(cql, table)

        assertRows(execute(cql, table, "SELECT DISTINCT staticValue FROM %s WHERE partitionKey = ?", 0),
                   row("A2"))

        assertInvalidMessage(cql, table, "clustering_1",
                             "UPDATE %s SET staticValue = ?, value = ? WHERE partitionKey = ?", "A2", 7, 0)

        execute(cql, table, "UPDATE %s SET staticValue = ?, value = ?  WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                "A3", 7, 0, 0, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 0, 1),
                   row(0, 0, 1, "A3", 7))

        assertInvalidMessage(cql, table, "Invalid restrictions on clustering columns since the UPDATE statement modifies only static columns",
                             "UPDATE %s SET staticValue = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                             "A3", 0, 0, 1)

def testUpdateWithStaticList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, clustering int, value int, l list<text> static, PRIMARY KEY (k, clustering))") as table:
        execute(cql, table, "INSERT INTO %s(k, clustering, value, l) VALUES (?, ?, ?, ?)", 0, 0, 0 ,["v1", "v2", "v3"])

        assertRows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), row(["v1", "v2", "v3"]))

        execute(cql, table, "UPDATE %s SET l[?] = ? WHERE k = ?", 1, "v4", 0)

        assertRows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), row(["v1", "v4", "v3"]))

@pytest.mark.xfail(reason="#12243")
def testUpdateWithDefaultTtl(cql, test_keyspace):
    secondsPerMinute = 60
    with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b int) WITH default_time_to_live = {10 * secondsPerMinute}") as table:
        execute(cql, table, "UPDATE %s SET b = 1 WHERE a = 1")
        resultSet = list(execute(cql, table, "SELECT ttl(b) FROM %s WHERE a = 1"))
        assert 1 == len(resultSet)
        assert getattr(resultSet[0], "ttl_b") >= (9 * secondsPerMinute)

        execute(cql, table, "UPDATE %s USING TTL ? SET b = 3 WHERE a = 1", 0)
        assertRows(execute(cql, table, "SELECT ttl(b) FROM %s WHERE a = 1"), row(None))

        execute(cql, table, "UPDATE %s SET b = 3 WHERE a = 1")
        resultSet = list(execute(cql, table, "SELECT ttl(b) FROM %s WHERE a = 1"))
        assert 1 == len(resultSet)
        assert getattr(resultSet[0], "ttl_b") >= (9 * secondsPerMinute)

        execute(cql, table, "UPDATE %s USING TTL ? SET b = 2 WHERE a = 2", unset())
        resultSet = list(execute(cql, table, "SELECT ttl(b) FROM %s WHERE a = 2"))
        assert 1 == len(resultSet)
        assert getattr(resultSet[0], "ttl_b") >= (9 * secondsPerMinute)

        # Reproduces #12243
        execute(cql, table, "UPDATE %s USING TTL ? SET b = ? WHERE a = ?", None, 3, 3)
        assertRows(execute(cql, table, "SELECT ttl(b) FROM %s WHERE a = 3"), row(None))

# Both Scylla and Cassandra define MAX_TTL or max_ttl with the same formula,
# 20 years in seconds. In both systems, it is not configurable.
MAX_TTL = 20 * 365 * 24 * 60 * 60

def testUpdateWithTtl(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v int)") as table:
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, 1) USING TTL ?", 3600)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (2, 2) USING TTL ?", 3600)

        # test with unset
        execute(cql, table, "UPDATE %s USING TTL ? SET v = ? WHERE k = ?", unset(), 1, 1); # treat as 'unlimited'
        assertRows(execute(cql, table, "SELECT ttl(v) FROM %s WHERE k = 1"), row(None))

        # test with null
        execute(cql, table, "UPDATE %s USING TTL ? SET v = ? WHERE k = ?", unset(), 2, 2)
        assertRows(execute(cql, table, "SELECT k, v, TTL(v) FROM %s WHERE k = 2"), row(2, 2, None))

        # test error handling
        assertInvalidMessage(cql, table, "A TTL must be greater or equal to 0",
                             "UPDATE %s USING TTL ? SET v = ? WHERE k = ?", -5, 1, 1)

        assertInvalidMessage(cql, table, "ttl is too large.",
                             "UPDATE %s USING TTL ? SET v = ? WHERE k = ?",
                             MAX_TTL + 1, 1, 1)

# Test for CASSANDRA-12829
def testUpdateWithEmptyInRestriction(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(a int, b int, c int, PRIMARY KEY (a,b))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,1,1)
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,2,2)
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,3,3)

        assertInvalidMessage(cql, table, " b",
                             "UPDATE %s SET c = 100 WHERE a IN ();")
        execute(cql, table, "UPDATE %s SET c = 100 WHERE a IN () AND b IN ();")
        execute(cql, table, "UPDATE %s SET c = 100 WHERE a IN () AND b = 1;")
        execute(cql, table, "UPDATE %s SET c = 100 WHERE a = 1 AND b IN ();")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1,1,1),
                   row(1,2,2),
                   row(1,3,3))

    with create_table(cql, test_keyspace, f"(a int, b int, c int, d int, s int static, PRIMARY KEY ((a,b), c))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)",1,1,1,1,1)
        execute(cql, table, "INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)",1,1,2,2,1)
        execute(cql, table, "INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)",1,1,3,3,1)

        execute(cql, table, "UPDATE %s SET d = 100 WHERE a = 1 AND b = 1 AND c IN ();")
        execute(cql, table, "UPDATE %s SET d = 100 WHERE a = 1 AND b IN () AND c IN ();")
        execute(cql, table, "UPDATE %s SET d = 100 WHERE a IN () AND b IN () AND c IN ();")
        execute(cql, table, "UPDATE %s SET d = 100 WHERE a IN () AND b IN () AND c = 1;")
        execute(cql, table, "UPDATE %s SET d = 100 WHERE a IN () AND b = 1 AND c IN ();")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1,1,1,1,1),
                   row(1,1,2,1,2),
                   row(1,1,3,1,3))

        # No clustering keys restricted, update whole partition
        execute(cql, table, "UPDATE %s set s = 100 where a = 1 AND b = 1;")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1,1,1,100,1),
                   row(1,1,2,100,2),
                   row(1,1,3,100,3))

        execute(cql, table, "UPDATE %s set s = 200 where a = 1 AND b IN ();")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1,1,1,100,1),
                   row(1,1,2,100,2),
                   row(1,1,3,100,3))

    with create_table(cql, test_keyspace, f"(a int, b int, c int, d int, e int, PRIMARY KEY ((a,b), c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)",1,1,1,1,1)
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)",1,1,1,2,2)
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)",1,1,1,3,3)
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)",1,1,1,4,4)

        execute(cql, table, "UPDATE %s SET e = 100 WHERE a = 1 AND b = 1 AND c = 1 AND d IN ();")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a = 1 AND b = 1 AND c IN () AND d IN ();")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a = 1 AND b IN () AND c IN () AND d IN ();")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c IN () AND d IN ();")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c IN () AND d = 1;")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c = 1 AND d = 1;")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c = 1 AND d IN ();")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1,1,1,1,1),
                   row(1,1,1,2,2),
                   row(1,1,1,3,3),
                   row(1,1,1,4,4))

# Test for CASSANDRA-13152
def testThatUpdatesWithEmptyInRestrictionDoNotCreateMutations(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(a int, b int, c int, PRIMARY KEY (a,b))") as table:
        execute(cql, table, "UPDATE %s SET c = 100 WHERE a IN () AND b = 1;")
        execute(cql, table, "UPDATE %s SET c = 100 WHERE a = 1 AND b IN ();")

        # I don't know how to check this with CQL...
        #assertTrue("The memtable should be empty but is not", isMemtableEmpty())

    with create_table(cql, test_keyspace, f"(a int, b int, c int, d int, s int static, PRIMARY KEY ((a,b), c))") as table:
        execute(cql, table, "UPDATE %s SET d = 100 WHERE a = 1 AND b = 1 AND c IN ();")
        execute(cql, table, "UPDATE %s SET d = 100 WHERE a = 1 AND b IN () AND c IN ();")
        execute(cql, table, "UPDATE %s SET d = 100 WHERE a IN () AND b IN () AND c IN ();")
        execute(cql, table, "UPDATE %s SET d = 100 WHERE a IN () AND b IN () AND c = 1;")
        execute(cql, table, "UPDATE %s SET d = 100 WHERE a IN () AND b = 1 AND c IN ();")

        # I don't know how to check this with CQL...
        #assertTrue("The memtable should be empty but is not", isMemtableEmpty())

    with create_table(cql, test_keyspace, f"(a int, b int, c int, d int, e int, PRIMARY KEY ((a,b), c, d))") as table:
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a = 1 AND b = 1 AND c = 1 AND d IN ();")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a = 1 AND b = 1 AND c IN () AND d IN ();")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a = 1 AND b IN () AND c IN () AND d IN ();")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c IN () AND d IN ();")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c IN () AND d = 1;")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c = 1 AND d = 1;")
        execute(cql, table, "UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c = 1 AND d IN ();")

        # I don't know how to check this with CQL...
        #assertTrue("The memtable should be empty but is not", isMemtableEmpty())

def testAdderNonCounter(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(pk int PRIMARY KEY, a int, b text)") as table:
        # if error ever includes "b" its safe to update this test
        with pytest.raises(InvalidRequest, match=re.escape('Invalid operation (a = a + 1) for non counter column a')):
            execute(cql, table, "UPDATE %s SET a = a + 1, b = b + 'fail' WHERE pk = 1")
