# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of Cassandra 4.1.1 (commit 8d91b469afd3fcafef7ef85c10c8acc11703ba2d)
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *
import random

# Test for cassandra 8558
@pytest.mark.parametrize("flushData", [False, True])
@pytest.mark.parametrize("flushTombstone", [False, True])
def testRangeDeletion(cql, test_keyspace, flushData, flushTombstone):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, primary key (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1)
        if flushData:
            flush(cql, table)
        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=?", 1, 1)
        if flushTombstone:
            flush(cql, table)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 1, 1, 1))

@pytest.mark.parametrize("flushData", [False, True])
@pytest.mark.parametrize("flushTombstone", [False, True])
def testDeleteRange(cql, test_keyspace, flushData, flushTombstone):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b))") as table:

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, 2)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 2, 3)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 3, 4)
        if flushData:
            flush(cql, table)

        execute(cql, table, "DELETE FROM %s WHERE a = ? AND b >= ?", 2, 2)
        if flushTombstone:
            flush(cql, table)

        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s"),
                                row(1, 1, 1),
                                row(2, 1, 2))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ?", 2, 1),
                   row(2, 1, 2))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ?", 2, 2))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ?", 2, 3))

@pytest.mark.parametrize("flushData", [False, True])
@pytest.mark.parametrize("flushTombstone", [False, True])
def testCrossMemSSTableMultiColumn(cql, test_keyspace, flushData, flushTombstone):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b))") as table:

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, 2)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 2, 2)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 3, 3)
        if flushData:
            flush(cql, table)

        execute(cql, table, "DELETE FROM %s WHERE a = ? AND (b) = (?)", 2, 2)
        execute(cql, table, "DELETE FROM %s WHERE a = ? AND (b) = (?)", 2, 3)

        if flushTombstone:
            flush(cql, table)

        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s"),
                                row(1, 1, 1),
                                row(2, 1, 2))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ?", 2, 1),
                   row(2, 1, 2))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ?", 2, 2))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ?", 2, 3))

# Test simple deletion and in particular check for cassandra-4193 bug
# migrated from cql_tests.py:TestCQL.deletion_test()
def testDeletion(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(username varchar, id int, name varchar, stuff varchar, primary key (username, id))") as table:
        execute(cql, table, "INSERT INTO %s (username, id, name, stuff) VALUES (?, ?, ?, ?)", "abc", 2, "rst", "some value")
        execute(cql, table, "INSERT INTO %s (username, id, name, stuff) VALUES (?, ?, ?, ?)", "abc", 4, "xyz", "some other value")

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row("abc", 2, "rst", "some value"),
                   row("abc", 4, "xyz", "some other value"))

        execute(cql, table, "DELETE FROM %s WHERE username='abc' AND id=2")

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row("abc", 4, "xyz", "some other value"))

def testDeletionWithContainsAndContainsKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b frozen<map<int, int>>, c int, primary key (a, b))") as table:
        row = [1, {1: 1, 2: 2}, 3]
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", *row)

        assertRows(execute(cql, table, "SELECT * FROM %s"), row)

        # Cassandra's and Scylla's error message are different
        assertInvalidMessage(cql, table, "CONTAINS",
                             "DELETE FROM %s WHERE a=1 AND b CONTAINS 1")

        assertRows(execute(cql, table, "SELECT * FROM %s"), row)

        # Cassandra's and Scylla's error message are different
        assertInvalidMessage(cql, table, "CONTAINS",
                             "DELETE FROM %s WHERE a=1 AND b CONTAINS KEY 1")

        assertRows(execute(cql, table, "SELECT * FROM %s"), row)

# Test deletion by 'composite prefix' (range tombstones)
# migrated from cql_tests.py:TestCQL.range_tombstones_test()
def testDeleteByCompositePrefix(cql, test_keyspace):
    # This test used 3 nodes just to make sure RowMutation are correctly serialized
    with create_table(cql, test_keyspace, "(k int, c1 int, c2 int, v1 int, v2 int, primary key (k, c1, c2))") as table:
        numRows = 5
        col1 = 2
        col2 = 2
        cpr = col1 * col2

        for i in range(numRows):
            for j in range(col1):
                for k in range(col2):
                    n = (i * cpr) + (j * col2) + k
                    execute(cql, table, "INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", i, j, k, n, n)

        for i in range(numRows):
            rows = list(execute(cql, table, "SELECT v1, v2 FROM %s where k = ?", i))
            for x in range(i * cpr, (i + 1) * cpr):
                assert x == rows[x - i * cpr][0]
                assert x == rows[x - i * cpr][1]

        for i in range(numRows):
            execute(cql, table, "DELETE FROM %s WHERE k = ? AND c1 = 0", i)

        for i in range(numRows):
            rows = list(execute(cql, table, "SELECT v1, v2 FROM %s WHERE k = ?", i))
            for x in range(i * cpr + col1, (i + 1) * cpr):
                assert x == rows[x - i * cpr - col1][0]
                assert x == rows[x - i * cpr - col1][1]

        for i in range(numRows):
            rows = list(execute(cql, table, "SELECT v1, v2 FROM %s WHERE k = ?", i))
            for x in range(i * cpr + col1, (i + 1) * cpr):
                assert x == rows[x - i * cpr - col1][0]
                assert x == rows[x - i * cpr - col1][1]

# Test deletion by 'composite prefix' (range tombstones) with compaction
# migrated from cql_tests.py:TestCQL.range_tombstones_compaction_test()
def testDeleteByCompositePrefixWithCompaction(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c1 int, c2 int, v1 text, primary key (k, c1, c2))") as table:
        for c1 in range(4):
            for c2 in range(2):
                execute(cql, table, "INSERT INTO %s (k, c1, c2, v1) VALUES (0, ?, ?, ?)", c1, c2, f"{c1}{c2}")
        flush(cql, table)
        execute(cql, table, "DELETE FROM %s WHERE k = 0 AND c1 = 1")
        flush(cql, table)
        compact(cql, table)
        rows = list(execute(cql, table, "SELECT v1 FROM %s WHERE k = 0"))
        idx = 0
        for c1 in range(4):
            for c2 in range(2):
                if c1 != 1:
                    assert f"{c1}{c2}" == rows[idx][0]
                    idx += 1

# Test deletion of rows
# migrated from cql_tests.py:TestCQL.delete_row_test()
def testRowDeletion(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c1 int, c2 int, v1 int, v2 int, primary key (k, c1, c2))") as table:
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 0, 2, 2, 2)
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 3, 3)

        execute(cql, table, "DELETE FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 0")

        assertRowCount(execute(cql, table, "SELECT * FROM %s"), 3)

# Check the semantic of CQL row existence (part of CASSANDRA-4361),
# migrated from cql_tests.py:TestCQL.row_existence_test()
def testRowExistence(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, v1 int, v2 int, primary key (k, c))") as table:
        execute(cql, table, "INSERT INTO %s (k, c, v1, v2) VALUES (1, 1, 1, 1)")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1, 1, 1, 1))

        assertInvalid(cql, table, "DELETE c FROM %s WHERE k = 1 AND c = 1")

        execute(cql, table, "DELETE v2 FROM %s WHERE k = 1 AND c = 1")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1, 1, 1, null))

        execute(cql, table, "DELETE v1 FROM %s WHERE k = 1 AND c = 1")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1, 1, null, null))

        execute(cql, table, "DELETE FROM %s WHERE k = 1 AND c = 1")
        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (k, c) VALUES (2, 2)")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(2, 2, null, null))

# Migrated from cql_tests.py:TestCQL.remove_range_slice_test()
def testRemoveRangeSlice(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v int)") as table:
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", i, i)

        execute(cql, table, "DELETE FROM %s WHERE k = 1")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 0),
                   row(2, 2))

# Test deletions
# migrated from cql_tests.py:TestCQL.no_range_ghost_test()
def testNoRangeGhost(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v int)") as table:
        for k in range(5):
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, 0)", k)

        rows = getRows(execute(cql, table, "SELECT k FROM %s"))

        ordered = sortIntRows(rows)
        for k in range(5):
            assert k == ordered[k]

        execute(cql, table, "DELETE FROM %s WHERE k=2")

        rows = getRows(execute(cql, table, "SELECT k FROM %s"))
        ordered = sortIntRows(rows)

        idx = 0
        for k in range(5):
            if k != 2:
                assertEquals(k, ordered[idx])
                idx += 1

    # Example from CASSANDRA-3505
    with create_table(cql, test_keyspace, "(KEY varchar PRIMARY KEY, password varchar, gender varchar, birth_year bigint)") as table:
        execute(cql, table, "INSERT INTO %s (KEY, password) VALUES ('user1', 'ch@ngem3a')")
        execute(cql, table, "UPDATE %s SET gender = 'm', birth_year = 1980 WHERE KEY = 'user1'")

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE KEY='user1'"),
                   row("user1", 1980, "m", "ch@ngem3a"))

        execute(cql, table, "TRUNCATE %s")
        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE KEY='user1'"))

def sortIntRows(rows):
    return sorted([rows[i][0] for i in range(len(rows))])

# Migrated from cql_tests.py:TestCQL.range_with_deletes_test()
def testRandomDeletions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v int)") as table:
        nb_keys = 30
        nb_deletes = 5
        deletions = []
        for i in range(nb_keys):
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", i, i)
            deletions.append(i)

        random.shuffle(deletions)

        for i in range(nb_deletes):
            execute(cql, table, "DELETE FROM %s WHERE k = ?", deletions[i])

        assertRowCount(execute(cql, table, "SELECT * FROM %s LIMIT ?", (nb_keys // 2)), nb_keys // 2)

# Test for CASSANDRA-8558, deleted row still can be selected out
# migrated from cql_tests.py:TestCQL.bug_8558_test()
def testDeletedRowCannotBeSelected(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c text, primary key (a,b))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES(1,1,'1')")
        flush(cql, table)

        execute(cql, table, "DELETE FROM %s  where a=1 and b=1")
        flush(cql, table)

        assertEmpty(execute(cql, table, "select * from %s  where a=1 and b=1"))

# Test that two deleted rows for the same partition but on different sstables do not resurface 
def testDeletedRowsDoNotResurface(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c text, primary key (a,b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES(1, 1, '1')")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES(1, 2, '2')")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES(1, 3, '3')")
        flush(cql, table)

        execute(cql, table, "DELETE FROM %s where a=1 and b = 1")
        flush(cql, table)

        execute(cql, table, "DELETE FROM %s where a=1 and b = 2")
        flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ?", 1),
                   row(1, 3, "3"))


@pytest.mark.parametrize("forceFlush", [False, True])
def testDeleteWithNoClusteringColumns(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int PRIMARY KEY, value int)") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, value) VALUES (0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, value) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey, value) VALUES (2, 2)")
        execute(cql, table, "INSERT INTO %s (partitionKey, value) VALUES (3, 3)")
        if forceFlush:
            flush(cql, table)

        execute(cql, table, "DELETE value FROM %s WHERE partitionKey = ?", 0)
        if forceFlush:
            flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 0),
                   row(0, null))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey IN (?, ?)", 0, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(2, 2),
                   row(3, 3))

        # test invalid queries

        # token function
        assertInvalidMessage(cql, table, "The token function cannot be used in WHERE clauses for",
                             "DELETE FROM %s WHERE token(partitionKey) = token(?)", 0)

        # multiple time same primary key element in WHERE clause
        # Scylla does allow silly queries using the same partition key
        # more than once, so this check is commented out. See #12472.
        #assertInvalidMessage(cql, table, "partitionkey cannot be restricted by more than one relation if it includes an Equal",
        #                     "DELETE FROM %s WHERE partitionKey = ? AND partitionKey = ?", 0, 1)

        # unknown identifiers
        assertInvalidMessage(cql, table, "unknown",
                             "DELETE unknown FROM %s WHERE partitionKey = ?", 0)

        assertInvalidMessage(cql, table, "partitionkey1",
                             "DELETE FROM %s WHERE partitionKey1 = ?", 0)

        # Invalid operator in the where clause
        assertInvalidMessage(cql, table, "Only EQ and IN relation are supported on the partition key (unless you use the token() function",
                             "DELETE FROM %s WHERE partitionKey > ? ", 0)

        assertInvalidMessage(cql, table, "CONTAINS",
                             "DELETE FROM %s WHERE partitionKey CONTAINS ?", 0)

        # Non primary key in the where clause
        # Reproduces #12474:
        assertInvalidMessage(cql, table, "where clause",
                             "DELETE FROM %s WHERE partitionKey = ? AND value = ?", 0, 1)



@pytest.mark.parametrize("forceFlush", [False, True])
def testDeleteWithOneClusteringColumns(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering int, value int, PRIMARY KEY (partitionKey, clustering))") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 2, 2)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 3, 3)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 4, 4)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 5, 5)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (1, 0, 6)")
        if forceFlush:
            flush(cql, table)

        execute(cql, table, "DELETE value FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1),
                   row(0, 1, null))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1)
        if forceFlush:
            flush(cql, table)
        assertEmpty(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey IN (?, ?) AND clustering = ?", 0, 1, 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                   row(0, 2, 2),
                   row(0, 3, 3),
                   row(0, 4, 4),
                   row(0, 5, 5))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering) IN ((?), (?))", 0, 4, 5)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                   row(0, 2, 2),
                   row(0, 3, 3))

        # test invalid queries

        # missing primary key element
        assertInvalidMessage(cql, table, "partitionkey",
                             "DELETE FROM %s WHERE clustering = ?", 1)

        # token function
        assertInvalidMessage(cql, table, "The token function cannot be used in WHERE clauses for",
                             "DELETE FROM %s WHERE token(partitionKey) = token(?) AND clustering = ? ", 0, 1)

        # multiple time same primary key element in WHERE clause
        # Scylla does allow silly queries using the same partition key
        # more than once, so this check is commented out. See #12472.
        #assertInvalidMessage(cql, table, "clustering cannot be restricted by more than one relation if it includes an Equal",
        #                     "DELETE FROM %s WHERE partitionKey = ? AND clustering = ? AND clustering = ?", 0, 1, 1)

        # unknown identifiers
        assertInvalidMessage(cql, table, "value1",
                             "DELETE value1 FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1)

        assertInvalidMessage(cql, table, "partitionkey1",
                             "DELETE FROM %s WHERE partitionKey1 = ? AND clustering = ?", 0, 1)

        assertInvalidMessage(cql, table, "clustering_3",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_3 = ?", 0, 1)

        # Invalid operator in the where clause
        assertInvalidMessage(cql, table, "Only EQ and IN relation are supported on the partition key (unless you use the token() function",
                             "DELETE FROM %s WHERE partitionKey > ? AND clustering = ?", 0, 1)

        assertInvalidMessage(cql, table, "CONTAINS",
                             "DELETE FROM %s WHERE partitionKey CONTAINS ? AND clustering = ?", 0, 1)

        # Non primary key in the where clause
        # Reproduces #12474:
        assertInvalidMessage(cql, table, "where clause",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering = ? AND value = ?", 0, 1, 3)

@pytest.mark.xfail(reason="#4244")
@pytest.mark.parametrize("forceFlush", [False, True])
def testDeleteWithTwoClusteringColumns(cql, test_keyspace, forceFlush):
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

        execute(cql, table, "DELETE value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1)
        if forceFlush:
            flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 1),
                   row(0, 1, 1, null))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) = (?, ?)", 0, 1, 1)
        if forceFlush:
            flush(cql, table)
        assertEmpty(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                            0, 1, 1))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 0, 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                   row(0, 0, 1, 1),
                   row(0, 0, 2, 2),
                   row(0, 0, 3, 3),
                   row(0, 1, 2, 5))

        execute(cql, table, "DELETE value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)", 0, 0, 2, 3)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1), row(0, 0, 1, 1),
                   row(0, 0, 2, null),
                   row(0, 0, 3, null),
                   row(0, 1, 2, 5))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))", 0, 0, 2, 1, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                   row(0, 0, 1, 1),
                   row(0, 0, 3, null))

        # Reproduces #4244:
        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?)) AND clustering_2 = ?", 0, 0, 2, 3)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                   row(0, 0, 1, 1))

        # test invalid queries

        # missing primary key element
        assertInvalidMessage(cql, table, "partitionkey",
                             "DELETE FROM %s WHERE clustering_1 = ? AND clustering_2 = ?", 1, 1)

        assertInvalidMessage(cql, table, "PRIMARY KEY column \"clustering_2\" cannot be restricted as preceding column \"clustering_1\" is not restricted",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_2 = ?", 0, 1)

        # token function
        assertInvalidMessage(cql, table, "The token function cannot be used in WHERE clauses for",
                             "DELETE FROM %s WHERE token(partitionKey) = token(?) AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1)

        # multiple time same primary key element in WHERE clause
        # Scylla does allow silly queries using the same partition key
        # more than once, so this check is commented out. See #12472.
        #assertInvalidMessage(cql, table, "clustering_1 cannot be restricted by more than one relation if it includes an Equal",
        #                     "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND clustering_1 = ?", 0, 1, 1, 1)

        # unknown identifiers
        assertInvalidMessage(cql, table, "value1",
                             "DELETE value1 FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1)

        assertInvalidMessage(cql, table, "partitionkey1",
                             "DELETE FROM %s WHERE partitionKey1 = ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1)

        assertInvalidMessage(cql, table, "clustering_3",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_3 = ?", 0, 1, 1)

        # Invalid operator in the where clause
        assertInvalidMessage(cql, table, "Only EQ and IN relation are supported on the partition key (unless you use the token() function",
                             "DELETE FROM %s WHERE partitionKey > ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1)

        assertInvalidMessage(cql, table, "CONTAINS",
                             "DELETE FROM %s WHERE partitionKey CONTAINS ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1)

        # Non primary key in the where clause
        # Reproduces #12474:
        assertInvalidMessage(cql, table, "where clause",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND value = ?", 0, 1, 1, 3)

def testDeleteWithNonoverlappingRange(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c text, primary key (a, b))") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES(1, ?, 'abc')", i)
        flush(cql, table)

        execute(cql, table, "DELETE FROM %s WHERE a=1 and b <= 3")
        flush(cql, table)

        # this query does not overlap the tombstone range above and caused the rows to be resurrected
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a=1 and b <= 2"))

def testDeleteWithIntermediateRangeAndOneClusteringColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c text, primary key (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES(1, 1, '1')")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES(1, 3, '3')")
        execute(cql, table, "DELETE FROM %s where a=1 and b >= 2 and b <= 3")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES(1, 2, '2')")
        flush(cql, table)

        execute(cql, table, "DELETE FROM %s where a=1 and b >= 2 and b <= 3")
        flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ?", 1),
                   row(1, 1, "1"))

@pytest.mark.parametrize("forceFlush", [False, True])
def testDeleteWithIntermediateRangeAndOneClusteringColumn(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering int, value int, primary key (partitionKey, clustering))") as table:
        value = 0
        for partitionKey in range(5):
            for clustering1 in range(5):
                execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (?, ?, ?)",
                        partitionKey, clustering1, value)
                value += 1

        if forceFlush:
            flush(cql, table)

        # test delete partition
        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ?", 1)
        if forceFlush:
            flush(cql, table)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 1))

        # test slices on the first clustering column

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND  clustering >= ?", 0, 4)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 0),
                   row(0, 0, 0),
                   row(0, 1, 1),
                   row(0, 2, 2),
                   row(0, 3, 3))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND  clustering > ?", 0, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 0),
                   row(0, 0, 0),
                   row(0, 1, 1),
                   row(0, 2, 2))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering <= ?", 0, 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 0),
                   row(0, 1, 1),
                   row(0, 2, 2))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering < ?", 0, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 0),
                   row(0, 2, 2))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering >= ? AND clustering < ?", 2, 0, 3)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 2),
                   row(2, 3, 13),
                   row(2, 4, 14))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering > ? AND clustering <= ?", 2, 3, 5)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 2),
                   row(2, 3, 13))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering < ? AND clustering > ?", 2, 3, 5)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 2),
                   row(2, 3, 13))

        # test multi-column slices
        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering) > (?)", 3, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 3),
                   row(3, 0, 15),
                   row(3, 1, 16),
                   row(3, 2, 17))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering) < (?)", 3, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 3),
                   row(3, 1, 16),
                   row(3, 2, 17))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering) >= (?) AND (clustering) <= (?)", 3, 0, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 3),
                   row(3, 2, 17))

        # Test invalid queries
        assertInvalidMessage(cql, table, "Range deletions are not supported for specific columns",
                             "DELETE value FROM %s WHERE partitionKey = ? AND clustering >= ?", 2, 1)
        # Scylla and Cassandra give different errors in this case - Cassandra
        # says "Range deletions are not supported for specific columns" and
        # Scylla says "Primary key column 'clustering' must be specified in
        # order to modify column 'value'
        assertInvalid(cql, table,
                             "DELETE value FROM %s WHERE partitionKey = ?", 2)

@pytest.mark.xfail(reason="#13250")
@pytest.mark.parametrize("forceFlush", [False, True])
def testDeleteWithRangeAndTwoClusteringColumns(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, clustering_2 int, value int, primary key (partitionKey, clustering_1, clustering_2))") as table:
        value = 0
        for partitionKey in range(5):
            for clustering1 in range(5):
                for clustering2 in range(5):
                    execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (?, ?, ?, ?)",
                            partitionKey, clustering1, clustering2, value)
                    value += 1
        if forceFlush:
            flush(cql, table)

        # test unspecified second clustering column
        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ?", 0, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 < ?", 0, 2),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 0, 2, 2),
                   row(0, 0, 3, 3),
                   row(0, 0, 4, 4))

        # test delete partition
        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ?", 1)
        if forceFlush:
            flush(cql, table)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 1))

        # test slices on the second clustering column

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 < ?", 0, 0, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 < ?", 0, 2),
                   row(0, 0, 2, 2),
                   row(0, 0, 3, 3),
                   row(0, 0, 4, 4))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 <= ?", 0, 0, 3)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 < ?", 0, 2),
                   row(0, 0, 4, 4))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 = ? AND clustering_2 > ? ", 0, 2, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND  clustering_1 = ?", 0, 2),
                   row(0, 2, 0, 10),
                   row(0, 2, 1, 11),
                   row(0, 2, 2, 12))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 = ? AND clustering_2 >= ? ", 0, 2, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND  clustering_1 = ?", 0, 2),
                   row(0, 2, 0, 10))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 = ? AND clustering_2 > ? AND clustering_2 < ? ",
                0, 3, 1, 4)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND  clustering_1 = ?", 0, 3),
                   row(0, 3, 0, 15),
                   row(0, 3, 1, 16),
                   row(0, 3, 4, 19))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 = ? AND clustering_2 > ? AND clustering_2 < ? ",
                0, 3, 4, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND  clustering_1 = ?", 0, 3),
                   row(0, 3, 0, 15),
                   row(0, 3, 1, 16),
                   row(0, 3, 4, 19))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 = ? AND clustering_2 >= ? AND clustering_2 <= ? ",
                0, 3, 1, 4)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND  clustering_1 = ?", 0, 3),
                   row(0, 3, 0, 15))

        # test slices on the first clustering column

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 >= ?", 0, 4)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 0),
                   row(0, 0, 4, 4),
                   row(0, 2, 0, 10),
                   row(0, 3, 0, 15))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND  clustering_1 > ?", 0, 3)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 0),
                   row(0, 0, 4, 4),
                   row(0, 2, 0, 10),
                   row(0, 3, 0, 15))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 < ?", 0, 3)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 0),
                   row(0, 3, 0, 15))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 >= ? AND clustering_1 < ?", 2, 0, 3)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 2),
                   row(2, 3, 0, 65),
                   row(2, 3, 1, 66),
                   row(2, 3, 2, 67),
                   row(2, 3, 3, 68),
                   row(2, 3, 4, 69),
                   row(2, 4, 0, 70),
                   row(2, 4, 1, 71),
                   row(2, 4, 2, 72),
                   row(2, 4, 3, 73),
                   row(2, 4, 4, 74))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 > ? AND clustering_1 <= ?", 2, 3, 5)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 2),
                   row(2, 3, 0, 65),
                   row(2, 3, 1, 66),
                   row(2, 3, 2, 67),
                   row(2, 3, 3, 68),
                   row(2, 3, 4, 69))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 < ? AND clustering_1 > ?", 2, 3, 5)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 2),
                   row(2, 3, 0, 65),
                   row(2, 3, 1, 66),
                   row(2, 3, 2, 67),
                   row(2, 3, 3, 68),
                   row(2, 3, 4, 69))

        # test multi-column slices
        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) > (?, ?)", 2, 3, 3)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 2),
                   row(2, 3, 0, 65),
                   row(2, 3, 1, 66),
                   row(2, 3, 2, 67),
                   row(2, 3, 3, 68))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) < (?, ?)", 2, 3, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 2),
                   row(2, 3, 1, 66),
                   row(2, 3, 2, 67),
                   row(2, 3, 3, 68))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) >= (?, ?) AND (clustering_1) <= (?)", 2, 3, 2, 4)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 2),
                   row(2, 3, 1, 66))

        # Test with a mix of single column and multi-column restrictions
        # Reproduces #13250:
        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND (clustering_2) < (?)", 3, 0, 3)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ?", 3, 0),
                   row(3, 0, 3, 78),
                   row(3, 0, 4, 79))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND (clustering_2) >= (?)", 3, 0, 1, 3)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 3, 0, 1),
                   row(3, 1, 0, 80),
                   row(3, 1, 1, 81),
                   row(3, 1, 2, 82))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?)) AND clustering_2 < ?", 3, 0, 1, 1)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 3, 0, 1),
                   row(3, 1, 1, 81),
                   row(3, 1, 2, 82))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1) = (?) AND clustering_2 >= ?", 3, 1, 2)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 3, 0, 1),
                   row(3, 1, 1, 81))

        # Test invalid queries
        assertInvalidMessage(cql, table, "Range deletions are not supported for specific columns",
                             "DELETE value FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) >= (?, ?)", 2, 3, 1)
        assertInvalidMessage(cql, table, "Range deletions are not supported for specific columns",
                             "DELETE value FROM %s WHERE partitionKey = ? AND clustering_1 >= ?", 2, 3)
        # Different error messages in Scylla and Cassandra
        assertInvalid(cql, table,
                             "DELETE value FROM %s WHERE partitionKey = ? AND clustering_1 = ?", 2, 3)
        assertInvalid(cql, table,
                             "DELETE value FROM %s WHERE partitionKey = ?", 2)

@pytest.mark.parametrize("forceFlush", [False, True])
def testDeleteWithAStaticColumn(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, clustering_2 int, value int, staticValue text static, primary key (partitionKey, clustering_1, clustering_2))") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value, staticValue) VALUES (0, 0, 0, 0, 'A')")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value, staticValue) VALUES (1, 0, 0, 6, 'B')")
        if forceFlush:
            flush(cql, table)

        execute(cql, table, "DELETE staticValue FROM %s WHERE partitionKey = ?", 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT DISTINCT staticValue FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                   row(None), row("B"))

        execute(cql, table, "DELETE staticValue, value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                1, 0, 0)
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1, 0, 0, null, null),
                   row(0, 0, 0, null, 0),
                   row(0, 0, 1, null, 1))

        assertInvalidMessage(cql, table, "Invalid restrictions on clustering columns since the DELETE statement modifies only static columns",
                             "DELETE staticValue FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                             0, 0, 1)

        assertInvalidMessage(cql, table, "Invalid restrictions on clustering columns since the DELETE statement modifies only static columns",
                             "DELETE staticValue FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) >= (?, ?)",
                             0, 0, 1)


@pytest.mark.parametrize("forceFlush", [False, True])
def testDeleteWithSecondaryIndices(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, value int, values set<int>, primary key (partitionKey, clustering_1))") as table:
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

        # Reproduces #12474:
        assertInvalidMessage(cql, table, "where clause",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND value = ?", 3, 3, 3)
        # Different error message returned, changed to assertInvalid instead of assertInvalidMessage 
        assertInvalid(cql, table,
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND values CONTAINS ?", 3, 3, 3)
        assertInvalidMessage(cql, table, "where clause",
                             "DELETE FROM %s WHERE partitionKey = ? AND value = ?", 3, 3)
        # Different error message returned, changed to assertInvalid instead of assertInvalidMessage
        assertInvalid(cql, table,
                             "DELETE FROM %s WHERE partitionKey = ? AND values CONTAINS ?", 3, 3)
        # Different error message returned, changed to assertInvalid instead of assertInvalidMessage
        assertInvalid(cql, table,
                             "DELETE FROM %s WHERE clustering_1 = ?", 3)
        # Reproduces #12474:
        # Different error message returned, changed to assertInvalid instead of assertInvalidMessage
        assertInvalid(cql, table,
                             "DELETE FROM %s WHERE value = ?", 3)
        # Different error messages in Scylla and Cassandra
        assertInvalid(cql, table,
                             "DELETE FROM %s WHERE values CONTAINS ?", 3)

def testDeleteWithOnlyPK(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, v int, primary key (k, v)) WITH gc_grace_seconds=1") as table:
        # This is a regression test for CASSANDRA-11102
        execute(cql, table, "INSERT INTO %s(k, v) VALUES (?, ?)", 1, 2)

        execute(cql, table, "DELETE FROM %s WHERE k = ? AND v = ?", 1, 2)
        execute(cql, table, "INSERT INTO %s(k, v) VALUES (?, ?)", 2, 3)

        time.sleep(0.5)

        execute(cql, table, "DELETE FROM %s WHERE k = ? AND v = ?", 2, 3)
        execute(cql, table, "INSERT INTO %s(k, v) VALUES (?, ?)", 1, 2)

        time.sleep(0.5)

        flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s"), row(1, 2))

        time.sleep(1.0)
        compact(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s"), row(1, 2))

def testDeleteColumnNoClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, v int) WITH gc_grace_seconds=0") as table:
        # This is a regression test for CASSANDRA-11068 (and ultimately another test for CASSANDRA-11102)
        # Creates a table without clustering, insert a row (with a column) and only remove the column.
        # We should still have a row (with a null column value) even post-compaction.

        execute(cql, table, "INSERT INTO %s(k, v) VALUES (?, ?)", 0, 0)
        execute(cql, table, "DELETE v FROM %s WHERE k=?", 0)

        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, null))

        flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, null))

        compact(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, null))

def testDeleteAndReverseQueries(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, i int, primary key (k, i))") as table:
        # This test insert rows in one sstable and a range tombstone covering some of those rows in another, and it
        # validates we correctly get only the non-removed rows when doing reverse queries.
        for i in range(10):
            execute(cql, table, "INSERT INTO %s(k, i) values (?, ?)", "a", i)

        flush(cql, table)

        execute(cql, table, "DELETE FROM %s WHERE k = ? AND i >= ? AND i <= ?", "a", 2, 7)

        assertRows(execute(cql, table, "SELECT i FROM %s WHERE k = ? ORDER BY i DESC", "a"),
            row(9), row(8), row(1), row(0)
        )

        flush(cql, table)

        assertRows(execute(cql, table, "SELECT i FROM %s WHERE k = ? ORDER BY i DESC", "a"),
            row(9), row(8), row(1), row(0)
        )

def testDeleteWithEmptyRestrictionValue(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk blob, c blob, v blob, primary key (pk, c))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"", b"1")
        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');")

        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"", b"1")
        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));")

        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"2", b"2")

        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('')")

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(b"foo123", b"", b"1"))

        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('')")

        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"2", b"2")

        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('')")

        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"2", b"2")

        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('')")
        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('')")

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(b"foo123", b"1", b"1"),
                   row(b"foo123", b"2", b"2"))

def testDeleteWithMultipleClusteringColumnsAndEmptyRestrictionValue(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk blob, c1 blob, c2 blob, v blob, primary key (pk, c1, c2))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"", b"1", b"1")
        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('');")

        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"", b"1", b"1")
        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1');")

        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"", b"1", b"0")
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"2", b"3")

        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('')")

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(b"foo123", b"", b"1", b"0"))

        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 >= textAsBlob('')")

        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"2", b"3")

        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('')")

        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"2", b"3")

        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 <= textAsBlob('')")
        execute(cql, table, "DELETE FROM %s WHERE pk = textAsBlob('foo123') AND c1 < textAsBlob('')")

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(b"foo123", b"1", b"1", b"1"),
                   row(b"foo123", b"1", b"2", b"3"))

# Test for CASSANDRA-12829
def testDeleteWithEmptyInRestriction(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?,?,?)", 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?,?,?)", 1, 2, 2)
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?,?,?)", 1, 3, 3)

        execute(cql, table, "DELETE FROM %s WHERE a IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b = 1;")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b IN ();")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3))

    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, s int static, primary key ((a,b),c))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)", 1, 1, 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)", 1, 1, 2, 2, 1)
        execute(cql, table, "INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)", 1, 1, 3, 3, 1)

        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b IN () AND c IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b = 1 AND c IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c = 1;")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1, 1, 1, 1, 1),
                   row(1, 1, 2, 1, 2),
                   row(1, 1, 3, 1, 3))

    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, primary key ((a,b),c,d))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)", 1, 1, 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)", 1, 1, 1, 2, 2)
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)", 1, 1, 1, 3, 3)
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)", 1, 1, 1, 4, 4)

        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN () AND d IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b IN () AND c IN () AND d IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c IN () AND d IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c IN () AND d = 1;")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c = 1 AND d = 1;")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c = 1 AND d IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b = 1")
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1, 1, 1, 1, 1),
                   row(1, 1, 1, 2, 2),
                   row(1, 1, 1, 3, 3),
                   row(1, 1, 1, 4, 4))

# Test for CASSANDRA-13152
def testThatDeletesWithEmptyInRestrictionDoNotCreateMutations(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b))") as table:
        execute(cql, table, "DELETE FROM %s WHERE a IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b = 1;")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b IN ();")

        # TODO: think if there's a way for us to test this via REST API
        #assertTrue("The memtable should be empty but is not", isMemtableEmpty())

    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, s int static, primary key ((a, b), c))") as table:
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b IN () AND c IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b = 1 AND c IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c = 1;")

        #assertTrue("The memtable should be empty but is not", isMemtableEmpty())

    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, primary key ((a, b), c, d))") as table:
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1 AND c IN () AND d IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b IN () AND c IN () AND d IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c IN () AND d IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c IN () AND d = 1;")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c = 1 AND d = 1;")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b IN () AND c = 1 AND d IN ();")
        execute(cql, table, "DELETE FROM %s WHERE a IN () AND b = 1")

        #assertTrue("The memtable should be empty but is not", isMemtableEmpty())

def testQueryingOnRangeTombstoneBoundForward(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, i int, primary key (k, i))") as table:
        execute(cql, table, "INSERT INTO %s (k, i) VALUES (?, ?)", "a", 0)

        execute(cql, table, "DELETE FROM %s WHERE k = ? AND i > ? AND i <= ?", "a", 0, 1)
        execute(cql, table, "DELETE FROM %s WHERE k = ? AND i > ?", "a", 1)

        flush(cql, table)

        assertEmpty(execute(cql, table, "SELECT i FROM %s WHERE k = ? AND i = ?", "a", 1))

def testQueryingOnRangeTombstoneBoundReverse(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, i int, primary key (k, i))") as table:
        execute(cql, table, "INSERT INTO %s (k, i) VALUES (?, ?)", "a", 0)

        execute(cql, table, "DELETE FROM %s WHERE k = ? AND i > ? AND i <= ?", "a", 0, 1)
        execute(cql, table, "DELETE FROM %s WHERE k = ? AND i > ?", "a", 1)

        flush(cql, table)

        assertRows(execute(cql, table, "SELECT i FROM %s WHERE k = ? AND i <= ? ORDER BY i DESC", "a", 1), row(0))

def testReverseQueryWithRangeTombstoneOnMultipleBlocks(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, i int, v text, primary key (k, i))") as table:
        longText = 'a'*1200

        for i in range(10):
            execute(cql, table, "INSERT INTO %s(k, i, v) VALUES (?, ?, ?) USING TIMESTAMP 3", "a", i*2, longText)

        execute(cql, table, "DELETE FROM %s USING TIMESTAMP 1 WHERE k = ? AND i >= ? AND i <= ?", "a", 12, 16)

        flush(cql, table)

        execute(cql, table, "INSERT INTO %s(k, i, v) VALUES (?, ?, ?) USING TIMESTAMP 0", "a", 3, longText)
        execute(cql, table, "INSERT INTO %s(k, i, v) VALUES (?, ?, ?) USING TIMESTAMP 3", "a", 11, longText)
        execute(cql, table, "INSERT INTO %s(k, i, v) VALUES (?, ?, ?) USING TIMESTAMP 0", "a", 15, longText)
        execute(cql, table, "INSERT INTO %s(k, i, v) VALUES (?, ?, ?) USING TIMESTAMP 0", "a", 17, longText)

        flush(cql, table)

        assertRows(execute(cql, table, "SELECT i FROM %s WHERE k = ? ORDER BY i DESC", "a"),
                   row(18),
                   row(17),
                   row(16),
                   row(14),
                   row(12),
                   row(11),
                   row(10),
                   row(8),
                   row(6),
                   row(4),
                   row(3),
                   row(2),
                   row(0))

# Test for CASSANDRA-13305
def testWithEmptyRange(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, a int, b int, primary key (k, a, b))") as table:
        # Both of the following should be doing nothing, but before CASSANDRA-13305 this inserted broken ranges. We do it twice
        # and the follow-up delete mainly as a way to show the bug as the combination of this will trigger an assertion
        # in RangeTombstoneList pre-CASSANDRA-13305 showing that something wrong happened.
        execute(cql, table, "DELETE FROM %s WHERE k = ? AND a >= ? AND a < ?", "a", 1, 1)
        execute(cql, table, "DELETE FROM %s WHERE k = ? AND a >= ? AND a < ?", "a", 1, 1)

        execute(cql, table, "DELETE FROM %s WHERE k = ? AND a >= ? AND a < ?", "a", 0, 2)

def testStaticColumnDeletionWithMultipleStaticColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck int, s1 int static, s2 int static, v int, primary key (pk, ck))") as table:
        execute(cql, table, "INSERT INTO %s (pk, s1, s2) VALUES (1, 1, 1) USING TIMESTAMP 1000")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, s1) VALUES (1, 2) USING TIMESTAMP 2000")
        flush(cql, table)
        execute(cql, table, "DELETE s1 FROM %s USING TIMESTAMP 3000 WHERE pk = 1")
        flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk=1"), row(1, null, null, 1, null))
        assertRows(execute(cql, table, "SELECT pk, s1, s2 FROM %s WHERE pk=1"), row(1, null, 1))
        assertRows(execute(cql, table, "SELECT s1, s2 FROM %s WHERE pk=1"), row(null, 1))
        assertRows(execute(cql, table, "SELECT pk, s1 FROM %s WHERE pk=1"), row(1, null))
        assertRows(execute(cql, table, "SELECT s1 FROM %s WHERE pk=1"), row(null))
        assertRows(execute(cql, table, "SELECT pk, s2 FROM %s WHERE pk=1"), row(1, 1))
        assertRows(execute(cql, table, "SELECT s2 FROM %s WHERE pk=1"), row(1))
        assertRows(execute(cql, table, "SELECT pk, ck FROM %s WHERE pk=1"), row(1, null))
        assertRows(execute(cql, table, "SELECT ck FROM %s WHERE pk=1"), row(null))
        assertRows(execute(cql, table, "SELECT DISTINCT pk, s1, s2 FROM %s WHERE pk=1"), row(1, null, 1))
        assertRows(execute(cql, table, "SELECT DISTINCT s1, s2 FROM %s WHERE pk=1"), row(null, 1))
        assertRows(execute(cql, table, "SELECT DISTINCT pk, s1 FROM %s WHERE pk=1"), row(1, null))
        assertRows(execute(cql, table, "SELECT DISTINCT pk, s2 FROM %s WHERE pk=1"), row(1, 1))
        assertRows(execute(cql, table, "SELECT DISTINCT s1 FROM %s WHERE pk=1"), row(null))
        assertRows(execute(cql, table, "SELECT DISTINCT s2 FROM %s WHERE pk=1"), row(1))

def testStaticColumnDeletionWithMultipleStaticColumnsAndRegularColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck int, s1 int static, s2 int static, v int, primary key (pk, ck))") as table:
        execute(cql, table, "INSERT INTO %s (pk, ck, v, s2) VALUES (1, 1, 1, 1) USING TIMESTAMP 1000")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, s1) VALUES (1, 2) USING TIMESTAMP 2000")
        flush(cql, table)
        execute(cql, table, "DELETE s1 FROM %s USING TIMESTAMP 3000 WHERE pk = 1")
        flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk=1"), row(1, 1, null, 1, 1))
        assertRows(execute(cql, table, "SELECT s1, s2 FROM %s WHERE pk=1"), row(null, 1))
        assertRows(execute(cql, table, "SELECT s1 FROM %s WHERE pk=1"), row(null))
        assertRows(execute(cql, table, "SELECT DISTINCT s1, s2 FROM %s WHERE pk=1"), row(null, 1))
        assertRows(execute(cql, table, "SELECT DISTINCT s1 FROM %s WHERE pk=1"), row(null))
