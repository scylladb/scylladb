# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 2a4cd36475de3eb47207cd88d2d472b876c6816d
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

from cassandra.query import BatchStatement, BatchType
from cassandra.protocol import InvalidRequest

# Migrated from cql_tests.py:TestCQL.test_select_distinct()
def testSelectDistinct(cql, test_keyspace):
    # Test a regular (CQL3) table.
    with create_table(cql, test_keyspace, "(pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY ((pk0, pk1), ck0))") as table:
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 0, 0)", i, i)
            execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 1, 1)", i, i)

        assertRows(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s LIMIT 1"),
                   row(0, 0))

        assertRows(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s LIMIT 3"),
                   row(0, 0),
                   row(2, 2),
                   row(1, 1))

        # Test selection validation.
        assertInvalidMessage(cql, table, "queries must request all the partition key columns", "SELECT DISTINCT pk0 FROM %s")
        assertInvalidMessage(cql, table, "queries must only request partition key columns", "SELECT DISTINCT pk0, pk1, ck0 FROM %s")

# Migrated from cql_tests.py:TestCQL.test_select_distinct_with_deletions()
def testSelectDistinctWithDeletions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, c int, v int)") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, i, i)

        rows = getRows(execute(cql, table, "SELECT DISTINCT k FROM %s"))
        assert len(rows) == 10
        key_to_delete = rows[3][0]

        execute(cql, table, "DELETE FROM %s WHERE k=?", key_to_delete)

        rows = getRows(execute(cql, table, "SELECT DISTINCT k FROM %s"))
        assert len(rows) == 9

        rows = getRows(execute(cql, table, "SELECT DISTINCT k FROM %s LIMIT 5"))
        assert len(rows) == 5

        rows = getRows(execute(cql, table, "SELECT DISTINCT k FROM %s"))
        assert len(rows) == 9

# Reproduces issue #10354
@pytest.mark.xfail(reason="#10354 - we forgot to allow SELECT DISTINCT filtering on static column")
def testSelectDistinctWithWhereClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, a int, b int, PRIMARY KEY (k, a))") as table:
        execute(cql, table, "CREATE INDEX ON %s (b)")

        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i, i)
            execute(cql, table, "INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i * 10, i * 10)

        # In issue #10354, Scylla forgot the "and/or static columns" part.
        distinctQueryErrorMsg = "SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns."
        assertInvalidMessage(cql, table, distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE a >= 80 ALLOW FILTERING")

        assertInvalidMessage(cql, table, distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE k IN (1, 2, 3) AND a = 10")

        assertInvalidMessage(cql, table, distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE b = 5")

        assertRows(execute(cql, table, "SELECT DISTINCT k FROM %s WHERE k = 1"),
                   row(1))
        assertRows(execute(cql, table, "SELECT DISTINCT k FROM %s WHERE k IN (5, 6, 7)"),
                   row(5),
                   row(6),
                   row(7))

    # With static columns
    with create_table(cql, test_keyspace, "(k int, a int, s int static, b int, PRIMARY KEY (k, a))") as table:
        execute(cql, table, "CREATE INDEX ON %s (b)")
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, a, b, s) VALUES (?, ?, ?, ?)", i, i, i, i)
            execute(cql, table, "INSERT INTO %s (k, a, b, s) VALUES (?, ?, ?, ?)", i, i * 10, i * 10, i * 10)

        assertRows(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k = 5"),
                   row(50))
        assertRows(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k IN (5, 6, 7)"),
                   row(50),
                   row(60),
                   row(70))

# Reproduces issue #10354
@pytest.mark.xfail(reason="#10354 - we forgot to allow SELECT DISTINCT filtering on static column")
def testSelectDistinctWithWhereClauseOnStaticColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, a int, s int static, s1 int static, b int, PRIMARY KEY (k, a))") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", i, i, i, i, i)
            execute(cql, table, "INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", i, i * 10, i * 10, i * 10, i * 10)

        execute(cql, table, "INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", 2, 10, 10, 10, 10)

        for _ in before_and_after_flush(cql, table):
            assertRows(execute(cql, table, "SELECT DISTINCT k, s, s1 FROM %s WHERE s = 90 AND s1 = 90 ALLOW FILTERING"),
                       row(9, 90, 90))

            assertRows(execute(cql, table, "SELECT DISTINCT k, s, s1 FROM %s WHERE s = 90 AND s1 = 90 ALLOW FILTERING"),
                       row(9, 90, 90))

            assertRows(execute(cql, table, "SELECT DISTINCT k, s, s1 FROM %s WHERE s = 10 AND s1 = 10 ALLOW FILTERING"),
                       row(1, 10, 10),
                       row(2, 10, 10))

            assertRows(execute(cql, table, "SELECT DISTINCT k, s, s1 FROM %s WHERE k = 1 AND s = 10 AND s1 = 10 ALLOW FILTERING"),
                       row(1, 10, 10))

def _testSelectDistinctWithPaging(cql, table):
    for pageSize in range(1,7):
        # Range query
        assert_rows(execute_with_paging(cql, table, "SELECT DISTINCT a, s FROM %s", pageSize),
                          row(1, 1),
                          row(0, 0),
                          row(2, 2),
                          row(4, 4),
                          row(3, 3))

        assert_rows(execute_with_paging(cql, table, "SELECT DISTINCT a, s FROM %s LIMIT 3", pageSize),
                          row(1, 1),
                          row(0, 0),
                          row(2, 2))

        assert_rows(execute_with_paging(cql, table, "SELECT DISTINCT a, s FROM %s WHERE s >= 2 ALLOW FILTERING", pageSize),
                          row(2, 2),
                          row(4, 4),
                          row(3, 3))

        # Multi partition query
        assert_rows(execute_with_paging(cql, table, "SELECT DISTINCT a, s FROM %s WHERE a IN (1, 2, 3, 4);", pageSize),
                          row(1, 1),
                          row(2, 2),
                          row(3, 3),
                          row(4, 4))

        assert_rows(execute_with_paging(cql, table, "SELECT DISTINCT a, s FROM %s WHERE a IN (1, 2, 3, 4) LIMIT 3;", pageSize),
                          row(1, 1),
                          row(2, 2),
                          row(3, 3))

        assert_rows(execute_with_paging(cql, table, "SELECT DISTINCT a, s FROM %s WHERE a IN (1, 2, 3, 4) AND s >= 2 ALLOW FILTERING;", pageSize),
                          row(2, 2),
                          row(3, 3),
                          row(4, 4))

# Reproduces issue #10354
@pytest.mark.xfail(reason="#10354 - we forgot to allow SELECT DISTINCT filtering on static column")
def testSelectDistinctWithStaticColumnsAndPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, s int static, c int, d int, PRIMARY KEY (a, b))") as table:
        # Test with only static data
        for i in range(5):
            execute(cql, table, "INSERT INTO %s (a, s) VALUES (?, ?)", i, i)

        _testSelectDistinctWithPaging(cql, table)

        # Test with a mix of partition with rows and partitions without rows
        for i in range(5):
            if i % 2 == 0:
                for j in range(1,4):
                    execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, j, i + j)

        _testSelectDistinctWithPaging(cql, table)

        # Test with all partition with rows
        for i in range(5):
            for j in range(1,4):
                execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, j, i + j)

        _testSelectDistinctWithPaging(cql, table)
