# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 8d91b469afd3fcafef7ef85c10c8acc11703ba2d
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

# Test limit queries on a sparse table,
# migrated from cql_tests.py:TestCQL.limit_sparse_test()
def testSparseTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid int, url text, day int, month text, year int, PRIMARY KEY (userid, url))") as table:
        for i in range(100):
            for tld in ["com", "org", "net"]:
                execute(cql, table, "INSERT INTO %s (userid, url, day, month, year) VALUES (?, ?, 1, 'jan', 2012)", i, f"http://foo.{tld}")
        assertRowCount(execute(cql, table, "SELECT * FROM %s LIMIT 4"), 4)

@pytest.mark.xfail(reason="issues #9879, #15099, #15109")
def testPerPartitionLimit(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        for i in range(5):
            for j in range(5):
                execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", i, j, j)

        assertInvalidMessage(cql, table, "LIMIT must be strictly positive",
                             "SELECT * FROM %s PER PARTITION LIMIT ?", 0)
        assertInvalidMessage(cql, table, "LIMIT must be strictly positive",
                             "SELECT * FROM %s PER PARTITION LIMIT ?", -1)

        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s PER PARTITION LIMIT ?", 2),
                                row(0, 0, 0),
                                row(0, 1, 1),
                                row(1, 0, 0),
                                row(1, 1, 1),
                                row(2, 0, 0),
                                row(2, 1, 1),
                                row(3, 0, 0),
                                row(3, 1, 1),
                                row(4, 0, 0),
                                row(4, 1, 1))

        # Combined Per Partition and "global" limit
        assertRowCount(execute(cql, table, "SELECT * FROM %s PER PARTITION LIMIT ? LIMIT ?", 2, 6),
                       6)

        # odd amount of results
        assertRowCount(execute(cql, table, "SELECT * FROM %s PER PARTITION LIMIT ? LIMIT ?", 2, 5),
                       5)

        # IN query
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (2,3) PER PARTITION LIMIT ?", 2),
                   row(2, 0, 0),
                   row(2, 1, 1),
                   row(3, 0, 0),
                   row(3, 1, 1))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (2,3) PER PARTITION LIMIT ? LIMIT 3", 2),
                   row(2, 0, 0),
                   row(2, 1, 1),
                   row(3, 0, 0))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1,2,3) PER PARTITION LIMIT ? LIMIT 3", 2),
                   row(1, 0, 0),
                   row(1, 1, 1),
                   row(2, 0, 0))

        # with restricted partition key
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? PER PARTITION LIMIT ?", 2, 3),
                   row(2, 0, 0),
                   row(2, 1, 1),
                   row(2, 2, 2))

        # with ordering
        # Reproduces #15099:
        assertRows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE a IN (3, 2) ORDER BY b DESC PER PARTITION LIMIT ?", 2),
                   row(2, 4, 4),
                   row(3, 4, 4),
                   row(2, 3, 3),
                   row(3, 3, 3))

        # Reproduces #15099:
        assertRows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE a IN (3, 2) ORDER BY b DESC PER PARTITION LIMIT ? LIMIT ?", 3, 4),
                   row(2, 4, 4),
                   row(3, 4, 4),
                   row(2, 3, 3),
                   row(3, 3, 3))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? ORDER BY b DESC PER PARTITION LIMIT ?", 2, 3),
                   row(2, 4, 4),
                   row(2, 3, 3),
                   row(2, 2, 2))

        # with filtering
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b > ? PER PARTITION LIMIT ? ALLOW FILTERING", 2, 0, 2),
                   row(2, 1, 1),
                   row(2, 2, 2))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b > ? ORDER BY b DESC PER PARTITION LIMIT ? ALLOW FILTERING", 2, 2, 2),
                   row(2, 4, 4),
                   row(2, 3, 3))

        # Reproduces #15109:
        assertInvalidMessage(cql, table, "PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries",
                             "SELECT DISTINCT a FROM %s PER PARTITION LIMIT ?", 3)
        # Reproduces #15109:
        assertInvalidMessage(cql, table, "PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries",
                             "SELECT DISTINCT a FROM %s PER PARTITION LIMIT ? LIMIT ?", 3, 4)
        # Reproduces #9879:
        assertInvalidMessage(cql, table, "PER PARTITION LIMIT is not allowed with aggregate queries.",
                             "SELECT COUNT(*) FROM %s PER PARTITION LIMIT ?", 3)

@pytest.mark.xfail(reason="issues #9879, #15109")
def testPerPartitionLimitWithStaticDataAndPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, s int static, c int, PRIMARY KEY (a, b))") as table:
        for i in range(5):
            execute(cql, table, "INSERT INTO %s (a, s) VALUES (?, ?)", i, i)

        for pageSize in range(1, 8):
            assert_rows_ignoring_order(execute_with_paging(cql, table, "SELECT * FROM %s PER PARTITION LIMIT 2", pageSize),
                          row(0, None, 0, None),
                          row(1, None, 1, None),
                          row(2, None, 2, None),
                          row(3, None, 3, None),
                          row(4, None, 4, None))

            # Combined Per Partition and "global" limit
            # Note that which partitions get returned depend on the token order...
            assert_rows_ignoring_order(execute_with_paging(cql, table, "SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 4", pageSize),
                          row(0, None, 0, None),
                          row(1, None, 1, None),
                          row(2, None, 2, None),
                          row(4, None, 4, None))

            # odd amount of results
            # Note that which partitions get returned depend on the token order...
            assert_rows_ignoring_order(execute_with_paging(cql, table, "SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 3", pageSize),
                          row(0, None, 0, None),
                          row(1, None, 1, None),
                          row(2, None, 2, None))

            # IN query
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a IN (1,3,4) PER PARTITION LIMIT 2", pageSize),
                          row(1, None, 1, None),
                          row(3, None, 3, None),
                          row(4, None, 4, None))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a IN (1,3,4) PER PARTITION LIMIT 2 LIMIT 2",
                                               pageSize),
                          row(1, None, 1, None),
                          row(3, None, 3, None))

            # with restricted partition key
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 2 PER PARTITION LIMIT 3", pageSize),
                          row(2, None, 2, None))

            # with ordering
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 2 ORDER BY b DESC PER PARTITION LIMIT 3",
                                               pageSize),
                          row(2, None, 2, None))

            # with filtering
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 2 AND s > 0 PER PARTITION LIMIT 2 ALLOW FILTERING",
                                               pageSize),
                          row(2, None, 2, None))

        for i in range(5):
            if i != 1:
                for j in range(5):
                    execute(cql, table, "INSERT INTO %s (a, b, s, c) VALUES (?, ?, ?, ?)", i, j, i, j)

        assertInvalidMessage(cql, table, "LIMIT must be strictly positive",
                             "SELECT * FROM %s PER PARTITION LIMIT ?", 0)
        assertInvalidMessage(cql, table, "LIMIT must be strictly positive",
                             "SELECT * FROM %s PER PARTITION LIMIT ?", -1)

        for pageSize in range(1,8):
            assert_rows_ignoring_order(execute_with_paging(cql, table, "SELECT * FROM %s PER PARTITION LIMIT 2", pageSize),
                          row(0, 0, 0, 0),
                          row(0, 1, 0, 1),
                          row(1, None, 1, None),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1),
                          row(3, 0, 3, 0),
                          row(3, 1, 3, 1),
                          row(4, 0, 4, 0),
                          row(4, 1, 4, 1))

            # Combined Per Partition and "global" limit
            assert_rows_ignoring_order(execute_with_paging(cql, table, "SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 4", pageSize),
                          row(0, 0, 0, 0),
                          row(0, 1, 0, 1),
                          row(1, None, 1, None),
                          row(2, 0, 2, 0))

            # odd amount of results
            assert_rows_ignoring_order(execute_with_paging(cql, table, "SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 5", pageSize),
                          row(0, 0, 0, 0),
                          row(0, 1, 0, 1),
                          row(1, None, 1, None),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1))

            # IN query
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a IN (2,3) PER PARTITION LIMIT 2", pageSize),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1),
                          row(3, 0, 3, 0),
                          row(3, 1, 3, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a IN (2,3) PER PARTITION LIMIT 2 LIMIT 3",
                                               pageSize),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1),
                          row(3, 0, 3, 0))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a IN (1,2,3) PER PARTITION LIMIT 2 LIMIT 3",
                                               pageSize),
                          row(1, null, 1, null),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1))

            # with restricted partition key
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 2 PER PARTITION LIMIT 3", pageSize),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1),
                          row(2, 2, 2, 2))

            # with ordering
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 2 ORDER BY b DESC PER PARTITION LIMIT 3",
                                               pageSize),
                          row(2, 4, 2, 4),
                          row(2, 3, 2, 3),
                          row(2, 2, 2, 2))

            # with filtering
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 2 AND b > 0 PER PARTITION LIMIT 2 ALLOW FILTERING",
                                               pageSize),
                          row(2, 1, 2, 1),
                          row(2, 2, 2, 2))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 2 AND b > 2 ORDER BY b DESC PER PARTITION LIMIT 2 ALLOW FILTERING",
                                               pageSize),
                          row(2, 4, 2, 4),
                          row(2, 3, 2, 3))

        # Reproduces #15109:
        assertInvalidMessage(cql, table, "PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries",
                             "SELECT DISTINCT a FROM %s PER PARTITION LIMIT ?", 3)
        # Reproduces #15109:
        assertInvalidMessage(cql, table, "PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries",
                             "SELECT DISTINCT a FROM %s PER PARTITION LIMIT ? LIMIT ?", 3, 4)
        # Reproduces #9879:
        assertInvalidMessage(cql, table, "PER PARTITION LIMIT is not allowed with aggregate queries.",
                             "SELECT COUNT(*) FROM %s PER PARTITION LIMIT ?", 3)

def testLimitWithDeletedRowsAndStaticColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c int, v int, s int static, PRIMARY KEY (pk, c))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c, v, s) VALUES (1, -1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (pk, c, v, s) VALUES (2, -1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (pk, c, v, s) VALUES (3, -1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (pk, c, v, s) VALUES (4, -1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (pk, c, v, s) VALUES (5, -1, 1, 1)")

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                   row(1, -1, 1, 1),
                   row(2, -1, 1, 1),
                   row(3, -1, 1, 1),
                   row(4, -1, 1, 1),
                   row(5, -1, 1, 1))

        execute(cql, table, "DELETE FROM %s WHERE pk = 2")

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                   row(1, -1, 1, 1),
                   row(3, -1, 1, 1),
                   row(4, -1, 1, 1),
                   row(5, -1, 1, 1))

        # Note that which partitions get returned depend on the token order...
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s LIMIT 2"),
                   row(1, -1, 1, 1),
                   row(5, -1, 1, 1))

@pytest.mark.xfail(reason="issue #15099")
def testFilteringOnClusteringColumnsWithLimitAndStaticColumns(cql, test_keyspace):
    # With only one clustering column
    with create_table(cql, test_keyspace, "(a int, b int, s int static, c int, PRIMARY KEY (a, b))") as table:
        for i in range(4):
            execute(cql, table, "INSERT INTO %s (a, s) VALUES (?, ?)", i, i)
            for j in range(3):
                if (not ((i == 0 or i == 3) and j == 1)):
                    execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", i, j, i + j)

        for _ in before_and_after_flush(cql, table):
            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                       row(0, 0, 0, 0),
                       row(0, 2, 0, 2),
                       row(1, 0, 1, 1),
                       row(1, 1, 1, 2),
                       row(1, 2, 1, 3),
                       row(2, 0, 2, 2),
                       row(2, 1, 2, 3),
                       row(2, 2, 2, 4),
                       row(3, 0, 3, 3),
                       row(3, 2, 3, 5))

            assertRows(execute(cql, table, "SELECT * FROM %s WHERE b = 1 ALLOW FILTERING"),
                       row(1, 1, 1, 2),
                       row(2, 1, 2, 3))

            # The problem was that the static row of the partition 0 used to be only filtered in SelectStatement and was
            # by consequence counted as a row. In which case the query was returning one row less.
            assertRows(execute(cql, table, "SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING"),
                       row(1, 1, 1, 2),
                       row(2, 1, 2, 3))

            assertRows(execute(cql, table, "SELECT * FROM %s WHERE b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING"),
                       row(1, 1, 1, 2),
                       row(2, 1, 2, 3))

            # Test with paging
            for pageSize in range(1,4):
                assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 2),
                              row(2, 1, 2, 3))

                assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 2),
                              row(2, 1, 2, 3))

                assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b = 1 GROUP BY a LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 2),
                              row(2, 1, 2, 3))

                assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b >= 1 AND b <= 1 GROUP BY a LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 2),
                              row(2, 1, 2, 3))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b = 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3))

        # Reproduces #15099:
        assertRows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b = 1 ORDER BY b DESC LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3))

        # Reproduces #15099:
        assertRows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b >= 1 AND b <= 1 ORDER BY b DESC LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3))

        execute(cql, table, "SELECT * FROM %s WHERE a IN (0, 1, 2, 3)"); # Load all data in the row cache

        # Partition range queries
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3))

        # Multiple partitions queries
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (0, 1, 2) AND b = 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (0, 1, 2) AND b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3))

        # Test with paging
        for pageSize in range(1,4):
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 1, 2),
                          row(2, 1, 2, 3))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 1, 2),
                          row(2, 1, 2, 3))

    # With multiple clustering columns
    with create_table(cql, test_keyspace, "(a int, b int, c int, s int static, d int, PRIMARY KEY (a, b, c))") as table:
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (a, s) VALUES (?, ?)", i, i)
            for j in range(3):
                if (not(i == 0 and j == 1)):
                    execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, j, i + j)

        for _ in before_and_after_flush(cql, table):
            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                       row(0, 0, 0, 0, 0),
                       row(0, 2, 2, 0, 2),
                       row(1, 0, 0, 1, 1),
                       row(1, 1, 1, 1, 2),
                       row(1, 2, 2, 1, 3),
                       row(2, 0, 0, 2, 2),
                       row(2, 1, 1, 2, 3),
                       row(2, 2, 2, 2, 4))

            assertRows(execute(cql, table, "SELECT * FROM %s WHERE b = 1 ALLOW FILTERING"),
                       row(1, 1, 1, 1, 2),
                       row(2, 1, 1, 2, 3))

            assertRows(execute(cql, table, "SELECT * FROM %s WHERE b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 LIMIT 2 ALLOW FILTERING"),
                       row(1, 1, 1, 1, 2),
                       row(2, 1, 1, 2, 3))

            # Test with paging
            for pageSize in range(1,4):
                assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 1, 2),
                              row(2, 1, 1, 2, 3))

                assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 1, 2),
                              row(2, 1, 1, 2, 3))

                assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b = 1 GROUP BY a, b LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 1, 2),
                              row(2, 1, 1, 2, 3))

                assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 GROUP BY a, b LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 1, 2),
                              row(2, 1, 1, 2, 3))

        execute(cql, table, "SELECT * FROM %s WHERE a IN (0, 1, 2)"); # Load data in the row cache

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE b = 1 ALLOW FILTERING"),
                   row(1, 1, 1, 1, 2),
                   row(2, 1, 1, 2, 3))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 1, 2),
                   row(2, 1, 1, 2, 3))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 3, 4) AND b = 1 ALLOW FILTERING"),
                   row(1, 1, 1, 1, 2),
                   row(2, 1, 1, 2, 3))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 3, 4) AND b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 1, 2),
                   row(2, 1, 1, 2, 3))

        # Test with paging
        for pageSize in range(1,4):
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 1, 1, 2),
                          row(2, 1, 1, 2, 3))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 1, 1, 2),
                          row(2, 1, 1, 2, 3))

def testIndexOnRegularColumnWithPartitionWithoutRows(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c int, s int static, v int, PRIMARY KEY (pk, c))") as table:
        execute(cql, table, "CREATE INDEX ON %s (v)")

        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 1, 9, 1)
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 2, 9, 2)
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 3, 1, 9, 1)
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 4, 1, 9, 1)
        flush(cql, table)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE v = ?", 1),
                   row(1, 1, 9, 1),
                   row(3, 1, 9, 1),
                   row(4, 1, 9, 1))

        execute(cql, table, "DELETE FROM %s WHERE pk = ? and c = ?", 3, 1)

        # Test without paging
        assertRows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE v = ? LIMIT 2", 1),
                   row(1, 1, 9, 1),
                   row(4, 1, 9, 1))

        assertRows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE v = ? GROUP BY pk LIMIT 2", 1),
                   row(1, 1, 9, 1),
                   row(4, 1, 9, 1))

        # Test with paging
        for pageSize in range(1,4):
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE v = 1 LIMIT 2", pageSize),
                          row(1, 1, 9, 1),
                          row(4, 1, 9, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE v = 1 GROUP BY pk LIMIT 2", pageSize),
                          row(1, 1, 9, 1),
                          row(4, 1, 9, 1))

@pytest.mark.xfail(reason="issue #10357")
def testFilteringWithPartitionWithoutRows(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c int, s int static, v int, PRIMARY KEY (pk, c))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 1, 9, 1)
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 2, 9, 2)
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 3, 1, 9, 1)
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 4, 1, 9, 1)
        flush(cql, table)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE v = ? ALLOW FILTERING", 1),
                   row(1, 1, 9, 1),
                   row(3, 1, 9, 1),
                   row(4, 1, 9, 1))

        execute(cql, table, "DELETE FROM %s WHERE pk = ? and c = ?", 3, 1)

        # Test without paging
        assertRows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE v = ? LIMIT 2 ALLOW FILTERING", 1),
                   row(1, 1, 9, 1),
                   row(4, 1, 9, 1))

        # Reproduces #10357 (matches a row that was already deleted and only a static row was left and no v=1)
        assertRows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE pk IN ? AND v = ? LIMIT 3 ALLOW FILTERING", [1, 3, 4], 1),
                   row(1, 1, 9, 1),
                   row(4, 1, 9, 1))

        # Test with paging
        for pageSize in range(1,4):
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE v = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 9, 1),
                          row(4, 1, 9, 1))

            # Reproduces #10357 (matches a row that was already deleted and only a static row was left and no v=1)
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE pk IN (1, 3, 4) AND v = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 9, 1),
                          row(4, 1, 9, 1))
