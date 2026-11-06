# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from .porting import *

# Some simple tests to verify the behaviour of paging during
# 2i queries. We only use a single index type (CompositesIndexOnRegular)
# as the code we want to exercise here is in their abstract
# base class.
# (CompositesIndexOnRegular was a class in Cassandra's implementation of
# secondary indexes, for an index on a regular column, since replaced by
# RegularColumnIndex. It isn't relevant to Scylla, but these tests check
# paging of index queries in general.)

def executePagingQuery(cql, table, cmd, rowCount):
    # Execute an index query which should return all rows,
    # setting the fetch size < than the row count. Assert
    # that all rows are returned, so we know that paging
    # of the results was involved.
    assert len(list(execute_with_paging(cql, table, cmd, rowCount - 1))) == rowCount

def testpagingOnRegularColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      " k1 int," +
                      " v1 int," +
                      "PRIMARY KEY (k1))") as table:
        execute(cql, table, "CREATE INDEX ON %s(v1)")

        rowCount = 3
        for i in range(rowCount):
            execute(cql, table, "INSERT INTO %s (k1, v1) VALUES (?, ?)", i, 0)

        executePagingQuery(cql, table, "SELECT * FROM %s WHERE v1=0", rowCount)

def testpagingOnRegularColumnWithPartitionRestriction(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      " k1 int," +
                      " c1 int," +
                      " v1 int," +
                      "PRIMARY KEY (k1, c1))") as table:
        execute(cql, table, "CREATE INDEX ON %s(v1)")

        partitions = 3
        rowCount = 3
        for i in range(partitions):
            for j in range(rowCount):
                execute(cql, table, "INSERT INTO %s (k1, c1, v1) VALUES (?, ?, ?)", i, j, 0)

        executePagingQuery(cql, table, "SELECT * FROM %s WHERE k1=0 AND v1=0", rowCount)

def testpagingOnRegularColumnWithClusteringRestrictions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      " k1 int," +
                      " c1 int," +
                      " v1 int," +
                      "PRIMARY KEY (k1, c1))") as table:
        execute(cql, table, "CREATE INDEX ON %s(v1)")

        partitions = 3
        rowCount = 3
        for i in range(partitions):
            for j in range(rowCount):
                execute(cql, table, "INSERT INTO %s (k1, c1, v1) VALUES (?, ?, ?)", i, j, 0)

        executePagingQuery(cql, table, "SELECT * FROM %s WHERE k1=0 AND c1>=0 AND c1<=3 AND v1=0", rowCount)

def testPagingOnPartitionsWithoutRows(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck))") as table:
        execute(cql, table, "CREATE INDEX on %s(s)")

        execute(cql, table, "INSERT INTO %s (pk, s) VALUES (201, 200);")
        execute(cql, table, "INSERT INTO %s (pk, s) VALUES (202, 200);")
        execute(cql, table, "INSERT INTO %s (pk, s) VALUES (203, 200);")
        execute(cql, table, "INSERT INTO %s (pk, s) VALUES (100, 100);")

        for pageSize in range(1, 10):
            assert_rows(execute_with_paging(cql, table, "select * from %s where s = 200 and pk = 201;", pageSize),
                        row(201, None, 200, None))

            assert_rows(execute_with_paging(cql, table, "select * from %s where s = 200;", pageSize),
                        row(201, None, 200, None),
                        row(203, None, 200, None),
                        row(202, None, 200, None))

            assert_rows(execute_with_paging(cql, table, "select * from %s where s = 100;", pageSize),
                        row(100, None, 100, None))

def testPagingOnPartitionsWithoutClusteringColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, v int)") as table:
        execute(cql, table, "CREATE INDEX on %s(v)")

        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (201, 200);")
        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (202, 200);")
        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (203, 200);")
        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (100, 100);")

        for pageSize in range(1, 10):
            assert_rows(execute_with_paging(cql, table, "select * from %s where v = 200 and pk = 201;", pageSize),
                        row(201, 200))

            assert_rows(execute_with_paging(cql, table, "select * from %s where v = 200;", pageSize),
                        row(201, 200),
                        row(203, 200),
                        row(202, 200))

            assert_rows(execute_with_paging(cql, table, "select * from %s where v = 100;", pageSize),
                        row(100, 100))
