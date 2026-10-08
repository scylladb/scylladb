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

def testDynamicCompactTables(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, t int, v text, PRIMARY KEY (k, t))") as table:
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key", 1, "v11")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key", 2, "v12")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key", 3, "v13")

        flush(cql, table)

        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key", 4, "v14")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key", 5, "v15")

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                    row("key", 1, "v11"),
                    row("key", 2, "v12"),
                    row("key", 3, "v13"),
                    row("key", 4, "v14"),
                    row("key", 5, "v15"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t > ?", "key", 3),
                    row("key", 4, "v14"),
                    row("key", 5, "v15"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ?", "key", 2, 4),
                    row("key", 2, "v12"),
                    row("key", 3, "v13"))

        # Reversed queries

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? ORDER BY t DESC", "key"),
                    row("key", 5, "v15"),
                    row("key", 4, "v14"),
                    row("key", 3, "v13"),
                    row("key", 2, "v12"),
                    row("key", 1, "v11"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t > ? ORDER BY t DESC", "key", 3),
                    row("key", 5, "v15"),
                    row("key", 4, "v14"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ? ORDER BY t DESC", "key", 2, 4),
                    row("key", 3, "v13"),
                    row("key", 2, "v12"))

def testTableWithoutClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text PRIMARY KEY, v1 int, v2 text)") as table:
        execute(cql, table, "INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "first", 1, "value1")
        execute(cql, table, "INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "second", 2, "value2")
        execute(cql, table, "INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "third", 3, "value3")

        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", "first"),
                    row("first", 1, "value1"))

        assert_rows(execute(cql, table, "SELECT v2 FROM %s WHERE k = ?", "second"),
                    row("value2"))

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                    row("third", 3, "value3"),
                    row("second", 2, "value2"),
                    row("first", 1, "value1"))

def testTableWithOneClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, t int, v1 text, v2 text, PRIMARY KEY (k, t))") as table:
        execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 1, "v11", "v21")
        execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 2, "v12", "v22")
        execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 3, "v13", "v23")

        flush(cql, table)

        execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 4, "v14", "v24")
        execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 5, "v15", "v25")

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                    row("key", 1, "v11", "v21"),
                    row("key", 2, "v12", "v22"),
                    row("key", 3, "v13", "v23"),
                    row("key", 4, "v14", "v24"),
                    row("key", 5, "v15", "v25"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t > ?", "key", 3),
                    row("key", 4, "v14", "v24"),
                    row("key", 5, "v15", "v25"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ?", "key", 2, 4),
                    row("key", 2, "v12", "v22"),
                    row("key", 3, "v13", "v23"))

        # Reversed queries

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? ORDER BY t DESC", "key"),
                    row("key", 5, "v15", "v25"),
                    row("key", 4, "v14", "v24"),
                    row("key", 3, "v13", "v23"),
                    row("key", 2, "v12", "v22"),
                    row("key", 1, "v11", "v21"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t > ? ORDER BY t DESC", "key", 3),
                    row("key", 5, "v15", "v25"),
                    row("key", 4, "v14", "v24"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ? ORDER BY t DESC", "key", 2, 4),
                    row("key", 3, "v13", "v23"),
                    row("key", 2, "v12", "v22"))

def testTableWithReverseClusteringOrder(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, t int, v1 text, v2 text, PRIMARY KEY (k, t)) WITH CLUSTERING ORDER BY (t DESC)") as table:
        execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 1, "v11", "v21")
        execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 2, "v12", "v22")
        execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 3, "v13", "v23")

        flush(cql, table)

        execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 4, "v14", "v24")
        execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 5, "v15", "v25")

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                    row("key", 5, "v15", "v25"),
                    row("key", 4, "v14", "v24"),
                    row("key", 3, "v13", "v23"),
                    row("key", 2, "v12", "v22"),
                    row("key", 1, "v11", "v21"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? ORDER BY t ASC", "key"),
                    row("key", 1, "v11", "v21"),
                    row("key", 2, "v12", "v22"),
                    row("key", 3, "v13", "v23"),
                    row("key", 4, "v14", "v24"),
                    row("key", 5, "v15", "v25"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t > ?", "key", 3),
                    row("key", 5, "v15", "v25"),
                    row("key", 4, "v14", "v24"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ?", "key", 2, 4),
                    row("key", 3, "v13", "v23"),
                    row("key", 2, "v12", "v22"))

        # Reversed queries

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? ORDER BY t DESC", "key"),
                    row("key", 5, "v15", "v25"),
                    row("key", 4, "v14", "v24"),
                    row("key", 3, "v13", "v23"),
                    row("key", 2, "v12", "v22"),
                    row("key", 1, "v11", "v21"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t > ? ORDER BY t DESC", "key", 3),
                    row("key", 5, "v15", "v25"),
                    row("key", 4, "v14", "v24"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ? ORDER BY t DESC", "key", 2, 4),
                    row("key", 3, "v13", "v23"),
                    row("key", 2, "v12", "v22"))

def testTableWithTwoClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, t1 text, t2 int, v text, PRIMARY KEY (k, t1, t2))") as table:
        execute(cql, table, "INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", "v1", 1, "v1")
        execute(cql, table, "INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", "v1", 2, "v2")
        execute(cql, table, "INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", "v2", 1, "v3")
        execute(cql, table, "INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", "v2", 2, "v4")
        execute(cql, table, "INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", "v2", 3, "v5")
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                    row("key", "v1", 1, "v1"),
                    row("key", "v1", 2, "v2"),
                    row("key", "v2", 1, "v3"),
                    row("key", "v2", 2, "v4"),
                    row("key", "v2", 3, "v5"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t1 >= ?", "key", "v2"),
                    row("key", "v2", 1, "v3"),
                    row("key", "v2", 2, "v4"),
                    row("key", "v2", 3, "v5"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t1 >= ? ORDER BY t1 DESC", "key", "v2"),
                    row("key", "v2", 3, "v5"),
                    row("key", "v2", 2, "v4"),
                    row("key", "v2", 1, "v3"))

def testTableWithLargePartition(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, t1 int, t2 int, v text, PRIMARY KEY (k, t1, t2))") as table:
        for t1 in range(20):
            for t2 in range(10):
                execute(cql, table, "INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", t1, t2, "someSemiLargeTextForValue_" + str(t1) + "_" + str(t2))

        flush(cql, table)

        expected = [row("key", 15, t2) for t2 in range(10)]

        assert_rows(execute(cql, table, "SELECT k, t1, t2 FROM %s WHERE k=? AND t1=?", "key", 15), *expected)

        expectedReverse = [row("key", 15, t2) for t2 in range(9, -1, -1)]

        assert_rows(execute(cql, table, "SELECT k, t1, t2 FROM %s WHERE k=? AND t1=? ORDER BY t1 DESC, t2 DESC", "key", 15), *expectedReverse)

def testRowDeletion(cql, test_keyspace):
    N = 4

    with create_table(cql, test_keyspace, "(k text, t int, v1 text, v2 int, PRIMARY KEY (k, t))") as table:
        for t in range(N):
            execute(cql, table, "INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", t, "v" + str(t), t + 10)

        flush(cql, table)

        for i in range(N // 2):
            execute(cql, table, "DELETE FROM %s WHERE k=? AND t=?", "key", i * 2)

        expected = []
        for i in range(N // 2):
            t = i * 2 + 1
            expected.append(row("key", t, "v" + str(t), t + 10))

        assert_rows(execute(cql, table, "SELECT * FROM %s"), *expected)

def testRangeTombstones(cql, test_keyspace):
    N = 100

    with create_table(cql, test_keyspace, "(k text, t1 int, t2 int, v text, PRIMARY KEY (k, t1, t2))") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (k, t1, t2, v) values (?, ?, ?, ?)")
        for t1 in range(3):
            for t2 in range(N):
                cql.execute(stmt, ["key", t1, t2, "someSemiLargeTextForValue_" + str(t1) + "_" + str(t2)])

        flush(cql, table)

        execute(cql, table, "DELETE FROM %s WHERE k=? AND t1=?", "key", 1)

        flush(cql, table)

        expected = ([row("key", 0, t2, "someSemiLargeTextForValue_0_" + str(t2)) for t2 in range(N)] +
                    [row("key", 2, t2, "someSemiLargeTextForValue_2_" + str(t2)) for t2 in range(N)])

        assert_rows(execute(cql, table, "SELECT * FROM %s"), *expected)

def test2ndaryIndexes(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, t int, v text, PRIMARY KEY (k, t))") as table:
        execute(cql, table, "CREATE INDEX ON %s(v)")

        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 1, "foo")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 2, "bar")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key2", 1, "foo")

        flush(cql, table)

        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key2", 2, "foo")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key2", 3, "bar")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v = ?", "foo"),
                    row("key1", 1, "foo"),
                    row("key2", 1, "foo"),
                    row("key2", 2, "foo"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v = ?", "bar"),
                    row("key1", 2, "bar"),
                    row("key2", 3, "bar"))

def testStaticColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, t int, s text static, v text, PRIMARY KEY (k, t))") as table:
        execute(cql, table, "INSERT INTO %s (k, t, v, s) values (?, ?, ?, ?)", "key1", 1, "foo1", "st1")
        execute(cql, table, "INSERT INTO %s (k, t, v, s) values (?, ?, ?, ?)", "key1", 2, "foo2", "st2")

        flush(cql, table)

        execute(cql, table, "INSERT INTO %s (k, t, v, s) values (?, ?, ?, ?)", "key1", 3, "foo3", "st3")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 4, "foo4")
        execute(cql, table, "INSERT INTO %s (k, t, v, s) values (?, ?, ?, ?)", "key1", 2, "foo2", "st2-repeat")

        flush(cql, table)

        execute(cql, table, "INSERT INTO %s (k, t, v, s) values (?, ?, ?, ?)", "key1", 5, "foo5", "st5")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 6, "foo6")

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                    row("key1", 1, "st5", "foo1"),
                    row("key1", 2, "st5", "foo2"),
                    row("key1", 3, "st5", "foo3"),
                    row("key1", 4, "st5", "foo4"),
                    row("key1", 5, "st5", "foo5"),
                    row("key1", 6, "st5", "foo6"))

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = ?", "key1"),
                    row("st5"),
                    row("st5"),
                    row("st5"),
                    row("st5"),
                    row("st5"),
                    row("st5"))

        assert_rows(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k = ?", "key1"),
                    row("st5"))

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t > ? AND t < ?", "key1", 7, 5))
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t > ? AND t < ? ORDER BY t DESC", "key1", 7, 5))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t = ?", "key1", 2),
                    row("key1", 2, "st5", "foo2"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND t = ? ORDER BY t DESC", "key1", 2),
                    row("key1", 2, "st5", "foo2"))

def testDistinct(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, t int, v text, PRIMARY KEY (k, t))") as table:
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 1, "foo1")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 2, "foo2")

        flush(cql, table)

        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 3, "foo3")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key2", 4, "foo4")
        execute(cql, table, "INSERT INTO %s (k, t, v) values (?, ?, ?)", "key2", 5, "foo5")

        assert_rows(execute(cql, table, "SELECT DISTINCT k FROM %s"),
                    row("key1"),
                    row("key2"))

def testcollectionDeletionTest(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s set<int>)") as table:
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (?, ?)", 1, {1})

        flush(cql, table)

        execute(cql, table, "INSERT INTO %s (k, s) VALUES (?, ?)", 1, {2})

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = ?", 1),
                    row({2}))

def testlimitWithMultigetTest(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v int)") as table:
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 0, 0)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 1, 1)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 2, 2)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 3, 3)

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k IN ? LIMIT ?", [0, 1, 2, 3], 2),
                    row(0),
                    row(1))

def teststaticDistinctTest(cql, test_keyspace):
    with create_table(cql, test_keyspace, "( k int, p int, s int static, PRIMARY KEY (k, p))") as table:
        execute(cql, table, "INSERT INTO %s (k, p) VALUES (?, ?)", 1, 1)
        execute(cql, table, "INSERT INTO %s (k, p) VALUES (?, ?)", 1, 2)

        assert_rows(execute(cql, table, "SELECT k, s FROM %s"),
                    row(1, None),
                    row(1, None))
        assert_rows(execute(cql, table, "SELECT DISTINCT k, s FROM %s"),
                    row(1, None))
        assert_rows(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k=?", 1),
                    row(None))
        assert_empty(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k=?", 2))

def test2ndaryIndexBug(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c1 int, c2 int, v int, PRIMARY KEY(k, c1, c2))") as table:
        execute(cql, table, "CREATE INDEX ON %s(v)")

        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v=?", 0),
                    row(0, 0, 0, 0),
                    row(0, 1, 0, 0))

        flush(cql, table)

        execute(cql, table, "DELETE FROM %s WHERE k=? AND c1=?", 0, 1)

        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v=?", 0),
                    row(0, 0, 0, 0))
