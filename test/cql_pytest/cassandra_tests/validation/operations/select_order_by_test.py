# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

def testSelectOrderBy(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 2)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0, 0, 0],
                       [0, 1, 1],
                       [0, 2, 2])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [0, 2, 2],
                       [0, 1, 1],
                       [0, 0, 0])

            # order by the only column in the selection
            assert_rows(execute(cql, table, "SELECT b FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0], [1], [2])

            assert_rows(execute(cql, table, "SELECT b FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [2], [1], [0])

            # order by a column not in the selection
            assert_rows(execute(cql, table, "SELECT c FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0], [1], [2])

            assert_rows(execute(cql, table, "SELECT c FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [2], [1], [0])

def testFunctionSelectionOrderSingleClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 2)

        for _ in before_and_after_flush(cql, table):
            # order by the only column in the selection
            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0], [1], [2])

            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [2], [1], [0])

            # order by a column not in the selection
            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0], [1], [2])

            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [2], [1], [0])

            assert_invalid(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY c ASC", 0)
            assert_invalid(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY c DESC", 0)

def testFieldSelectionOrderSingleClustering(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as type:
        with create_table(cql, test_keyspace, "(a int, b int, c frozen<" + type + ">, PRIMARY KEY (a, b))") as table:
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, {a: ?})", 0, 0, 0)
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, {a: ?})", 0, 1, 1)
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, {a: ?})", 0, 2, 2)

            for _ in before_and_after_flush(cql, table):
                # order by a column not in the selection
                assert_rows(execute(cql, table, "SELECT c.a FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0], [1], [2])

                assert_rows(execute(cql, table, "SELECT c.a FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [2], [1], [0])

                assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(c.a)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [2], [1], [0])

def testNormalSelectionOrderMultipleClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 2, 2)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 3)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 2, 5)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0, 0, 0, 0],
                       [0, 0, 1, 1],
                       [0, 0, 2, 2],
                       [0, 1, 0, 3],
                       [0, 1, 1, 4],
                       [0, 1, 2, 5])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [0, 1, 2, 5],
                       [0, 1, 1, 4],
                       [0, 1, 0, 3],
                       [0, 0, 2, 2],
                       [0, 0, 1, 1],
                       [0, 0, 0, 0])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                       [0, 1, 2, 5],
                       [0, 1, 1, 4],
                       [0, 1, 0, 3],
                       [0, 0, 2, 2],
                       [0, 0, 1, 1],
                       [0, 0, 0, 0])

            assert_invalid(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY c ASC", 0)
            assert_invalid(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY c DESC", 0)
            assert_invalid(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY b ASC, c DESC", 0)
            assert_invalid(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY b DESC, c ASC", 0)
            assert_invalid(cql, table, "SELECT * FROM %s WHERE a=? ORDER BY d ASC", 0)

            # select and order by b
            assert_rows(execute(cql, table, "SELECT b FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0], [0], [0], [1], [1], [1])
            assert_rows(execute(cql, table, "SELECT b FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [1], [1], [1], [0], [0], [0])

            # select c, order by b
            assert_rows(execute(cql, table, "SELECT c FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0], [1], [2], [0], [1], [2])
            assert_rows(execute(cql, table, "SELECT c FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [2], [1], [0], [2], [1], [0])

            # select c, order by b, c
            assert_rows(execute(cql, table, "SELECT c FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                       [0], [1], [2], [0], [1], [2])
            assert_rows(execute(cql, table, "SELECT c FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                       [2], [1], [0], [2], [1], [0])

            # select d, order by b, c
            assert_rows(execute(cql, table, "SELECT d FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                       [0], [1], [2], [3], [4], [5])
            assert_rows(execute(cql, table, "SELECT d FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                       [5], [4], [3], [2], [1], [0])

def testFunctionSelectionOrderMultipleClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 2, 2)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 3)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 2, 5)

        for _ in before_and_after_flush(cql, table):
            assert_invalid(cql, table, "SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY c ASC", 0)
            assert_invalid(cql, table, "SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY c DESC", 0)
            assert_invalid(cql, table, "SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC, c DESC", 0)
            assert_invalid(cql, table, "SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC, c ASC", 0)
            assert_invalid(cql, table, "SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY d ASC", 0)

            # select and order by b
            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0], [0], [0], [1], [1], [1])
            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [1], [1], [1], [0], [0], [0])

            assert_rows(execute(cql, table, "SELECT b, blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0, 0], [0, 0], [0, 0], [1, 1], [1, 1], [1, 1])
            assert_rows(execute(cql, table, "SELECT b, blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [1, 1], [1, 1], [1, 1], [0, 0], [0, 0], [0, 0])

            # select c, order by b
            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                       [0], [1], [2], [0], [1], [2])
            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                       [2], [1], [0], [2], [1], [0])

            # select c, order by b, c
            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                       [0], [1], [2], [0], [1], [2])
            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                       [2], [1], [0], [2], [1], [0])

            # select d, order by b, c
            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(d)) FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                       [0], [1], [2], [3], [4], [5])
            assert_rows(execute(cql, table, "SELECT blobAsInt(intAsBlob(d)) FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                       [5], [4], [3], [2], [1], [0])

# Check we don't allow order by on row key (CASSANDRA-4246)
# migrated from cql_tests.py:TestCQL.order_by_validation_test()
def testInvalidOrderBy(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, k2 int, v int, PRIMARY KEY (k1,k2))") as table:
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 2, 2, 2)

        assert_invalid(cql, table, "SELECT * FROM %s ORDER BY k2")

# Check that order-by works with IN (CASSANDRA-4327)
# migrated from cql_tests.py:TestCQL.order_by_with_in_test()
# Reproduces issue #9435
@pytest.mark.xfail(reason="Issue #9435")
def testOrderByForInClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(my_id varchar, col1 int, value varchar, PRIMARY KEY (my_id, col1))") as table:

        execute(cql, table, "INSERT INTO %s (my_id, col1, value) VALUES ( 'key1', 1, 'a')")
        execute(cql, table, "INSERT INTO %s (my_id, col1, value) VALUES ( 'key2', 3, 'c')")
        execute(cql, table, "INSERT INTO %s (my_id, col1, value) VALUES ( 'key3', 2, 'b')")
        execute(cql, table, "INSERT INTO %s (my_id, col1, value) VALUES ( 'key4', 4, 'd')")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute_without_paging(cql, table, "SELECT col1 FROM %s WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                       [1], [2], [3])

            assert_rows(execute_without_paging(cql, table, "SELECT col1 FROM %s WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1 LIMIT 2"),
                       [1], [2])

            assert_rows(execute_without_paging(cql, table, "SELECT col1 FROM %s WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1 LIMIT 10"),
                       [1], [2], [3])

            assert_rows(execute_without_paging(cql, table, "SELECT col1, my_id FROM %s WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                       [1, "key1"], [2, "key3"], [3, "key2"])

            assert_rows(execute_without_paging(cql, table, "SELECT my_id, col1 FROM %s WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                       ["key1", 1], ["key3", 2], ["key2", 3])

    with create_table(cql, test_keyspace, "(pk1 int, pk2 int, c int, v text, PRIMARY KEY ((pk1, pk2), c) )") as table:
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, v) VALUES (?, ?, ?, ?)", 1, 1, 2, "A")
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, v) VALUES (?, ?, ?, ?)", 1, 2, 1, "B")
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, v) VALUES (?, ?, ?, ?)", 1, 3, 3, "C")
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, v) VALUES (?, ?, ?, ?)", 1, 1, 4, "D")

        for _ in before_and_after_flush(cql, table):
            # Reproduces #9435
            assert_rows(execute_without_paging(cql, table, "SELECT v, ttl(v), c FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c; ", 1, 1, 2),
                       ["B", None, 1],
                       ["A", None, 2],
                       ["D", None, 4])

            # Reproduces #9435
            assert_rows(execute_without_paging(cql, table, "SELECT v, ttl(v), c as name_1 FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c; ", 1, 1, 2),
                       ["B", None, 1],
                       ["A", None, 2],
                       ["D", None, 4])

            assert_rows(execute_without_paging(cql, table, "SELECT v FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c; ", 1, 1, 2),
                       ["B"],
                       ["A"],
                       ["D"])

            assert_rows(execute_without_paging(cql, table, "SELECT v FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c LIMIT 2; ", 1, 1, 2),
                       ["B"],
                       ["A"])

            assert_rows(execute_without_paging(cql, table, "SELECT v FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c LIMIT 10; ", 1, 1, 2),
                       ["B"],
                       ["A"],
                       ["D"])

            assert_rows(execute_without_paging(cql, table, "SELECT v as c FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c; ", 1, 1, 2),
                       ["B"],
                       ["A"],
                       ["D"])

    with create_table(cql, test_keyspace, "(pk1 int, pk2 int, c1 int, c2 int, v text, PRIMARY KEY ((pk1, pk2), c1, c2) )") as table:
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 4, 4, "A")
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", 1, 2, 1, 2, "B")
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", 1, 3, 3, 3, "C")
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 4, 1, "D")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute_without_paging(cql, table, "SELECT v, ttl(v), c1, c2 FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c1, c2; ", 1, 1, 2),
                       ["B", None, 1, 2],
                       ["D", None, 4, 1],
                       ["A", None, 4, 4])

            assert_rows(execute_without_paging(cql, table, "SELECT v, ttl(v), c1 as name_1, c2 as name_2 FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c1, c2; ", 1, 1, 2),
                       ["B", None, 1, 2],
                       ["D", None, 4, 1],
                       ["A", None, 4, 4])

            assert_rows(execute_without_paging(cql, table, "SELECT v FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c1, c2; ", 1, 1, 2),
                       ["B"],
                       ["D"],
                       ["A"])

            assert_rows(execute_without_paging(cql, table, "SELECT v as c2 FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c1, c2; ", 1, 1, 2),
                       ["B"],
                       ["D"],
                       ["A"])

            assert_rows(execute_without_paging(cql, table, "SELECT v as c2 FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c1, c2 LIMIT 2; ", 1, 1, 2),
                       ["B"],
                       ["D"])

            assert_rows(execute_without_paging(cql, table, "SELECT v as c2 FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c1, c2 LIMIT 10; ", 1, 1, 2),
                       ["B"],
                       ["D"],
                       ["A"])

            assert_rows(execute_without_paging(cql, table, "SELECT v as c2 FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c1 DESC , c2 DESC; ", 1, 1, 2),
                       ["A"],
                       ["D"],
                       ["B"])

            assert_rows(execute_without_paging(cql, table, "SELECT v as c2 FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c1 DESC , c2 DESC LIMIT 2; ", 1, 1, 2),
                       ["A"],
                       ["D"])

            assert_rows(execute_without_paging(cql, table, "SELECT v as c2 FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c1 DESC , c2 DESC LIMIT 10; ", 1, 1, 2),
                       ["A"],
                       ["D"],
                       ["B"])

            assert_invalid_message(cql, table, "LIMIT must be strictly positive",
                                 "SELECT v as c2 FROM %s where pk1 = ? AND pk2 IN (?, ?) ORDER BY c1 DESC , c2 DESC LIMIT 0; ", 1, 1, 2)

# Reproduces #7751
@pytest.mark.xfail(reason="Issue #7751")
def testOrderByForInClauseWithCollectionElementSelection(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c frozen<set<int>>, v int, PRIMARY KEY (pk, c))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (0, {1, 2}, 0)")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (0, {1, 2, 3}, 1)")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, {2, 3}, 2)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT c[2], v FROM %s WHERE pk = 0 ORDER BY c"),
                       [2, 0], [2, 1])
            assert_rows(execute_without_paging(cql, table, "SELECT c[2], v FROM %s WHERE pk IN (0, 1) ORDER BY c"),
                       [2, 0], [2, 1], [2, 2])

def testOrderByForInClauseWithNullValue(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, s int static, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 1, 2, 1)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 2, 1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 2, 2, 1)")

        execute(cql, table, "UPDATE %s SET s = 1 WHERE a = 1")
        execute(cql, table, "UPDATE %s SET s = 2 WHERE a = 2")
        execute(cql, table, "UPDATE %s SET s = 3 WHERE a = 3")

        for _ in before_and_after_flush(cql, table):
            # Commenting this test out - there are ties for b, so the correct
            # order isn't completely specified, and happens to be different in
            # Scylla and Cassandra.
            #assert_rows(execute_without_paging(cql, table, "SELECT a, b, c, d, s FROM %s WHERE a IN (1, 2, 3) ORDER BY b DESC"),
            #           [2, 2, 2, 1, 2],
            #           [2, 2, 1, 1, 2],
            #           [1, 1, 2, 1, 1],
            #           [1, 1, 1, 1, 1],
            #           [3, None, None, None, 3])

            #assert_rows(execute_without_paging(cql, table, "SELECT a, b, c, d, s FROM %s WHERE a IN (1, 2, 3) ORDER BY b ASC"),
            #           [3, None, None, None, 3],
            #           [1, 1, 1, 1, 1],
            #           [1, 1, 2, 1, 1],
            #           [2, 2, 1, 1, 2],
            #           [2, 2, 2, 1, 2])

            assert_rows(execute_without_paging(cql, table, "SELECT a, b, c, d, s FROM %s WHERE a IN (1, 2, 3) ORDER BY b DESC , c DESC"),
                       [2, 2, 2, 1, 2],
                       [2, 2, 1, 1, 2],
                       [1, 1, 2, 1, 1],
                       [1, 1, 1, 1, 1],
                       [3, None, None, None, 3])

            assert_rows(execute_without_paging(cql, table, "SELECT a, b, c, d, s FROM %s WHERE a IN (1, 2, 3) ORDER BY b ASC, c ASC"),
                       [3, None, None, None, 3],
                       [1, 1, 1, 1, 1],
                       [1, 1, 2, 1, 1],
                       [2, 2, 1, 1, 2],
                       [2, 2, 2, 1, 2])

# Test reversed comparators
# migrated from cql_tests.py:TestCQL.reversed_comparator_test()
def testReversedComparator(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT c, v FROM %s WHERE k = 0 ORDER BY c ASC"),
                       [0, 0], [1, 1], [2, 2], [3, 3], [4, 4],
                       [5, 5], [6, 6], [7, 7], [8, 8], [9, 9])

            assert_rows(execute(cql, table, "SELECT c, v FROM %s WHERE k = 0 ORDER BY c DESC"),
                       [9, 9], [8, 8], [7, 7], [6, 6], [5, 5],
                       [4, 4], [3, 3], [2, 2], [1, 1], [0, 0])

    with create_table(cql, test_keyspace, "(k int, c1 int, c2 int, v text, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)") as table:
        for i in range(10):
            for j in range(10):
                execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (0, ?, ?, ?)", i, j, str(i)+str(j))

        for _ in before_and_after_flush(cql, table):
            assert_invalid(cql, table, "SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 ASC")
            assert_invalid(cql, table, "SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c1 DESC, c2 DESC")

            expectedRows = [[] for i in range(100)]
            for i in range(10):
                for j in reversed(range(10)):
                    expectedRows[i * 10 + (9 - j)] = [i, j, str(i)+str(j)]

            assert_rows(execute(cql, table, "SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c1 ASC"),
                       *expectedRows)

            assert_rows(execute(cql, table, "SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC"),
                       *expectedRows)

            for i in reversed(range(10)):
                for j in range(10):
                    expectedRows[(9 - i) * 10 + j] = [i, j, str(i)+str(j)]

            assert_rows(execute(cql, table, "SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c1 DESC, c2 ASC"),
                       *expectedRows)

            assert_invalid(cql, table, "SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c2 DESC, c1 ASC")

# Migrated from cql_tests.py:TestCQL.multiordering_test()
def testMultiordering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)") as table:
        for i in range(2):
            for j in range(2):
                execute(cql, table, "INSERT INTO %s (k, c1, c2) VALUES ('foo', ?, ?)", i, j)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE k = 'foo'"),
                       [0, 1], [0, 0], [1, 1], [1, 0])

            assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c1 ASC, c2 DESC"),
                       [0, 1], [0, 0], [1, 1], [1, 0])

            assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c1 DESC, c2 ASC"),
                       [1, 0], [1, 1], [0, 0], [0, 1])

            assert_invalid(cql, table, "SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c2 DESC")
            assert_invalid(cql, table, "SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c2 ASC")
            assert_invalid(cql, table, "SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c1 ASC, c2 ASC")

# Migrated from cql_tests.py:TestCQL.in_with_desc_order_test()
def testSelectInStatementWithDesc(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2))") as table:
        execute(cql, table, "INSERT INTO %s(k, c1, c2) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s(k, c1, c2) VALUES (0, 0, 1)")
        execute(cql, table, "INSERT INTO %s(k, c1, c2) VALUES (0, 0, 2)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC"),
                       [0, 0, 2],
                       [0, 0, 0])

# Test that columns don't need to be selected for ORDER BY when there is a IN
# (CASSANDRA-4911),
# migrated from cql_tests.py:TestCQL.in_order_by_without_selecting_test()
def testInOrderByWithoutSelecting(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))") as table:
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 2, 2)")
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 0, 3)")
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 1, 4)")
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 2, 5)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)"),
                       [0, 0, 0, 0],
                       [0, 0, 2, 2])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC, c2 ASC"),
                       [0, 0, 0, 0],
                       [0, 0, 2, 2])

            # check that we don't need to select the column on which we order
            assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)"),
                       [0],
                       [2])
            assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC"),
                       [0],
                       [2])
            assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC"),
                       [2],
                       [0])

            assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k IN (1, 0)"),
                       [0],
                       [1],
                       [2],
                       [3],
                       [4],
                       [5])

            assert_rows(execute_without_paging(cql, table, "SELECT v FROM %s WHERE k IN (1, 0) ORDER BY c1 ASC"),
                       [0],
                       [1],
                       [2],
                       [3],
                       [4],
                       [5])

            # we should also be able to use functions in the select clause (additional test for CASSANDRA - 8286)
            results = list(execute_without_paging(cql, table, "SELECT writetime(v) FROM %s WHERE k IN (1, 0) ORDER BY c1 ASC"))

            # since we don 't know the write times, just assert that the order matches the order we expect
            prev = 0
            for row in results:
                assert row[0] >= prev
                prev = row[0]

def testInOrderByWithTwoPartitionKeyColumns(cql, test_keyspace):
    for option in ["", "WITH CLUSTERING ORDER BY (col_3 DESC)"]:
        with create_table(cql, test_keyspace, "(col_1 int, col_2 int, col_3 int, PRIMARY KEY ((col_1, col_2), col_3)) " + option) as table:
            execute(cql, table, "INSERT INTO %s (col_1, col_2, col_3) VALUES(?, ?, ?)", 1, 1, 1)
            execute(cql, table, "INSERT INTO %s (col_1, col_2, col_3) VALUES(?, ?, ?)", 1, 1, 2)
            execute(cql, table, "INSERT INTO %s (col_1, col_2, col_3) VALUES(?, ?, ?)", 1, 1, 13)
            execute(cql, table, "INSERT INTO %s (col_1, col_2, col_3) VALUES(?, ?, ?)", 1, 2, 10)
            execute(cql, table, "INSERT INTO %s (col_1, col_2, col_3) VALUES(?, ?, ?)", 1, 2, 11)

            for _ in before_and_after_flush(cql, table):
                assert_rows(execute_without_paging(cql, table, "select * from %s where col_1=? and col_2 IN (?, ?) order by col_3;", 1, 1, 2),
                           [1, 1, 1],
                           [1, 1, 2],
                           [1, 2, 10],
                           [1, 2, 11],
                           [1, 1, 13])

                assert_rows(execute_without_paging(cql, table, "select * from %s where col_1=? and col_2 IN (?, ?) order by col_3 desc;", 1, 1, 2),
                           [1, 1, 13],
                           [1, 2, 11],
                           [1, 2, 10],
                           [1, 1, 2],
                           [1, 1, 1])

                assert_rows(execute_without_paging(cql, table, "select * from %s where col_2 IN (?, ?) and col_1=? order by col_3;", 1, 2, 1),
                           [1, 1, 1],
                           [1, 1, 2],
                           [1, 2, 10],
                           [1, 2, 11],
                           [1, 1, 13])

                assert_rows(execute_without_paging(cql, table, "select * from %s where col_2 IN (?, ?) and col_1=? order by col_3 desc;", 1, 2, 1),
                           [1, 1, 13],
                           [1, 2, 11],
                           [1, 2, 10],
                           [1, 1, 2],
                           [1, 1, 1])

# Test that ORDER BY columns allow skipping equality-restricted clustering columns, see CASSANDRA-10271.
# Reproduces Scylla issue #2247.
def testAllowSkippingEqualityAndSingleValueInRestrictedClusteringColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 2, 2)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 3)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 2, 5)

        assert_invalid_message(cql, table, "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got d",
                             "SELECT * FROM %s WHERE a=? ORDER BY d DESC", 0)

        assert_invalid_message(cql, table, "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got d",
                             "SELECT * FROM %s WHERE a=? ORDER BY b ASC, c ASC, d ASC", 0)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? ORDER BY c", 0, 0),
                   [0, 0, 0, 0],
                   [0, 0, 1, 1],
                   [0, 0, 2, 2])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? ORDER BY c ASC", 0, 0),
                   [0, 0, 0, 0],
                   [0, 0, 1, 1],
                   [0, 0, 2, 2])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? ORDER BY c DESC", 0, 0),
                   [0, 0, 2, 2],
                   [0, 0, 1, 1],
                   [0, 0, 0, 0])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c>=? ORDER BY c ASC", 0, 0, 1),
                   [0, 0, 1, 1],
                   [0, 0, 2, 2])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c>=? ORDER BY c DESC", 0, 0, 1),
                   [0, 0, 2, 2],
                   [0, 0, 1, 1])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c IN (?, ?) ORDER BY c ASC", 0, 0, 1, 2),
                   [0, 0, 1, 1],
                   [0, 0, 2, 2])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c IN (?, ?) ORDER BY c DESC", 0, 0, 1, 2),
                   [0, 0, 2, 2],
                   [0, 0, 1, 1])

        # Minimal error message part that works both with Scylla and Cassandra
        # Cassandra says: Order by currently only supports the ordering of columns following their declared order in the PRIMARY KEY
        # Scylla says: Unsupported order by relation - column {} doesn't have an ordering or EQ relation.
        errorMsg = "rder by"

        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND b<? ORDER BY c DESC", 0, 1)

        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND (b, c) > (?, ?) ORDER BY c", 0, 0, 0)
        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND (b, c) >= (?, ?) ORDER BY c", 0, 0, 0)
        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND (b, c) < (?, ?) ORDER BY c", 0, 0, 0)
        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND (b, c) <= (?, ?) ORDER BY c", 0, 0, 0)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c) = (?, ?) ORDER BY c ASC", 0, 0, 0),
                   [0, 0, 0, 0])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c) = (?, ?) ORDER BY c DESC", 0, 0, 0),
                   [0, 0, 0, 0])

        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND (b, c) > ? ORDER BY c", 0, (0, 0))
        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND (b, c) >= ? ORDER BY c", 0, (0, 0))
        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND (b, c) < ? ORDER BY c", 0, (0, 0))
        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND (b, c) <= ? ORDER BY c", 0, (0, 0))
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c) = ? ORDER BY c ASC", 0, (0, 0)),
                   [0, 0, 0, 0])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c) = ? ORDER BY c DESC", 0, (0, 0)),
                   [0, 0, 0, 0])

        # Some of these queries need to be without paging, otherwise we get
        # from Cassandra: "Cannot page queries with both ORDER BY and a IN
        # restriction on the partition key; you must either remove the
        # ORDER BY or the IN and sort client side, or disable paging for this
        # query"
        assert_rows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE a IN (?, ?) AND b=? AND c>=? ORDER BY c ASC", 0, 1, 0, 0),
                   [0, 0, 0, 0],
                   [0, 0, 1, 1],
                   [0, 0, 2, 2])

        assert_rows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE a IN (?, ?) AND b=? AND c>=? ORDER BY c DESC", 0, 1, 0, 0),
                   [0, 0, 2, 2],
                   [0, 0, 1, 1],
                   [0, 0, 0, 0])

        assert_rows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE a IN (?, ?) AND b=? ORDER BY c ASC", 0, 1, 0),
                   [0, 0, 0, 0],
                   [0, 0, 1, 1],
                   [0, 0, 2, 2])

        assert_rows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE a IN (?, ?) AND b=? ORDER BY c DESC", 0, 1, 0),
                   [0, 0, 2, 2],
                   [0, 0, 1, 1],
                   [0, 0, 0, 0])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b IN (?) ORDER BY c ASC", 0, 1),
                   [0, 1, 0, 3],
                   [0, 1, 1, 4],
                   [0, 1, 2, 5])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b IN (?) ORDER BY c DESC", 0, 1),
                   [0, 1, 2, 5],
                   [0, 1, 1, 4],
                   [0, 1, 0, 3])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c) IN ((?, ?)) ORDER BY c ASC", 0, 1, 1),
                   [0, 1, 1, 4])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c) IN ((?, ?)) ORDER BY c DESC", 0, 1, 1),
                   [0, 1, 1, 4])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b IN (?, ?) AND c=? ORDER BY b ASC", 0, 0, 1, 2),
                   [0, 0, 2, 2],
                   [0, 1, 2, 5])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b IN (?, ?) AND c=? ORDER BY b DESC", 0, 0, 1, 2),
                   [0, 1, 2, 5],
                   [0, 0, 2, 2])

        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND b IN ? ORDER BY c", 0, [0])
        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND b IN (?,?) ORDER BY c", 0, 1, 3)
        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND (b,c) IN ? ORDER BY c", 0, [(0, 0)])
        assert_invalid_message(cql, table, errorMsg, "SELECT * FROM %s WHERE a=? AND (b,c) IN ((?,?), (?,?)) ORDER BY c", 0, 0, 0, 0, 1)


def testSelectWithReversedTypeInReverseOrderWithStaticColumnsWithoutStaticRow(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int static, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1, 1);")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, 2);")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, 3);")

        # read in comparator order
        assert_rows(execute(cql, table, "SELECT b, c FROM %s WHERE a = 1 ORDER BY b DESC;"),
                   [3, 3],
                   [2, 2],
                   [1, 1])

        # read in reverse comparator order
        assert_rows(execute(cql, table, "SELECT b, c FROM %s WHERE a = 1 ORDER BY b ASC;"),
                   [1, 1],
                   [2, 2],
                   [3, 3])

        # Flush the sstable. We *should* see the same results when reading in both directions, but prior to CASSANDRA-14910
        # fix this would now have returned an empty result set when reading in reverse comparator order.
        flush(cql, table)

        # read in comparator order
        assert_rows(execute(cql, table, "SELECT b, c FROM %s WHERE a = 1 ORDER BY b DESC;"),
                   [3, 3],
                   [2, 2],
                   [1, 1])

        # read in reverse comparator order
        assert_rows(execute(cql, table, "SELECT b, c FROM %s WHERE a = 1 ORDER BY b ASC;"),
                   [1, 1],
                   [2, 2],
                   [3, 3])

def testSelectWithReversedTypeInReverseOrderWithStaticColumnsWithStaticRow(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int static, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, d) VALUES (1, 0);")

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1, 1);")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, 2);")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, 3);")

        # read in comparator order
        assert_rows(execute(cql, table, "SELECT b, c, d FROM %s WHERE a = 1 ORDER BY b DESC;"),
                   [3, 3, 0],
                   [2, 2, 0],
                   [1, 1, 0])

        # read in reverse comparator order
        assert_rows(execute(cql, table, "SELECT b, c, d FROM %s WHERE a = 1 ORDER BY b ASC;"),
                   [1, 1, 0],
                   [2, 2, 0],
                   [3, 3, 0])

        flush(cql, table)

        # read in comparator order
        assert_rows(execute(cql, table, "SELECT b, c, d FROM %s WHERE a = 1 ORDER BY b DESC;"),
                   [3, 3, 0],
                   [2, 2, 0],
                   [1, 1, 0])

        # read in reverse comparator order
        assert_rows(execute(cql, table, "SELECT b, c, d FROM %s WHERE a = 1 ORDER BY b ASC;"),
                   [1, 1, 0],
                   [2, 2, 0],
                   [3, 3, 0])
