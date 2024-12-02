# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit a87055d56a33a9b17606f14535f48eb461965b82
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *
from cassandra.query import UNSET_VALUE

def testInvalidCollectionEqualityRelation(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b set<int>, c list<int>, d map<int, int>)") as table:
        execute(cql, table, "CREATE INDEX ON %s (b)")
        execute(cql, table, "CREATE INDEX ON %s (c)")
        execute(cql, table, "CREATE INDEX ON %s (d)")
        assert_invalid_message(cql, table, "Collection column 'b' (set<int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND b=?", {0})
        assert_invalid_message(cql, table, "Collection column 'c' (list<int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND c=?", [0])
        assert_invalid_message(cql, table, "Collection column 'd' (map<int, int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND d=?", {0: 0})

def testInvalidCollectionNonEQRelation(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b set<int>, c int)") as table:
        execute(cql, table, "CREATE INDEX ON %s (c)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, {0}, 0)")

        # non-EQ operators
        assert_invalid_message(cql, table, "Collection column 'b' (set<int>) cannot be restricted by a '>' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b > ?", {0})
        assert_invalid_message(cql, table, "Collection column 'b' (set<int>) cannot be restricted by a '>=' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b >= ?", {0})
        assert_invalid_message(cql, table, "Collection column 'b' (set<int>) cannot be restricted by a '<' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b < ?", {0})
        assert_invalid_message(cql, table, "Collection column 'b' (set<int>) cannot be restricted by a '<=' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b <= ?", {0})
        # Reproduces #10631:
        assert_invalid_message(cql, table, "Collection column 'b' (set<int>) cannot be restricted by a 'IN' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b IN (?)", {0})
        assert_invalid_message(cql, table, "Unsupported \"!=\" relation: b != 5",
                "SELECT * FROM %s WHERE c = 0 AND b != 5")
        # different error message in Scylla and Cassandra. Note that in the
        # future, Scylla may want to support this restriction so the error
        # message will change again.
        assert_invalid_message(cql, table, "IS NOT",
                "SELECT * FROM %s WHERE c = 0 AND b IS NOT NULL")

def testClusteringColumnRelations(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a text, b int, c int, d int, primary key (a, b, c))") as table:
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4)

        assert_rows(execute(cql, table, "select * from %s where a in (?, ?)", "first", "second"),
                   ["first", 1, 5, 1],
                   ["first", 2, 6, 2],
                   ["first", 3, 7, 3],
                   ["second", 4, 8, 4])

        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c in (?, ?)", "first", 2, 6, 7),
                   ["first", 2, 6, 2])

        assert_rows(execute(cql, table, "select * from %s where a = ? and b in (?, ?) and c in (?, ?)", "first", 2, 3, 6, 7),
                   ["first", 2, 6, 2],
                   ["first", 3, 7, 3])

        assert_rows(execute(cql, table, "select * from %s where a = ? and b in (?, ?) and c in (?, ?)", "first", 3, 2, 7, 6),
                   ["first", 2, 6, 2],
                   ["first", 3, 7, 3])

        assert_rows(execute(cql, table, "select * from %s where a = ? and c in (?, ?) and b in (?, ?)", "first", 7, 6, 3, 2),
                   ["first", 2, 6, 2],
                   ["first", 3, 7, 3])

        assert_rows(execute(cql, table, "select c, d from %s where a = ? and c in (?, ?) and b in (?, ?)", "first", 7, 6, 3, 2),
                   [6, 2],
                   [7, 3])

        assert_rows(execute(cql, table, "select c, d from %s where a = ? and c in (?, ?) and b in (?, ?, ?)", "first", 7, 6, 3, 2, 3),
                   [6, 2],
                   [7, 3])

        assert_rows(execute(cql, table, "select * from %s where a = ? and b in (?, ?) and c = ?", "first", 3, 2, 7),
                   ["first", 3, 7, 3])

        assert_rows(execute(cql, table, "select * from %s where a = ? and b in ? and c in ?",
                           "first", [3, 2], [7, 6]),
                   ["first", 2, 6, 2],
                   ["first", 3, 7, 3])

        # Scylla does allow IN NULL (see commit 52bbc1065c8) so this test
        # is commented out
        #assert_invalid_message(cql, table, "Invalid null value for column b",
        #                     "select * from %s where a = ? and b in ? and c in ?", "first", None, [7, 6])

        assert_rows(execute(cql, table, "select * from %s where a = ? and c >= ? and b in (?, ?)", "first", 6, 3, 2),
                   ["first", 2, 6, 2],
                   ["first", 3, 7, 3])

        assert_rows(execute(cql, table, "select * from %s where a = ? and c > ? and b in (?, ?)", "first", 6, 3, 2),
                   ["first", 3, 7, 3])

        assert_rows(execute(cql, table, "select * from %s where a = ? and c <= ? and b in (?, ?)", "first", 6, 3, 2),
                   ["first", 2, 6, 2])

        assert_rows(execute(cql, table, "select * from %s where a = ? and c < ? and b in (?, ?)", "first", 7, 3, 2),
                   ["first", 2, 6, 2])

        assert_rows(execute(cql, table, "select * from %s where a = ? and c >= ? and c <= ? and b in (?, ?)", "first", 6, 7, 3, 2),
                   ["first", 2, 6, 2],
                   ["first", 3, 7, 3])

        assert_rows(execute(cql, table, "select * from %s where a = ? and c > ? and c <= ? and b in (?, ?)", "first", 6, 7, 3, 2),
                   ["first", 3, 7, 3])

        assert_empty(execute(cql, table, "select * from %s where a = ? and c > ? and c < ? and b in (?, ?)", "first", 6, 7, 3, 2))

        # Scylla does allow such queries, and their correctness is tested in
        # test_filtering.py::test_multiple_restrictions_on_same_column
        #assert_invalid_message(cql, table, "Column \"c\" cannot be restricted by both an equality and an inequality relation",
        #                     "select * from %s where a = ? and c > ? and c = ? and b in (?, ?)", "first", 6, 7, 3, 2)

        #assert_invalid_message(cql, table, "c cannot be restricted by more than one relation if it includes an Equal",
        #                     "select * from %s where a = ? and c = ? and c > ?  and b in (?, ?)", "first", 6, 7, 3, 2)

        assert_rows(execute(cql, table, "select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b DESC",
                           "first", 7, 6, 3, 2),
                   ["first", 3, 7, 3],
                   ["first", 2, 6, 2])

        # Scylla does allow such queries, and their correctness is tested in
        # test_filtering.py::test_multiple_restrictions_on_same_column
        #assert_invalid_message(cql, table, "More than one restriction was found for the start bound on b",
        #                     "select * from %s where a = ? and b > ? and b > ?", "first", 6, 3, 2)

        #assert_invalid_message(cql, table, "More than one restriction was found for the end bound on b",
        #                     "select * from %s where a = ? and b < ? and b <= ?", "first", 6, 3, 2)

REQUIRES_ALLOW_FILTERING_MESSAGE = "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"

def testPartitionKeyColumnRelations(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a text, b int, c int, d int, primary key ((a, b), c))") as table:
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 1, 1)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 2, 2)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 3, 3)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 4, 4, 4)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 1, 1, 1)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 4, 4)

        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ?", "first", 2),
                   ["first", 2, 2, 2])

        assert_rows(execute(cql, table, "select * from %s where a in (?, ?) and b in (?, ?)", "first", "second", 2, 3),
                   ["first", 2, 2, 2],
                   ["first", 3, 3, 3])

        assert_rows(execute(cql, table, "select * from %s where a in (?, ?) and b = ?", "first", "second", 4),
                   ["first", 4, 4, 4],
                   ["second", 4, 4, 4])

        assert_rows(execute(cql, table, "select * from %s where a = ? and b in (?, ?)", "first", 3, 4),
                   ["first", 3, 3, 3],
                   ["first", 4, 4, 4])

        assert_rows(execute(cql, table, "select * from %s where a in (?, ?) and b in (?, ?)", "first", "second", 1, 4),
                   ["first", 1, 1, 1],
                   ["first", 4, 4, 4],
                   ["second", 1, 1, 1],
                   ["second", 4, 4, 4])

        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "select * from %s where a in (?, ?)", "first", "second")
        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "select * from %s where a = ?", "first")
        # Scylla does allow such queries, and their correctness is tested in
        # test_filtering.py::test_multiple_restrictions_on_same_column
        #assert_invalid_message(cql, table, "b cannot be restricted by more than one relation if it includes a IN",
        #                     "select * from %s where a = ? AND b IN (?, ?) AND b = ?", "first", 2, 2, 3)
        #assert_invalid_message(cql, table, "b cannot be restricted by more than one relation if it includes an Equal",
        #                     "select * from %s where a = ? AND b = ? AND b IN (?, ?)", "first", 2, 2, 3)
        #assert_invalid_message(cql, table, "a cannot be restricted by more than one relation if it includes a IN",
        #                     "select * from %s where a IN (?, ?) AND a = ? AND b = ?", "first", "second", "first", 3)
        #assert_invalid_message(cql, table, "a cannot be restricted by more than one relation if it includes an Equal",
        #                     "select * from %s where a = ? AND a IN (?, ?) AND b IN (?, ?)", "first", "second", "first", 2, 3)

def testClusteringColumnRelationsWithClusteringOrder(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a text, b int, c int, d int, primary key (a, b, c)) WITH CLUSTERING ORDER BY (b DESC, c ASC)") as table:
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4)

        assert_rows(execute(cql, table, "select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b DESC",
                           "first", 7, 6, 3, 2),
                   ["first", 3, 7, 3],
                   ["first", 2, 6, 2])

        assert_rows(execute(cql, table, "select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b ASC",
                           "first", 7, 6, 3, 2),
                   ["first", 2, 6, 2],
                   ["first", 3, 7, 3])

def testAllowFilteringWithClusteringColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, v int, primary key (k, c))") as table:
        execute(cql, table, "INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 1, 2, 1)
        execute(cql, table, "INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 1, 3, 2)
        execute(cql, table, "INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 2, 2, 3)

        # Don't require filtering, always allowed
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 1),
                   [1, 2, 1],
                   [1, 3, 2])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND c > ?", 1, 2), [1, 3, 2])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND c = ?", 1, 2), [1, 2, 1])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? ALLOW FILTERING", 1),
                   [1, 2, 1],
                   [1, 3, 2])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND c > ? ALLOW FILTERING", 1, 2), [1, 3, 2])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? AND c = ? ALLOW FILTERING", 1, 2), [1, 2, 1])

        # Require filtering, allowed only with ALLOW FILTERING
        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = ?", 2)
        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > ? AND c <= ?", 2, 4)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c = ? ALLOW FILTERING", 2),
                   [1, 2, 1],
                   [2, 2, 3])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > ? AND c <= ? ALLOW FILTERING", 2, 4), [1, 3, 2])

def testAllowFilteringWithIndexedColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, a int, b int)") as table:
        execute(cql, table, "CREATE INDEX ON %s(a)")

        execute(cql, table, "INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 1, 10, 100)
        execute(cql, table, "INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 2, 20, 200)
        execute(cql, table, "INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 3, 30, 300)
        execute(cql, table, "INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 4, 40, 400)

        # Don't require filtering, always allowed
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 1), [1, 10, 100])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ?", 20), [2, 20, 200])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ? ALLOW FILTERING", 1), [1, 10, 100])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? ALLOW FILTERING", 20), [2, 20, 200])

        assert_invalid(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ?")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? ALLOW FILTERING", 20, 200), [2, 20, 200])

def testAllowFilteringWithIndexedColumnAndStaticColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, s int static, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "CREATE INDEX ON %s(c)")

        execute(cql, table, "INSERT INTO %s(a, b, c, s) VALUES(?, ?, ?, ?)", 1, 1, 1, 1)
        execute(cql, table, "INSERT INTO %s(a, b, c) VALUES(?, ?, ?)", 1, 2, 1)
        execute(cql, table, "INSERT INTO %s(a, s) VALUES(?, ?)", 3, 3)
        execute(cql, table, "INSERT INTO %s(a, b, c, s) VALUES(?, ?, ?, ?)", 2, 1, 1, 2)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c = ? AND s > ? ALLOW FILTERING", 1, 1),
                   [2, 1, 2, 1])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c = ? AND s < ? ALLOW FILTERING", 1, 2),
                   [1, 1, 1, 1],
                   [1, 2, 1, 1])

def testIndexQueriesOnComplexPrimaryKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk0 int, pk1 int, ck0 int, ck1 int, ck2 int, value int, PRIMARY KEY ((pk0, pk1), ck0, ck1, ck2))") as table:
        execute(cql, table, "CREATE INDEX ON %s(ck1)")
        execute(cql, table, "CREATE INDEX ON %s(ck2)")
        execute(cql, table, "CREATE INDEX ON %s(pk0)")
        execute(cql, table, "CREATE INDEX ON %s(ck0)")

        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 0, 1, 2, 3, 4, 5)
        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 1, 2, 3, 4, 5, 0)
        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 2, 3, 4, 5, 0, 1)
        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 3, 4, 5, 0, 1, 2)
        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 4, 5, 0, 1, 2, 3)
        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 5, 0, 1, 2, 3, 4)

        assert_rows(execute(cql, table, "SELECT value FROM %s WHERE pk0 = 2"), [1])
        assert_rows(execute(cql, table, "SELECT value FROM %s WHERE ck0 = 0"), [3])
        assert_rows(execute(cql, table, "SELECT value FROM %s WHERE pk0 = 3 AND pk1 = 4 AND ck1 = 0"), [2])
        assert_rows(execute(cql, table, "SELECT value FROM %s WHERE pk0 = 5 AND pk1 = 0 AND ck0 = 1 AND ck2 = 3 ALLOW FILTERING"), [4])

def testIndexOnClusteringColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id1 int, id2 int, author text, time bigint, v1 text, v2 text, PRIMARY KEY ((id1, id2), author, time))") as table:
        execute(cql, table, "CREATE INDEX ON %s(time)")
        execute(cql, table, "CREATE INDEX ON %s(id2)")

        execute(cql, table, "INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 0, 'A', 'A')")
        execute(cql, table, "INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 1, 'B', 'B')")
        execute(cql, table, "INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 1, 'bob', 2, 'C', 'C')")
        execute(cql, table, "INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 0, 'tom', 0, 'D', 'D')")
        execute(cql, table, "INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 1, 'tom', 1, 'E', 'E')")

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE time = 1"), ["B"], ["E"])

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE id2 = 1"), ["C"], ["E"])

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 0"), ["A"])

        # Test for CASSANDRA-8206
        execute(cql, table, "UPDATE %s SET v2 = null WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 1")

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE id2 = 0"), ["A"], ["B"], ["D"])

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE time = 1"), ["B"], ["E"])

        # Scylla does support IN restrictions of any column when ALLOW
        # FILTERING is used, so the following does work. See
        # test_filtering.py::test_filter_in_restriction for a test that
        # this support is correct.
        #assert_invalid_message(cql, table, "IN restrictions are not supported on indexed columns",
        #                     "SELECT v1 FROM %s WHERE id2 = 0 and time IN (1, 2) ALLOW FILTERING")

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE author > 'ted' AND time = 1 ALLOW FILTERING"), ["E"])
        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE author > 'amy' AND author < 'zoe' AND time = 0 ALLOW FILTERING"),
                           ["A"], ["D"])

def testCompositeIndexWithPrimaryKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(blog_id int, time1 int, time2 int, author text, content text,  PRIMARY KEY (blog_id, time1, time2))") as table:
        execute(cql, table, "CREATE INDEX ON %s(author)")
        req = "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)"
        execute(cql, table, req, 1, 0, 0, "foo", "bar1")
        execute(cql, table, req, 1, 0, 1, "foo", "bar2")
        execute(cql, table, req, 2, 1, 0, "foo", "baz")
        execute(cql, table, req, 3, 0, 1, "gux", "qux")

        assert_rows(execute(cql, table, "SELECT blog_id, content FROM %s WHERE author='foo'"),
                   [1, "bar1"],
                   [1, "bar2"],
                   [2, "baz"])
        assert_rows(execute(cql, table, "SELECT blog_id, content FROM %s WHERE time1 > 0 AND author='foo' ALLOW FILTERING"), [2, "baz"])
        assert_rows(execute(cql, table, "SELECT blog_id, content FROM %s WHERE time1 = 1 AND author='foo' ALLOW FILTERING"), [2, "baz"])
        assert_rows(execute(cql, table, "SELECT blog_id, content FROM %s WHERE time1 = 1 AND time2 = 0 AND author='foo' ALLOW FILTERING"),
                   [2, "baz"])
        assert_empty(execute(cql, table, "SELECT content FROM %s WHERE time1 = 1 AND time2 = 1 AND author='foo' ALLOW FILTERING"))
        assert_empty(execute(cql, table, "SELECT content FROM %s WHERE time1 = 1 AND time2 > 0 AND author='foo' ALLOW FILTERING"))

        # Scylla and Cassandra chose to print different errors in this case -
        # Cassandra says that ALLOW FILTERING would have made this query
        # work, while Scylla says that time1 should have also been
        # restricted.
        assert_invalid(cql, table,
                             "SELECT content FROM %s WHERE time2 >= 0 AND author='foo'")

def testRangeQueryOnIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int primary key, row int, setid int)") as table:
        execute(cql, table, "CREATE INDEX ON %s(setid)")

        q = "INSERT INTO %s (id, row, setid) VALUES (?, ?, ?);"
        execute(cql, table, q, 0, 0, 0)
        execute(cql, table, q, 1, 1, 0)
        execute(cql, table, q, 2, 2, 0)
        execute(cql, table, q, 3, 3, 0)

        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE setid = 0 AND row < 1;")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE setid = 0 AND row < 1 ALLOW FILTERING;"), [0, 0, 0])

def testEmptyIN(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, k2 int, v int, PRIMARY KEY (k1, k2))") as table:
        for i in range(3):
            for j in range(3):
                execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", i, j, i + j)

        assert_empty(execute(cql, table, "SELECT v FROM %s WHERE k1 IN ()"))
        assert_empty(execute(cql, table, "SELECT v FROM %s WHERE k1 = 0 AND k2 IN ()"))

def testINWithDuplicateValue(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, k2 int, v int, PRIMARY KEY (k1, k2))") as table:
        execute(cql, table, "INSERT INTO %s (k1,  k2, v) VALUES (?, ?, ?)", 1, 1, 1)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k1 IN (?, ?)", 1, 1),
                   [1, 1, 1])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k1 IN (?, ?) AND k2 IN (?, ?)", 1, 1, 1, 1),
                   [1, 1, 1])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k1 = ? AND k2 IN (?, ?)", 1, 1, 1),
                   [1, 1, 1])

@pytest.mark.xfail(reason="#10577 - max-clustering-key-restrictions-per-query is too low for this test")
def testLargeClusteringINValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, v int, PRIMARY KEY (k, c))") as table:
        execute(cql, table, "INSERT INTO %s (k, c, v) VALUES (0, 0, 0)")
        inValues = list(range(10000))
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k=? AND c IN ?", 0, inValues),
                [0, 0, 0])

def testMultiplePartitionKeyWithIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, f int, PRIMARY KEY ((a, b), c, d, e))") as table:
        execute(cql, table, "CREATE INDEX ON %s (c)")
        execute(cql, table, "CREATE INDEX ON %s (f)")

        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 1, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 1, 1, 2)

        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 0, 0, 3)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 1, 0, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 1, 1, 5)

        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 2, 0, 0, 5)

        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND c = ?", 0, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND c = ? ALLOW FILTERING", 0, 1),
                   [0, 0, 1, 0, 0, 3],
                   [0, 0, 1, 1, 0, 4],
                   [0, 0, 1, 1, 1, 5])

        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND c = ? AND d = ?", 0, 1, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND c = ? AND d = ? ALLOW FILTERING", 0, 1, 1),
                   [0, 0, 1, 1, 0, 4],
                   [0, 0, 1, 1, 1, 5])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND c IN (?) AND  d IN (?) ALLOW FILTERING", 0, 1, 1),
                [0, 0, 1, 1, 0, 4],
                [0, 0, 1, 1, 1, 5])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) ALLOW FILTERING", 0, 1, 1),
                [0, 0, 1, 1, 0, 4],
                [0, 0, 1, 1, 1, 5],
                [0, 0, 2, 0, 0, 5])

        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND f = ?", 0, 0, 1, 5)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND f = ? ALLOW FILTERING", 0, 1, 3, 5),
                   [0, 0, 1, 1, 1, 5])

        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND f = ?", 0, 1, 2, 5)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND f = ? ALLOW FILTERING", 0, 1, 2, 5),
                   [0, 0, 1, 1, 1, 5],
                   [0, 0, 2, 0, 0, 5])

        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND d IN (?) AND f = ?", 0, 1, 3, 0, 3)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND d IN (?) AND f = ? ALLOW FILTERING", 0, 1, 3, 0, 3),
                   [0, 0, 1, 0, 0, 3])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND c >= ? ALLOW FILTERING", 0, 1),
                [0, 0, 1, 0, 0, 3],
                [0, 0, 1, 1, 0, 4],
                [0, 0, 1, 1, 1, 5],
                [0, 0, 2, 0, 0, 5])

        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND c >= ? AND f = ?", 0, 1, 5)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND c >= ? AND f = ?", 0, 0, 1, 5),
                   [0, 0, 1, 1, 1, 5],
                   [0, 0, 2, 0, 0, 5])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND c >= ? AND f = ? ALLOW FILTERING", 0, 1, 5),
                   [0, 0, 1, 1, 1, 5],
                   [0, 0, 2, 0, 0, 5])

        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND c = ? AND d >= ? AND f = ?", 0, 1, 1, 5)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND c = ? AND d >= ? AND f = ?", 0, 0, 1, 1, 5),
                   [0, 0, 1, 1, 1, 5])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND c = ? AND d >= ? AND f = ? ALLOW FILTERING", 0, 1, 1, 5),
                   [0, 0, 1, 1, 1, 5])

def testFunctionCallWithUnset(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s text, i int)") as table:
        # The error messages in Scylla and Cassandra here are slightly
        # different.
        assert_invalid_message(cql, table, "unset",
                             "SELECT * FROM %s WHERE token(k) >= token(?)", UNSET_VALUE)
        assert_invalid_message(cql, table, "unset",
                             "SELECT * FROM %s WHERE k = blobAsInt(?)", UNSET_VALUE)

def testLimitWithUnset(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, i int)") as table:
        execute(cql, table, "INSERT INTO %s (k, i) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (k, i) VALUES (2, 1)")
        assert_rows(execute(cql, table, "SELECT k FROM %s LIMIT ?", UNSET_VALUE), # treat as 'unlimited'
                [1],
                [2]
        )

@pytest.mark.xfail(reason="#10358 - comparison with unset doesn't generate error")
def testWithUnsetValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, i int, j int, s text, PRIMARY KEY (k,i,j))") as table:
        execute(cql, table, "CREATE INDEX ON %s (s)")
        # partition key
        # Test commented out because the Python driver can't send an
        # UNSET_VALUE for the partition key (it is needed to decide
        # which coordinator to send the request to!)
        #assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k = ?", UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k IN ?", UNSET_VALUE)
        # Test commented out because the Python driver can't send an
        # UNSET_VALUE for the partition key (it is needed to decide
        # which coordinator to send the request to!)
        #assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k IN(?)", UNSET_VALUE)
        #assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k IN(?,?)", 1, UNSET_VALUE)
        # clustering column
        # Reproduces #10358:
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k = 1 AND i = ?", UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k = 1 AND i IN ?", UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k = 1 AND i IN(?)", UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k = 1 AND i IN(?,?)", 1, UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE i = ? ALLOW FILTERING", UNSET_VALUE)
        # indexed column
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE s = ?", UNSET_VALUE)
        # range
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k = 1 AND i > ?", UNSET_VALUE)

def testInvalidSliceRestrictionOnPartitionKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c text)") as table:
        # Scylla and Cassandra choose to print a different error message
        # here: Cassandra tells you this query would have worked with
        # ALLOW FILTERING, while Scylla *also* tells you that this
        # query would have worked with EQ or IN relations or with token().
        # The word "filtering" is common to both messages.
        assert_invalid_message(cql, table, 'FILTERING',
                             "SELECT * FROM %s WHERE a >= 1 and a < 4")
        # Again, different error messages. Cassandra says "Multi-column
        # relations can only be applied to clustering columns but was
        # applied to: a", Scylla says "Only EQ and IN relation are supported
        # on the partition key (unless you use the token() function or allow
        # filtering)". There is no word in common :-(
        assert_invalid(cql, table,
                             "SELECT * FROM %s WHERE (a) >= (1) and (a) < (4)")

def testInvalidMulticolumnSliceRestrictionOnPartitionKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c text, PRIMARY KEY ((a, b)))") as table:
        assert_invalid_message(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b) >= (1, 1) and (a, b) < (4, 1)")
        # Again, different error messages. Cassandra says "Multi-column
        # relations can only be applied to clustering columns but was
        # applied to: a", Scylla says "Only EQ and IN relation are supported
        # on the partition key (unless you use the token() function or allow
        # filtering)". There is no word in common :-(
        assert_invalid(cql, table,
                             "SELECT * FROM %s WHERE a >= 1 and (a, b) < (4, 1)")
        assert_invalid(cql, table,
                             "SELECT * FROM %s WHERE b >= 1 and (a, b) < (4, 1)")
        assert_invalid_message(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b) >= (1, 1) and (b) < (4)")
        assert_invalid_message(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: b",
                             "SELECT * FROM %s WHERE (b) < (4) and (a, b) >= (1, 1)")
        assert_invalid_message(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b) >= (1, 1) and a = 1")

def testInvalidColumnNames(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c map<int, int>, PRIMARY KEY (a, b))") as table:
        # Slightly different error messages in Scylla and Cassandra. Both
        # include the string "name d".
        assert_invalid_message(cql, table, "name d", "SELECT * FROM %s WHERE d = 0")
        assert_invalid_message(cql, table, "name d", "SELECT * FROM %s WHERE d IN (0, 1)")
        assert_invalid_message(cql, table, "name d", "SELECT * FROM %s WHERE d > 0 and d <= 2")
        assert_invalid_message(cql, table, "name d", "SELECT * FROM %s WHERE d CONTAINS 0")
        assert_invalid_message(cql, table, "name d", "SELECT * FROM %s WHERE d CONTAINS KEY 0")
        # Here, Cassandra says "Undefined column name d" but Scylla gives
        # a clearer error message about the real cause: "Aliases aren't
        # allowed in the where clause ('d = 0')".
        assert_invalid(cql, table, "SELECT a AS d FROM %s WHERE d = 0")
        assert_invalid(cql, table, "SELECT b AS d FROM %s WHERE d IN (0, 1)")
        assert_invalid(cql, table, "SELECT b AS d FROM %s WHERE d > 0 and d <= 2")
        assert_invalid(cql, table, "SELECT c AS d FROM %s WHERE d CONTAINS 0")
        assert_invalid(cql, table, "SELECT c AS d FROM %s WHERE d CONTAINS KEY 0")
        assert_invalid_message(cql, table, "name d", "SELECT d FROM %s WHERE a = 0")

@pytest.mark.xfail(reason="#10632 - strange error message")
def testInvalidNonFrozenUDTRelation(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as type:
        with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b {type})") as table:
            udt = user_type("a", 1)
            ks, t = type.split('.')

            # All operators
            # As decided https://issues.apache.org/jira/browse/CASSANDRA-13247,
            # Cassandra does not allow restrictions on non-frozen UDTs. 
            # Scylla does implement them, so the commented out tests below
            # are not relevant (Scylla will complain that ALLOW FILTERING
            # is missing, not about the non-frozen UDT).
            msg = "Non-frozen UDT column 'b' (" + t + ") cannot be restricted by any relation"
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b = ?", udt)
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b > ?", udt)
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b < ?", udt)
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b >= ?", udt)
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b <= ?", udt)
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b IN (?)", udt)
            # Scylla and Cassandra print different errors here - Scylla
            # says that b is not a string, Cassandra says it is a non-frozen
            # UDT.
            assert_invalid(cql, table, "SELECT * FROM %s WHERE b LIKE ?", udt)
            assert_invalid_message(cql, table, "Unsupported \"!=\" relation",
                             "SELECT * FROM %s WHERE b != {a: 0}", udt)
            # Reproduces #10632:
            assert_invalid_message(cql, table, "b IS NOT",
                             "SELECT * FROM %s WHERE b IS NOT NULL", udt)
            assert_invalid_message(cql, table, "Cannot use CONTAINS on non-collection column",
                             "SELECT * FROM %s WHERE b CONTAINS ?", udt)
