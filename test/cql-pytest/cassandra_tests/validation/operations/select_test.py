# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit a87055d56a33a9b17606f14535f48eb461965b82
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *
from uuid import UUID
from cassandra.query import UNSET_VALUE
from cassandra.util import Duration

REQUIRES_ALLOW_FILTERING_MESSAGE = "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"

def testSingleClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(p text, c text, v text, s text static, PRIMARY KEY (p, c))") as table:
        execute(cql, table, "INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1")
        execute(cql, table, "INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2")
        execute(cql, table, "INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=?", "p1"),
            ["p1", "k1", "sv1", "v1"],
            ["p1", "k2", "sv1", "v2"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=?", "p2"),
            ["p2", None, "sv2", None])

        # Ascending order
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p1"),
            ["p1", "k1", "sv1", "v1"],
            ["p1", "k2", "sv1", "v2"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p2"),
            ["p2", None, "sv2", None])

        # Descending order
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p1"),
            ["p1", "k2", "sv1", "v2"],
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p2"),
            ["p2", None, "sv2", None])

        # No order with one relation
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"],
            ["p1", "k2", "sv1", "v2"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k2"),
            ["p1", "k2", "sv1", "v2"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k3"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c =?", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k0"))

        # Ascending with one relation
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"],
            ["p1", "k2", "sv1", "v2"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k2"),
            ["p1", "k2", "sv1", "v2"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k3"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c =? ORDER BY c ASC", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k0"))

        # Descending with one relation
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k1"),
            ["p1", "k2", "sv1", "v2"],
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k2"),
            ["p1", "k2", "sv1", "v2"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k3"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c =? ORDER BY c DESC", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k0"))

        # IN
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c IN (?, ?)", "p1", "k1", "k2"),
            ["p1", "k1", "sv1", "v1"],
            ["p1", "k2", "sv1", "v2"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c ASC", "p1", "k1", "k2"),
            ["p1", "k1", "sv1", "v1"],
            ["p1", "k2", "sv1", "v2"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c DESC", "p1", "k1", "k2"),
            ["p1", "k2", "sv1", "v2"],
            ["p1", "k1", "sv1", "v1"])

def testSingleClusteringReversed(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(p text, c text, v text, s text static, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)") as table:
        execute(cql, table, "INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1")
        execute(cql, table, "INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2")
        execute(cql, table, "INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=?", "p1"),
            ["p1", "k2", "sv1", "v2"],
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=?", "p2"),
            ["p2", None, "sv2", None])

        # Ascending order
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p1"),
            ["p1", "k1", "sv1", "v1"],
            ["p1", "k2", "sv1", "v2"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p2"),
            ["p2", None, "sv2", None])

        # Descending order
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p1"),
            ["p1", "k2", "sv1", "v2"],
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p2"),
            ["p2", None, "sv2", None])

        # No order with one relation
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k1"),
            ["p1", "k2", "sv1", "v2"],
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k2"),
            ["p1", "k2", "sv1", "v2"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k3"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c=?", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k0"))

        # Ascending with one relation
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"],
            ["p1", "k2", "sv1", "v2"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k2"),
            ["p1", "k2", "sv1", "v2"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k3"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c=? ORDER BY c ASC", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k0"))

        # Descending with one relation
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k1"),
            ["p1", "k2", "sv1", "v2"],
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k2"),
            ["p1", "k2", "sv1", "v2"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k3"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c=? ORDER BY c DESC", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k1"),
            ["p1", "k1", "sv1", "v1"])

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k0"))

        # IN
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c IN (?, ?)", "p1", "k1", "k2"),
            ["p1", "k2", "sv1", "v2"],
            ["p1", "k1", "sv1", "v1"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c ASC", "p1", "k1", "k2"),
            ["p1", "k1", "sv1", "v1"],
            ["p1", "k2", "sv1", "v2"])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c DESC", "p1", "k1", "k2"),
            ["p1", "k2", "sv1", "v2"],
            ["p1", "k1", "sv1", "v1"])

# Check query with KEY IN clause
# migrated from cql_tests.py:TestCQL.select_key_in_test()
def testSelectKeyIn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid uuid PRIMARY KEY, firstname text, lastname text, age int)") as table:
        id1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        id2 = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")

        execute(cql, table, "INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, 'Frodo', 'Baggins', 32)", id1)
        execute(cql, table, "INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, 'Samwise', 'Gamgee', 33)", id2)

        assert_row_count(execute(cql, table, "SELECT firstname, lastname FROM %s WHERE userid IN (?, ?)", id1, id2), 2)

def testSetContainsWithIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(account text, id int, categories set<text>, PRIMARY KEY (account, id))") as table:
        execute(cql, table, "CREATE INDEX ON %s(categories)")

        execute(cql, table, "INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, {"lmn"})

        for _ in before_and_after_flush(cql, table):
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "xyz", "lmn"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE categories CONTAINS ?", "lmn"),
                       ["test", 5, {"lmn"}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "lmn"),
                       ["test", 5, {"lmn"}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, "lmn"),
                       ["test", 5, {"lmn"}])

            # Scylla does not consider "= null" an error, it just matches nothing.
            # See issue #4776.
            #assert_invalid_message(cql, table, "Unsupported null value for column categories",
            #                     "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, None)

            assert_invalid_message(cql, table, "unset value",
                                 "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, UNSET_VALUE)

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS ?", "xyz", "lmn", "notPresent")
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS ? ALLOW FILTERING", "xyz", "lmn", "notPresent"))

def testListContainsWithIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(account text, id int, categories list<text>, PRIMARY KEY (account, id))") as table:
        execute(cql, table, "CREATE INDEX ON %s(categories)")

        execute(cql, table, "INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, ["lmn"])
        for _ in before_and_after_flush(cql, table):
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "xyz", "lmn"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?;", "test", "lmn"),
                       ["test", 5, ["lmn"]])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE categories CONTAINS ?", "lmn"),
                       ["test", 5, ["lmn"]])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?;", "test", 5, "lmn"),
                       ["test", 5, ["lmn"]])

            # Scylla does not consider "= null" an error, it just matches nothing.
            # See issue #4776.
            #assert_invalid_message(cql, table, "Unsupported null value for column categories",
            #                     "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, None)

            assert_invalid_message(cql, table, "unset value",
                                 "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, UNSET_VALUE)

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ?",
                                 "test", 5, "lmn", "notPresent")
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ? ALLOW FILTERING",
                                "test", 5, "lmn", "notPresent"))

def testListContainsWithIndexAndFiltering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(e int PRIMARY KEY, f list<text>, s int)") as table:
        execute(cql, table, "CREATE INDEX ON %s(f)")
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (e, f, s) VALUES (?, ?, ?)", i, ["Dubai"], 4)
        for i in range(3, 5):
            execute(cql, table, "INSERT INTO %s (e, f, s) VALUES (?, ?, ?)", i, ["Dubai"], 3)

        for _ in before_and_after_flush(cql, table):
            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE f CONTAINS ? AND s=? allow filtering", "Dubai", 3),
                       [4, ["Dubai"], 3],
                       [3, ["Dubai"], 3])

def testMapKeyContainsWithIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(account text, id int, categories map<text, text>, PRIMARY KEY (account, id))") as table:
        execute(cql, table, "CREATE INDEX ON %s(keys(categories))")

        execute(cql, table, "INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, {"lmn": "foo"})

        for _ in before_and_after_flush(cql, table):
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "xyz", "lmn"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "test", "lmn"),
                       ["test", 5, {"lmn": "foo"}])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE categories CONTAINS KEY ?", "lmn"),
                       ["test", 5, {"lmn": "foo"}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ?", "test", 5, "lmn"),
                       ["test", 5, {"lmn": "foo"}])

            # Scylla does not consider "= null" an error, it just matches nothing.
            # See issue #4776.
            #assert_invalid_message(cql, table, "Unsupported null value for column categories",
            #                     "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ?", "test", 5, None)

            assert_invalid_message(cql, table, "unset value",
                                 "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ?", "test", 5, UNSET_VALUE)

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ? AND categories CONTAINS KEY ?",
                                 "test", 5, "lmn", "notPresent")
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ? AND categories CONTAINS KEY ? ALLOW FILTERING",
                                "test", 5, "lmn", "notPresent"))

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ? AND categories CONTAINS ?",
                                 "test", 5, "lmn", "foo")

def testMapValueContainsWithIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(account text, id int, categories map<text, text>, PRIMARY KEY (account, id))") as table:
        execute(cql, table, "CREATE INDEX ON %s(categories)")

        execute(cql, table, "INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, {"lmn": "foo"})

        for _ in before_and_after_flush(cql, table):
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "xyz", "foo"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "foo"),
                       ["test", 5, {"lmn": "foo"}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE categories CONTAINS ?", "foo"),
                       ["test", 5, {"lmn": "foo"}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, "foo"),
                       ["test", 5, {"lmn": "foo"}])

            # Scylla does not consider "= null" an error, it just matches nothing.
            # See issue #4776.
            #assert_invalid_message(cql, table, "Unsupported null value for column categories",
            #                     "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, None)

            assert_invalid_message(cql, table, "unset value",
                                 "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, UNSET_VALUE)

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ?",
                                 "test", 5, "foo", "notPresent")

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ? ALLOW FILTERING",
                                "test", 5, "foo", "notPresent"))

# See CASSANDRA-7525
def testQueryMultipleIndexTypes(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(account text, id int, categories map<text, text>, PRIMARY KEY (account, id))") as table:
        # create an index on
        execute(cql, table, "CREATE INDEX ON %s(id)")
        execute(cql, table, "CREATE INDEX ON %s(categories)")

        for _ in before_and_after_flush(cql, table):
            execute(cql, table, "INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, {"lmn": "foo"})

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE categories CONTAINS ? AND id = ? ALLOW FILTERING", "foo", 5),
                       ["test", 5, {"lmn": "foo"}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND id = ? ALLOW FILTERING", "test", "foo", 5),
                       ["test", 5, {"lmn": "foo"}])

# See CASSANDRA-8033
def testFilterWithIndexForContains(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, k2 int, v set<int>, PRIMARY KEY ((k1, k2)))") as table:
        execute(cql, table, "CREATE INDEX ON %s(k2)")
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 0, {1, 2, 3})
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 1, {2, 3, 4})
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 0, {3, 4, 5})
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 1, {4, 5, 6})

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k2 = ?", 1),
                       [0, 1, {2, 3, 4}],
                       [1, 1, {4, 5, 6}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 6),
                       [1, 1, {4, 5, 6}])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 7))

# See CASSANDRA-8073
def testIndexLookupWithClusteringPrefix(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d set<int>, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "CREATE INDEX ON %s(d)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, {1, 2, 3})
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, {3, 4, 5})
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, {1, 2, 3})
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, {3, 4, 5})

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND d CONTAINS ?", 0, 1, 3),
                       [0, 1, 0, {1, 2, 3}],
                       [0, 1, 1, {3, 4, 5}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND d CONTAINS ?", 0, 1, 2),
                       [0, 1, 0, {1, 2, 3}])

            assert_rows(execute(cql, table,"SELECT * FROM %s WHERE a=? AND b=? AND d CONTAINS ?", 0, 1, 5),
                       [0, 1, 1, {3, 4, 5}])

def testContainsKeyAndContainsWithIndexOnMapKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(account text, id int, categories map<text, text>, PRIMARY KEY (account, id))") as table:
        execute(cql, table, "CREATE INDEX ON %s(keys(categories))")
        execute(cql, table, "INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, {"lmn": "foo"})
        execute(cql, table, "INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 6, {"lmn": "foo2"})

        for _ in before_and_after_flush(cql, table):
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "foo")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "test", "lmn"),
                       ["test", 5, {"lmn": "foo"}],
                       ["test", 6, {"lmn": "foo2"}])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ? AND categories CONTAINS ? ALLOW FILTERING",
                               "test", "lmn", "foo"),
                       ["test", 5, {"lmn": "foo"}])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS KEY ? ALLOW FILTERING",
                               "test", "foo", "lmn"),
                       ["test", 5, {"lmn": "foo"}])

def testContainsKeyAndContainsWithIndexOnMapValue(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(account text, id int, categories map<text, text>, PRIMARY KEY (account, id))") as table:
        execute(cql, table, "CREATE INDEX ON %s(categories)")
        execute(cql, table, "INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, {"lmn": "foo"})
        execute(cql, table, "INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 6, {"lmn2": "foo"})

        for _ in before_and_after_flush(cql, table):
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "test", "lmn")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "foo"),
                       ["test", 5, {"lmn": "foo"}],
                       ["test", 6, {"lmn2": "foo"}])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ? AND categories CONTAINS ? ALLOW FILTERING",
                               "test", "lmn", "foo"),
                       ["test", 5, {"lmn": "foo"}])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS KEY ? ALLOW FILTERING",
                               "test", "foo", "lmn"),
                       ["test", 5, {"lmn": "foo"}])

# Test token ranges
# migrated from cql_tests.py:TestCQL.token_range_test()
def testTokenRange(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, c int, v int)") as table:
        c = 100
        for i in range(c):
            execute(cql, table, "INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, i, i)

        res = list(execute(cql, table, "SELECT k FROM %s"))
        assert c == len(res)

        inOrder = [r[0] for r in res]

        min_token = -9223372036854775808
        res = list(execute(cql, table, f"SELECT k FROM %s WHERE token(k) >= {min_token}"))
        assert c == len(res)

        res = list(execute(cql, table, f"SELECT k FROM %s WHERE token(k) >= token({inOrder[32]}) AND token(k) < token({inOrder[65]})"))
        for i in range(32, 65):
            assert inOrder[i] == res[i - 32][0]

# Test select count
# migrated from cql_tests.py:TestCQL.count_test()
def testSelectCount(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(kind text, time int, value1 int, value2 int, PRIMARY KEY (kind, time))") as table:
        execute(cql, table, "INSERT INTO %s (kind, time, value1, value2) VALUES ('ev1', ?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (kind, time, value1, value2) VALUES ('ev1', ?, ?, ?)", 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (kind, time, value1) VALUES ('ev1', ?, ?)", 2, 2)
        execute(cql, table, "INSERT INTO %s (kind, time, value1, value2) VALUES ('ev1', ?, ?, ?)", 3, 3, 3)
        execute(cql, table, "INSERT INTO %s (kind, time, value1) VALUES ('ev1', ?, ?)", 4, 4)
        execute(cql, table, "INSERT INTO %s (kind, time, value1, value2) VALUES ('ev2', 0, 0, 0)")

        assert_rows(execute(cql, table, "SELECT COUNT(*) FROM %s WHERE kind = 'ev1'"),
                   [5])

        assert_rows(execute(cql, table, "SELECT COUNT(1) FROM %s WHERE kind IN ('ev1', 'ev2') AND time=0"),
                   [2])

# Range test query from CASSANDRA-4372
# migrated from cql_tests.py:TestCQL.range_query_test()
def testRangeQuery(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, f text, PRIMARY KEY (a, b, c, d, e))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 2, '2')")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, '1')")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 2, 1, '1')")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 3, '3')")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 5, '5')")

        assert_rows(execute(cql, table, "SELECT a, b, c, d, e, f FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 AND e >= 2"),
                   [1, 1, 1, 1, 2, "2"],
                   [1, 1, 1, 1, 3, "3"],
                   [1, 1, 1, 1, 5, "5"])

# Migrated from cql_tests.py:TestCQL.composite_row_key_test()
def testCompositeRowKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, k2 int, c int, v int, PRIMARY KEY ((k1, k2), c))") as table:
        for i in range(4):
            execute(cql, table, "INSERT INTO %s (k1, k2, c, v) VALUES (?, ?, ?, ?)", 0, i, i, i)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, 2, 2, 2],
                   [0, 3, 3, 3],
                   [0, 0, 0, 0],
                   [0, 1, 1, 1])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k1 = 0 and k2 IN (1, 3)"),
                   [0, 1, 1, 1],
                   [0, 3, 3, 3])

        assert_invalid(cql, table, "SELECT * FROM %s WHERE k2 = 3")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(k1, k2) = token(0, 1)"),
                   [0, 1, 1, 1])


        MIN_VALUE = -9223372036854775808
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(k1, k2) > ?", MIN_VALUE),
                   [0, 2, 2, 2],
                   [0, 3, 3, 3],
                   [0, 0, 0, 0],
                   [0, 1, 1, 1])

# Test for Cassandra-4532, NPE when trying to select a slice from a composite table
# migrated from cql_tests.py:TestCQL.bug_4532_test()
def testSelectSliceFromComposite(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(status ascii, ctime bigint, key ascii, nil ascii, PRIMARY KEY (status, ctime, key))") as table:
        execute(cql, table, "INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345678,'key1','')")
        execute(cql, table, "INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345678,'key2','')")
        execute(cql, table, "INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345679,'key3','')")
        execute(cql, table, "INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345679,'key4','')")
        execute(cql, table, "INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345679,'key5','')")
        execute(cql, table, "INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345680,'key6','')")

        assert_invalid(cql, table,"SELECT * FROM %s WHERE ctime>=12345679 AND key='key3' AND ctime<=12345680 LIMIT 3;")
        assert_invalid(cql, table,"SELECT * FROM %s WHERE ctime=12345679  AND key='key3' AND ctime<=12345680 LIMIT 3")

# Migrated from cql_tests.py:TestCQL.bug_4882_test()
def testDifferentOrdering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)") as table:
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (0, 1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 2, 2)")
        execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (0, 1, 3, 3)")

        assert_rows(execute(cql, table, "select * from %s where k = 0 limit 1"),
                   [0, 0, 2, 2])

# Migrated from cql_tests.py:TestCQL.allow_filtering_test()
def testAllowFiltering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, v int, PRIMARY KEY (k, c))") as table:
        for i in range(3):
            for j in range(3):
                execute(cql, table, "INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, j, j)

        # Don't require filtering, always allowed
        queries = [ "SELECT * FROM %s WHERE k = 1",
                    "SELECT * FROM %s WHERE k = 1 AND c > 2",
                    "SELECT * FROM %s WHERE k = 1 AND c = 2"]

        for q in queries:
            execute(cql, table, q)
            execute(cql, table, q + " ALLOW FILTERING")

        # Require filtering, allowed only with ALLOW FILTERING
        queries = [ "SELECT * FROM %s WHERE c = 2",
                     "SELECT * FROM %s WHERE c > 2 AND c <= 4"]

        for q in queries:
            assert_invalid(cql, table, q)
            execute(cql, table, q + " ALLOW FILTERING")

    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, a int, b int)") as table:
        execute(cql, table, "CREATE INDEX ON %s (a)")

        for i in range(5):
            execute(cql, table, "INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i * 10, i * 100)

        # Don't require filtering, always allowed
        queries = [ "SELECT * FROM %s WHERE k = 1",
                    "SELECT * FROM %s WHERE a = 20"]

        for q in queries:
            execute(cql, table, q)
            execute(cql, table, q + " ALLOW FILTERING")

        # Require filtering, allowed only with ALLOW FILTERING
        queries = [ "SELECT * FROM %s WHERE a = 20 AND b = 200" ]

        for q in queries:
            assert_invalid(cql, table, q)
            execute(cql, table, q + " ALLOW FILTERING")

# Test for bug from CASSANDRA-5122,
# migrated from cql_tests.py:TestCQL.composite_partition_key_validation_test()
def testSelectOnCompositeInvalid(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b text, c uuid, PRIMARY KEY ((a,b)))") as table:
        execute(cql, table, "INSERT INTO %s (a, b , c ) VALUES (1, 'aze', 4d481800-4c5f-11e1-82e0-3f484de45426)")
        execute(cql, table, "INSERT INTO %s (a, b , c ) VALUES (1, 'ert', 693f5800-8acb-11e3-82e0-3f484de45426)")
        execute(cql, table, "INSERT INTO %s (a, b , c ) VALUES (1, 'opl', d4815800-2d8d-11e0-82e0-3f484de45426)")

        assert_row_count(execute(cql, table, "SELECT * FROM %s"), 3)
        assert_invalid(cql, table, "SELECT * FROM %s WHERE a=1")

# Migrated from cql_tests.py:TestCQL.multi_in_test()
def testMultiSelects(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(group text, zipcode text, state text, fips_regions int, city text, PRIMARY KEY (group, zipcode, state, fips_regions))") as table:
        str = "INSERT INTO %s (group, zipcode, state, fips_regions, city) VALUES (?, ?, ?, ?, ?)"
        execute(cql, table, str, "test", "06029", "CT", 9, "Ellington")
        execute(cql, table, str, "test", "06031", "CT", 9, "Falls Village")
        execute(cql, table, str, "test", "06902", "CT", 9, "Stamford")
        execute(cql, table, str, "test", "06927", "CT", 9, "Stamford")
        execute(cql, table, str, "test", "10015", "NY", 36, "New York")
        execute(cql, table, str, "test", "07182", "NJ", 34, "Newark")
        execute(cql, table, str, "test", "73301", "TX", 48, "Austin")
        execute(cql, table, str, "test", "94102", "CA", 6, "San Francisco")

        execute(cql, table, str, "test2", "06029", "CT", 9, "Ellington")
        execute(cql, table, str, "test2", "06031", "CT", 9, "Falls Village")
        execute(cql, table, str, "test2", "06902", "CT", 9, "Stamford")
        execute(cql, table, str, "test2", "06927", "CT", 9, "Stamford")
        execute(cql, table, str, "test2", "10015", "NY", 36, "New York")
        execute(cql, table, str, "test2", "07182", "NJ", 34, "Newark")
        execute(cql, table, str, "test2", "73301", "TX", 48, "Austin")
        execute(cql, table, str, "test2", "94102", "CA", 6, "San Francisco")

        assert_row_count(execute(cql, table, "select zipcode from %s"), 16)
        assert_row_count(execute(cql, table, "select zipcode from %s where group='test'"), 8)
        assert_invalid(cql, table, "select zipcode from %s where zipcode='06902'")
        assert_row_count(execute(cql, table, "select zipcode from %s where zipcode='06902' ALLOW FILTERING"), 2)
        assert_row_count(execute(cql, table, "select zipcode from %s where group='test' and zipcode='06902'"), 1)
        assert_row_count(execute(cql, table, "select zipcode from %s where group='test' and zipcode IN ('06902','73301','94102')"), 3)
        assert_row_count(execute(cql, table, "select zipcode from %s where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA')"), 2)
        assert_row_count(execute(cql, table, "select zipcode from %s where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions = 9"), 1)
        assert_row_count(execute(cql, table, "select zipcode from %s where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') ORDER BY zipcode DESC"), 2)
        assert_row_count(execute(cql, table, "select zipcode from %s where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions > 0"), 2)
        assert_empty(execute(cql, table, "select zipcode from %s where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions < 0"))

# Migrated from cql_tests.py:TestCQL.ticket_5230_test()
def testMultipleClausesOnPrimaryKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key text, c text, v text, PRIMARY KEY (key, c))") as table:
        execute(cql, table, "INSERT INTO %s (key, c, v) VALUES ('foo', '1', '1')")
        execute(cql, table, "INSERT INTO %s(key, c, v) VALUES ('foo', '2', '2')")
        execute(cql, table, "INSERT INTO %s(key, c, v) VALUES ('foo', '3', '3')")
        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE key = 'foo' AND c IN ('1', '2')"),
                   ["1"], ["2"])

# Migrated from cql_tests.py:TestCQL.bug_5404()
def testSelectWithToken(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key text PRIMARY KEY)") as table:
        # We just want to make sure this doesn 't NPE server side
        assert_invalid(cql, table, "select * from %s where token(key) > token(int(3030343330393233)) limit 1")

# Migrated from cql_tests.py:TestCQL.clustering_order_and_functions_test()
def testFunctionsWithClusteringDesc(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, t timeuuid, PRIMARY KEY (k, t) ) WITH CLUSTERING ORDER BY (t DESC)") as table:
        for i in range(5):
            execute(cql, table, "INSERT INTO %s (k, t) VALUES (?, now())", i)
        execute(cql, table, "SELECT dateOf(t) FROM %s")

# Migrated from cql_tests.py:TestCQL.select_with_alias_test()
def testSelectWithAlias(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int PRIMARY KEY, name text)") as table:
        for id in range(5):
            execute(cql, table, "INSERT INTO %s (id, name) VALUES (?, ?) USING TTL 1000 AND TIMESTAMP 0", id, "name" + str(id))

        # test aliasing count( *)
        rs = execute(cql, table, "SELECT count(*) AS user_count FROM %s")
        assert rs.one()._asdict() == {"user_count": 5}

        # test aliasing regular value
        rs = execute(cql, table, "SELECT name AS user_name FROM %s WHERE id = 0")
        assert rs.one()._asdict() == {"user_name": "name0"}

        # test aliasing writetime
        rs = execute(cql, table, "SELECT writeTime(name) AS name_writetime FROM %s WHERE id = 0")
        assert rs.one()._asdict() == {"name_writetime": 0}

        # test aliasing ttl
        rs = execute(cql, table, "SELECT ttl(name) AS name_ttl FROM %s WHERE id = 0")
        assert rs.one().name_ttl >= 100 and rs.one().name_ttl <= 1000

        # test aliasing a regular function
        rs = execute(cql, table, "SELECT intAsBlob(id) AS id_blob FROM %s WHERE id = 0")
        assert rs.one()._asdict() == {"id_blob": bytearray([0,0,0,0])}

        # test that select throws a meaningful exception for aliases in where clause
        assert_invalid_message(cql, table, "user_id",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE user_id = 0")

        # test that select throws a meaningful exception for aliases in order by clause
        assert_invalid_message(cql, table, "user_name",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE id IN (0) ORDER BY user_name")

# Migrated from cql_tests.py:TestCQL.select_distinct_test()
def testSelectDistinct(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY ((pk0, pk1), ck0))") as table:
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 0, 0)", i, i)
            execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 1, 1)", i, i)

        assert_rows(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s LIMIT 1"),
                   [0, 0])

        assert_rows(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s LIMIT 3"),
                   [0, 0],
                   [2, 2],
                   [1, 1])

        # Test selection validation.
        assert_invalid_message(cql, table, "queries must request all the partition key columns", "SELECT DISTINCT pk0 FROM %s")
        assert_invalid_message(cql, table, "queries must only request partition key columns", "SELECT DISTINCT pk0, pk1, ck0 FROM %s")

# Migrated from cql_tests.py:TestCQL.select_distinct_with_deletions_test()
def testSelectDistinctWithDeletions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, c int, v int)") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, i, i)

        rows = list(execute(cql, table, "SELECT DISTINCT k FROM %s"))
        assert len(rows) == 10
        key_to_delete = rows[3][0]

        execute(cql, table, "DELETE FROM %s WHERE k=?", key_to_delete)

        rows = list(execute(cql, table, "SELECT DISTINCT k FROM %s"))
        assert len(rows) == 9

        rows = list(execute(cql, table, "SELECT DISTINCT k FROM %s LIMIT 5"))
        assert len(rows) == 5

        rows = list(execute(cql, table, "SELECT DISTINCT k FROM %s"))
        assert len(rows) == 9

def testSelectDistinctWithWhereClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, a int, b int, PRIMARY KEY (k, a))") as table:
        execute(cql, table, "CREATE INDEX ON %s (b)")

        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i, i)
            execute(cql, table, "INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i * 10, i * 10)

        # Cassandra's message is "SELECT DISTINCT with WHERE clause only
        # supports restriction by partition key and/or static columns."
        # Scylla's message was a bit different when we didn't support
        # static columns, but that's not the goal of this test (see next one).
        distinctQueryErrorMsg = "SELECT DISTINCT with WHERE"
        assert_invalid_message(cql, table, distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE a >= 80 ALLOW FILTERING")

        assert_invalid_message(cql, table, distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE k IN (1, 2, 3) AND a = 10")

        assert_invalid_message(cql, table, distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE b = 5")

        assert_rows(execute(cql, table, "SELECT DISTINCT k FROM %s WHERE k = 1"),
                   [1])
        assert_rows(execute(cql, table, "SELECT DISTINCT k FROM %s WHERE k IN (5, 6, 7)"),
                   [5],
                   [6],
                   [7])

    # With static columns
    with create_table(cql, test_keyspace, "(k int, a int, s int static, b int, PRIMARY KEY (k, a))") as table:
        execute(cql, table, "CREATE INDEX ON %s (b)")
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, a, b, s) VALUES (?, ?, ?, ?)", i, i, i, i)
            execute(cql, table, "INSERT INTO %s (k, a, b, s) VALUES (?, ?, ?, ?)", i, i * 10, i * 10, i * 10)

        assert_rows(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k = 5"),
                   [50])
        assert_rows(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k IN (5, 6, 7)"),
                   [50],
                   [60],
                   [70])

# Reproduces issue #10354
@pytest.mark.xfail(reason="#10354 - we forgot to allow SELECT DISTINCT filtering on static column")
def testSelectDistinctWithWhereClauseOnStaticColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, a int, s int static, s1 int static, b int, PRIMARY KEY (k, a))") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", i, i, i, i, i)
            execute(cql, table, "INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", i, i * 10, i * 10, i * 10, i * 10)

        execute(cql, table, "INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", 2, 10, 10, 10, 10)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT DISTINCT k, s, s1 FROM %s WHERE s = 90 AND s1 = 90 ALLOW FILTERING"),
                       [9, 90, 90])

            assert_rows(execute(cql, table, "SELECT DISTINCT k, s, s1 FROM %s WHERE s = 90 AND s1 = 90 ALLOW FILTERING"),
                       [9, 90, 90])

            assert_rows(execute(cql, table, "SELECT DISTINCT k, s, s1 FROM %s WHERE s = 10 AND s1 = 10 ALLOW FILTERING"),
                       [1, 10, 10],
                       [2, 10, 10])

            assert_rows(execute(cql, table, "SELECT DISTINCT k, s, s1 FROM %s WHERE k = 1 AND s = 10 AND s1 = 10 ALLOW FILTERING"),
                       [1, 10, 10])

# Migrated from cql_tests.py:TestCQL.bug_6327_test()
def testSelectInClauseAtOne(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, v int, PRIMARY KEY (k, v))") as table:
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, 0)")

        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k=0 AND v IN (1, 0)"),
                   [0])

# Test for the CASSANDRA-6579 'select count' paging bug,
# migrated from cql_tests.py:TestCQL.select_count_paging_test()
def testSelectCountPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(field1 text, field2 timeuuid, field3 boolean, PRIMARY KEY (field1, field2))") as table:
        execute(cql, table, "create index on %s (field3)")

        execute(cql, table, "insert into %s (field1, field2, field3) values ('hola', now(), false)")
        execute(cql, table, "insert into %s (field1, field2, field3) values ('hola', now(), false)")

        assert_rows(execute(cql, table, "select count(*) from %s where field3 = false limit 1"),
                   [2])

# Test for CASSANDRA-7105 bug,
# migrated from cql_tests.py:TestCQL.clustering_order_in_test()
def testClusteringOrder(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY ((a, b), c)) with clustering order by (c desc)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, 5, 6)")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=1 AND b=2 AND c IN (3)"),
                   [1, 2, 3])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=1 AND b=2 AND c IN (3, 4)"),
                   [1, 2, 3])

# Test for CASSANDRA-7105 bug,
# SELECT with IN on final column of composite and compound primary key fails
# migrated from cql_tests.py:TestCQL.bug7105_test()
def testSelectInFinalColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 2, 3, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 4, 6, 5)")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=1 AND b=2 ORDER BY b DESC"),
                   [1, 2, 3, 3])

def testAlias(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int PRIMARY KEY, name text)") as table:
        for i in range(5):
            execute(cql, table, "INSERT INTO %s (id, name) VALUES (?, ?) USING TTL 10 AND TIMESTAMP 0", i, str(i))

        # The error message in Cassandra and Scylla are different: Cassandra
        # Says "Undefined column name user_id", Scylla says "Aliases aren't
        # allowed in the where clause ('user_id = 0')
        assert_invalid_message(cql, table, "user_id",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE user_id = 0")

        # test that select throws a meaningful exception for aliases in order by clause
        # The error message in Cassandra and Scylla are different: Cassandra
        # Says "Undefined column name user_name", Scylla says "Aliases aren't
        # allowed in the order by clause (user_name)
        assert_invalid_message(cql, table, "user_name",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE id IN (0) ORDER BY user_name")


# Reproduces issue #10357
@pytest.mark.xfail(reason="#10357")
def testAllowFilteringOnPartitionKeyOnStaticColumnsWithRowsWithOnlyStaticValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, s int static, c int, d int, PRIMARY KEY (a, b))") as table:
        for i in range(5):
            execute(cql, table, "INSERT INTO %s (a, s) VALUES (?, ?)", i, i)
            if i != 2:
                for j in range(4):
                    execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, j, i + j)

        for _ in before_and_after_flush(cql, table):
            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND c = 2 AND s >= 1 ALLOW FILTERING"),
                                    [1, 2, 1, 2, 3],
                                    [3, 2, 3, 2, 5],
                                    [4, 2, 4, 2, 6])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND c = 2 AND s >= 1 LIMIT 2 ALLOW FILTERING"),
                       [1, 2, 1, 2, 3],
                       [4, 2, 4, 2, 6])

            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a >= 3 AND c = 2 AND s >= 1 LIMIT 2 ALLOW FILTERING"),
                                    [4, 2, 4, 2, 6],
                                    [3, 2, 3, 2, 5])

# Reproduces issue #10357
@pytest.mark.xfail(reason="#10357")
def testFilteringOnStaticColumnsWithRowsWithOnlyStaticValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, s int static, c int, d int, PRIMARY KEY (a, b))") as table:
        for i in range(5):
            execute(cql, table, "INSERT INTO %s (a, s) VALUES (?, ?)", i, i)
            if i != 2:
                for j in range(4):
                    execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, j, i + j)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c = 2 AND s >= 1 LIMIT 2 ALLOW FILTERING"),
                       [1, 2, 1, 2, 3],
                       [4, 2, 4, 2, 6])

# Reproduces #10357, #10358
@pytest.mark.xfail(reason="#10357, #10358")
def testFilteringWithoutIndices(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, s int static, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 2, 4, 8)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 3, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 4, 4, 8)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 3, 7, 12)")
        execute(cql, table, "UPDATE %s SET s = 1 WHERE a = 1")
        execute(cql, table, "UPDATE %s SET s = 2 WHERE a = 2")
        execute(cql, table, "UPDATE %s SET s = 3 WHERE a = 3")

        # Adds tomstones
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 1, 4, 8)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 2, 7, 12)")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1")
        execute(cql, table, "DELETE FROM %s WHERE a = 2 AND b = 2")

        for _ in before_and_after_flush(cql, table):
            # Checks filtering
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c = 4 AND d = 8")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c = 4 AND d = 8 ALLOW FILTERING"),
                       [1, 2, 1, 4, 8],
                       [1, 4, 1, 4, 8])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND d = 8")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 4 AND d = 8 ALLOW FILTERING"),
                       [1, 4, 1, 4, 8])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE s = 1 AND d = 12")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE s = 1 AND d = 12 ALLOW FILTERING"),
                       [1, 3, 1, 6, 12])

            # The first call fails differently in Scylla and Cassandra, and
            # the second call passes on Scylla - see discussion why, and why
            # we don't consider this a bug, in #5545.
            #assert_invalid_message(cql, table, "IN predicates on non-primary-key columns (c) is not yet supported",
            #                     "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7)")
            #assert_invalid_message(cql, table, "IN predicates on non-primary-key columns (c) is not yet supported",
            #                     "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING")

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       [1, 3, 1, 6, 12],
                       [2, 3, 2, 7, 12])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE s > 1")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE s > 1 ALLOW FILTERING"),
                       [2, 3, 2, 7, 12],
                       [3, None, 3, None, None])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b < 3 AND c <= 4 ALLOW FILTERING"),
                       [1, 2, 1, 4, 8])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c >= 3 AND c <= 6 ALLOW FILTERING"),
                       [1, 2, 1, 4, 8],
                       [1, 3, 1, 6, 12],
                       [1, 4, 1, 4, 8])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE s >= 1 LIMIT 2 ALLOW FILTERING"),
                       [1, 2, 1, 4, 8],
                       [1, 3, 1, 6, 12])

        # Checks filtering with null
        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null")
        # Scylla does not consider "= null" an error, rather it just matches
        # nothing. See discussion in test_null.py::test_filtering_eq_null
        #assert_invalid_message(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c = null ALLOW FILTERING")
        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null")
        # Scylla does not consider "> null" an error, rather it just matches
        # nothing. See discussion in test_null.py::test_filtering_inequality_null
        #assert_invalid_message(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c > null ALLOW FILTERING")
        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE s > null")
        # Scylla does not consider "> null" an error, rather it just matches
        # nothing. See discussion in test_null.py::test_filtering_inequality_null
        #assert_invalid_message(cql, table, "Unsupported null value for column s",
        #                     "SELECT * FROM %s WHERE s > null ALLOW FILTERING")

        # Checks filtering with unset
        # Reproduces #10358
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE s = ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             UNSET_VALUE)

# Reproduces #10358, #10361
def testFilteringWithoutIndicesWithCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c list<int>, d set<int>, e map<int, int>, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, [1, 6], {2, 12}, {1: 6})")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, [3, 2], {6, 4}, {3: 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, [1, 2], {2, 4}, {1: 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (2, 3, [3, 6], {6, 12}, {3: 6})")

        for _ in before_and_after_flush(cql, table):
            # Checks filtering for lists
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}],
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 3 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            # Checks filtering for sets
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE d CONTAINS 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d CONTAINS 4 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}],
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d CONTAINS 4 AND d CONTAINS 6 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            # Checks filtering for maps
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE e CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE e CONTAINS 2 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}],
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE e CONTAINS KEY 1 ALLOW FILTERING"),
                       [1, 2, [1, 6], {2, 12}, {1: 6}],
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE e[1] = 6 ALLOW FILTERING"),
                       [1, 2, [1, 6], {2, 12}, {1: 6}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE e CONTAINS KEY 1 AND e CONTAINS 2 ALLOW FILTERING"),
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND d CONTAINS 4 AND e CONTAINS KEY 3 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

        # Checks filtering with null
        # Scylla does not consider "CONTAINS null" an error, rather should
        # just matches nothing. See issue #10359.
        #assert_invalid_message(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column d",
        #                     "SELECT * FROM %s WHERE d CONTAINS null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column e",
        #                     "SELECT * FROM %s WHERE e CONTAINS null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column e",
        #                     "SELECT * FROM %s WHERE e CONTAINS KEY null ALLOW FILTERING")
        # Scylla does not consider "e[null]" an error, it just returns NULL.
        # See issue #10361.
        #assert_invalid_message(cql, table, "Unsupported null map key for column e",
        #                     "SELECT * FROM %s WHERE e[null] = 2 ALLOW FILTERING")
        # Scylla does not consider "= null" an error, it just matches nothing.
        # See issue #4776.
        #assert_invalid_message(cql, table, "Unsupported null map value for column e",
        #                     "SELECT * FROM %s WHERE e[1] = null ALLOW FILTERING")

        # Checks filtering with unset
        # Reproduces #10358:
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE d CONTAINS ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE e CONTAINS ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE e CONTAINS KEY ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset",
                             "SELECT * FROM %s WHERE e[?] = 2 ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset",
                             "SELECT * FROM %s WHERE e[1] = ? ALLOW FILTERING",
                             UNSET_VALUE)

def testFilteringWithoutIndicesWithFrozenCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c frozen<list<int>>, d frozen<set<int>>, e frozen<map<int, int>>, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, [1, 6], {2, 12}, {1: 6})")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, [3, 2], {6, 4}, {3: 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, [1, 2], {2, 4}, {1: 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (2, 3, [3, 6], {6, 12}, {3: 6})")

        for _ in before_and_after_flush(cql, table):
            # Checks filtering for lists
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c = [3, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c = [3, 2] ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > [1, 5] AND c < [3, 6]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > [1, 5] AND c < [3, 6] ALLOW FILTERING"),
                       [1, 2, [1, 6], {2, 12}, {1: 6}],
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c >= [1, 6] AND c < [3, 3] ALLOW FILTERING"),
                       [1, 2, [1, 6], {2, 12}, {1: 6}],
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}],
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 3 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            # Checks filtering for sets
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE d = {6, 4}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d = {6, 4} ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE d > {4, 5} AND d < {6}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d > {4, 5} AND d < {6} ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d >= {2, 12} AND d <= {4, 6} ALLOW FILTERING"),
                       [1, 2, [1, 6], {2, 12}, {1: 6}],
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE d CONTAINS 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d CONTAINS 4 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}],
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d CONTAINS 4 AND d CONTAINS 6 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4},{3: 2}])

            # Checks filtering for maps
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE e = {1 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE e = {1 : 2} ALLOW FILTERING"),
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE e > {1 : 4} AND e < {3 : 6}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE e > {1 : 4} AND e < {3 : 6} ALLOW FILTERING"),
                       [1, 2, [1, 6], {2, 12}, {1: 6}],
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE e >= {1 : 6} AND e <= {3 : 2} ALLOW FILTERING"),
                       [1, 2, [1, 6], {2, 12}, {1: 6}],
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE e CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE e CONTAINS 2 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}],
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE e CONTAINS KEY 1 ALLOW FILTERING"),
                       [1, 2, [1, 6], {2, 12}, {1: 6}],
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_invalid_message(cql, table, "Map-entry equality predicates on frozen map column e are not supported",
                                 "SELECT * FROM %s WHERE e[1] = 6 ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE e CONTAINS KEY 1 AND e CONTAINS 2 ALLOW FILTERING"),
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND d CONTAINS 4 AND e CONTAINS KEY 3 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

        # Checks filtering with null
        # Scylla does not consider "= null" an error, rather it just matches
        # nothing. See discussion in test_null.py::test_filtering_eq_null
        #assert_invalid_message(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c = null ALLOW FILTERING")
        # Scylla does not consider "CONTAINS null" an error, rather should
        # just matches nothing. See issue #10359.
        #assert_invalid_message(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column d",
        #                     "SELECT * FROM %s WHERE d = null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column d",
        #                     "SELECT * FROM %s WHERE d CONTAINS null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column e",
        #                     "SELECT * FROM %s WHERE e = null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column e",
        #                     "SELECT * FROM %s WHERE e CONTAINS null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column e",
        #                     "SELECT * FROM %s WHERE e CONTAINS KEY null ALLOW FILTERING")
        # Scylla does not consider "e[null]" an error, it just returns NULL.
        # See issue #10361.
        #assert_invalid_message(cql, table, "Map-entry equality predicates on frozen map column e are not supported",
        #                     "SELECT * FROM %s WHERE e[null] = 2 ALLOW FILTERING")
        # Scylla does not consider "= null" an error, it just matches nothing.
        # See issue #4776.
        #assert_invalid_message(cql, table, "Map-entry equality predicates on frozen map column e are not supported",
        #                     "SELECT * FROM %s WHERE e[1] = null ALLOW FILTERING")

        # Checks filtering with unset
        # Reproduces #10358:
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE d = ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE d CONTAINS ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE e = ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE e CONTAINS ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE e CONTAINS KEY ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "Map-entry equality predicates on frozen map column e are not supported",
                             "SELECT * FROM %s WHERE e[?] = 2 ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "Map-entry equality predicates on frozen map column e are not supported",
                             "SELECT * FROM %s WHERE e[1] = ? ALLOW FILTERING",
                             UNSET_VALUE)
# Reproduces #8627
@pytest.mark.xfail(reason="#8627")
def testIndexQueryWithValueOver64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c blob, PRIMARY KEY (a, b))") as table:
        TOO_BIG = 1024 * 65
        too_big = bytearray([1])*TOO_BIG
        execute(cql, table, "CREATE INDEX ON %s (c)")

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, bytearray([1]))
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, bytearray([1, 1]))

        assert_invalid_message(cql, table, "Index expression values may not be larger than 64K",
                             "SELECT * FROM %s WHERE c = ?  ALLOW FILTERING", too_big)

        ks_name = table.split('.')[0]
        index_name = table.split('.')[1] + "_c_idx"
        execute(cql, table, f"DROP INDEX {ks_name}.{index_name}")
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE c = ?  ALLOW FILTERING", too_big))

# Reproduces #10366
@pytest.mark.xfail(reason="#10366 - server error instead of InvalidRequest")
def testPKQueryWithValueOver64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a text, b text, PRIMARY KEY (a, b))") as table:
        TOO_BIG = 1024 * 65
        too_big = 'x'*TOO_BIG
        assert_invalid_throw(cql, table, InvalidRequest,
                           "SELECT * FROM %s WHERE a = ?", too_big)

# Reproduces #10366
@pytest.mark.xfail(reason="#10366 - server error instead of silent return")
def testCKQueryWithValueOver64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a text, b text, PRIMARY KEY (a, b))") as table:
        TOO_BIG = 1024 * 65
        too_big = 'x'*TOO_BIG
        execute(cql, table, "SELECT * FROM %s WHERE a = 'foo' AND b = ?", too_big)

def testAllowFilteringOnPartitionKeyWithDistinct(cql, test_keyspace):
    # Test a regular(CQL3) table.
    with create_table(cql, test_keyspace, "(pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY ((pk0, pk1), ck0))") as table:
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 0, 0)", i, i)
            execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 1, 1)", i, i)

        for _ in before_and_after_flush(cql, table):
            assert_invalid_message(cql, table, 'FILTERING',
                    "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk1 = 1 LIMIT 3")

            assert_invalid_message(cql, table, 'FILTERING',
                    "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk0 > 0 AND pk1 = 1 LIMIT 3")

            assert_rows(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk0 = 1 LIMIT 1 ALLOW FILTERING"),
                    [1, 1])

            assert_rows(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk1 = 1 LIMIT 3 ALLOW FILTERING"),
                    [1, 1])

            assert_empty(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk0 < 0 AND pk1 = 1 LIMIT 3 ALLOW FILTERING"))

            # Test selection validation.
            assert_invalid_message(cql, table, "queries must request all the partition key columns",
                    "SELECT DISTINCT pk0 FROM %s ALLOW FILTERING")
            assert_invalid_message(cql, table, "queries must only request partition key columns",
                    "SELECT DISTINCT pk0, pk1, ck0 FROM %s ALLOW FILTERING")

def testAllowFilteringOnPartitionKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY ((a, b), c))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (11, 12, 13, 14)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (11, 15, 16, 17)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (21, 22, 23, 24)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (31, 32, 33, 34)")

        for _ in before_and_after_flush(cql, table):
            # IN restrictions *are* allowed in Scylla (the correctness of them
            # is tested in test_filtering.py::test_filtering_with_in_relation
            #assert_invalid_message(cql, table, "IN restrictions are not supported when the query involves filtering",
            #        "SELECT * FROM %s WHERE b in (11,12) ALLOW FILTERING")

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE a = 11")

            # different error messages in Scylla and Cassandra
            assert_invalid(cql, table,
                    "SELECT * FROM %s WHERE a > 11")
            assert_invalid(cql, table,
                    "SELECT * FROM %s WHERE a > 11 and b = 1")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 11 ALLOW FILTERING"),
                    [11, 12, 13, 14],
                    [11, 15, 16, 17])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a in (11) and b in (12,15,22)"),
                    [11, 12, 13, 14],
                    [11, 15, 16, 17])

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b in (12,15,22)")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a in (11) and b in (12,15,22) ALLOW FILTERING"),
                    [11, 12, 13, 14],
                    [11, 15, 16, 17])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a in (11) ALLOW FILTERING"),
                    [11, 12, 13, 14],
                    [11, 15, 16, 17])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b = 15 ALLOW FILTERING"),
                    [11, 15, 16, 17])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b >= 15 ALLOW FILTERING"),
                    [11, 15, 16, 17],
                    [31, 32, 33, 34],
                    [21, 22, 23, 24])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 11 ALLOW FILTERING"),
                    [11, 12, 13, 14],
                    [11, 15, 16, 17],
                    [31, 32, 33, 34],
                    [21, 22, 23, 24])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 11 AND b <= 15 ALLOW FILTERING"),
                    [11, 12, 13, 14],
                    [11, 15, 16, 17])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a <= 11 AND b >= 14 ALLOW FILTERING"),
                    [11, 15, 16, 17])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a < 11 ALLOW FILTERING"))
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE b > 32 ALLOW FILTERING"))

        # Checks filtering with unset
        # Reproduces #10358
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE a = ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE a > ? ALLOW FILTERING",
                             UNSET_VALUE)

    # No clustering key
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY ((a, b)))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (11, 12, 13, 14)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (11, 15, 16, 17)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (21, 22, 23, 24)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (31, 32, 33, 34)")

        for _ in before_and_after_flush(cql, table):
            # IN restrictions *are* allowed in Scylla (the correctness of them
            # is tested in test_filtering.py::test_filtering_with_in_relation
            #assert_invalid_message(cql, table, "IN restrictions are not supported when the query involves filtering",
            #        "SELECT * FROM %s WHERE b in (11,12) ALLOW FILTERING")

            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE a = 11")

            # different error messages in Scylla and Cassandra
            assert_invalid(cql, table,
                    "SELECT * FROM %s WHERE a > 11")
            assert_invalid(cql, table,
                    "SELECT * FROM %s WHERE a > 11 and b = 1")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 11 ALLOW FILTERING"),
                    [11, 12, 13, 14],
                    [11, 15, 16, 17])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 11 ALLOW FILTERING"),
                    [11, 12, 13, 14],
                    [11, 15, 16, 17],
                    [31, 32, 33, 34],
                    [21, 22, 23, 24])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 11 AND b <= 15 ALLOW FILTERING"),
                    [11, 12, 13, 14],
                    [11, 15, 16, 17])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a <= 11 AND b >= 14 ALLOW FILTERING"),
                    [11, 15, 16, 17])

    # one partition key
    with create_table(cql, test_keyspace, "(a int primary key, b int, c int)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, 4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 1, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (3, 2, 4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, 1, 7)")

        # Adds tomstones
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 1, 4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (5, 2, 7)")
        execute(cql, table, "DELETE FROM %s WHERE a = 0")
        execute(cql, table, "DELETE FROM %s WHERE a = 5")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING"),
                    [1, 2, 4])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 ALLOW FILTERING"),
                    [1, 2, 4],
                    [2, 1, 6],
                    [4, 1, 7],
                    [3, 2, 4])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b >= 2  ALLOW FILTERING"),
                    [1, 2, 4],
                    [3, 2, 4])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b >= 2 AND c <= 4 ALLOW FILTERING"),
                    [1, 2, 4],
                    [3, 2, 4])

def testAllowFilteringOnPartitionAndClusteringKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (11, 12, 13, 14, 15)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (11, 15, 16, 17, 18)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (21, 22, 23, 24, 25)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (31, 32, 33, 34, 35)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 11 AND b = 15 AND c = 16"),
                    [11, 15, 16, 17, 18])

            assert_invalid_message(cql, table,
                    "Clustering column \"d\" cannot be restricted (preceding column \"c\" is restricted by a non-EQ relation)",
                    "SELECT * FROM %s WHERE a = 11 AND b = 12 AND c > 13 AND d = 14")
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 11 AND b = 15 AND c = 16 AND d > 16"),
                    [11, 15, 16, 17, 18])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 11 AND b = 15 AND c > 13 AND d >= 17 ALLOW FILTERING"),
                    [11, 15, 16, 17, 18])
            assert_invalid_message(cql, table,
                    "Clustering column \"d\" cannot be restricted (preceding column \"c\" is restricted by a non-EQ relation)",
                    "SELECT * FROM %s WHERE a = 11 AND b = 12 AND c > 13 AND d > 17")
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > 30 AND d >= 34 ALLOW FILTERING"),
                    [31, 32, 33, 34, 35])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a <= 11 AND c > 15 AND d >= 16 ALLOW FILTERING"),
                    [11, 15, 16, 17, 18])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a <= 11 AND b >= 15 AND c > 15 AND d >= 16 ALLOW FILTERING"),
                    [11, 15, 16, 17, 18])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a <= 100 AND b >= 15 AND c > 0 AND d <= 100 ALLOW FILTERING"),
                    [11, 15, 16, 17, 18],
                    [31, 32, 33, 34, 35],
                    [21, 22, 23, 24, 25])

            # Scylla's and Cassandra's error messages differ here because there
            # are multiple errors in the request... Cassandra says
            # "Clustering column \"d\" cannot be restricted (preceding column
            # \"c\" is restricted by a non-EQ relation)", while Scylla says:
            # "Only EQ and IN relation are supported on the partition key
            # (unless you use the token() function or ALLOW FILTERING)
            assert_invalid(cql, table,
                    "SELECT * FROM %s WHERE a <= 11 AND c > 15 AND d >= 16")

    # test clutering order
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c, d)) WITH CLUSTERING ORDER BY (c DESC, d ASC)") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (11, 11, 13, 14, 15)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (11, 11, 14, 17, 18)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (11, 12, 15, 14, 15)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (11, 12, 16, 17, 18)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (21, 11, 23, 24, 25)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (21, 11, 24, 34, 35)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (21, 12, 25, 24, 25)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (21, 12, 26, 34, 35)")

        for _ in before_and_after_flush(cql, table):
            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE b >= 12 ALLOW FILTERING"),
                                    [11, 12, 15, 14, 15],
                                    [11, 12, 16, 17, 18],
                                    [21, 12, 25, 24, 25],
                                    [21, 12, 26, 34, 35])

@pytest.mark.xfail(reason="#10358")
def testAllowFilteringOnPartitionKeyWithoutIndicesWithCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c list<int>, d set<int>, e map<int, int>, PRIMARY KEY ((a, b)))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, [1, 6], {2, 12}, {1: 6})")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, [3, 2], {6, 4}, {3: 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, [1, 2], {2, 4}, {1: 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (2, 3, [3, 6], {6, 12}, {3: 6})")

        for _ in before_and_after_flush(cql, table):
            # Checks filtering for lists
            # The error message is different in Scylla and Cassandra - Cassandra
            # prints the generic REQUIRES_ALLOW_FILTERING_MESSAGE, while Scylla
            # prints the more specific "Only EQ and IN relation are supported
            # on the partition key (unless you use the token() function or allow
            # filtering)".
            assert_invalid_message(cql, table, "filtering",
                    "SELECT * FROM %s WHERE b < 0 AND c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b >= 4 AND c CONTAINS 2 ALLOW FILTERING"),
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(
                    execute(cql, table, "SELECT * FROM %s WHERE a > 0 AND b <= 3 AND c CONTAINS 2 AND c CONTAINS 3 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            # Checks filtering for sets
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE a = 1 AND d CONTAINS 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d CONTAINS 4 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}],
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d CONTAINS 4 AND d CONTAINS 6 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

            # Checks filtering for maps
            assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE e CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a < 2 AND b >= 3 AND e CONTAINS 2 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}],
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND e CONTAINS KEY 1 ALLOW FILTERING"),
                       [1, 4, [1, 2], {2, 4}, {1: 2}],
                       [1, 2, [1, 6], {2, 12}, {1: 6}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a in (1) AND b in (2) AND e[1] = 6 ALLOW FILTERING"),
                       [1, 2, [1, 6], {2, 12}, {1: 6}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND e CONTAINS KEY 1 AND e CONTAINS 2 ALLOW FILTERING"),
                       [1, 4, [1, 2], {2, 4}, {1: 2}])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b in (3) AND c CONTAINS 2 AND d CONTAINS 4 AND e CONTAINS KEY 3 ALLOW FILTERING"),
                       [1, 3, [3, 2], {6, 4}, {3: 2}])

        # Checks filtering with null
        # Scylla does not consider "CONTAINS null" an error, rather should
        # just matches nothing. See issue #10359.
        #assert_invalid_message(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column d",
        #                     "SELECT * FROM %s WHERE b < 1 AND d CONTAINS null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column e",
        #                     "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e CONTAINS null ALLOW FILTERING")
        #assert_invalid_message(cql, table, "Unsupported null value for column e",
        #                     "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e CONTAINS KEY null ALLOW FILTERING")
        # Scylla does not consider "e[null]" an error, it just returns NULL.
        # See issue #10361.
        #assert_invalid_message(cql, table, "Unsupported null map key for column e",
        #                     "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e[null] = 2 ALLOW FILTERING")
        # Scylla does not consider "= null" an error, it just matches nothing.
        # See issue #4776.
        #assert_invalid_message(cql, table, "Unsupported null map value for column e",
        #                     "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e[1] = null ALLOW FILTERING")

        # Checks filtering with unset
        # Reproduces #10358
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND c CONTAINS ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND d CONTAINS ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e CONTAINS ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e CONTAINS KEY ? ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "Unsupported unset map key for column e",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e[?] = 2 ALLOW FILTERING",
                             UNSET_VALUE)
        assert_invalid_message(cql, table, "Unsupported unset map value for column e",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e[1] = ? ALLOW FILTERING",
                             UNSET_VALUE)

def executeFilteringOnly(cql, table, statement):
    assert_invalid(cql, table, statement)
    return execute_without_paging(cql, table, statement + " ALLOW FILTERING")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testAllowFilteringOnPartitionKeyWithCounters(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, cnt counter, PRIMARY KEY ((a, b), c))") as table:
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 14, 11, 12, 13)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24, 21, 22, 23)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 27, 21, 22, 26)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 34, 31, 32, 33)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24, 41, 42, 43)

        for _ in before_and_after_flush(cql, table):

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE cnt = 24"),
                       [41, 42, 43, 24],
                       [21, 22, 23, 24])
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 22 AND cnt = 24"),
                       [41, 42, 43, 24])
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 10 AND b < 25 AND cnt = 24"),
                       [21, 22, 23, 24])
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 10 AND c < 25 AND cnt = 24"),
                       [21, 22, 23, 24])

            assert_invalid_message(cql, table,
            "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.",
            "SELECT * FROM %s WHERE a = 21 AND b > 10 AND cnt > 23 ORDER BY c DESC ALLOW FILTERING")

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE a = 21 AND b = 22 AND cnt > 23 ORDER BY c DESC"),
                       [21, 22, 26, 27],
                       [21, 22, 23, 24])

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE cnt > 20 AND cnt < 30"),
                       [41, 42, 43, 24],
                       [21, 22, 23, 24],
                       [21, 22, 26, 27])

def testFilteringOnClusteringColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (11, 12, 13, 14)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (11, 15, 16, 17)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (21, 22, 23, 24)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (31, 32, 33, 34)")

        for _ in before_and_after_flush(cql, table):

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 11 AND b = 15"),
                       [11, 15, 16, 17])

            assert_invalid_message(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE a = 11 AND b > 12 AND c = 15")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 11 AND b = 15 AND c > 15"),
                       [11, 15, 16, 17])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 11 AND b > 12 AND c > 13 AND d = 17 ALLOW FILTERING"),
                       [11, 15, 16, 17])
            assert_invalid_message(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE a = 11 AND b > 12 AND c > 13 and d = 17")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b > 20 AND c > 30 ALLOW FILTERING"),
                       [31, 32, 33, 34])
            assert_invalid_message(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE b > 20 AND c > 30")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b > 20 AND c < 30 ALLOW FILTERING"),
                       [21, 22, 23, 24])
            assert_invalid_message(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE b > 20 AND c < 30")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b > 20 AND c = 33 ALLOW FILTERING"),
                       [31, 32, 33, 34])
            assert_invalid_message(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE b > 20 AND c = 33")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c = 33 ALLOW FILTERING"),
                       [31, 32, 33, 34])
            assert_invalid_message(cql, table, "PRIMARY KEY column \"c\" cannot be restricted as preceding column \"b\" is not restricted",
                                 "SELECT * FROM %s WHERE c = 33")

    # --------------------------------------------------
    # Clustering column within and across partition keys
    # --------------------------------------------------
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (11, 12, 13, 14)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (11, 15, 16, 17)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (11, 18, 19, 20)")

        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (21, 22, 23, 24)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (21, 25, 26, 27)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (21, 28, 29, 30)")

        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (31, 32, 33, 34)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (31, 35, 36, 37)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (31, 38, 39, 40)")

        for _ in before_and_after_flush(cql, table):

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE a = 21 AND c > 23"),
                       [21, 25, 26, 27],
                       [21, 28, 29, 30])
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE a = 21 AND c > 23 ORDER BY b DESC"),
                       [21, 28, 29, 30],
                       [21, 25, 26, 27])
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE c > 16 and c < 36"),
                       [11, 18, 19, 20],
                       [21, 22, 23, 24],
                       [21, 25, 26, 27],
                       [21, 28, 29, 30],
                       [31, 32, 33, 34])

@pytest.mark.xfail(reason="#4244")
def testFilteringWithMultiColumnSlices(cql, test_keyspace):
    #/----------------------------------------
    #/ Multi-column slices for clustering keys
    #/----------------------------------------
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (11, 12, 13, 14, 15)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (21, 22, 23, 24, 25)")
        execute(cql, table, "INSERT INTO %s (a,b,c,d,e) VALUES (31, 32, 33, 34, 35)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b = 22 AND d = 24 ALLOW FILTERING"),
                       [21, 22, 23, 24, 25])
            assert_invalid_message(cql, table, "PRIMARY KEY column \"d\" cannot be restricted as preceding column \"c\" is not restricted",
                                 "SELECT * FROM %s WHERE b = 22 AND d = 24")

            # As of issue #4244 in Scylla, "Mixing single column relations and
            # multi column relations on clustering columns is not allowed".
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE (b, c) > (20, 30) AND d = 34 ALLOW FILTERING"),
                       [31, 32, 33, 34, 35])
            assert_invalid_message(cql, table, "Clustering column \"d\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE (b, c) > (20, 30) AND d = 34")

def testContainsFilteringForClusteringKeys(cql, test_keyspace):
    #/-------------------------------------------------
    #/ Frozen collections filtering for clustering keys
    #/-------------------------------------------------
    # first clustering column
    with create_table(cql, test_keyspace, "(a int, b frozen<list<int>>, c int, PRIMARY KEY (a, b, c))") as table:

        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?, ?, ?)", 11, [1, 3], 14)
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?, ?, ?)", 21, [2, 3], 24)
        execute(cql, table, "INSERT INTO %s (a,b,c) VALUES (?, ?, ?)", 21, [3, 3], 34)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 21 AND b CONTAINS 2 ALLOW FILTERING"),
                       [21, [2, 3], 24])
            # The wording of the error message in Cassandra and Scylla is a
            # bit different.
            assert_invalid_message(cql, table, "CONTAINS",
                                 "SELECT * FROM %s WHERE a = 21 AND b CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b CONTAINS 2 ALLOW FILTERING"),
                       [21, [2, 3], 24])
            assert_invalid_message(cql, table, "CONTAINS",
                                 "SELECT * FROM %s WHERE b CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b CONTAINS 3 ALLOW FILTERING"),
                       [11, [1, 3], 14],
                       [21, [2, 3], 24],
                       [21, [3, 3], 34])

    # non-first clustering column
    with create_table(cql, test_keyspace, "(a int, b int, c frozen<list<int>>, d int, PRIMARY KEY (a, b, c))") as table:

        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 11, 12, [1, 3], 14)
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 21, 22, [2, 3], 24)
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 21, 22, [3, 3], 34)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 21 AND c CONTAINS 2 ALLOW FILTERING"),
                       [21, 22, [2, 3], 24])
            assert_invalid_message(cql, table, "CONTAINS",
                                 "SELECT * FROM %s WHERE a = 21 AND c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b > 20 AND c CONTAINS 2 ALLOW FILTERING"),
                       [21, 22, [2, 3], 24])
            assert_invalid_message(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE b > 20 AND c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 3 ALLOW FILTERING"),
                       [11, 12, [1, 3], 14],
                       [21, 22, [2, 3], 24],
                       [21, 22, [3, 3], 34])

    with create_table(cql, test_keyspace, "(a int, b int, c frozen<map<text, text>>, d int, PRIMARY KEY (a, b, c))") as table:

        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 11, 12, {"1": "3"}, 14)
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 21, 22, {"2": "3"}, 24)
        execute(cql, table, "INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 21, 22, {"3": "3"}, 34)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b > 20 AND c CONTAINS KEY '2' ALLOW FILTERING"),
                       [21, 22, {"2": "3"}, 24])
            assert_invalid_message(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE b > 20 AND c CONTAINS KEY '2'")

def dotestContainsOnPartitionKey(cql, test_keyspace, schema):
    with create_table(cql, test_keyspace, schema) as table:
        execute(cql, table, "INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", {1: 2}, 1, 1)
        execute(cql, table, "INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", {1: 2}, 2, 2)

        execute(cql, table, "INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", {1: 2, 3: 4}, 1, 3)
        execute(cql, table, "INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", {1: 2, 3: 4}, 2, 3)

        execute(cql, table, "INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", {5: 6}, 5, 5)
        execute(cql, table, "INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", {7: 8}, 6, 6)

        assert_invalid_message(cql, table, 'ALLOW FILTERING',
                             "SELECT * FROM %s WHERE pk CONTAINS KEY 1")

        for _ in before_and_after_flush(cql, table):
            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE pk CONTAINS KEY 1 ALLOW FILTERING"),
                                    [{1: 2}, 1, 1],
                                    [{1: 2}, 2, 2],
                                    [{1: 2, 3: 4}, 1, 3],
                                    [{1: 2, 3: 4}, 2, 3])

            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE pk CONTAINS KEY 1 AND pk CONTAINS 4 ALLOW FILTERING"),
                                    [{1: 2, 3: 4}, 1, 3],
                                    [{1: 2, 3: 4}, 2, 3])

            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE pk CONTAINS KEY 1 AND pk CONTAINS KEY 3 ALLOW FILTERING"),
                                    [{1: 2, 3: 4}, 1, 3],
                                    [{1: 2, 3: 4}, 2, 3])

            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE pk CONTAINS KEY 1 AND v = 3 ALLOW FILTERING"),
                                    [{1: 2, 3: 4}, 1, 3],
                                    [{1: 2, 3: 4}, 2, 3])

            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE pk CONTAINS KEY 1 AND ck = 1 AND v = 3 ALLOW FILTERING"),
                                    [{1: 2, 3: 4}, 1, 3])


def testContainsOnPartitionKey(cql, test_keyspace):
    dotestContainsOnPartitionKey(cql, test_keyspace, "(pk frozen<map<int, int>>, ck int, v int, PRIMARY KEY (pk, ck))")

def testContainsOnPartitionKeyPart(cql, test_keyspace):
    dotestContainsOnPartitionKey(cql, test_keyspace, "(pk frozen<map<int, int>>, ck int, v int, PRIMARY KEY ((pk, ck)))")

@pytest.mark.xfail(reason="#10443")
def testFilteringWithOrderClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d list<int>, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, 13, [1,4])
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, 23, [2,4])
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, 26, [2,7])
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, 33, [3,4])

        for _ in before_and_after_flush(cql, table):
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 21 AND c > 20 ORDER BY b DESC"),
                       [21, 25, 26, [2, 7]],
                       [21, 22, 23, [2, 4]])

            # reproduces #10443
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN(21, 31) AND c > 20 ORDER BY b DESC"),
                       [31, 32, 33, [3, 4]],
                       [21, 25, 26, [2, 7]],
                       [21, 22, 23, [2, 4]])

def testfilteringOnStaticColumnTest(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, s int static, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, s) VALUES (11, 12, 13, 14, 15)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, s) VALUES (21, 22, 23, 24, 25)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, s) VALUES (21, 26, 27, 28, 29)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, s) VALUES (31, 32, 33, 34, 35)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, s) VALUES (11, 42, 43, 44, 45)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d, s FROM %s WHERE s = 29"),
                       [21, 22, 23, 24, 29],
                       [21, 26, 27, 28, 29])
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d, s FROM %s WHERE b > 22 AND s = 29"),
                       [21, 26, 27, 28, 29])
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d, s FROM %s WHERE b > 10 and b < 26 AND s = 29"),
                       [21, 22, 23, 24, 29])
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d, s FROM %s WHERE c > 10 and c < 27 AND s = 29"),
                       [21, 22, 23, 24, 29])
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d, s FROM %s WHERE c > 10 and c < 43 AND s = 29"),
                       [21, 22, 23, 24, 29],
                       [21, 26, 27, 28, 29])
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d, s FROM %s WHERE c > 10 AND s > 15 AND s < 45"),
                       [21, 22, 23, 24, 29],
                       [21, 26, 27, 28, 29],
                       [31, 32, 33, 34, 35])
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d, s FROM %s WHERE a = 21 AND s > 15 AND s < 45 ORDER BY b DESC"),
                       [21, 26, 27, 28, 29],
                       [21, 22, 23, 24, 29])
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d, s FROM %s WHERE c > 13 and d < 44"),
                       [21, 22, 23, 24, 29],
                       [21, 26, 27, 28, 29],
                       [31, 32, 33, 34, 35])

def testcontainsFilteringOnNonClusteringColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d list<int>, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, 13, [1,4])
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, 23, [2,4])
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, 26, [2,7])
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, 33, [3,4])

        for _ in before_and_after_flush(cql, table):
            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d FROM %s WHERE b > 20 AND d CONTAINS 2"),
                       [21, 22, 23, [2, 4]],
                       [21, 25, 26, [2, 7]])

            assert_rows(executeFilteringOnly(cql, table, "SELECT a, b, c, d FROM %s WHERE b > 20 AND d CONTAINS 2 AND d contains 4"),
                       [21, 22, 23, [2, 4]])

# Test for CASSANDRA-11310 compatibility with 2i
def testCustomIndexWithFiltering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a text, b int, c text, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "CREATE INDEX ON %s(c)")

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", "a", 0, "b", 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", "a", 1, "b", 2)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", "a", 2, "b", 3)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", "c", 3, "b", 4)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", "d", 4, "d", 5)

        for _ in before_and_after_flush(cql, table):
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE a='a' AND b > 0 AND c = 'b'"),
                       ["a", 1, "b", 2],
                       ["a", 2, "b", 3])

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE c = 'b' AND d = 4"),
                       ["c", 3, "b", 4])

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testFilteringWithCounters(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 14, 11, 12, 13)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24, 21, 22, 23)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 27, 21, 25, 26)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 34, 31, 32, 33)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24, 41, 42, 43)

        for _ in before_and_after_flush(cql, table):
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE cnt = 24"),
                       [21, 22, 23, 24],
                       [41, 42, 43, 24])
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 22 AND cnt = 24"),
                       [41, 42, 43, 24])
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 10 AND b < 25 AND cnt = 24"),
                       [21, 22, 23, 24])
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 10 AND c < 25 AND cnt = 24"),
                       [21, 22, 23, 24])
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE a = 21 AND b > 10 AND cnt > 23 ORDER BY b DESC"),
                       [21, 25, 26, 27],
                       [21, 22, 23, 24])
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE cnt > 20 AND cnt < 30"),
                       [21, 22, 23, 24],
                       [21, 25, 26, 27],
                       [41, 42, 43, 24])

# Check select with ith different column order. See CASSANDRA-10988
def testClusteringOrderWithSlice(cql, test_keyspace):
    # non-compound, ASC order
    with create_table(cql, test_keyspace, "(a text, b int, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b ASC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES ('a', 2)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES ('a', 3)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   ["a", 2],
                   ["a", 3])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b DESC"),
                   ["a", 3],
                   ["a", 2])

    # non-compound, DESC order
    with create_table(cql, test_keyspace, "(a text, b int, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES ('a', 2)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES ('a', 3)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   ["a", 3],
                   ["a", 2])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   ["a", 2],
                   ["a", 3])

    # compound, first column DESC order
    with create_table(cql, test_keyspace, "(a text, b int, c int, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b DESC, c ASC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   ["a", 3, 5],
                   ["a", 2, 4])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   ["a", 2, 4],
                   ["a", 3, 5])

    # compound, mixed order
    with create_table(cql, test_keyspace, "(a text, b int, c int, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b ASC, c DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   ["a", 2, 4],
                   ["a", 3, 5])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   ["a", 2, 4],
                   ["a", 3, 5])

def testFilteringWithSecondaryIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c1 int, c2 int, c3 int, v int, PRIMARY KEY (pk, c1, c2, c3))") as table:
        execute(cql, table, "CREATE INDEX v_idx_1 ON %s (v);")
        for i in range(1, 5+1):
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 1, 1, i)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 1, i, i)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 1, 1, i, i, i)
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 1, i, i, i, i)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND  c1 > 0 AND c1 < 5 AND c2 = 1 AND v = 3 ALLOW FILTERING;"),
                       [1, 1, 1, 3, 3])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND  c1 > 1 AND c1 < 5 AND c2 = 1 AND v = 3 ALLOW FILTERING;"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND  c1 > 1 AND c2 > 2 AND c3 > 2 AND v = 3 ALLOW FILTERING;"),
                       [1, 3, 3, 3, 3])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND  c1 > 1 AND c2 > 2 AND c3 = 3 AND v = 3 ALLOW FILTERING;"),
                       [1, 3, 3, 3, 3])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND  c1 IN(0,1,2) AND c2 > 1 AND c2 < 1 AND v = 3 ALLOW FILTERING;"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND  c1 IN(0,1,2) AND c2 = 1 AND v = 3 ALLOW FILTERING;"),
                       [1, 1, 1, 3, 3])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1 AND  c1 IN(0,1,2) AND c2 = 1 AND v = 3"),
                       [1, 1, 1, 3, 3])

def testIndexQueryWithCompositePartitionKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(p1 int, p2 int, v int, PRIMARY KEY ((p1, p2)))") as table:
        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE p1 = 1 AND v = 3")
        execute(cql, table, "CREATE INDEX ON %s(v)")

        execute(cql, table, "INSERT INTO %s(p1, p2, v) values (?, ?, ?)", 1, 1, 3)
        execute(cql, table, "INSERT INTO %s(p1, p2, v) values (?, ?, ?)", 1, 2, 3)
        execute(cql, table, "INSERT INTO %s(p1, p2, v) values (?, ?, ?)", 2, 1, 3)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE p1 = 1 AND v = 3 ALLOW FILTERING"),
                       [1, 2, 3],
                       [1, 1, 3])

def testEmptyRestrictionValue(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk blob, c blob, v blob, PRIMARY KEY ((pk), c))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                b"foo123", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                b"foo123", b"2", b"2")

        for _ in before_and_after_flush(cql, table):
            # Cassandra does not allow looking for an empty-string partition
            # key. Scylla considers this a bug, because such partition keys are
            # allowed in views. See full explanation in test_materialized_view.py
            # test_mv_empty_string_partition_key_individual
            #assert_invalid_message(cql, table, "Key may not be empty", "SELECT * FROM %s WHERE pk = textAsBlob('');")
            #assert_invalid_message(cql, table, "Key may not be empty", "SELECT * FROM %s WHERE pk IN (textAsBlob(''), textAsBlob('1'));")

            #assert_invalid_message(cql, table, "Key may not be empty",
            #                     "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
            #                     b"", b"2", b"2")

            # Test clustering columns restrictions
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) = (textAsBlob(''));"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));"),
                       [b"foo123", b"1", b"1"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) IN ((textAsBlob('')), (textAsBlob('1')));"),
                       [b"foo123", b"1", b"1"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('');"),
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) > (textAsBlob(''));"),
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('');"),
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob(''));"),
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"2"])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('');"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) <= (textAsBlob(''));"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('');"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) < (textAsBlob(''));"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('') AND c < textAsBlob('');"))


        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                b"foo123", b"", b"4")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');"),
                       [b"foo123", b"", b"4"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) = (textAsBlob(''));"),
                       [b"foo123", b"", b"4"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));"),
                       [b"foo123", b"", b"4"],
                       [b"foo123", b"1", b"1"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) IN ((textAsBlob('')), (textAsBlob('1')));"),
                       [b"foo123", b"", b"4"],
                       [b"foo123", b"1", b"1"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('');"),
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) > (textAsBlob(''));"),
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('');"),
                       [b"foo123", b"", b"4"],
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob(''));"),
                       [b"foo123", b"", b"4"],
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('');"),
                       [b"foo123", b"", b"4"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) <= (textAsBlob(''));"),
                       [b"foo123", b"", b"4"])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('');"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) < (textAsBlob(''));"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('') AND c < textAsBlob('');"))

        # Test restrictions on non-primary key value
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('') ALLOW FILTERING;"))

        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                b"foo123", b"3", b"")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('') ALLOW FILTERING;"),
                       [b"foo123", b"3", b""])

def testEmptyRestrictionValueWithMultipleClusteringColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"2", b"2")

        for _ in before_and_after_flush(cql, table):

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('');"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 = textAsBlob('');"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) = (textAsBlob('1'), textAsBlob(''));"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1');"),
                       [b"foo123", b"1", b"1", b"1"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 IN (textAsBlob(''), textAsBlob('1'));"),
                       [b"foo123", b"1", b"1", b"1"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')));"),
                       [b"foo123", b"1", b"1", b"1"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('');"),
                       [b"foo123", b"1", b"1", b"1"],
                       [b"foo123", b"1", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 > textAsBlob('');"),
                       [b"foo123", b"1", b"1", b"1"],
                       [b"foo123", b"1", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'));"),
                       [b"foo123", b"1", b"1", b"1"],
                       [b"foo123", b"1", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 >= textAsBlob('');"),
                       [b"foo123", b"1", b"1", b"1"],
                       [b"foo123", b"1", b"2", b"2"])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 <= textAsBlob('');"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) <= (textAsBlob('1'), textAsBlob(''));"))

        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                b"foo123", b"", b"1", b"4")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('');"),
                       [b"foo123", b"", b"1", b"4"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('') AND c2 = textAsBlob('1');"),
                       [b"foo123", b"", b"1", b"4"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) = (textAsBlob(''), textAsBlob('1'));"),
                       [b"foo123", b"", b"1", b"4"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1');"),
                       [b"foo123", b"", b"1", b"4"],
                       [b"foo123", b"1", b"1", b"1"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')));"),
                       [b"foo123", b"", b"1", b"4"],
                       [b"foo123", b"1", b"1", b"1"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'));"),
                       [b"foo123", b"1", b"1", b"1"],
                       [b"foo123", b"1", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) >= (textAsBlob(''), textAsBlob('1'));"),
                       [b"foo123", b"", b"1", b"4"],
                       [b"foo123", b"1", b"1", b"1"],
                       [b"foo123", b"1", b"2", b"2"])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) <= (textAsBlob(''), textAsBlob('1'));"),
                       [b"foo123", b"", b"1", b"4"])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) < (textAsBlob(''), textAsBlob('1'));"))

def testEmptyRestrictionValueWithOrderBy(cql, test_keyspace):
    for options in ["", " WITH CLUSTERING ORDER BY (c DESC)" ]:
        orderingClause = ""
        if not "ORDER" in options:
            orderingClause = "ORDER BY c DESC"
        with create_table(cql, test_keyspace, "(pk blob, c blob, v blob, PRIMARY KEY ((pk), c))" + options) as table:
            execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    b"foo123",
                    b"1",
                    b"1")
            execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    b"foo123",
                    b"2",
                    b"2")

            for _ in before_and_after_flush(cql, table):
                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('')" + orderingClause),
                           [b"foo123", b"2", b"2"],
                           [b"foo123", b"1", b"1"])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('')" + orderingClause),
                           [b"foo123", b"2", b"2"],
                           [b"foo123", b"1", b"1"])

                assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('')" + orderingClause))

                assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('')" + orderingClause))

            execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    b"foo123", b"", b"4")

            for _ in before_and_after_flush(cql, table):
                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                           [b"foo123", b"1", b"1"],
                           [b"foo123", b"", b"4"])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('')" + orderingClause),
                           [b"foo123", b"2", b"2"],
                           [b"foo123", b"1", b"1"])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('')" + orderingClause),
                           [b"foo123", b"2", b"2"],
                           [b"foo123", b"1", b"1"],
                           [b"foo123", b"", b"4"])

                assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('')" + orderingClause))

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('')" + orderingClause),
                           [b"foo123", b"", b"4"])

def testEmptyRestrictionValueWithMultipleClusteringColumnsAndOrderBy(cql, test_keyspace):
    for options in ["", " WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)" ]:
        orderingClause = ""
        if not "ORDER" in options:
            orderingClause = "ORDER BY c1 DESC, c2 DESC"
        with create_table(cql, test_keyspace, "(pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))" + options) as table:
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"1", b"1")
            execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"2", b"2")

            for _ in before_and_after_flush(cql, table):

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('')" + orderingClause),
                           [b"foo123", b"1", b"2", b"2"],
                           [b"foo123", b"1", b"1", b"1"])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 > textAsBlob('')" + orderingClause),
                           [b"foo123", b"1", b"2", b"2"],
                           [b"foo123", b"1", b"1", b"1"])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                           [b"foo123", b"1", b"2", b"2"],
                           [b"foo123", b"1", b"1", b"1"])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 >= textAsBlob('')" + orderingClause),
                           [b"foo123", b"1", b"2", b"2"],
                           [b"foo123", b"1", b"1", b"1"])

            execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                    b"foo123", b"", b"1", b"4")

            for _ in before_and_after_flush(cql, table):

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1')" + orderingClause),
                           [b"foo123", b"1", b"1", b"1"],
                           [b"foo123", b"", b"1", b"4"])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')))" + orderingClause),
                           [b"foo123", b"1", b"1", b"1"],
                           [b"foo123", b"", b"1", b"4"])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                           [b"foo123", b"1", b"2", b"2"],
                           [b"foo123", b"1", b"1", b"1"])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) >= (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                           [b"foo123", b"1", b"2", b"2"],
                           [b"foo123", b"1", b"1", b"1"],
                           [b"foo123", b"", b"1", b"4"])

def testWithDistinctAndJsonAsColumnName(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(distinct int, json int, value int, PRIMARY KEY (distinct, json))") as table:
        execute(cql, table, "INSERT INTO %s (distinct, json, value) VALUES (0, 0, 0)")

        assert_rows(execute(cql, table, "SELECT distinct, json FROM %s"), [0, 0])
        assert_rows(execute(cql, table, "SELECT distinct distinct FROM %s"), [0])

def testFilteringOnDurationColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, d duration)") as table:
        execute(cql, table, "INSERT INTO %s (k, d) VALUES (0, 1s)")
        execute(cql, table, "INSERT INTO %s (k, d) VALUES (1, 2s)")
        execute(cql, table, "INSERT INTO %s (k, d) VALUES (2, 1s)")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d=1s ALLOW FILTERING"),
                   [0, Duration(0, 0, 1000000000)],
                   [2, Duration(0, 0, 1000000000)])

        # Scylla does support this case - see
        #  test_filtering.py::test_filtering_with_in_relation
        #assert_invalid_message(cql, table, "IN predicates on non-primary-key columns (d) is not yet supported",
        #                     "SELECT * FROM %s WHERE d IN (1s, 2s) ALLOW FILTERING")

        assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                             "SELECT * FROM %s WHERE d > 1s ALLOW FILTERING")

        assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                             "SELECT * FROM %s WHERE d >= 1s ALLOW FILTERING")

        assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                             "SELECT * FROM %s WHERE d <= 1s ALLOW FILTERING")

        assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                             "SELECT * FROM %s WHERE d < 1s ALLOW FILTERING")

def testFilteringOnListContainingDurations(cql, test_keyspace):
    for frozen in [False, True]:
        listType = "frozen<list<duration>>" if frozen else "list<duration>"
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, l {listType})") as table:
            execute(cql, table, "INSERT INTO %s (k, l) VALUES (0, [1s, 2s])")
            execute(cql, table, "INSERT INTO %s (k, l) VALUES (1, [2s, 3s])")
            execute(cql, table, "INSERT INTO %s (k, l) VALUES (2, [1s, 3s])")
            if frozen:
                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE l = [1s, 2s] ALLOW FILTERING"),
                           [0, [Duration(0, 0, 1000000000), Duration(0, 0, 2000000000)]])

            # Scylla does support this case - see
            #  test_filtering.py::test_filtering_with_in_relation
            #assert_invalid_message(cql, table, "IN predicates on non-primary-key columns (l) is not yet supported",
            #                     "SELECT * FROM %s WHERE l IN ([1s, 2s], [2s, 3s]) ALLOW FILTERING")

            assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                                 "SELECT * FROM %s WHERE l > [2s, 3s] ALLOW FILTERING")

            assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                                 "SELECT * FROM %s WHERE l >= [2s, 3s] ALLOW FILTERING")

            assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                                 "SELECT * FROM %s WHERE l <= [2s, 3s] ALLOW FILTERING")

            assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                                 "SELECT * FROM %s WHERE l < [2s, 3s] ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE l CONTAINS 1s ALLOW FILTERING"),
                       [0, [Duration(0, 0, 1000000000), Duration(0, 0, 2000000000)]],
                       [2, [Duration(0, 0, 1000000000), Duration(0, 0, 3000000000)]])

def testFilteringOnMapContainingDurations(cql, test_keyspace):
    for frozen in [False, True]:
        mapType = "frozen<map<int, duration>>" if frozen else "map<int, duration>"
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, m {mapType})") as table:
            execute(cql, table, "INSERT INTO %s (k, m) VALUES (0, {1:1s, 2:2s})")
            execute(cql, table, "INSERT INTO %s (k, m) VALUES (1, {2:2s, 3:3s})")
            execute(cql, table, "INSERT INTO %s (k, m) VALUES (2, {1:1s, 3:3s})")

            if frozen:
                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE m = {1:1s, 2:2s} ALLOW FILTERING"),
                           [0, {1: Duration(0, 0, 1000000000), 2: Duration(0, 0, 2000000000)}])

            # Scylla does support this case - see
            #  test_filtering.py::test_filtering_with_in_relation
            #assert_invalid_message(cql, table, "IN predicates on non-primary-key columns (m) is not yet supported",
            #        "SELECT * FROM %s WHERE m IN ({1:1s, 2:2s}, {1:1s, 3:3s}) ALLOW FILTERING")

            assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                    "SELECT * FROM %s WHERE m > {1:1s, 3:3s} ALLOW FILTERING")

            assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                    "SELECT * FROM %s WHERE m >= {1:1s, 3:3s} ALLOW FILTERING")

            assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                    "SELECT * FROM %s WHERE m <= {1:1s, 3:3s} ALLOW FILTERING")

            assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                    "SELECT * FROM %s WHERE m < {1:1s, 3:3s} ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE m CONTAINS 1s ALLOW FILTERING"),
                       [0, {1: Duration(0, 0, 1000000000), 2: Duration(0, 0, 2000000000)}],
                       [2, {1: Duration(0, 0, 1000000000), 3: Duration(0, 0, 3000000000)}])

def testFilteringOnTupleContainingDurations(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, t tuple<int, duration>)") as table:
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (0, (1, 2s))")
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (1, (2, 3s))")
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (2, (1, 3s))")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE t = (1, 2s) ALLOW FILTERING"),
                   [0, (1, Duration(0, 0, 2000000000))])

        # Scylla does support this case - see
        #  test_filtering.py::test_filtering_with_in_relation
        #assert_invalid_message(cql, table, "IN predicates on non-primary-key columns (t) is not yet supported",
        #        "SELECT * FROM %s WHERE t IN ((1, 2s), (1, 3s)) ALLOW FILTERING")

        assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                "SELECT * FROM %s WHERE t > (1, 2s) ALLOW FILTERING")

        assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                "SELECT * FROM %s WHERE t >= (1, 2s) ALLOW FILTERING")

        assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                "SELECT * FROM %s WHERE t <= (1, 2s) ALLOW FILTERING")

        assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                "SELECT * FROM %s WHERE t < (1, 2s) ALLOW FILTERING")

def testFilteringOnUdtContainingDurations(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(i int, d duration)") as oudt:
        for frozen in [False, True]:
            udt = f"frozen<{oudt}>" if frozen else oudt
            with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, u {udt})") as table:
                execute(cql, table, "INSERT INTO %s (k, u) VALUES (0, {i: 1, d:2s})")
                execute(cql, table, "INSERT INTO %s (k, u) VALUES (1, {i: 2, d:3s})")
                execute(cql, table, "INSERT INTO %s (k, u) VALUES (2, {i: 1, d:3s})")

                if frozen:
                    assert_rows(execute(cql, table, "SELECT * FROM %s WHERE u = {i: 1, d:2s} ALLOW FILTERING"),
                           [0, user_type("i", 1, "d", Duration(0, 0, 2000000000))])

                # Scylla does support this case - see
                #  test_filtering.py::test_filtering_with_in_relation
                #assert_invalid_message(cql, table, "IN predicates on non-primary-key columns (u) is not yet supported",
                #    "SELECT * FROM %s WHERE u IN ({i: 2, d:3s}, {i: 1, d:3s}) ALLOW FILTERING")

                assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                    "SELECT * FROM %s WHERE u > {i: 1, d:3s} ALLOW FILTERING")

                assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                    "SELECT * FROM %s WHERE u >= {i: 1, d:3s} ALLOW FILTERING")

                assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                    "SELECT * FROM %s WHERE u <= {i: 1, d:3s} ALLOW FILTERING")

                assert_invalid_message_re(cql, table, ".*[dD]uration.*",
                    "SELECT * FROM %s WHERE u < {i: 1, d:3s} ALLOW FILTERING")

def testFilteringOnCollectionsWithNull(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k int, v int, l list<int>, s set<text>, m map<text, int>, PRIMARY KEY (k, v))") as table:
        execute(cql, table, "CREATE INDEX ON %s (v)")
        execute(cql, table, "CREATE INDEX ON %s (s)")
        execute(cql, table, "CREATE INDEX ON %s (m)")

        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (0, 0, [1, 2],    {'a'},      {'a' : 1})")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (0, 1, [3, 4],    {'b', 'c'}, {'a' : 1, 'b' : 2})")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (0, 2, [1],       {'a', 'c'}, {'c' : 3})")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (1, 0, [1, 2, 4], {},         {'b' : 1})")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (1, 1, [4, 5],    {'d'},      {'a' : 1, 'b' : 3})")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (1, 2, null,      null,       null)")

        for _ in before_and_after_flush(cql, table):
            # lists
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE l CONTAINS 1 ALLOW FILTERING"), [1, 0], [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND l CONTAINS 1 ALLOW FILTERING"), [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE l CONTAINS 2 ALLOW FILTERING"), [1, 0], [0, 0])
            assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE l CONTAINS 6 ALLOW FILTERING"))

            # sets
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE s CONTAINS 'a' ALLOW FILTERING" ), [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND s CONTAINS 'a' ALLOW FILTERING"), [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE s CONTAINS 'd' ALLOW FILTERING"), [1, 1])
            assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE s CONTAINS 'e' ALLOW FILTERING"))

            # maps
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS 1 ALLOW FILTERING"), [1, 0], [1, 1], [0, 0], [0, 1])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS 1 ALLOW FILTERING"), [0, 0], [0, 1])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS 2 ALLOW FILTERING"), [0, 1])
            assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE m CONTAINS 4 ALLOW FILTERING"))

            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS KEY 'a' ALLOW FILTERING"), [1, 1], [0, 0], [0, 1])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS KEY 'a' ALLOW FILTERING"), [0, 0], [0, 1])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS KEY 'c' ALLOW FILTERING"), [0, 2])

def testMixedTTLOnColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, i int)") as table:
        execute(cql, table, "INSERT INTO %s (k) VALUES (2);")
        execute(cql, table, "INSERT INTO %s (k, i) VALUES (1, 1) USING TTL 100;")
        execute(cql, table, "INSERT INTO %s (k, i) VALUES (3, 3) USING TTL 100;")
        assert_rows(execute(cql, table, "SELECT k, i FROM %s "),
                   [1, 1],
                   [2, None],
                   [3, 3])

        rs = execute(cql, table, "SELECT k, i, ttl(i) AS name_ttl FROM %s")
        i = 0
        for row in rs:
            if i % 2 == 0: # Every odd row has a null i/ttl
                assert row.name_ttl >= 70 and row.name_ttl <= 100
            else:
                assert row.name_ttl is None
            i += 1

def testMixedTTLOnColumnsWide(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k int, c int, i int, PRIMARY KEY (k, c))") as table:
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (2, 2);")
        execute(cql, table, "INSERT INTO %s (k, c, i) VALUES (1, 1, 1) USING TTL 100;")
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (1, 2) ;")
        execute(cql, table, "INSERT INTO %s (k, c, i) VALUES (1, 3, 3) USING TTL 100;")
        execute(cql, table, "INSERT INTO %s (k, c, i) VALUES (3, 3, 3) USING TTL 100;")
        assert_rows(execute(cql, table, "SELECT k, c, i FROM %s "),
                   [1, 1, 1],
                   [1, 2, None],
                   [1, 3, 3],
                   [2, 2, None],
                   [3, 3, 3])

        rs = execute(cql, table, "SELECT k, c, i, ttl(i) AS name_ttl FROM %s")
        i = 0
        for row in rs:
            if i % 2 == 0: # Every odd row has a null i/ttl
                assert row.name_ttl >= 70 and row.name_ttl <= 100
            else:
                assert row.name_ttl is None
            i += 1

# CASSANDRA-14989 (Scylla issue #10448)
def testTokenFctAcceptsValidArguments(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))") as table:
        execute(cql, table, "INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')")
        assert_row_count(execute(cql, table, "SELECT token(k1, k2) FROM %s"), 1)

def testTokenFctRejectsInvalidColumnName(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))") as table:
        execute(cql, table, "INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')")
        # Slightly different error messages in Scylla and Cassandra
        assert_invalid_message(cql, table, "name s1", "SELECT token(s1, k1) FROM %s")

def testTokenFctRejectsInvalidColumnType(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))") as table:
        execute(cql, table, "INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')")
        assert_invalid_message(cql, table, "Type error: k2 cannot be passed as argument 0 of function system.token of type uuid",
                             "SELECT token(k2, k1) FROM %s")

def testTokenFctRejectsInvalidColumnCount(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))") as table:
        execute(cql, table, "INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')")
        assert_invalid_message(cql, table, "Invalid number of arguments in call to function system.token: 2 required but 1 provided",
                             "SELECT token(k1) FROM %s")

# UDF test skipped because of different languages in Scylla.
# TODO: Finish translating this test.
@pytest.mark.skip("UDF tests not yet translated")
def testCreatingUDFWithSameNameAsBuiltin_PrefersCompatibleArgs_SameKeyspace(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))") as table:
        createFunctionOverload(KEYSPACE + ".token", "double",
                               "CREATE FUNCTION %s (val double) RETURNS null ON null INPUT RETURNS double LANGUAGE java AS 'return 10.0d;'")
        execute(cql, table, "INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')")
        assert_rows(execute(cql, table, "SELECT token(10) FROM %s"), row(10.0))

# UDF test skipped because of different languages in Scylla.
# TODO: Finish translating this test.
@pytest.mark.skip("UDF tests not yet translated")
def testCreatingUDFWithSameNameAsBuiltin_FullyQualifiedFunctionNameWorks(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))") as table:
        createFunctionOverload(KEYSPACE + ".token", "double",
                               "CREATE FUNCTION %s (val double) RETURNS null ON null INPUT RETURNS double LANGUAGE java AS 'return 10.0d;'")
        execute(cql, table, "INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')")
        assert_rows(execute(cql, table, "SELECT " + KEYSPACE + ".token(10) FROM %s"), row(10.0))

# UDF test skipped because of different languages in Scylla.
# TODO: Finish translating this test.
@pytest.mark.skip("UDF tests not yet translated")
def testCreatingUDFWithSameNameAsBuiltin_PrefersCompatibleArgs(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))") as table:
        createFunctionOverload(KEYSPACE + ".token", "double",
                               "CREATE FUNCTION %s (val double) RETURNS null ON null INPUT RETURNS double LANGUAGE java AS 'return 10.0d;'")
        execute(cql, table, "INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')")
        assertRowCount(execute(cql, table, "SELECT token(k1, k2) FROM %s"), 1)

# UDF test skipped because of different languages in Scylla.
# TODO: Finish translating this test.
@pytest.mark.skip("UDF tests not yet translated")
def testCreatingUDFWithSameNameAsBuiltin_FullyQualifiedFunctionNameWorks_SystemKeyspace(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))") as table:
        createFunctionOverload(KEYSPACE + ".token", "double",
                               "CREATE FUNCTION %s (val double) RETURNS null ON null INPUT RETURNS double LANGUAGE java AS 'return 10.0d;'")
        execute(cql, table, "INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')")
        assertRowCount(execute(cql, table, "SELECT system.token(k1, k2) FROM %s"), 1)
