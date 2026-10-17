# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2022-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ...porting import *
from cassandra.query import UNSET_VALUE
from ....util import is_scylla

# The Java test is called textInvalidMapEntryPredicate (sic), but pytest only
# runs functions whose name begins with "test".
# Cassandra 5 fails this test, with the error "Column ck cannot be used as a
# map", because of a bug with the descending order fixed in Cassandra 6
# (CASSANDRA-19950), so the test is marked new_to_cassandra_6.
# Reproduces SCYLLADB-5210 (server error std::bad_cast, for a restriction on an
# element of a frozen collection clustering column with descending order)
@pytest.mark.xfail(reason="SCYLLADB-5210")
def testInvalidMapEntryPredicate(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck frozen<map<int, int>>, v int, PRIMARY KEY(pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)") as table:
        # Cassandra 6 says "Map-entry predicates", Cassandra 5 and Scylla say
        # "Map-entry equality predicates".
        assert_invalid_message_re(cql, table, "Map-entry (equality )?predicates on frozen map column ck are not supported",
                             "SELECT * FROM %s WHERE pk=? AND ck[0] = ?", 0, 0)

def testInvalidCollectionEqualityRelation(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b set<int>, c list<int>, d map<int, int>)") as table:
        execute(cql, table, "CREATE INDEX ON %s (b)")
        execute(cql, table, "CREATE INDEX ON %s (c)")
        execute(cql, table, "CREATE INDEX ON %s (d)")
        assert_invalid_message(cql, table, "Collection column 'b' (set<int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND b = ?", {0})
        assert_invalid_message(cql, table, "Collection column 'c' (list<int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND c = ?", [0])
        assert_invalid_message(cql, table, "Collection column 'd' (map<int, int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND d = ?", {0: 0})

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
        # Cassandra 6 changed this message from Cassandra 5's
        # 'Unsupported "!=" relation: b != 5'
        assert_invalid_message_re(cql, table, "Unsupported \"!=\" relation: b != 5|Collection column 'b' \\(set<int>\\) cannot be restricted by a '!=' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b != 5")
        # Scylla now supports IS NOT NULL on non-frozen collection columns in
        # regular SELECT queries (with ALLOW FILTERING) - the predicate only
        # tests whether the column has any value, so unlike the relations above
        # it is meaningful for non-frozen collections. Cassandra still rejects
        # it outside of materialized view creation.
        if is_scylla(cql):
            assert_invalid_message(cql, table, "ALLOW FILTERING",
                    "SELECT * FROM %s WHERE c = 0 AND b IS NOT NULL")
            assert_row_count(execute(cql, table, "SELECT * FROM %s WHERE c = 0 AND b IS NOT NULL ALLOW FILTERING"), 1)
        else:
            assert_invalid_message(cql, table, "IS NOT",
                    "SELECT * FROM %s WHERE c = 0 AND b IS NOT NULL")

# The steps that Cassandra 6 added to testInvalidCollectionNonEQRelation for
# its new BETWEEN and NOT IN operators. They are in a separate test so the
# original test keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testInvalidCollectionNonEQRelationWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b set<int>, c int)") as table:
        execute(cql, table, "CREATE INDEX ON %s (c)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, {0}, 0)")

        assert_invalid_message(cql, table, "Collection column 'b' (set<int>) cannot be restricted by a 'BETWEEN' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b BETWEEN ? AND ?", {0}, {0})
        assert_invalid_message(cql, table, "Collection column 'b' (set<int>) cannot be restricted by a 'NOT IN' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b NOT IN (?)", {0})

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
        #assert_invalid_message(cql, table, "c cannot be restricted by more than one relation if it includes an Equal",
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

# The steps that Cassandra 6 added to testClusteringColumnRelations for its
# new BETWEEN operator. They are in a separate test so the original test keeps
# running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testClusteringColumnRelationsWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a text, b int, c int, d int, primary key (a, b, c))") as table:
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND c BETWEEN ? AND ?", "first", 1, 4, 5),
                   ["first", 1, 5, 1])

        # Scylla does allow such queries, and their correctness is tested in
        # test_filtering.py::test_multiple_restrictions_on_same_column
        #assert_invalid_message(cql, table, "More than one restriction was found for the start bound on b",
        #                     "SELECT * FROM %s WHERE a = ? AND b > ? AND b BETWEEN ? AND ?", "first", 6, 3, 2)

        #assert_invalid_message(cql, table, "More than one restriction was found for the end bound on b",
        #                     "SELECT * FROM %s WHERE a = ? AND b < ? AND b BETWEEN ? AND ?", "first", 6, 3, 2)

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

# The steps that Cassandra 6 added to testClusteringColumnRelationsWithClusteringOrder
# for its new BETWEEN operator. They are in a separate test so the original
# test keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testClusteringColumnRelationsWithClusteringOrderWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a text, b int, c int, d int, primary key (a, b, c)) WITH CLUSTERING ORDER BY (b DESC, c ASC)") as table:
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4)

        assert_rows(execute(cql, table, "select * from %s where a = ? and b between ? and ? order by b ASC",
                           "first", 1, 2),
                   ["first", 1, 5, 1],
                   ["first", 2, 6, 2])

        assert_rows(execute(cql, table, "select * from %s where a = ? and b between ? and ? order by b DESC",
                           "first", 1, 2),
                   ["first", 2, 6, 2],
                   ["first", 1, 5, 1])

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

# The steps that Cassandra 6 added to testAllowFilteringWithClusteringColumn
# for its new BETWEEN operator. They are in a separate test so the original
# test keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testAllowFilteringWithClusteringColumnWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(k int, c int, v int, primary key (k, c))") as table:
        execute(cql, table, "INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 1, 2, 1)
        execute(cql, table, "INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 1, 3, 2)
        execute(cql, table, "INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 2, 2, 3)

        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c BETWEEN ? AND ?", 2, 4)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c BETWEEN ? AND ? ALLOW FILTERING", 2, 3),
                   [1, 2, 1],
                   [1, 3, 2],
                   [2, 2, 3])

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

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c = ? AND s >= ? AND s <= ? ALLOW FILTERING", 1, 1, 1),
                   [1, 1, 1, 1],
                   [1, 2, 1, 1])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c = ? AND s < ? ALLOW FILTERING", 1, 2),
                   [1, 1, 1, 1],
                   [1, 2, 1, 1])

# The step that Cassandra 6 added to testAllowFilteringWithIndexedColumnAndStaticColumns
# for its new BETWEEN operator. It is in a separate test so the original
# test keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testAllowFilteringWithIndexedColumnAndStaticColumnsWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, s int static, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "CREATE INDEX ON %s(c)")

        execute(cql, table, "INSERT INTO %s(a, b, c, s) VALUES(?, ?, ?, ?)", 1, 1, 1, 1)
        execute(cql, table, "INSERT INTO %s(a, b, c) VALUES(?, ?, ?)", 1, 2, 1)
        execute(cql, table, "INSERT INTO %s(a, s) VALUES(?, ?)", 3, 3)
        execute(cql, table, "INSERT INTO %s(a, b, c, s) VALUES(?, ?, ?, ?)", 2, 1, 1, 2)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c = ? AND s BETWEEN ? AND ? ALLOW FILTERING", 1, 1, 1),
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

        # Checks that IN restrictions are not used for index queries
        assert_invalid_message(cql, table, "PRIMARY KEY column \"time\" cannot be restricted as preceding column \"author\" is not restricted",
                            "SELECT v1 FROM %s WHERE time IN (1, 2)")
        assert_invalid_message(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT v1 FROM %s WHERE id2 IN (0, 2)")

        # Checks that the IN queries works with filtering
        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE time IN (1, 2) ALLOW FILTERING"), ["B"], ["C"], ["E"])
        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE id2 IN (0, 2) ALLOW FILTERING"), ["A"], ["B"], ["D"])

        # Checks index query with filtering
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

# The step that Cassandra 6 added to testCompositeIndexWithPrimaryKey for its
# new BETWEEN operator. It is in a separate test so the original test keeps
# running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testCompositeIndexWithPrimaryKeyWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(blog_id int, time1 int, time2 int, author text, content text,  PRIMARY KEY (blog_id, time1, time2))") as table:
        execute(cql, table, "CREATE INDEX ON %s(author)")
        req = "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)"
        execute(cql, table, req, 1, 0, 0, "foo", "bar1")
        execute(cql, table, req, 1, 0, 1, "foo", "bar2")
        execute(cql, table, req, 2, 1, 0, "foo", "baz")
        execute(cql, table, req, 3, 0, 1, "gux", "qux")

        assert_empty(execute(cql, table, "SELECT content FROM %s WHERE time1 = 1 AND time2 BETWEEN 1 AND 2 AND author='foo' ALLOW FILTERING"))

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

# The steps that Cassandra 6 added to testMultiplePartitionKeyWithIndex for its
# new BETWEEN operator. They are in a separate test so the original test keeps
# running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testMultiplePartitionKeyWithIndexWithBetween(cql, test_keyspace, new_to_cassandra_6):
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
                             "SELECT * FROM %s WHERE a = ? AND c = ? AND d BETWEEN ? AND ? AND f = ?", 0, 1, 1, 5, 0)

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND c = ? AND d BETWEEN ? AND ? AND f = ?", 0, 0, 1, 1, 0, 5))

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND c = ? AND d BETWEEN ? AND ? AND f = ? ALLOW FILTERING", 0, 1, 1, 0, 5))

def testFunctionCallWithUnset(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s text, i int)") as table:
        # The error messages in Scylla and Cassandra here are slightly
        # different.
        assert_invalid_message(cql, table, "unset",
                             "SELECT * FROM %s WHERE token(k) >= token(?)", UNSET_VALUE)
        # Cassandra now uses the snake_case name blob_as_int(), which
        # Scylla doesn't support yet (SCYLLADB-5141). Since Cassandra still
        # supports the old name blobAsInt(), we keep using it.
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

# The steps that Cassandra 6 added to testWithUnsetValues for its new NOT IN
# operator. They are in a separate test so the original test keeps running on
# older Cassandra and on Scylla.
# Reproduces SCYLLADB-5213 (UNSET_VALUE not detected when no row reaches the
# filter - here the table is empty)
@pytest.mark.xfail(reason="SCYLLADB-5213")
def testWithUnsetValuesWithNotIn(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(k int, i int, j int, s text, PRIMARY KEY (k,i,j))") as table:
        execute(cql, table, "CREATE INDEX ON %s (s)")
        # partition key
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k NOT IN ? ALLOW FILTERING", UNSET_VALUE)
        # The NOT IN(?) and NOT IN(?,?) checks are in a separate test,
        # testWithUnsetValuesWithNotInPartitionKey, below.
        # clustering column
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k = 1 AND i NOT IN ?", UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k = 1 AND i NOT IN(?)", UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k = 1 AND i NOT IN(?,?)", 1, UNSET_VALUE)

# Two more checks that Cassandra 6 added to testWithUnsetValues. They fail
# on Cassandra 6 because it reports the "?" in "k NOT IN(?)" as a partition
# key bind marker in the prepared statement's metadata (CASSANDRA-21740), so
# the Python driver refuses to bind it to UNSET_VALUE. A value in NOT IN is
# a value to exclude, and can't be used to route the request.
# Reproduces SCYLLADB-5213 (UNSET_VALUE not detected when no row reaches the
# filter - here the table is empty)
@pytest.mark.xfail(reason="SCYLLADB-5213")
def testWithUnsetValuesWithNotInPartitionKey(cql, test_keyspace, new_to_cassandra_6, cassandra_bug):
    with create_table(cql, test_keyspace, "(k int, i int, j int, s text, PRIMARY KEY (k,i,j))") as table:
        execute(cql, table, "CREATE INDEX ON %s (s)")
        # partition key
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k NOT IN(?) ALLOW FILTERING", UNSET_VALUE)
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k NOT IN(?,?) ALLOW FILTERING", 1, UNSET_VALUE)

# The step that Cassandra 6 added to testWithUnsetValues for its new BETWEEN
# operator. It is in a separate test so the original test keeps running on
# older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testWithUnsetValuesWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(k int, i int, j int, s text, PRIMARY KEY (k,i,j))") as table:
        execute(cql, table, "CREATE INDEX ON %s (s)")
        # range
        assert_invalid_message(cql, table, "unset value", "SELECT * from %s WHERE k = 1 AND i BETWEEN ? AND ?", 1, UNSET_VALUE)

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

# The step that Cassandra 6 added to testInvalidSliceRestrictionOnPartitionKey
# for its new BETWEEN operator. It is in a separate test so the original test
# keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testInvalidSliceRestrictionOnPartitionKeyWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c text)") as table:
        assert_invalid_message(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a) BETWEEN (1) AND (4)")

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

# The step that Cassandra 6 added to testInvalidMulticolumnSliceRestrictionOnPartitionKey
# for its new BETWEEN operator. It is in a separate test so the original test
# keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testInvalidMulticolumnSliceRestrictionOnPartitionKeyWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c text, PRIMARY KEY ((a, b)))") as table:
        assert_invalid_message(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b) BETWEEN (1, 1) AND (4, 5) and a = 1")

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

# The steps that Cassandra 6 added to testInvalidColumnNames for its new
# NOT IN, NOT CONTAINS, NOT CONTAINS KEY and BETWEEN operators. They are in
# a separate test so the original test keeps running on older Cassandra and
# on Scylla.
# Reproduces #12911 (NOT CONTAINS, NOT CONTAINS KEY) and SCYLLADB-5153
# (the BETWEEN operator)
@pytest.mark.xfail(reason="#12911, SCYLLADB-5153")
def testInvalidColumnNamesWithNot(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c map<int, int>, PRIMARY KEY (a, b))") as table:
        # Slightly different error messages in Scylla and Cassandra. Both
        # include the string "name d".
        assert_invalid_message(cql, table, "name d", "SELECT * FROM %s WHERE d NOT CONTAINS 0")
        assert_invalid_message(cql, table, "name d", "SELECT * FROM %s WHERE d NOT CONTAINS KEY 0")
        # Here, Cassandra says "Undefined column name d" but Scylla gives
        # a clearer error message about the real cause: "Aliases aren't
        # allowed in the where clause ('d = 0')".
        assert_invalid(cql, table, "SELECT b AS d FROM %s WHERE d NOT IN (0, 1)")
        assert_invalid(cql, table, "SELECT c AS d FROM %s WHERE d NOT CONTAINS 0")
        assert_invalid(cql, table, "SELECT c AS d FROM %s WHERE d NOT CONTAINS KEY 0")
        assert_invalid_message(cql, table, "name d", "SELECT d FROM %s WHERE b BETWEEN 0 AND 0")

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
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b BETWEEN ? AND ?", udt, udt)
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b IN (?)", udt)
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b NOT IN (?)", udt)
            # Scylla and Cassandra print different errors here - Scylla
            # says that b is not a string, Cassandra says it is a non-frozen
            # UDT.
            assert_invalid(cql, table, "SELECT * FROM %s WHERE b LIKE ?", udt)
            # Before Cassandra 6, Cassandra did not support the "!=" operator
            # in this context, and the test checked for that error message.
            # Cassandra 6 supports "!=", so it checks the non-frozen UDT
            # message, which is not relevant to Scylla (see above).
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b != {a: 0}", udt)
            #assert_invalid_message(cql, table, msg, "SELECT * FROM %s WHERE b != {a: 0}", udt)
            # Reproduces #10632:
            # Scylla now supports IS NOT NULL on non-frozen UDT columns in
            # regular SELECT queries (with ALLOW FILTERING), so the restriction
            # itself is no longer rejected with "b IS NOT NULL is only supported
            # in materialized view creation".
            if is_scylla(cql):
                assert_empty(execute(cql, table, "SELECT * FROM %s WHERE b IS NOT NULL ALLOW FILTERING"))
            else:
                assert_invalid_message(cql, table, "b IS NOT",
                                 "SELECT * FROM %s WHERE b IS NOT NULL", udt)
            assert_invalid_message(cql, table, "Cannot use CONTAINS on non-collection column",
                             "SELECT * FROM %s WHERE b CONTAINS ?", udt)

# The steps that Cassandra 6 added to testInvalidNonFrozenUDTRelation for its
# new NOT CONTAINS operator. They are in a separate test so the original test
# keeps running on older Cassandra and on Scylla.
# Reproduces #12911 (NOT CONTAINS, NOT CONTAINS KEY, map element !=)
@pytest.mark.xfail(reason="#12911")
def testInvalidNonFrozenUDTRelationWithNot(cql, test_keyspace, new_to_cassandra_6):
    with create_type(cql, test_keyspace, "(a int)") as type:
        with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b {type})") as table:
            udt = user_type("a", 1)
            assert_invalid_message(cql, table, "Cannot use NOT CONTAINS on non-collection column b",
                             "SELECT * FROM %s WHERE b NOT CONTAINS ?", udt)
            assert_invalid_message(cql, table, "Cannot use NOT CONTAINS on non-collection column b",
                             "SELECT * FROM %s WHERE b NOT CONTAINS ?", udt)

def testInRestrictionWithClusteringColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key int, c1 int, c2 int, s1 text static, PRIMARY KEY ((key, c1), c2))") as table:
        execute(cql, table, "INSERT INTO %s (key, c1, c2, s1) VALUES ( 10, 11, 1, 's1')")
        execute(cql, table, "INSERT INTO %s (key, c1, c2, s1) VALUES ( 10, 12, 2, 's2')")
        execute(cql, table, "INSERT INTO %s (key, c1, c2, s1) VALUES ( 10, 13, 3, 's3')")
        execute(cql, table, "INSERT INTO %s (key, c1, c2, s1) VALUES ( 10, 13, 4, 's4')")
        execute(cql, table, "INSERT INTO %s (key, c1, c2, s1) VALUES ( 20, 21, 1, 's1')")
        execute(cql, table, "INSERT INTO %s (key, c1, c2, s1) VALUES ( 20, 22, 2, 's2')")
        execute(cql, table, "INSERT INTO %s (key, c1, c2, s1) VALUES ( 20, 22, 3, 's3')")

        assert_rows(execute(cql, table, "SELECT * from %s WHERE key = ? AND c1 IN (?, ?)", 10, 21, 13),
                   [10, 13, 3, "s4"],
                   [10, 13, 4, "s4"])

        assert_rows(execute(cql, table, "SELECT * from %s WHERE key = ? AND c2 IN (?, ?) ALLOW FILTERING", 20, 1, 2),
                   [20, 22, 2, "s3"],
                   [20, 21, 1, "s1"])

        assert_rows(execute(cql, table, "SELECT * from %s WHERE c1 = ? AND c2 IN (?, ?) ALLOW FILTERING", 13, 2, 3),
                   [10, 13, 3, "s4"])

        assert_rows_ignoring_order(execute(cql, table, "SELECT * from %s WHERE c2 IN (?, ?) ALLOW FILTERING", 1, 2),
                                [10, 11, 1, "s1"],
                                [10, 12, 2, "s2"],
                                [20, 21, 1, "s1"],
                                [20, 22, 2, "s3"])

        # Scylla does allow IN NULL (see commit 52bbc1065c8) so this test
        # is commented out
        #assert_invalid_message(cql, table, "Invalid null value for column c2",
        #                     "SELECT * from %s WHERE key = 10 AND c2 IN (1, null) ALLOW FILTERING")

def testInRestrictionsWithAllowFiltering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk1 int, pk2 int, c text, s int static, v int, primary key((pk1, pk2), c))") as table:
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) values (?, ?, ?, ?, ?)", 1, 0, "5", 1, 3)
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) values (?, ?, ?, ?, ?)", 1, 0, "7", 1, 2)
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) values (?, ?, ?, ?, ?)", 1, 1, "7", 1, 3)
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) values (?, ?, ?, ?, ?)", 2, 0, "4", 2, 1)
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) values (?, ?, ?, ?, ?)", 2, 3, "6", 2, 8)

        # Test filtering on regular columns
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v IN (?, ?) ALLOW FILTERING", 4, 3),
                   [1, 0, "5", 1, 3],
                   [1, 1, "7", 1, 3])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v IN ? ALLOW FILTERING", [4, 3]),
                   [1, 0, "5", 1, 3],
                   [1, 1, "7", 1, 3])

        # Test filtering on clustering columns
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c IN (?, ?, ?) ALLOW FILTERING", "7", "6", "8"),
                   [2, 3, "6", 2, 8],
                   [1, 0, "7", 1, 2],
                   [1, 1, "7", 1, 3])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c IN ? ALLOW FILTERING", ["7", "6", "8"]),
                   [2, 3, "6", 2, 8],
                   [1, 0, "7", 1, 2],
                   [1, 1, "7", 1, 3])

        # Test filtering on partition keys
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk1 IN (?, ?) ALLOW FILTERING", 1, 3),
                   [1, 0, "5", 1, 3],
                   [1, 0, "7", 1, 2],
                   [1, 1, "7", 1, 3])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk1 IN ? ALLOW FILTERING", [1, 3]),
                   [1, 0, "5", 1, 3],
                   [1, 0, "7", 1, 2],
                   [1, 1, "7", 1, 3])

        # Test filtering on static columns
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE s IN (?, ?) ALLOW FILTERING", 1, 3),
                   [1, 0, "5", 1, 3],
                   [1, 0, "7", 1, 2],
                   [1, 1, "7", 1, 3])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE s IN ? ALLOW FILTERING", [1, 3]),
                   [1, 0, "5", 1, 3],
                   [1, 0, "7", 1, 2],
                   [1, 1, "7", 1, 3])

# Reproduces #15099 (with IN and ORDER BY, Scylla breaks ties between rows of
# different partitions differently from Cassandra: here Scylla returns
# (2, "0") before (1, "0"))
@pytest.mark.xfail(reason="#15099")
def testInRestrictionsWithAllowFilteringAndOrdering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c text, v int, primary key(pk, c)) WITH CLUSTERING ORDER BY (c DESC)") as table:
        execute(cql, table, "INSERT INTO %s (pk, c, v) values (?, ?, ?)", 1, "0", 5)
        execute(cql, table, "INSERT INTO %s (pk, c, v) values (?, ?, ?)", 1, "1", 7)
        execute(cql, table, "INSERT INTO %s (pk, c, v) values (?, ?, ?)", 1, "2", 7)
        execute(cql, table, "INSERT INTO %s (pk, c, v) values (?, ?, ?)", 2, "0", 4)
        execute(cql, table, "INSERT INTO %s (pk, c, v) values (?, ?, ?)", 2, "2", 6)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c IN (?, ?, ?) ALLOW FILTERING", 1, "2", "0", "8"),
                   [1, "2", 7],
                   [1, "0", 5])

        # The queries with ORDER BY and IN on the partition key need to be
        # without paging, otherwise we get from Cassandra (and Scylla):
        # "Cannot page queries with both ORDER BY and a IN restriction on the
        # partition key; you must either remove the ORDER BY or the IN and
        # sort client side, or disable paging for this query"
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c IN ? ORDER BY c ASC ALLOW FILTERING", 2, ["2", "8", "0"]),
                   [2, "0", 4],
                   [2, "2", 6])

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk IN (?, ?) AND c IN (?, ?, ?) ALLOW FILTERING", 1, 2, "2", "0", "8"),
                   [1, "2", 7],
                   [1, "0", 5],
                   [2, "2", 6],
                   [2, "0", 4])

        assert_rows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE pk IN ? AND c IN ? ORDER BY c ASC ALLOW FILTERING", [1, 2], ["2", "8", "0"]),
                   [1, "0", 5],
                   [2, "0", 4],
                   [1, "2", 7],
                   [2, "2", 6])

def testInRestrictionsWithCountersAndAllowFiltering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, v counter, primary key (pk))") as table:
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE v IN (?, ?) ALLOW FILTERING", 0, 1))

        execute(cql, table, "UPDATE %s SET v = v + 1 WHERE pk = 1")
        execute(cql, table, "UPDATE %s SET v = v + 2 WHERE pk = 2")
        execute(cql, table, "UPDATE %s SET v = v + 1 WHERE pk = 3")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v IN (?, ?) ALLOW FILTERING", 0, 1),
                   [1, 1],
                   [3, 1])

def testSliceRestrictionWithNegativeClusteringColumnValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c int, v int, PRIMARY KEY (pk, c))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, -2, -2)")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, -1, -1)")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, 0, 0)")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, 2, 2)")

        assert_rows(execute(cql, table, "SELECT * from %s WHERE pk = ? AND c > ? AND c <= ?", 1, 0, 2),
                   [1, 1, 1],
                   [1, 2, 2])

        assert_rows(execute(cql, table, "SELECT * from %s WHERE pk = ? AND c > ? AND c <= ?", 1, -4, -1),
                   [1, -2, -2],
                   [1, -1, -1])

        # The last step of this test, using the new BETWEEN operator of
        # Cassandra 6, is in a separate test below, so this test also runs
        # on older Cassandra and on Scylla.

# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testSliceRestrictionWithNegativeClusteringColumnValuesWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, c int, v int, PRIMARY KEY (pk, c))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, -2, -2)")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, -1, -1)")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, 0, 0)")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (1, 2, 2)")

        assert_rows(execute(cql, table, "SELECT * from %s WHERE pk = ? AND c BETWEEN ? AND ?", 1, -4, -1),
                   [1, -2, -2],
                   [1, -1, -1])

def testClusteringSlicesWithNotIn(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a text, b int, c int, d int, primary key(a, b, c))") as table:
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 1, 4, 1)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 5, 2)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 6, 3)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 7, 4)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 3, 8, 5)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 3, 9, 6)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 4, 1, 7)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 4, 2, 8)

        # restrict first clustering column by NOT IN
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ?", "key", [2, 4, 5]),
                   ["key", 1, 4, 1],
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in (?, ?, ?)", "key", 2, 4, 5),
                   ["key", 1, 4, 1],
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])

        # use different order of items in NOT IN list:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in (?, ?, ?)", "key", 5, 2, 4),
                   ["key", 1, 4, 1],
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in (?, ?, ?)", "key", 5, 4, 2),
                   ["key", 1, 4, 1],
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])

        # restrict last clustering column by NOT IN
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in ?", "key", 2, [5, 6]),
                   ["key", 2, 7, 4])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in (?, ?)", "key", 2, 5, 6),
                   ["key", 2, 7, 4])

        # empty NOT IN should have no effect:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in ?", "key", 2, []),
                   ["key", 2, 5, 2],
                   ["key", 2, 6, 3],
                   ["key", 2, 7, 4])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in ()", "key", 2),
                   ["key", 2, 5, 2],
                   ["key", 2, 6, 3],
                   ["key", 2, 7, 4])

        # NOT IN value that doesn't match any data should have no effect:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in (?)", "key", 2, 0),
                   ["key", 2, 5, 2],
                   ["key", 2, 6, 3],
                   ["key", 2, 7, 4])

        # Duplicate NOT IN values:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in (?, ?)", "key", 2, 5, 5),
                   ["key", 2, 6, 3],
                   ["key", 2, 7, 4])

        # mix NOT IN and '<' and '<=' comparison on the same column
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b < ?", "key", [2, 5], 1)) # empty
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b < ?", "key", [2, 5], 3),
                   ["key", 1, 4, 1])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b <= ?", "key", [2], 2),
                   ["key", 1, 4, 1])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b <= ?", "key", [2], 3),
                   ["key", 1, 4, 1],
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b <= ?", "key", [2], 10),
                   ["key", 1, 4, 1],
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6],
                   ["key", 4, 1, 7],
                   ["key", 4, 2, 8])

        # mix NOT IN and '>' and '>=' comparison on the same column
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ?", "key", [2], 1),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6],
                   ["key", 4, 1, 7],
                   ["key", 4, 2, 8])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ?", "key", [2], 2),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6],
                   ["key", 4, 1, 7],
                   ["key", 4, 2, 8])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b >= ?", "key", [2], 2),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6],
                   ["key", 4, 1, 7],
                   ["key", 4, 2, 8])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b >= ?", "key", [2], 4),
                   ["key", 4, 1, 7],
                   ["key", 4, 2, 8])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b >= ?", "key", [2], 0),
                   ["key", 1, 4, 1],
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6],
                   ["key", 4, 1, 7],
                   ["key", 4, 2, 8])

        # mix NOT IN and range slice
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", [2], 1, 4),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b >= ? and b < ?", "key", [2], 1, 4),
                   ["key", 1, 4, 1],
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ? and b <= ?", "key", [2], 1, 4),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6],
                   ["key", 4, 1, 7],
                   ["key", 4, 2, 8])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b >= ? and b <= ?", "key", [2], 1, 4),
                   ["key", 1, 4, 1],
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6],
                   ["key", 4, 1, 7],
                   ["key", 4, 2, 8])

        # Collision between a slice bound and NOT IN value:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b < ?", "key", [2], 2),
                   ["key", 1, 4, 1])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ?", "key", [2], 2),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6],
                   ["key", 4, 1, 7],
                   ["key", 4, 2, 8])

        # NOT IN value outside the slice range:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", [0], 2, 4),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", [10], 2, 4),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])

        # multiple NOT IN on the same column, use different ways of passing a list
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b not in ?", "key", [1, 2], [4]),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in (?, ?) and b not in (?)", "key", 1, 2, 4),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in (?, ?) and b not in ?", "key", 1, 2, [4]),
                   ["key", 3, 8, 5],
                   ["key", 3, 9, 6])

        # mix IN and NOT IN
        assert_rows(execute(cql, table, "select * from %s where a = ? and b in ? and c not in ?", "key", [2, 3], [5, 6, 9]),
                   ["key", 2, 7, 4],
                   ["key", 3, 8, 5])

def testClusteringSlicesWithNotInAndReverseOrdering(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a text, b int, c int, d int, primary key(a, b, c)) with clustering order by (b desc, c desc)") as table:
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 1, 4, 1)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 5, 2)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 6, 3)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 7, 4)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 3, 8, 5)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 3, 9, 6)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 4, 1, 7)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 4, 2, 8)

        # restrict first clustering column by NOT IN
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ?", "key", [2, 4, 5]),
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5],
                   ["key", 1, 4, 1])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in (?, ?, ?)", "key", 2, 4, 5),
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5],
                   ["key", 1, 4, 1])

        # restrict last clustering column by NOT IN
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in ?", "key", 2, [5, 6]),
                   ["key", 2, 7, 4])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in (?, ?)", "key", 2, 5, 6),
                   ["key", 2, 7, 4])

        # empty NOT IN should have no effect:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in ?", "key", 2, []),
                   ["key", 2, 7, 4],
                   ["key", 2, 6, 3],
                   ["key", 2, 5, 2])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in ()", "key", 2),
                   ["key", 2, 7, 4],
                   ["key", 2, 6, 3],
                   ["key", 2, 5, 2])

        # NOT IN value that doesn't match any data should have no effect:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in (?)", "key", 2, 0),
                   ["key", 2, 7, 4],
                   ["key", 2, 6, 3],
                   ["key", 2, 5, 2])

        # Duplicate NOT IN values:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c not in (?, ?)", "key", 2, 5, 5),
                   ["key", 2, 7, 4],
                   ["key", 2, 6, 3])

        # mix NOT IN and '<' and '<=' comparison on the same column
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b < ?", "key", [2, 5], 1)) # empty
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b < ?", "key", [2, 5], 3),
                   ["key", 1, 4, 1])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b <= ?", "key", [2], 2),
                   ["key", 1, 4, 1])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b <= ?", "key", [2], 3),
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5],
                   ["key", 1, 4, 1])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b <= ?", "key", [2], 10),
                   ["key", 4, 2, 8],
                   ["key", 4, 1, 7],
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5],
                   ["key", 1, 4, 1])

        # mix NOT IN and '>' and '>=' comparison on the same column
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ?", "key", [2], 1),
                   ["key", 4, 2, 8],
                   ["key", 4, 1, 7],
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ?", "key", [2], 2),
                   ["key", 4, 2, 8],
                   ["key", 4, 1, 7],
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b >= ?", "key", [2], 2),
                   ["key", 4, 2, 8],
                   ["key", 4, 1, 7],
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b >= ?", "key", [2], 4),
                   ["key", 4, 2, 8],
                   ["key", 4, 1, 7])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b >= ?", "key", [2], 0),
                   ["key", 4, 2, 8],
                   ["key", 4, 1, 7],
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5],
                   ["key", 1, 4, 1])

        # mix NOT IN and range slice
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", [2], 1, 4),
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b >= ? and b < ?", "key", [2], 1, 4),
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5],
                   ["key", 1, 4, 1])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ? and b <= ?", "key", [2], 1, 4),
                   ["key", 4, 2, 8],
                   ["key", 4, 1, 7],
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b >= ? and b <= ?", "key", [2], 1, 4),
                   ["key", 4, 2, 8],
                   ["key", 4, 1, 7],
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5],
                   ["key", 1, 4, 1])

        # Collision between a slice bound and NOT IN value:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b < ?", "key", [2], 2),
                   ["key", 1, 4, 1])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ?", "key", [2], 2),
                   ["key", 4, 2, 8],
                   ["key", 4, 1, 7],
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])

        # NOT IN value outside the slice range:
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", [0], 2, 4),
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", [10], 2, 4),
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])

        # multiple NOT IN on the same column, use different ways of passing a list
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in ? and b not in ?", "key", [1, 2], [4]),
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in (?, ?) and b not in (?)", "key", 1, 2, 4),
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])
        assert_rows(execute(cql, table, "select * from %s where a = ? and b not in (?, ?) and b not in ?", "key", 1, 2, [4]),
                   ["key", 3, 9, 6],
                   ["key", 3, 8, 5])

        # mix IN and NOT IN
        assert_rows(execute(cql, table, "select * from %s where a = ? and b in ? and c not in ?", "key", [2, 3], [5, 6, 9]),
                   ["key", 3, 8, 5],
                   ["key", 2, 7, 4])

def testNotInRestrictionsWithAllowFiltering(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, c int, v int, primary key(pk, c))") as table:
        execute(cql, table, "insert into %s (pk, c, v) values (?, ?, ?)", 1, 1, 1)
        execute(cql, table, "insert into %s (pk, c, v) values (?, ?, ?)", 1, 2, 2)
        execute(cql, table, "insert into %s (pk, c, v) values (?, ?, ?)", 1, 3, 3)
        execute(cql, table, "insert into %s (pk, c, v) values (?, ?, ?)", 1, 4, 4)
        execute(cql, table, "insert into %s (pk, c, v) values (?, ?, ?)", 1, 5, 5)

        # empty NOT IN set
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? allow filtering", 1, []),
                   [1, 1, 1],
                   [1, 2, 2],
                   [1, 3, 3],
                   [1, 4, 4],
                   [1, 5, 5])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in () allow filtering", 1),
                   [1, 1, 1],
                   [1, 2, 2],
                   [1, 3, 3],
                   [1, 4, 4],
                   [1, 5, 5])

        # NOT IN with values that don't match any data
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in (?, ?) allow filtering", 1, -6, 20),
                   [1, 1, 1],
                   [1, 2, 2],
                   [1, 3, 3],
                   [1, 4, 4],
                   [1, 5, 5])

        # NOT IN that excludes a few values
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? allow filtering", 1, [2, 3]),
                   [1, 1, 1],
                   [1, 4, 4],
                   [1, 5, 5])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in (?, ?) allow filtering", 1, 2, 3),
                   [1, 1, 1],
                   [1, 4, 4],
                   [1, 5, 5])

        # NOT IN with one-sided slice filters:
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v < ? allow filtering", 1, [2, 3], 5),
                   [1, 1, 1],
                   [1, 4, 4])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v < ? allow filtering", 1, [2, 3, 10], 5),
                   [1, 1, 1],
                   [1, 4, 4])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v <= ? allow filtering", 1, [2, 3], 5),
                   [1, 1, 1],
                   [1, 4, 4],
                   [1, 5, 5])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v <= ? allow filtering", 1, [2, 3, 10], 5),
                   [1, 1, 1],
                   [1, 4, 4],
                   [1, 5, 5])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v > ? allow filtering", 1, [2, 3], 1),
                   [1, 4, 4],
                   [1, 5, 5])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v > ? allow filtering", 1, [0, 2, 3], 1),
                   [1, 4, 4],
                   [1, 5, 5])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v >= ? allow filtering", 1, [2, 3], 1),
                   [1, 1, 1],
                   [1, 4, 4],
                   [1, 5, 5])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v >= ? allow filtering", 1, [0, 2, 3], 1),
                   [1, 1, 1],
                   [1, 4, 4],
                   [1, 5, 5])

        # NOT IN with range filters:
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v > ? and v < ? allow filtering", 1, [2, 3], 1, 4)) # empty
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v > ? and v < ? allow filtering", 1, [2, 3], 1, 4)) # empty
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v > ? and v < ? allow filtering", 1, [2, 3], 0, 5),
                   [1, 1, 1],
                   [1, 4, 4])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v >= ? and v < ? allow filtering", 1, [2, 3], 1, 4),
                   [1, 1, 1])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v > ? and v <= ? allow filtering", 1, [2, 3], 1, 4),
                   [1, 4, 4])
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v >= ? and v <= ? allow filtering", 1, [2, 3], 1, 4),
                   [1, 1, 1],
                   [1, 4, 4])

        # more than one NOT IN clause
        assert_rows(execute(cql, table, "select * from %s where pk = ? and v not in ? and v not in ? allow filtering", 1, [2], [3]),
                   [1, 1, 1],
                   [1, 4, 4],
                   [1, 5, 5])

# Reproduces #12911 (the "!=" operator in WHERE)
@pytest.mark.xfail(reason="#12911")
def testNonEqualsRelationWithFiltering(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 2)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 3, 3)

        assert_rows(execute(cql, table, "SELECT a, b FROM %s WHERE a = ? AND c != ? ALLOW FILTERING", 0, 0),
                   [0, 1],
                   [0, 2],
                   [0, 3])
        assert_rows(execute(cql, table, "SELECT a, b FROM %s WHERE a = ? AND c != ? ALLOW FILTERING", 0, 1),
                   [0, 0],
                   [0, 2],
                   [0, 3])
        assert_rows(execute(cql, table, "SELECT a, b FROM %s WHERE a = ? AND c != ? ALLOW FILTERING", 0, -1),
                   [0, 0],
                   [0, 1],
                   [0, 2],
                   [0, 3])
        assert_rows(execute(cql, table, "SELECT a, b FROM %s WHERE a = ? AND c != ? ALLOW FILTERING", 0, 5),
                   [0, 0],
                   [0, 1],
                   [0, 2],
                   [0, 3])
        assert_rows(execute(cql, table, "SELECT a, b FROM %s WHERE a = ? AND c != ? AND c != ? ALLOW FILTERING", 0, 1, 2),
                   [0, 0],
                   [0, 3])
        assert_rows(execute(cql, table, "SELECT a, b FROM %s WHERE a = ? AND c != ? AND c < ? ALLOW FILTERING", 0, 1, 2),
                   [0, 0])
        assert_rows(execute(cql, table, "SELECT a, b FROM %s WHERE a = ? AND c != ? AND c <= ? ALLOW FILTERING", 0, 1, 2),
                   [0, 0],
                   [0, 2])
        assert_rows(execute(cql, table, "SELECT a, b FROM %s WHERE a = ? AND c != ? AND c > ? ALLOW FILTERING", 0, 2, 0),
                   [0, 1],
                   [0, 3])
        assert_rows(execute(cql, table, "SELECT a, b FROM %s WHERE a = ? AND c != ? AND c >= ? ALLOW FILTERING", 0, 2, 1),
                   [0, 1],
                   [0, 3])

# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testBetweenFilteringWithReversedOrdering(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(p int, c int, c2 int, abbreviation ascii, PRIMARY KEY (p, c, c2))") as table:
        for _ in before_and_after_flush(cql, table):
            execute(cql, table, "INSERT INTO %s(p, c, c2, abbreviation) VALUES (0, 1, 1, 'CA')")
            execute(cql, table, "INSERT INTO %s(p, c, c2, abbreviation) VALUES (0, 2, 2, 'MA')")
            execute(cql, table, "INSERT INTO %s(p, c, c2, abbreviation) VALUES (0, 3, 3, 'MA')")
            execute(cql, table, "INSERT INTO %s(p, c, c2, abbreviation) VALUES (0, 4, 4, 'TX')")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c2 BETWEEN 2 AND 3 ALLOW FILTERING"),
                       [0, 2, 2, "MA"],
                       [0, 3, 3, "MA"])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE c2 BETWEEN 3 AND 2 ALLOW FILTERING"))

    with create_table(cql, test_keyspace, "(p int, c int, c2 int, abbreviation ascii, PRIMARY KEY (p, c, c2)) WITH CLUSTERING ORDER BY (c DESC, c2 DESC)") as table:
        for _ in before_and_after_flush(cql, table):
            execute(cql, table, "INSERT INTO %s(p, c, c2, abbreviation) VALUES (0, 1, 1, 'CA')")
            execute(cql, table, "INSERT INTO %s(p, c, c2, abbreviation) VALUES (0, 2, 2, 'MA')")
            execute(cql, table, "INSERT INTO %s(p, c, c2, abbreviation) VALUES (0, 3, 3, 'MA')")
            execute(cql, table, "INSERT INTO %s(p, c, c2, abbreviation) VALUES (0, 4, 4, 'TX')")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c2 BETWEEN 2 AND 3 ALLOW FILTERING"),
                       [0, 3, 3, "MA"],
                       [0, 2, 2, "MA"])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE c2 BETWEEN 3 AND 2 ALLOW FILTERING"))
