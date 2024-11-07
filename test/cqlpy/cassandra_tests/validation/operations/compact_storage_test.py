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

compactOption = " WITH COMPACT STORAGE"

# ALTER ... DROP COMPACT STORAGE was recently dropped (unless a special
# flag is used) by Cassandra, and it was never implemented in Scylla, so
# let's skip its test.
# See issue #3882
@pytest.mark.skip
def testSparseCompactTableIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key ascii PRIMARY KEY, val ascii) WITH COMPACT STORAGE") as table:

        # Indexes are allowed only on the sparse compact tables
        execute(cql, table, "CREATE INDEX ON %s(val)")
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", str(i), str(i * 10))

        execute(cql, table, "ALTER TABLE %s DROP COMPACT STORAGE")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE val = '50'"),
                   ["5", None, "50", None])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE key = '5'"),
                   ["5", None, "50", None])

def testBefore(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key TEXT, column TEXT, value BLOB, PRIMARY KEY (key, column)) WITH COMPACT STORAGE") as table:
        largeBytes = bytearray(100000)
        execute(cql, table, "INSERT INTO %s (key, column, value) VALUES (?, ?, ?)", "test", "a", largeBytes)
        smallBytes = bytearray(10)
        execute(cql, table, "INSERT INTO %s (key, column, value) VALUES (?, ?, ?)", "test", "c", smallBytes)

        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT column FROM %s WHERE key = ? AND column IN (?, ?, ?)", "test", "c", "a", "b"),
                   ["a"],
                   ["c"])

def testStaticCompactTables(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text PRIMARY KEY, v1 int, v2 text) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "first", 1, "value1")
        execute(cql, table, "INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "second", 2, "value2")
        execute(cql, table, "INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "third", 3, "value3")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", "first"),
                   ["first", 1, "value1"]
        )

        assert_rows(execute(cql, table, "SELECT v2 FROM %s WHERE k = ?", "second"),
                   ["value2"]
        )

        # Murmur3 order
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   ["third", 3, "value3"],
                   ["second", 2, "value2"],
                   ["first", 1, "value1"]
        )

def testCompactStorageUpdateWithNull(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, value int, PRIMARY KEY (partitionKey, clustering_1)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 1, 1)")

        flush(cql, table)

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", None, 0, 0)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))", 0, 0, 1),
                   [0, 1, 1]
        )

# Migrated from cql_tests.py:TestCQL.collection_compact_test()
def testCompactCollections(cql, test_keyspace):
    tableName = test_keyspace + "." + unique_name()
    assert_invalid(cql, test_keyspace, f"CREATE TABLE {tableName} (user ascii PRIMARY KEY, mails list < text >) WITH COMPACT STORAGE;")

# Check for a table with counters,
# migrated from cql_tests.py:TestCQL.counters_test()
@pytest.mark.xfail(reason="Cassandra 3.10's += syntax not yet supported. Issue #7735")
def testCounters(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid int, url text, total counter, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'")
        assert_rows(execute(cql, table, "SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   [1])

        execute(cql, table, "UPDATE %s SET total = total - 4 WHERE userid = 1 AND url = 'http://foo.com'")
        assert_rows(execute(cql, table, "SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   [-3])

        execute(cql, table, "UPDATE %s SET total = total+1 WHERE userid = 1 AND url = 'http://foo.com'")
        assert_rows(execute(cql, table, "SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   [-2])

        execute(cql, table, "UPDATE %s SET total = total -2 WHERE userid = 1 AND url = 'http://foo.com'")
        assert_rows(execute(cql, table, "SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   [-4])

        # Reproduces #7735:
        execute(cql, table, "UPDATE %s SET total += 6 WHERE userid = 1 AND url = 'http://foo.com'")
        assert_rows(execute(cql, table, "SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   [2])

        execute(cql, table, "UPDATE %s SET total -= 1 WHERE userid = 1 AND url = 'http://foo.com'")
        assert_rows(execute(cql, table, "SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   [1])

        execute(cql, table, "UPDATE %s SET total += -2 WHERE userid = 1 AND url = 'http://foo.com'")
        assert_rows(execute(cql, table, "SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   [-1])

        execute(cql, table, "UPDATE %s SET total -= -2 WHERE userid = 1 AND url = 'http://foo.com'")
        assert_rows(execute(cql, table, "SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   [1])

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCounterFiltering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, a COUNTER) WITH COMPACT STORAGE") as table:
        for i in range(10):
            execute(cql, table, "UPDATE %s SET a = a + ? WHERE k = ?", i, i)

        execute(cql, table, "UPDATE %s SET a = a + ? WHERE k = ?", 6, 10)

        # GT
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a > ? ALLOW FILTERING", 5),
                                [6, 6],
                                [7, 7],
                                [8, 8],
                                [9, 9],
                                [10, 6])

        # GTE
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a >= ? ALLOW FILTERING", 6),
                                [6, 6],
                                [7, 7],
                                [8, 8],
                                [9, 9],
                                [10, 6])

        # LT
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a < ? ALLOW FILTERING", 3),
                                [0, 0],
                                [1, 1],
                                [2, 2])

        # LTE
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a <= ? ALLOW FILTERING", 3),
                                [0, 0],
                                [1, 1],
                                [2, 2],
                                [3, 3])

        # EQ
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a = ? ALLOW FILTERING", 6),
                                [6, 6],
                                [10, 6])

# Test for the bug of CASSANDRA-11726.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCounterAndColumnSelection(cql, test_keyspace):
    for compactStorageClause in ["", " WITH COMPACT STORAGE"]:
        with create_table(cql, test_keyspace, "(k int PRIMARY KEY, c counter) " + compactStorageClause) as table:
            # Flush 2 updates in different sstable so that the following select does a merge, which is what triggers
            # the problem from CASSANDRA-11726
            execute(cql, table, "UPDATE %s SET c = c + ? WHERE k = ?", 1, 0)
            flush(cql, table)
            execute(cql, table, "UPDATE %s SET c = c + ? WHERE k = ?", 1, 0)
            flush(cql, table)
            # Querying, but not including the counter. Pre-CASSANDRA-11726, this made us query the counter but include
            # it's value, which broke at merge (post-CASSANDRA-11726 are special cases to never skip values).
            assert_rows(execute(cql, table, "SELECT k FROM %s"), [0])

# Check that a counter batch works as intended
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCounterBatch(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid int, url text, total counter, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE") as table:
        # Ensure we handle updates to the same CQL row in the same partition properly
        execute(cql, table, "BEGIN UNLOGGED BATCH " +
                "UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "APPLY BATCH; ")
        assert_rows(execute(cql, table, "SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   [3])

        # Ensure we handle different CQL rows in the same partition properly
        execute(cql, table, "BEGIN UNLOGGED BATCH " +
                "UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://bar.com'; " +
                "UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://baz.com'; " +
                "UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://bad.com'; " +
                "APPLY BATCH; ")
        assert_rows(execute(cql, table, "SELECT url, total FROM %s WHERE userid = 1"),
                   ["http://bad.com", 1],
                   ["http://bar.com", 1],
                   ["http://baz.com", 1],
                   ["http://foo.com", 3]) # from previous batch

    with create_table(cql, test_keyspace, "(userid int, url text, first counter, second counter, third counter, PRIMARY KEY (userid, url))") as table:
        # Different counters in the same CQL Row
        execute(cql, table, "BEGIN UNLOGGED BATCH " +
                "UPDATE %s SET first = first + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "UPDATE %s SET first = first + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "UPDATE %s SET second = second + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "APPLY BATCH; ")
        assert_rows(execute(cql, table, "SELECT first, second, third FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   [2, 1, None])

        # Different counters in different CQL Rows
        execute(cql, table, "BEGIN UNLOGGED BATCH " +
                "UPDATE %s SET first = first + 1 WHERE userid = 1 AND url = 'http://bad.com'; " +
                "UPDATE %s SET first = first + 1, second = second + 1 WHERE userid = 1 AND url = 'http://bar.com'; " +
                "UPDATE %s SET first = first - 1, second = second - 1 WHERE userid = 1 AND url = 'http://bar.com'; " +
                "UPDATE %s SET second = second + 1 WHERE userid = 1 AND url = 'http://baz.com'; " +
                "APPLY BATCH; ")
        assert_rows(execute(cql, table, "SELECT url, first, second, third FROM %s WHERE userid = 1"),
                   ["http://bad.com", 1, None, None],
                   ["http://bar.com", 0, 0, None],
                   ["http://baz.com", None, 1, None],
                   ["http://foo.com", 2, 1, None]) # from previous batch

        # Different counters in different partitions
        execute(cql, table, "BEGIN UNLOGGED BATCH " +
                "UPDATE %s SET first = first + 1 WHERE userid = 2 AND url = 'http://bad.com'; " +
                "UPDATE %s SET first = first + 1, second = second + 1 WHERE userid = 3 AND url = 'http://bar.com'; " +
                "UPDATE %s SET first = first - 1, second = second - 1 WHERE userid = 4 AND url = 'http://bar.com'; " +
                "UPDATE %s SET second = second + 1 WHERE userid = 5 AND url = 'http://baz.com'; " +
                "APPLY BATCH; ")
        assert_rows_ignoring_order(execute(cql, table, "SELECT userid, url, first, second, third FROM %s WHERE userid IN (2, 3, 4, 5)"),
                                [2, "http://bad.com", 1, None, None],
                                [3, "http://bar.com", 1, 1, None],
                                [4, "http://bar.com", -1, -1, None],
                                [5, "http://baz.com", None, 1, None])

# from FrozenCollectionsTest
def testClusteringKeyUsageSet(cql, test_keyspace):
    runClusteringKeyUsage(cql, test_keyspace, "set<int>", set(),
                               {1, 2, 3},
                               {4, 5, 6},
                               {7, 8, 9})

def testClusteringKeyUsageList(cql, test_keyspace):
    runClusteringKeyUsage(cql, test_keyspace, "list<int>",
                               [],
                               [1, 2, 3],
                               [4, 5, 6],
                               [7, 8, 9])

def testClusteringKeyUsageMap(cql, test_keyspace):
    runClusteringKeyUsage(cql, test_keyspace, "map<int, int>",
                               {},
                               {1: 10, 2: 20, 3: 30},
                               {4: 40, 5: 50, 6: 60},
                               {7: 70, 8: 80, 9: 90})

def runClusteringKeyUsage(cql, test_keyspace, typ, v1, v2, v3, v4):
    with create_table(cql, test_keyspace, f"(a int, b frozen<{typ}>, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v2, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v3, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v4, 0)

        # overwrite with an update
        execute(cql, table, "UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, v1)
        execute(cql, table, "UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, v2)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, v1, 0],
                   [0, v2, 0],
                   [0, v3, 0],
                   [0, v4, 0]
        )

        assert_rows(execute(cql, table, "SELECT b FROM %s"),
                   [v1],
                   [v2],
                   [v3],
                   [v4]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s LIMIT 2"),
                   [0, v1, 0],
                   [0, v2, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, v3),
                   [0, v3, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, v1),
                   [0, v1, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b IN ?", 0, [v3, v1]),
                   [0, v1, 0],
                   [0, v3, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ?", 0, v3),
                   [0, v4, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b >= ?", 0, v3),
                   [0, v3, 0],
                   [0, v4, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b < ?", 0, v3),
                   [0, v1, 0],
                   [0, v2, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b <= ?", 0, v3),
                   [0, v1, 0],
                   [0, v2, 0],
                   [0, v3, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, v2, v3),
                   [0, v3, 0]
        )

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=?", 0, v1)
        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=?", 0, v3)
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, v2, 0],
                   [0, v4, 0]
        )

def testNestedClusteringKeyUsage(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(a int, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {}, set(), 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset(): [1, 2, 3]}, set(), 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, {}, set(), 0],
                   [0, {frozenset(): [1, 2, 3]}, set(), 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT b FROM %s"),
                   [{}],
                   [{frozenset(): [1, 2, 3]}],
                   [{frozenset({1, 2, 3}): [1, 2, 3]}],
                   [{frozenset({4, 5, 6}): [1, 2, 3]}],
                   [{frozenset({7, 8, 9}): [1, 2, 3]}]
        )

        assert_rows(execute(cql, table, "SELECT c FROM %s"),
                   [set()],
                   [set()],
                   [{1, 2, 3}],
                   [{1, 2, 3}],
                   [{1, 2, 3}]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s LIMIT 3"),
                   [0, {}, set(), 0],
                   [0, {frozenset(): [1, 2, 3]}, set(), 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=0 ORDER BY b DESC LIMIT 4"),
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset(): [1, 2, 3]}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {}),
                   [0, {}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {frozenset(): [1, 2, 3]}),
                   [0, {frozenset(): [1, 2, 3]}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {frozenset({1, 2, 3}): [1, 2, 3]}),
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset(): [1, 2, 3]}, set()),
                   [0, {frozenset(): [1, 2, 3]}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c) IN ?", 0, [({frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}), ({}, set())]),
                   [0, {}, set(), 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b >= ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b < ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {}, set(), 0],
                   [0, {frozenset(): [1, 2, 3]}, set(), 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b <= ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {}, set(), 0],
                   [0, {frozenset(): [1, 2, 3]}, set(), 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, {frozenset({1, 2, 3}): [1, 2, 3]}, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {}, set())
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {}, set()))

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset(): [1, 2, 3]}, set())
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset(): [1, 2, 3]}, set()))

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3})
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}))

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

def testNestedClusteringKeyUsageWithReverseOrder(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(a int, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {}, set(), 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset(): [1, 2, 3]}, set(), 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset(): [1, 2, 3]}, set(), 0),
                   row(0, {}, set(), 0)
        )

        assert_rows(execute(cql, table, "SELECT b FROM %s"),
                   row({frozenset({7, 8, 9}): [1, 2, 3]}),
                   row({frozenset({4, 5, 6}): [1, 2, 3]}),
                   row({frozenset({1, 2, 3}): [1, 2, 3]}),
                   row({frozenset(): [1, 2, 3]}),
                   row({})
        )

        assert_rows(execute(cql, table, "SELECT c FROM %s"),
                   row({1, 2, 3}),
                   row({1, 2, 3}),
                   row({1, 2, 3}),
                   row(set()),
                   row(set())
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s LIMIT 3"),
                   row(0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=0 ORDER BY b DESC LIMIT 4"),
                   row(0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset(): [1, 2, 3]}, set(), 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {}),
                   row(0, {}, set(), 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {frozenset(): [1, 2, 3]}),
                   row(0, {frozenset(): [1, 2, 3]}, set(), 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {frozenset({1, 2, 3}): [1, 2, 3]}),
                   row(0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset(): [1, 2, 3]}, set()),
                   row(0, {frozenset(): [1, 2, 3]}, set(), 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c) IN ?", 0, [({frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}),
                                                                                 ({}, set())]),
                   row(0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {}, set(), 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   row(0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b >= ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   row(0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b < ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   row(0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset(): [1, 2, 3]}, set(), 0),
                   row(0, {}, set(), 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b <= ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   row(0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset(): [1, 2, 3]}, set(), 0),
                   row(0, {}, set(), 0)
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, {frozenset({1, 2, 3}): [1, 2, 3]}, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   row(0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0)
        )

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {}, set())
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {}, set()))

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset(): [1, 2, 3]}, set())
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset(): [1, 2, 3]}, set()))

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3})
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}))

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0),
                   row(0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0)
        )

def testNormalColumnUsage(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>)" + compactOption ) as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, {}, set())
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, {frozenset(): [99999, 999999, 99999]}, set())
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3})
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 3, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3})
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 4, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3})

        # overwrite with update
        execute(cql, table, "UPDATE %s SET b=? WHERE a=?", {frozenset(): [1, 2, 3]}, 1)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                                [0, {}, set()],
                                [1, {frozenset(): [1, 2, 3]}, set()],
                                [2, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}],
                                [3, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}],
                                [4, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT b FROM %s"),
                                [{}],
                                [{frozenset(): [1, 2, 3]}],
                                [{frozenset({1, 2, 3}): [1, 2, 3]}],
                                [{frozenset({4, 5, 6}): [1, 2, 3]}],
                                [{frozenset({7, 8, 9}): [1, 2, 3]}]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT c FROM %s"),
                                [set()],
                                [set()],
                                [{1, 2, 3}],
                                [{1, 2, 3}],
                                [{1, 2, 3}]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a=?", 3),
                                [3, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}]
        )

        execute(cql, table, "UPDATE %s SET b=? WHERE a=?", null, 1)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a=?", 1),
                                [1, None, set()]
        )

        execute(cql, table, "UPDATE %s SET b=? WHERE a=?", {}, 1)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a=?", 1),
                                [1, {}, set()]
        )

        execute(cql, table, "UPDATE %s SET c=? WHERE a=?", None, 2)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a=?", 2),
                                [2, {frozenset({1, 2, 3}): [1, 2, 3]}, None]
        )

        execute(cql, table, "UPDATE %s SET c=? WHERE a=?", set(), 2)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a=?", 2),
                                [2, {frozenset({1, 2, 3}): [1, 2, 3]}, set()]
        )

        execute(cql, table, "DELETE b FROM %s WHERE a=?", 3)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a=?", 3),
                                [3, None, {1, 2, 3}]
        )

        execute(cql, table, "DELETE c FROM %s WHERE a=?", 4)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a=?", 4),
                                [4, {frozenset({7, 8, 9}): [1, 2, 3]}, None]
        )

TOO_BIG = 1024 * 65

# from SecondaryIndexTest
# Reproduces #8627
@pytest.mark.xfail(reason="issue #8627")
def testCompactTableWithValueOver64k(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int,  b blob, PRIMARY KEY (a)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "CREATE INDEX ON %s(b)")
        too_big = bytearray([1])*TOO_BIG
        failInsert(cql, table, "INSERT INTO %s (a, b) VALUES (0, ?)", too_big)
        failInsert(cql, table, "INSERT INTO %s (a, b) VALUES (0, ?) IF NOT EXISTS", too_big)
        failInsert(cql, table, "BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b) VALUES (0, ?);\n" +
                   "APPLY BATCH",
                   too_big)
        failInsert(cql, table, "BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b) VALUES (0, ?) IF NOT EXISTS;\n" +
                   "APPLY BATCH",
                   too_big)

def failInsert(cql, table, insertCQL, *args):
    with pytest.raises(InvalidRequest):
        execute(cql, table, insertCQL, *args)

# Migrated from cql_tests.py:TestCQL.invalid_clustering_indexing_test()
def testIndexesOnClusteringInvalid(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int,  b int, c int, d int, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE") as table:
        assert_invalid(cql, table, "CREATE INDEX ON %s (a)")
        assert_invalid(cql, table, "CREATE INDEX ON %s (b)")
    with create_table(cql, test_keyspace, "(a int,  b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        assert_invalid(cql, table, "CREATE INDEX ON %s (a)")
        assert_invalid(cql, table, "CREATE INDEX ON %s (b)")
        assert_invalid(cql, table, "CREATE INDEX ON %s (c)")

def testEmptyRestrictionValueWithSecondaryIndexAndCompactTables(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk blob, c blob, v blob, PRIMARY KEY ((pk), c)) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "COMPACT STORAGE",
                             "CREATE INDEX on %s(c)")

    with create_table(cql, test_keyspace, "(pk blob PRIMARY KEY, v blob) WITH COMPACT STORAGE") as table:
        execute(cql, table, "CREATE INDEX on %s(v)")
        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (?, ?)", b"foo123", b"1")

        # Test restrictions on non-primary key value
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('');"))

        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (?, ?)", b"foo124", bytearray([]))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v = textAsBlob('');"),
                   row(b"foo124", bytearray([])))

def testIndicesOnCompactTable(cql, test_keyspace):
    assertInvalidMessage(cql, test_keyspace, "COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY",
                             "CREATE TABLE " + test_keyspace + "." + unique_name() + " (pk int, c int, v1 int, v2 int, PRIMARY KEY(pk, c)) WITH COMPACT STORAGE")
    with create_table(cql, test_keyspace, "(pk blob, c int, v int, PRIMARY KEY (pk, c)) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "COMPACT STORAGE",
                             "CREATE INDEX ON %s(v)")

    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, v int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "CREATE INDEX ON %s(v)")

        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (?, ?)", 1, 1)
        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (?, ?)", 2, 1)
        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (?, ?)", 3, 3)

        assert_rows(execute(cql, table, "SELECT pk, v FROM %s WHERE v = 1"),
                   row(1, 1),
                   row(2, 1))

        assert_rows(execute(cql, table, "SELECT pk, v FROM %s WHERE v = 3"),
                   row(3, 3))

        assertEmpty(execute(cql, table, "SELECT pk, v FROM %s WHERE v = 5"))

    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, v1 int, v2 int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "CREATE INDEX ON %s(v1)")

        execute(cql, table, "INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", 2, 1, 2)
        execute(cql, table, "INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", 3, 3, 3)

        assert_rows(execute(cql, table, "SELECT pk, v2 FROM %s WHERE v1 = 1"),
                   row(1, 1),
                   row(2, 2))

        assert_rows(execute(cql, table, "SELECT pk, v2 FROM %s WHERE v1 = 3"),
                   row(3, 3))

        assertEmpty(execute(cql, table, "SELECT pk, v2 FROM %s WHERE v1 = 5"))

# OverflowTest

# Test regression from CASSANDRA-5189,
# migrated from cql_tests.py:TestCQL.compact_metadata_test()
def testCompactMetadata(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int primary key, i int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (id, i) VALUES (1, 2)")
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(1, 2))

def testEmpty(cql, test_keyspace):
    # Same test, but for compact
    with create_table(cql, test_keyspace, "(k1 int, k2 int, v int, PRIMARY KEY (k1, k2)) WITH COMPACT STORAGE") as table:
        # Inserts a few rows to make sure we don 't actually query something
        rows = fill(cql, table)

        assertEmpty(execute(cql, table, "SELECT v FROM %s WHERE k1 IN ()"))
        assertEmpty(execute(cql, table, "SELECT v FROM %s WHERE k1 = 0 AND k2 IN ()"))

        # Test empty IN() in DELETE
        execute(cql, table, "DELETE FROM %s WHERE k1 IN ()")
        assertArrayEquals(rows, getRows(execute(cql, table, "SELECT * FROM %s")))

        # Test empty IN() in UPDATE
        execute(cql, table, "UPDATE %s SET v = 3 WHERE k1 IN () AND k2 = 2")
        assertArrayEquals(rows, getRows(execute(cql, table, "SELECT * FROM %s")))

def fill(cql, table):
    for i in range(2):
        for j in range(2):
            execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", i, j, i + j)
    return list(execute(cql, table, "SELECT * FROM %s"))

# AggregationTest
def testFunctionsWithCompactStorage(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c double, primary key (a,b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1, 11.5)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, 9.5)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, 9.0)")

        assert_rows(execute(cql, table, "SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c) FROM %s"),
                   row(3, 1, 6, 2, 11.5, 30.0, 10.0))

        assert_rows(execute(cql, table, "SELECT COUNT(*) FROM %s"), row(3))
        assert_rows(execute(cql, table, "SELECT COUNT(1) FROM %s"), row(3))
        assert_rows(execute(cql, table, "SELECT COUNT(*) FROM %s WHERE a = 1 AND b > 1"), row(2))
        assert_rows(execute(cql, table, "SELECT COUNT(1) FROM %s WHERE a = 1 AND b > 1"), row(2))
        assert_rows(execute(cql, table, "SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c) FROM %s WHERE a = 1 AND b > 1"),
                   row(3, 2, 5, 2, 9.5, 18.5, 9.25))

# BatchTest
@pytest.mark.xfail(reason="issue #12471")
def testBatchRangeDelete(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering int, value int, primary key (partitionKey, clustering)) WITH COMPACT STORAGE") as table:
        value = 0
        for partitionKey in range(4):
            for clustering1 in range(5):
                execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (?, ?, ?)",
                    partitionKey, clustering1, value)
                value += 1
        execute(cql, table, "BEGIN BATCH " +
                "DELETE FROM %s WHERE partitionKey = 1;" +
                "DELETE FROM %s WHERE partitionKey = 0 AND  clustering >= 4;" +
                "DELETE FROM %s WHERE partitionKey = 0 AND clustering <= 0;" +
                "DELETE FROM %s WHERE partitionKey = 2 AND clustering >= 0 AND clustering <= 3;" +
                "DELETE FROM %s WHERE partitionKey = 2 AND clustering <= 3 AND clustering >= 4;" +
                "DELETE FROM %s WHERE partitionKey = 3 AND (clustering) >= (3) AND (clustering) <= (6);" +
                "APPLY BATCH;")

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 1, 1),
                   row(0, 2, 2),
                   row(0, 3, 3),
                   row(2, 4, 14),
                   row(3, 0, 15),
                   row(3, 1, 16),
                   row(3, 2, 17))

# CreateTest
# Creation and basic operations on a static table with compact storage,
# migrated from cql_tests.py:TestCQL.noncomposite_static_cf_test()
def testDenseStaticTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid uuid PRIMARY KEY, firstname text, lastname text, age int) WITH COMPACT STORAGE") as table:
        id1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        id2 = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")
        execute(cql, table, "INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, ?, ?, ?)", id1, "Frodo", "Baggins", 32)
        execute(cql, table, "UPDATE %s SET firstname = ?, lastname = ?, age = ? WHERE userid = ?", "Samwise", "Gamgee", 33, id2)

        assert_rows(execute(cql, table, "SELECT firstname, lastname FROM %s WHERE userid = ?", id1),
                   row("Frodo", "Baggins"))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE userid = ?", id1),
                   row(id1, 32, "Frodo", "Baggins"))

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(id2, 33, "Samwise", "Gamgee"),
                   row(id1, 32, "Frodo", "Baggins")
        )

        batch = "BEGIN BATCH INSERT INTO %s (userid, age) VALUES (?, ?) UPDATE %s SET age = ? WHERE userid = ? DELETE firstname, lastname FROM %s WHERE userid = ? DELETE firstname, lastname FROM %s WHERE userid = ? APPLY BATCH"

        execute(cql, table, batch, id1, 36, 37, id2, id1, id2)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(id2, 37, None, None),
                   row(id1, 36, None, None))

# Creation and basic operations on a non-composite table with compact storage,
# migrated from cql_tests.py:TestCQL.dynamic_cf_test()
def testDenseNonCompositeTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid uuid, url text, time bigint, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE") as table:
        id1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        id2 = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")
        id3 = UUID("810e8500-e29b-41d4-a716-446655440000")

        execute(cql, table, "INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://foo.bar", 42)
        execute(cql, table, "INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://foo-2.bar", 24)
        execute(cql, table, "INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://bar.bar", 128)
        execute(cql, table, "UPDATE %s SET time = 24 WHERE userid = ? and url = 'http://bar.foo'", id2)
        execute(cql, table, "UPDATE %s SET time = 12 WHERE userid IN (?, ?) and url = 'http://foo-3'", id2, id1)

        assert_rows(execute(cql, table, "SELECT url, time FROM %s WHERE userid = ?", id1),
                   row("http://bar.bar", 128),
                   row("http://foo-2.bar", 24),
                   row("http://foo-3", 12),
                   row("http://foo.bar", 42))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE userid = ?", id2),
                   row(id2, "http://bar.foo", 24),
                   row(id2, "http://foo-3", 12))

        assert_rows(execute(cql, table, "SELECT time FROM %s"),
                   row(24), # id2
                   row(12),
                   row(128), # id1
                   row(24),
                   row(12),
                   row(42)
        )

        # Check we don't allow empty values for url since this is the full underlying cell name (CASSANDRA-6152)
        assert_invalid(cql, table, "INSERT INTO %s (userid, url, time) VALUES (?, '', 42)", id3)

# Creation and basic operations on a composite table with compact storage,
# migrated from cql_tests.py:TestCQL.dense_cf_test()
def testDenseCompositeTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid uuid, ip text, port int, time bigint, PRIMARY KEY (userid, ip, port)) WITH COMPACT STORAGE") as table:
        id1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        id2 = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")

        execute(cql, table, "INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.1', 80, 42)", id1)
        execute(cql, table, "INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.2', 80, 24)", id1)
        execute(cql, table, "INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.2', 90, 42)", id1)
        execute(cql, table, "UPDATE %s SET time = 24 WHERE userid = ? AND ip = '192.168.0.2' AND port = 80", id2)

        # we don't have to include all of the clustering columns (see CASSANDRA-7990)
        execute(cql, table, "INSERT INTO %s (userid, ip, time) VALUES (?, '192.168.0.3', 42)", id2)
        execute(cql, table, "UPDATE %s SET time = 42 WHERE userid = ? AND ip = '192.168.0.4'", id2)

        assert_rows(execute(cql, table, "SELECT ip, port, time FROM %s WHERE userid = ?", id1),
                   row("192.168.0.1", 80, 42),
                   row("192.168.0.2", 80, 24),
                   row("192.168.0.2", 90, 42))

        assert_rows(execute(cql, table, "SELECT ip, port, time FROM %s WHERE userid = ? and ip >= '192.168.0.2'", id1),
                   row("192.168.0.2", 80, 24),
                   row("192.168.0.2", 90, 42))

        assert_rows(execute(cql, table, "SELECT ip, port, time FROM %s WHERE userid = ? and ip = '192.168.0.2'", id1),
                   row("192.168.0.2", 80, 24),
                   row("192.168.0.2", 90, 42))

        assertEmpty(execute(cql, table, "SELECT ip, port, time FROM %s WHERE userid = ? and ip > '192.168.0.2'", id1))

        assert_rows(execute(cql, table, "SELECT ip, port, time FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2),
                   row("192.168.0.3", None, 42))

        assert_rows(execute(cql, table, "SELECT ip, port, time FROM %s WHERE userid = ? AND ip = '192.168.0.4'", id2),
                   row("192.168.0.4", None, 42))

        execute(cql, table, "DELETE time FROM %s WHERE userid = ? AND ip = '192.168.0.2' AND port = 80", id1)

        assertRowCount(execute(cql, table, "SELECT * FROM %s WHERE userid = ?", id1), 2)

        execute(cql, table, "DELETE FROM %s WHERE userid = ?", id1)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE userid = ?", id1))

        execute(cql, table, "DELETE FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2))

def testCreateIndexOnCompactTableWithClusteringColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "COMPACT STORAGE",
                             "CREATE INDEX ON %s (a);")

        assertInvalidMessage(cql, table, "COMPACT STORAGE",
                             "CREATE INDEX ON %s (b);")

        assertInvalidMessage(cql, table, "COMPACT STORAGE",
                             "CREATE INDEX ON %s (c);")

def testCreateIndexOnCompactTableWithoutClusteringColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables",
                             "CREATE INDEX ON %s (a);")

        execute(cql, table, "CREATE INDEX ON %s (b);")

        execute(cql, table, "INSERT INTO %s (a, b) values (1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b) values (2, 4)")
        execute(cql, table, "INSERT INTO %s (a, b) values (3, 6)")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b = ?", 4), row(2, 4))

# DeleteTest


@pytest.mark.parametrize("forceFlush", [False, True])
def testDeleteWithNoClusteringColumns(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionkey int PRIMARY KEY, value int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, value) VALUES (0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, value) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey, value) VALUES (2, 2)")
        execute(cql, table, "INSERT INTO %s (partitionKey, value) VALUES (3, 3)")
        if forceFlush:
            flush(cql, table)

        execute(cql, table, "DELETE value FROM %s WHERE partitionKey = ?", 0)
        if forceFlush:
            flush(cql, table)

        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ?", 0))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey IN (?, ?)", 0, 1)
        if forceFlush:
            flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(2, 2),
                   row(3, 3))

        # test invalid queries

        # token function
        assertInvalidMessage(cql, table, "The token function cannot be used in WHERE clauses for",
                             "DELETE FROM %s WHERE token(partitionKey) = token(?)", 0)

        # multiple time same primary key element in WHERE clause
        # As explained in #12472, Scylla allows this case that Cassandra
        # forbids, so the check is commented out:
        #assertInvalidMessage(cql, table, "partitionkey cannot be restricted by more than one relation if it includes an Equal",
        #                     "DELETE FROM %s WHERE partitionKey = ? AND partitionKey = ?", 0, 1)

        # Undefined column names
        assertInvalidMessage(cql, table, "unknown",
                             "DELETE unknown FROM %s WHERE partitionKey = ?", 0)

        assertInvalidMessage(cql, table, "partitionkey1",
                             "DELETE FROM %s WHERE partitionKey1 = ?", 0)

        # Invalid operator in the where clause
        assertInvalidMessage(cql, table, "Only EQ and IN relation are supported on the partition key",
                             "DELETE FROM %s WHERE partitionKey > ? ", 0)

        assertInvalidMessage(cql, table, "Cannot use CONTAINS on non-collection column",
                             "DELETE FROM %s WHERE partitionKey CONTAINS ?", 0)

        # Non primary key in the where clause
        # Reproduces #12474:
        assertInvalidMessage(cql, table, "where clause",
                             "DELETE FROM %s WHERE partitionKey = ? AND value = ?", 0, 1)


@pytest.mark.parametrize("forceFlush", [False, True])
def testDeleteWithOneClusteringColumns(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering int, value int, PRIMARY KEY (partitionKey, clustering)) WITH COMPACT STORAGE") as table:
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

        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1)
        if forceFlush:
            flush(cql, table)
        assertEmpty(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey IN (?, ?) AND clustering = ?", 0, 1, 0)
        if forceFlush:
            flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                   row(0, 2, 2),
                   row(0, 3, 3),
                   row(0, 4, 4),
                   row(0, 5, 5))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering) IN ((?), (?))", 0, 4, 5)
        if forceFlush:
            flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
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
        # As explained in #12472, Scylla allows this case that Cassandra
        # forbids, so the check is commented out:
        #assertInvalidMessage(cql, table, "clustering cannot be restricted by more than one relation if it includes an Equal",
        #                     "DELETE FROM %s WHERE partitionKey = ? AND clustering = ? AND clustering = ?", 0, 1, 1)

        # Undefined column names
        assertInvalidMessage(cql, table, "value1",
                             "DELETE value1 FROM %s WHERE partitionKey = ? AND clustering = ?", 0, 1)

        assertInvalidMessage(cql, table, "partitionkey1",
                             "DELETE FROM %s WHERE partitionKey1 = ? AND clustering = ?", 0, 1)

        assertInvalidMessage(cql, table, "clustering_3",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_3 = ?", 0, 1)

        # Invalid operator in the where clause
        assertInvalidMessage(cql, table, "Only EQ and IN relation are supported on the partition key",
                             "DELETE FROM %s WHERE partitionKey > ? AND clustering = ?", 0, 1)

        assertInvalidMessageRE(cql, table, "Cannot use CONTAINS on non-collection column|Cannot use DELETE with CONTAINS",
                             "DELETE FROM %s WHERE partitionKey CONTAINS ? AND clustering = ?", 0, 1)

        # Non primary key in the where clause
        assertInvalidMessage(cql, table, "value",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering = ? AND value = ?", 0, 1, 3)

@pytest.mark.xfail(reason="issue #4244")
@pytest.mark.parametrize("forceFlush", [False, True])
def testDeleteWithTwoClusteringColumns(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, clustering_2 int, value int, PRIMARY KEY (partitionKey, clustering_1, clustering_2)) WITH COMPACT STORAGE") as table:

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

        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                            0, 1, 1))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) = (?, ?)", 0, 1, 1)
        if forceFlush:
            flush(cql, table)
        assertEmpty(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                            0, 1, 1))

        execute(cql, table, "DELETE FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 0, 0)
        if forceFlush:
            flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
                   row(0, 0, 1, 1),
                   row(0, 0, 2, 2),
                   row(0, 0, 3, 3),
                   row(0, 1, 2, 5))

        rows = [row(0, 0, 1, 1), row(0, 1, 2, 5)]

        execute(cql, table, "DELETE value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)", 0, 0, 2, 3)
        if forceFlush:
            flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1), *rows)

        rows = [row(0, 0, 1, 1)]

        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))", 0, 0, 2, 1, 2)
        if forceFlush:
            flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1), *rows)

        # Reproduces #4244:
        execute(cql, table, "DELETE FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?)) AND clustering_2 = ?", 0, 0, 2, 3)
        if forceFlush:
            flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?)", 0, 1),
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
        # As explained in #12472, Scylla allows this case that Cassandra
        # forbids, so the check is commented out:
        #assertInvalidMessage(cql, table, "clustering_1 cannot be restricted by more than one relation if it includes an Equal",
        #                     "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND clustering_1 = ?", 0, 1, 1, 1)

        # Undefined column names
        assertInvalidMessage(cql, table, "value1",
                             "DELETE value1 FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1)

        assertInvalidMessage(cql, table, "partitionkey1",
                             "DELETE FROM %s WHERE partitionKey1 = ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1)

        assertInvalidMessage(cql, table, "clustering_3",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_3 = ?", 0, 1, 1)

        # Invalid operator in the where clause
        assertInvalidMessage(cql, table, "Only EQ and IN relation are supported on the partition key",
                             "DELETE FROM %s WHERE partitionKey > ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1)

        assertInvalidMessage(cql, table, "Cannot use CONTAINS on non-collection column",
                             "DELETE FROM %s WHERE partitionKey CONTAINS ? AND clustering_1 = ? AND clustering_2 = ?", 0, 1, 1)

        # Non primary key in the where clause
        assertInvalidMessage(cql, table, "value",
                             "DELETE FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND value = ?", 0, 1, 1, 3)

# InsertTest
@pytest.mark.parametrize("forceFlush", [False, True])
def testInsertWithCompactFormat(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering int, value int, PRIMARY KEY (partitionKey, clustering)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 1, 1)")
        if forceFlush:
            flush(cql, table)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 0, 0),
                   row(0, 1, 1))

        # Invalid Null values for the clustering key or the regular column
        assertInvalidMessage(cql, table, "clustering",
                             "INSERT INTO %s (partitionKey, value) VALUES (0, 0)")
        assertInvalidMessage(cql, table, "Column value is mandatory for this COMPACT STORAGE table",
                             "INSERT INTO %s (partitionKey, clustering) VALUES (0, 0)")

        # Missing primary key columns
        assertInvalidMessage(cql, table, "partitionkey",
                             "INSERT INTO %s (clustering, value) VALUES (0, 1)")

        # multiple time the same value
        assertInvalidMessageRE(cql, table, "duplicates|Multiple",
                             "INSERT INTO %s (partitionKey, clustering, value, value) VALUES (0, 0, 2, 2)")

        # multiple time same primary key element in WHERE clause
        assertInvalidMessageRE(cql, table, "duplicates|Multiple",
                             "INSERT INTO %s (partitionKey, clustering, clustering, value) VALUES (0, 0, 0, 2)")

        # Undefined column names
        assertInvalidMessage(cql, table, "clusteringx",
                             "INSERT INTO %s (partitionKey, clusteringx, value) VALUES (0, 0, 2)")

        assertInvalidMessage(cql, table, "valuex",
                             "INSERT INTO %s (partitionKey, clustering, valuex) VALUES (0, 0, 2)")

@pytest.mark.parametrize("forceFlush", [False, True])
def testInsertWithCompactStorageAndTwoClusteringColumns(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, clustering_2 int, value int, PRIMARY KEY (partitionKey, clustering_1, clustering_2)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)")
        if forceFlush:
            flush(cql, table)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 0, null, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1))

        # Invalid Null values for the clustering key or the regular column
        assertInvalidMessage(cql, table, "PRIMARY KEY column \"clustering_2\" cannot be restricted as preceding column \"clustering_1\" is not restricted",
                             "INSERT INTO %s (partitionKey, clustering_2, value) VALUES (0, 0, 0)")
        assertInvalidMessage(cql, table, "Column value is mandatory for this COMPACT STORAGE table",
                             "INSERT INTO %s (partitionKey, clustering_1, clustering_2) VALUES (0, 0, 0)")

        # Missing primary key columns
        assertInvalidMessage(cql, table, "partitionkey",
                             "INSERT INTO %s (clustering_1, clustering_2, value) VALUES (0, 0, 1)")
        assertInvalidMessage(cql, table, "PRIMARY KEY column \"clustering_2\" cannot be restricted as preceding column \"clustering_1\" is not restricted",
                             "INSERT INTO %s (partitionKey, clustering_2, value) VALUES (0, 0, 2)")

        # multiple time the same value
        assertInvalidMessageRE(cql, table, "duplicates|Multiple",
                             "INSERT INTO %s (partitionKey, clustering_1, value, clustering_2, value) VALUES (0, 0, 2, 0, 2)")

        # multiple time same primary key element in WHERE clause
        assertInvalidMessageRE(cql, table, "duplicates|Multiple",
                             "INSERT INTO %s (partitionKey, clustering_1, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0, 2)")

        # Undefined column names
        assertInvalidMessage(cql, table, "clustering_1x",
                             "INSERT INTO %s (partitionKey, clustering_1x, clustering_2, value) VALUES (0, 0, 0, 2)")

        assertInvalidMessage(cql, table, "valuex",
                             "INSERT INTO %s (partitionKey, clustering_1, clustering_2, valuex) VALUES (0, 0, 0, 2)")

# InsertUpdateIfConditionTest
# Test for CAS with compact storage table, and CASSANDRA-6813 in particular,
# migrated from cql_tests.py:TestCQL.cas_and_compact_test()
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testCompactStorage(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(partition text, key text, owner text, PRIMARY KEY (partition, key)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (partition, key, owner) VALUES ('a', 'b', null)")
        # As explained in docs/kb/lwt-differences.rst, Scylla deliberately
        # differs from Cassandra in that it always returns the tested column's
        # value, even in True (successful) conditional writes.
        assert_rows2(execute(cql, table, "UPDATE %s SET owner='z' WHERE partition='a' AND key='b' IF owner=null"), [row(True)], [row(True, None)])

        assert_rows(execute(cql, table, "UPDATE %s SET owner='b' WHERE partition='a' AND key='b' IF owner='a'"), row(False, "z"))
        assert_rows2(execute(cql, table, "UPDATE %s SET owner='b' WHERE partition='a' AND key='b' IF owner='z'"), [row(True)], [row(True, 'z')])

        assert_rows2(execute(cql, table, "INSERT INTO %s (partition, key, owner) VALUES ('a', 'c', 'x') IF NOT EXISTS"), [row(True)], [row(True,None,None,None)])

# SelectGroupByTest
@pytest.mark.xfail(reason="issue #4244, #5361, #5362, #5363")
def testGroupByWithoutPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (2, 2, 3, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (2, 4, 3, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (3, 3, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (4, 8, 2, 12, 24)")

        # Makes sure that we have some tombstones
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2 AND d = 12")
        execute(cql, table, "DELETE FROM %s WHERE a = 3")

        # Range queries
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a"),
                   row(1, 2, 6, 4, 24),
                   row(2, 2, 6, 2, 12),
                   row(4, 8, 24, 1, 24))

        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b"),
                   row(1, 2, 6, 2, 12),
                   row(1, 4, 12, 2, 24),
                   row(2, 2, 6, 1, 6),
                   row(2, 4, 12, 1, 12),
                   row(4, 8, 24, 1, 24))

        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE b = 2 GROUP BY a, b ALLOW FILTERING"),
                   row(1, 2, 6, 2, 12),
                   row(2, 2, 6, 1, 6))

        # Reproduces #12477:
        assertEmpty(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE b IN () GROUP BY a, b ALLOW FILTERING"))

        # Range queries without aggregates
        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12))

        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12))

        # Range queries with wildcard
        assert_rows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        assert_rows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        # Range query with LIMIT
        # Reproduces #5361:
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b LIMIT 2"),
                   row(1, 2, 6, 2, 12),
                   row(1, 4, 12, 2, 24))

        # Range queries with PER PARTITION LIMIT
        # Reproduces #5363:
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 6, 2, 12),
                   row(2, 2, 6, 1, 6),
                   row(4, 8, 24, 1, 24))

        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 4, 24),
                   row(2, 2, 6, 2, 12),
                   row(4, 8, 24, 1, 24))

        # Range query with PER PARTITION LIMIT and LIMIT
        # Reproduces #5361, #5363:
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2"),
                   row(1, 2, 6, 2, 12),
                   row(2, 2, 6, 1, 6))

        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 4, 24),
                   row(2, 2, 6, 2, 12),
                   row(4, 8, 24, 1, 24))

        # Range queries without aggregates and with LIMIT
        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c LIMIT 3"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6))

        # Reproduces #5362:
        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b LIMIT 3"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3))

        # Range queries with wildcard and with LIMIT
        assert_rows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b, c LIMIT 3"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12))

        # Reproduces #5362:
        assert_rows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b LIMIT 3"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6))

        # Range queries without aggregates and with PER PARTITION LIMIT
        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12))

        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3),
                   row(2, 2, 3, 3),
                   row(4, 8, 2, 12))

        # Range queries with wildcard and with PER PARTITION LIMIT
        assert_rows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        assert_rows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3, 6),
                   row(2, 2, 3, 3, 6),
                   row(4, 8, 2, 12, 24))

        # Range queries without aggregates, with PER PARTITION LIMIT and LIMIT
        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(2, 2, 3, 3))

        # Range queries with wildcard, with PER PARTITION LIMIT and LIMIT
        assert_rows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(2, 2, 3, 3, 6))

        # Range query with DISTINCT
        assert_rows(execute(cql, table, "SELECT DISTINCT a, count(a)FROM %s GROUP BY a"),
                   row(1, 1),
                   row(2, 1),
                   row(4, 1))

        # Reproduces #12479:
        assertInvalidMessage(cql, table, "Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s GROUP BY a, b")

        # Range query with DISTINCT and LIMIT
        # Reproduces #5361:
        assert_rows(execute(cql, table, "SELECT DISTINCT a, count(a)FROM %s GROUP BY a LIMIT 2"),
                   row(1, 1),
                   row(2, 1))

        # Reproduces #12479:
        assertInvalidMessage(cql, table, "Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s GROUP BY a, b LIMIT 2")

        # Range query with ORDER BY
        assertInvalidMessage(cql, table, "ORDER BY is only supported when the partition key is restricted by an EQ or an IN",
                             "SELECT a, b, c, count(b), max(e) FROM %s GROUP BY a, b ORDER BY b DESC, c DESC")

        # Single partition queries
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(1, 4, 12, 2, 24))

        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY b, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(1, 4, 12, 2, 24))

        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY a, b, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12))

        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY a, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12))

        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12))

        # Single partition queries without aggregates
        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6))

        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6))

        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6))

        # Reproduces #4244:
        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 and token(a) = token(1) GROUP BY b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6))

        # Single partition queries with wildcard
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12))

        # Single partition queries with DISTINCT
        assert_rows(execute(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 1))

        # Reproduces #12479:
        assertInvalidMessage(cql, table, "Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a, b")

        # Single partition queries with LIMIT
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 10"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(1, 4, 12, 2, 24))

        # Reproduces #5361:
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12))

        assert_rows(execute(cql, table, "SELECT count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 1"),
                   row(1, 6))

        # Single partition queries with PER PARTITION LIMIT
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 10"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(1, 4, 12, 2, 24))

        # Reproduces #5363:
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12))

        assert_rows(execute(cql, table, "SELECT count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 1"),
                   row(1, 6))

        # Single partition queries without aggregates and with LIMIT
        # Reproduces #5362:
        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6))

        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1"),
                   row(1, 2, 1, 3))

        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6))

        # Single partition queries with wildcard and with LIMIT
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1"),
                   row(1, 2, 1, 3, 6))

        # Single partition queries without aggregates and with PER PARTITION LIMIT
        # Reproduces #5363:
        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6))

        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3))

        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6))

        # Single partition queries with wildcard and with PER PARTITION LIMIT
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3, 6))

        # Single partition queries with ORDER BY
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC"),
                   row(1, 4, 24, 2, 24),
                   row(1, 2, 12, 1, 12),
                   row(1, 2, 6, 1, 6))

        # Single partition queries with ORDER BY and PER PARTITION LIMIT
        # Reproduces #5363:
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC PER PARTITION LIMIT 1"),
                   row(1, 4, 24, 2, 24))

        # Single partition queries with ORDER BY and LIMIT
        # Reproduces #5361:
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 2"),
                   row(1, 4, 24, 2, 24),
                   row(1, 2, 12, 1, 12))

        # Multi-partitions queries
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(1, 4, 12, 2, 24),
                   row(2, 2, 6, 1, 6),
                   row(2, 4, 12, 1, 12),
                   row(4, 8, 24, 1, 24))

        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) AND b = 2 GROUP BY a, b, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(2, 2, 6, 1, 6))

        # Multi-partitions queries without aggregates
        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12))

        assert_rows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12))

        # Multi-partitions with wildcard
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        # Multi-partitions query with DISTINCT
        assert_rows(execute(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a"),
                   row(1, 1),
                   row(2, 1),
                   row(4, 1))

        # Reproduces #12479:
        assertInvalidMessage(cql, table, "Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b")

        # Multi-partitions query with DISTINCT and LIMIT
        # Reproduces #5361:
        assert_rows(execute(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a LIMIT 2"),
                   row(1, 1),
                   row(2, 1))

        # Multi-partitions queries with PER PARTITION LIMIT
        # Reproduces #5363:
        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 1"),
                   row(1, 2, 6, 1, 6),
                   row(2, 2, 6, 1, 6),
                   row(4, 8, 24, 1, 24))

        assert_rows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(2, 2, 6, 1, 6),
                   row(2, 4, 12, 1, 12),
                   row(4, 8, 24, 1, 24))

        # Multi-partitions with wildcard and PER PARTITION LIMIT
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3, 6),
                   row(2, 2, 3, 3, 6),
                   row(4, 8, 2, 12, 24))

        # Multi-partitions queries with ORDER BY
        # (NYH: note that because the partition order isn't known, we can't
        # actually check the result order here, hence I added ignoring_order.
        # this makes this test weaker).
        assert_rows_ignoring_order(execute_without_paging(cql, table, "SELECT a, b, c, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC"),
                   row(4, 8, 2, 1, 24),
                   row(2, 4, 3, 1, 12),
                   row(1, 4, 2, 2, 24),
                   row(2, 2, 3, 1, 6),
                   row(1, 2, 2, 2, 12))

        assert_rows_ignoring_order(execute_without_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c ORDER BY b DESC, c DESC"),
                   row(4, 8, 2, 12),
                   row(2, 4, 3, 6),
                   row(1, 4, 2, 12),
                   row(2, 2, 3, 3),
                   row(1, 2, 2, 6),
                   row(1, 2, 1, 3))

        # Multi-partitions queries with ORDER BY and LIMIT
        # NYH: with LIMIT, even ignoring_order is not enough, the LIMIT
        # may cut it in the wrong place. I unfortunately had to comment
        # out these tests.
        #assert_rows_ignoring_order(execute_without_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 3"),
        #           row(4, 8, 2, 12),
        #           row(2, 4, 3, 6),
        #           row(1, 4, 2, 12))

        # Multi-partitions with wildcard, ORDER BY and LIMIT
        #assert_rows_ignoring_order(execute_without_paging(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 3"),
        #           row(4, 8, 2, 12, 24),
        #           row(2, 4, 3, 6, 12),
        #           row(1, 4, 2, 12, 24))

        # Invalid queries
        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, e")

        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY c")

        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, c, b")

        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, a")

        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, b, c, d FROM %s WHERE token(a) = token(1) GROUP BY b, c")

        assertInvalidMessage(cql, table, "clustering1",
                             "SELECT a, b as clustering1, max(c) FROM %s WHERE a = 1 GROUP BY a, clustering1")

        assertInvalidMessage(cql, table, "z",
                             "SELECT a, b, max(c) FROM %s WHERE a = 1 GROUP BY a, b, z")

    # Test with composite partition key
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c, d)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 1, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 3, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)")

        assertInvalidMessage(cql, table, "partition key",
                             "SELECT a, b, max(d) FROM %s GROUP BY a")

        assert_rows(execute(cql, table, "SELECT a, b, max(d) FROM %s GROUP BY a, b"),
                   row(1, 2, 12),
                   row(1, 1, 12))

        assert_rows(execute(cql, table, "SELECT a, b, max(d) FROM %s WHERE a = 1 AND b = 1 GROUP BY b"),
                   row(1, 1, 12))

    # Test with table without clustering key
    with create_table(cql, test_keyspace, "(a int primary key, b int, c int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (3, 12, 24)")

        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, max(c) FROM %s WHERE a = 1 GROUP BY a, a")

def testGroupByWithoutPagingWithDeletions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 9, 18)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 9, 18)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 9, 18)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 12, 24)")

        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 2 AND c = 1 AND d = 12")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 2 AND c = 2 AND d = 9")

        assert_rows(execute(cql, table, "SELECT a, b, c, count(b), max(d) FROM %s GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 9),
                   row(1, 2, 2, 3, 12),
                   row(1, 2, 3, 4, 12))

@pytest.mark.xfail(reason="issue #5361, #5363")
def testGroupByWithRangeNamesQueryWithoutPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE") as table:
        for i in range(1, 5):
            for j in range(1, 5):
                for k in range(1, 5):
                    execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, k, i + j)

        # Makes sure that we have some tombstones
        execute(cql, table, "DELETE FROM %s WHERE a = 3")

        # Range queries
        assert_rows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        assert_rows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        assert_rows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(1, 2, 3, 2, 3),
                   row(2, 1, 3, 2, 3),
                   row(2, 2, 4, 2, 4),
                   row(4, 1, 5, 2, 5),
                   row(4, 2, 6, 2, 6))

        # Range queries with LIMIT
        assert_rows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a LIMIT 5 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        assert_rows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        # Reproduces #5361:
        assert_rows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(1, 2, 3, 2, 3),
                   row(2, 1, 3, 2, 3))

        # Range queries with PER PARTITION LIMIT
        assert_rows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        # Reproduces #5363:
        assert_rows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        # Range queries with PER PARTITION LIMIT and LIMIT
        assert_rows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 5 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        # Reproduces #5361, #5363
        assert_rows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3))

# SelectSingleColumn
def testClusteringColumnRelationsWithCompactStorage(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a text, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3)
        execute(cql, table, "insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4)

        assert_rows(execute(cql, table, "select * from %s where a in (?, ?)", "first", "second"),
                   row("first", 1, 5, 1),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3),
                   row("second", 4, 8, 4))

        assert_rows(execute(cql, table, "select * from %s where a = ? and b = ? and c in (?, ?)", "first", 2, 6, 7),
                   row("first", 2, 6, 2))

        assert_rows(execute(cql, table, "select * from %s where a = ? and b in (?, ?) and c in (?, ?)", "first", 2, 3, 6, 7),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3))

        assert_rows(execute(cql, table, "select * from %s where a = ? and b in (?, ?) and c in (?, ?)", "first", 3, 2, 7, 6),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3))

        assert_rows(execute(cql, table, "select * from %s where a = ? and c in (?, ?) and b in (?, ?)", "first", 7, 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3))

        assert_rows(execute(cql, table, "select c, d from %s where a = ? and c in (?, ?) and b in (?, ?)", "first", 7, 6, 3, 2),
                   row(6, 2),
                   row(7, 3))

        assert_rows(execute(cql, table, "select c, d from %s where a = ? and c in (?, ?) and b in (?, ?, ?)", "first", 7, 6, 3, 2, 3),
                   row(6, 2),
                   row(7, 3))

        assert_rows(execute(cql, table, "select * from %s where a = ? and b in (?, ?) and c = ?", "first", 3, 2, 7),
                   row("first", 3, 7, 3))

        assert_rows(execute(cql, table, "select * from %s where a = ? and b in ? and c in ?",
                           "first", [3, 2], [7, 6]),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3))

        # Check commented out because Scylla *does* allow equality check
        # with NULL (and it returns nothing).
        #assertInvalidMessage(cql, table, "Invalid null value for column b",
        #                     "select * from %s where a = ? and b in ? and c in ?", "first", None, [7, 6])

        assert_rows(execute(cql, table, "select * from %s where a = ? and c >= ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3))

        assert_rows(execute(cql, table, "select * from %s where a = ? and c > ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 3, 7, 3))

        assert_rows(execute(cql, table, "select * from %s where a = ? and c <= ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 2, 6, 2))

        assert_rows(execute(cql, table, "select * from %s where a = ? and c < ? and b in (?, ?)", "first", 7, 3, 2),
                   row("first", 2, 6, 2))

        assert_rows(execute(cql, table, "select * from %s where a = ? and c >= ? and c <= ? and b in (?, ?)", "first", 6, 7, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3))

        assert_rows(execute(cql, table, "select * from %s where a = ? and c > ? and c <= ? and b in (?, ?)", "first", 6, 7, 3, 2),
                   row("first", 3, 7, 3))

        assertEmpty(execute(cql, table, "select * from %s where a = ? and c > ? and c < ? and b in (?, ?)", "first", 6, 7, 3, 2))

        # Whereas Cassandra doesn't allow the seemingly-non-useful filter
        # "c = ? AND c > ?", Scylla does allow them, intentionally (see
        # discussion in issue #12472. So we commented out these checks:
        #assertInvalidMessage(cql, table, "Column \"c\" cannot be restricted by both an equality and an inequality relation",
        #                     "select * from %s where a = ? and c > ? and c = ? and b in (?, ?)", "first", 6, 7, 3, 2)
        #
        #assertInvalidMessage(cql, table, "c cannot be restricted by more than one relation if it includes an Equal",
        #                     "select * from %s where a = ? and c = ? and c > ?  and b in (?, ?)", "first", 6, 7, 3, 2)

        assert_rows(execute(cql, table, "select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b DESC",
                           "first", 7, 6, 3, 2),
                   row("first", 3, 7, 3),
                   row("first", 2, 6, 2))

        #assertInvalidMessage(cql, table, "More than one restriction was found for the start bound on b",
        #                     "select * from %s where a = ? and b > ? and b > ?", "first", 6, 3, 2)
        #
        #assertInvalidMessage(cql, table, "More than one restriction was found for the end bound on b",
        #                     "select * from %s where a = ? and b < ? and b <= ?", "first", 6, 3, 2)

# SelectTest
# Check query with KEY IN clause for wide row tables
# migrated from cql_tests.py:TestCQL.in_clause_wide_rows_test()
def testSelectKeyInForWideRows(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i)

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = 0 AND c IN (5, 2, 8)"),
                   row(2), row(5), row(8))

    with create_table(cql, test_keyspace, "(k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)) WITH COMPACT STORAGE") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, ?, ?)", i, i)

        assertEmpty(execute(cql, table, "SELECT v FROM %s WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3"))

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = 0 AND c1 = 0 AND c2 IN (5, 2, 8)"),
                   row(2), row(5), row(8))

# Check SELECT respects inclusive and exclusive bounds
# migrated from cql_tests.py:TestCQL.exclusive_slice_test()
def testSelectBounds(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i)

        assertRowCount(execute(cql, table, "SELECT v FROM %s WHERE k = 0"), 10)

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c <= 6"),
                   row(2), row(3), row(4), row(5), row(6))

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = 0 AND c > 2 AND c <= 6"),
                   row(3), row(4), row(5), row(6))

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c < 6"),
                   row(2), row(3), row(4), row(5))

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = 0 AND c > 2 AND c < 6"),
                   row(3), row(4), row(5))

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = 0 AND c > 2 AND c <= 6 LIMIT 2"),
                   row(3), row(4))

        assert_rows(execute(cql, table, "SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c < 6 ORDER BY c DESC LIMIT 2"),
                   row(5), row(4))

# Test for CASSANDRA-4716 bug and more generally for good behavior of ordering,
# migrated from cql_tests.py:TestCQL.reversed_compact_test()
def testReverseCompact(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k text, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c DESC)") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (k, c, v) VALUES ('foo', ?, ?)", i, i)

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo'"),
                   row(5), row(4), row(3))

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo'"),
                   row(6), row(5), row(4), row(3), row(2))

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC"),
                   row(3), row(4), row(5))

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC"),
                   row(2), row(3), row(4), row(5), row(6))

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC"),
                   row(5), row(4), row(3))

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC"),
                   row(6), row(5), row(4), row(3), row(2))

    with create_table(cql, test_keyspace, "(k text, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s(k, c, v) VALUES ('foo', ?, ?)", i, i)

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo'"),
                   row(3), row(4), row(5))

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo'"),
                   row(2), row(3), row(4), row(5), row(6))

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC"),
                   row(3), row(4), row(5))

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC"),
                   row(2), row(3), row(4), row(5), row(6))

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC"),
                   row(5), row(4), row(3))

        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC"),
                   row(6), row(5), row(4), row(3), row(2))

# Test for the bug from CASSANDRA-4760 and CASSANDRA-4759,
# migrated from cql_tests.py:TestCQL.reversed_compact_multikey_test()
def testReversedCompactMultikey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key text, c1 int, c2 int, value text, PRIMARY KEY (key, c1, c2)) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c1 DESC, c2 DESC)") as table:
        for i in range(3):
            for j in range(3):
                execute(cql, table, "INSERT INTO %s (key, c1, c2, value) VALUES ('foo', ?, ?, 'bar')", i, j)

        # Equalities
        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1"),
                   row(1, 2), row(1, 1), row(1, 0))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1 ORDER BY c1 ASC, c2 ASC"),
                   row(1, 0), row(1, 1), row(1, 2))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1 ORDER BY c1 DESC, c2 DESC"),
                   row(1, 2), row(1, 1), row(1, 0))

        # GT
        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1"),
                   row(2, 2), row(2, 1), row(2, 0))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1 ORDER BY c1 ASC, c2 ASC"),
                   row(2, 0), row(2, 1), row(2, 2))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1 ORDER BY c1 DESC, c2 DESC"),
                   row(2, 2), row(2, 1), row(2, 0))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1"),
                   row(2, 2), row(2, 1), row(2, 0), row(1, 2), row(1, 1), row(1, 0))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC, c2 ASC"),
                   row(1, 0), row(1, 1), row(1, 2), row(2, 0), row(2, 1), row(2, 2))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC"),
                   row(1, 0), row(1, 1), row(1, 2), row(2, 0), row(2, 1), row(2, 2))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 DESC, c2 DESC"),
                   row(2, 2), row(2, 1), row(2, 0), row(1, 2), row(1, 1), row(1, 0))

        # LT
        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1"),
                   row(0, 2), row(0, 1), row(0, 0))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1 ORDER BY c1 ASC, c2 ASC"),
                   row(0, 0), row(0, 1), row(0, 2))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1 ORDER BY c1 DESC, c2 DESC"),
                   row(0, 2), row(0, 1), row(0, 0))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1"),
                   row(1, 2), row(1, 1), row(1, 0), row(0, 2), row(0, 1), row(0, 0))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC, c2 ASC"),
                   row(0, 0), row(0, 1), row(0, 2), row(1, 0), row(1, 1), row(1, 2))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC"),
                   row(0, 0), row(0, 1), row(0, 2), row(1, 0), row(1, 1), row(1, 2))

        assert_rows(execute(cql, table, "SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 DESC, c2 DESC"),
                   row(1, 2), row(1, 1), row(1, 0), row(0, 2), row(0, 1), row(0, 0))

# Migrated from cql_tests.py:TestCQL.multi_in_compact_non_composite_test()
def testMultiSelectsNonCompositeCompactStorage(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key int, c int, v int, PRIMARY KEY (key, c)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (key, c, v) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (key, c, v) VALUES (0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (key, c, v) VALUES (0, 2, 2)")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE key=0 AND c IN (0, 2)"),
                   row(0, 0, 0), row(0, 2, 2))

def testSelectDistinct(cql, test_keyspace):
    # Test a 'compact storage' table.
    with create_table(cql, test_keyspace, "(pk0 int, pk1 int, val int, PRIMARY KEY ((pk0, pk1))) WITH COMPACT STORAGE") as table:
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (pk0, pk1, val) VALUES (?, ?, ?)", i, i, i)

        assert_rows(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s LIMIT 1"),
                   row(0, 0))

        assert_rows(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s LIMIT 3"),
                   row(0, 0),
                   row(2, 2),
                   row(1, 1))

    # Test a 'wide row' thrift table.
    with create_table(cql, test_keyspace, "(pk int, name text, val int, PRIMARY KEY (pk, name)) WITH COMPACT STORAGE") as table:
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (pk, name, val) VALUES (?, 'name0', 0)", i)
            execute(cql, table, "INSERT INTO %s (pk, name, val) VALUES (?, 'name1', 1)", i)

        assert_rows(execute(cql, table, "SELECT DISTINCT pk FROM %s LIMIT 1"),
                   row(1))

        assert_rows(execute(cql, table, "SELECT DISTINCT pk FROM %s LIMIT 3"),
                   row(1),
                   row(0),
                   row(2))

REQUIRES_ALLOW_FILTERING_MESSAGE = "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"

@pytest.mark.xfail(reason="issue #12526")
def testFilteringOnCompactTablesWithoutIndices(cql, test_keyspace):
    # Test COMPACT table with clustering columns
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, 4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 4, 4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 3, 7)")

        # Adds tomstones
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1, 4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 2, 7)")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1")
        execute(cql, table, "DELETE FROM %s WHERE a = 2 AND b = 2")

        for _ in before_and_after_flush(cql, table):
            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4 ALLOW FILTERING"),
                       row(1, 4, 4))

            assertInvalidMessage(cql, table, "IN predicates on non-primary-key columns (c) is not yet supported",
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7)")

            assertInvalidMessage(cql, table, "IN predicates on non-primary-key columns (c) is not yet supported",
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING")

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       row(1, 3, 6),
                       row(2, 3, 7))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b < 3 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c >= 3 AND c <= 6 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(1, 3, 6),
                       row(1, 4, 4))

        # Checks filtering with null
        # tests commented out because Scylla does support comparison with null
        # (returning nothing)
        #assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
        #                     "SELECT * FROM %s WHERE c = null")
        #assertInvalidMessage(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c = null ALLOW FILTERING")
        #assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
        #                     "SELECT * FROM %s WHERE c > null")
        #assertInvalidMessage(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c > null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset())

    # Test COMPACT table without clustering columns
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE") as table:
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

            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 4 ALLOW FILTERING"),
                       row(1, 2, 4))

            assertInvalidMessage(cql, table, "IN predicates on non-primary-key columns (c) is not yet supported",
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7)")

            assertInvalidMessage(cql, table, "IN predicates on non-primary-key columns (c) is not yet supported",
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING")

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b < 3 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c >= 3 AND c <= 6 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(2, 1, 6),
                       row(3, 2, 4))

        # Checks filtering with null
        assertInvalidMessage(cql, table,REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null")
        assertInvalidMessage(cql, table,"Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null")
        assertInvalidMessage(cql, table,"Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING")

        # // Checks filtering with unset
        assertInvalidMessage(cql, table,"Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table,"Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset())

@pytest.mark.xfail(reason="issue #12526")
def testFilteringOnCompactTablesWithoutIndicesAndWithLists(cql, test_keyspace):
    # Test COMPACT table with clustering columns
    with create_table(cql, test_keyspace, "(a int, b int, c frozen<list<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, [6, 2])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 4, [4, 1])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 3, [7, 1])")

        for _ in before_and_after_flush(cql, table):
            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = [4, 1]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = [4, 1] ALLOW FILTERING"),
                       row(1, 4, [4, 1]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > [4, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > [4, 2] ALLOW FILTERING"),
                       row(1, 3, [6, 2]),
                       row(2, 3, [7, 1]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b <= 3 AND c < [6, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b <= 3 AND c < [6, 2] ALLOW FILTERING"),
                       row(1, 2, [4, 2]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= [4, 2] AND c <= [6, 4]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c >= [4, 2] AND c <= [6, 4] ALLOW FILTERING"),
                       row(1, 2, [4, 2]),
                       row(1, 3, [6, 2]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, [4, 2]),
                       row(1, 3, [6, 2]))

            assertInvalidMessage(cql, table, "Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, [6, 2]))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset())

    # Test COMPACT table without clustering columns
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c frozen<list<int>>) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 1, [6, 2])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (3, 2, [4, 1])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, 1, [7, 1])")

        for _ in before_and_after_flush(cql, table):
            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = [4, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = [4, 2] ALLOW FILTERING"),
                       row(1, 2, [4, 2]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > [4, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > [4, 2] ALLOW FILTERING"),
                       row(2, 1, [6, 2]),
                       row(4, 1, [7, 1]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= [4, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b < 3 AND c <= [4, 2] ALLOW FILTERING"),
                       row(1, 2, [4, 2]),
                       row(3, 2, [4, 1]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= [4, 3] AND c <= [7]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c >= [4, 3] AND c <= [7] ALLOW FILTERING"),
                       row(2, 1, [6, 2]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, [4, 2]),
                       row(2, 1, [6, 2]))

            assertInvalidMessage(cql, table, "Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, [6, 2]))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset())

@pytest.mark.xfail(reason="issue #12526")
def testFilteringOnCompactTablesWithoutIndicesAndWithSets(cql, test_keyspace):
    # Test COMPACT table with clustering columns
    with create_table(cql, test_keyspace, "(a int, b int, c frozen<set<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, {6, 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 4, {4, 1})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 3, {7, 1})")

        for _ in before_and_after_flush(cql, table):
            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4, 1}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4, 1} ALLOW FILTERING"),
                       row(1, 4, {4, 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4, 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > {4, 2} ALLOW FILTERING"),
                       row(1, 3, {6, 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b <= 3 AND c < {6, 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b <= 3 AND c < {6, 2} ALLOW FILTERING"),
                       row(1, 2, {2, 4}),
                       row(2, 3, {1, 7}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4, 2} AND c <= {6, 4}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c >= {4, 2} AND c <= {6, 4} ALLOW FILTERING"),
                       row(1, 2, {4, 2}),
                       row(1, 3, {6, 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, {4, 2}),
                       row(1, 3, {6, 2}))

            assertInvalidMessage(cql, table, "Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, {6, 2}))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset())

    # Test COMPACT table without clustering columns
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c frozen<set<int>>) WITH COMPACT STORAGE") as table:

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 1, {6, 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (3, 2, {4, 1})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, 1, {7, 1})")

        for _ in before_and_after_flush(cql, table):
            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4, 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4, 2} ALLOW FILTERING"),
                       row(1, 2, {4, 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4, 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > {4, 2} ALLOW FILTERING"),
                       row(2, 1, {6, 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= {4, 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b < 3 AND c <= {4, 2} ALLOW FILTERING"),
                       row(1, 2, {4, 2}),
                       row(4, 1, {1, 7}),
                       row(3, 2, {4, 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4, 3} AND c <= {7}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c >= {5, 2} AND c <= {7} ALLOW FILTERING"),
                       row(2, 1, {6, 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, {4, 2}),
                       row(2, 1, {6, 2}))

            assertInvalidMessage(cql, table, "Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, {6, 2}))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset())

def testAllowFilteringOnPartitionKeyWithDistinct(cql, test_keyspace):
    # Test a 'compact storage' table.
    with create_table(cql, test_keyspace, "(pk0 int, pk1 int, val int, PRIMARY KEY ((pk0, pk1))) WITH COMPACT STORAGE") as table:
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (pk0, pk1, val) VALUES (?, ?, ?)", i, i, i)

        for _ in before_and_after_flush(cql, table):
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk1 = 1 LIMIT 3")

            assert_rows(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk0 < 2 AND pk1 = 1 LIMIT 1 ALLOW FILTERING"),
                       row(1, 1))

            assert_rows(execute(cql, table, "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk1 > 1 LIMIT 3 ALLOW FILTERING"),
                       row(2, 2))

    # Test a 'wide row' thrift table.
    with create_table(cql, test_keyspace, "(pk int, name text, val int, PRIMARY KEY (pk, name)) WITH COMPACT STORAGE") as table:
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (pk, name, val) VALUES (?, 'name0', 0)", i)
            execute(cql, table, "INSERT INTO %s (pk, name, val) VALUES (?, 'name1', 1)", i)

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT DISTINCT pk FROM %s WHERE pk > 1 LIMIT 1 ALLOW FILTERING"),
                       row(2))

            assert_rows(execute(cql, table, "SELECT DISTINCT pk FROM %s WHERE pk > 0 LIMIT 3 ALLOW FILTERING"),
                       row(1),
                       row(2))

def executeFilteringOnly(cql, table, statement):
    assert_invalid(cql, table, statement)
    return execute_without_paging(cql, table, statement + " ALLOW FILTERING")

@pytest.mark.xfail(reason="issue #12526")
def testAllowFilteringOnPartitionKeyWithCounters(cql, test_keyspace):
    for compactStorageClause in ["", " WITH COMPACT STORAGE"]:
        with create_table(cql, test_keyspace, "(a int, b int, c int, cnt counter, PRIMARY KEY ((a, b), c)) " + compactStorageClause) as table:
            execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 14, 11, 12, 13)
            execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24, 21, 22, 23)
            execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 27, 21, 22, 26)
            execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 34, 31, 32, 33)
            execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24, 41, 42, 43)

            for _ in before_and_after_flush(cql, table):
                assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE cnt = 24"),
                           row(41, 42, 43, 24),
                           row(21, 22, 23, 24))
                assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 22 AND cnt = 24"),
                           row(41, 42, 43, 24))
                assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 10 AND b < 25 AND cnt = 24"),
                           row(21, 22, 23, 24))
                assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 10 AND c < 25 AND cnt = 24"),
                           row(21, 22, 23, 24))

                assertInvalidMessage(cql, table,
                "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.",
                "SELECT * FROM %s WHERE a = 21 AND b > 10 AND cnt > 23 ORDER BY c DESC ALLOW FILTERING")

                assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE a = 21 AND b = 22 AND cnt > 23 ORDER BY c DESC"),
                           row(21, 22, 26, 27),
                           row(21, 22, 23, 24))

                assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE cnt > 20 AND cnt < 30"),
                           row(41, 42, 43, 24),
                           row(21, 22, 23, 24),
                           row(21, 22, 26, 27))

@pytest.mark.xfail(reason="issue #12526")
def testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndicesAndWithLists(cql, test_keyspace):
    # Test COMPACT table with clustering columns
    with create_table(cql, test_keyspace, "(a int, b int, c frozen<list<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, [6, 2])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 4, [4, 1])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 3, [7, 1])")

        for _ in before_and_after_flush(cql, table):
            # Checks filtering
            assertInvalidMessage(cql, table, "filtering",
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = [4, 1]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b >= 4 AND c = [4, 1] ALLOW FILTERING"),
                       row(1, 4, [4, 1]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 0 AND c > [4, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND c > [4, 2] ALLOW FILTERING"),
                       row(1, 3, [6, 2]),
                       row(2, 3, [7, 1]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND b <= 3 AND c < [6, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a <= 1 AND b <= 3 AND c < [6, 2] ALLOW FILTERING"),
                       row(1, 2, [4, 2]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a <= 1 AND c >= [4, 2] AND c <= [6, 4]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a > 0 AND b <= 3 AND c >= [4, 2] AND c <= [6, 4] ALLOW FILTERING"),
                       row(1, 2, [4, 2]),
                       row(1, 3, [6, 2]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a > 0 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, [4, 2]),
                       row(1, 3, [6, 2]))

            assertInvalidMessage(cql, table, "Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a > 1 AND c CONTAINS KEY 2 ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a < 2 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, [6, 2]))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c > null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset())

    # Test COMPACT table without clustering columns
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c frozen<list<int>>) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 1, [6, 2])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (3, 2, [4, 1])")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, 1, [7, 1])")

        for _ in before_and_after_flush(cql, table):

            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = [4, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = [4, 2] ALLOW FILTERING"),
                       row(1, 2, [4, 2]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c > [4, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a > 3 AND c > [4, 2] ALLOW FILTERING"),
                       row(4, 1, [7, 1]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a < 1 AND b < 3 AND c <= [4, 2]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a < 3 AND b < 3 AND c <= [4, 2] ALLOW FILTERING"),
                       row(1, 2, [4, 2]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c >= [4, 3] AND c <= [7]")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 2 AND c >= [4, 3] AND c <= [7] ALLOW FILTERING"),
                       row(2, 1, [6, 2]))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 3 AND c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, [4, 2]),
                       row(2, 1, [6, 2]))

            assertInvalidMessage(cql, table, "Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a >=1 AND c CONTAINS KEY 2 ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a < 3 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, [6, 2]))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c > null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset())


@pytest.mark.xfail(reason="issue #12526")
def testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndicesAndWithMaps(cql, test_keyspace):
    # Test COMPACT table with clustering columns
    with create_table(cql, test_keyspace, "(a int, b int, c frozen<map<int, int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, {6 : 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 4, {4 : 1})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 3, {7 : 1})")

        for _ in before_and_after_flush(cql, table):

            # Checks filtering
            assertInvalidMessage(cql, table, "filtering",
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = {4 : 1}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a <= 1 AND b = 4 AND c = {4 : 1} ALLOW FILTERING"),
                       row(1, 4, {4: 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c > {4 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a > 1 AND c > {4 : 2} ALLOW FILTERING"),
                       row(2, 3, {7: 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND b <= 3 AND c < {6 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b <= 3 AND c < {6 : 2} ALLOW FILTERING"),
                       row(1, 2, {4: 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c >= {4 : 2} AND c <= {6 : 4}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a > 0 AND c >= {4 : 2} AND c <= {6 : 4} ALLOW FILTERING"),
                       row(1, 2, {4: 2}),
                       row(1, 3, {6: 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 10 AND c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a > 0 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, {4: 2}),
                       row(1, 3, {6: 2}))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a < 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, {6: 2}))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, {6: 2}))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY ? ALLOW FILTERING",
                             unset())

    # Test COMPACT table without clustering columns
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c frozen<map<int, int>>) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 1, {6 : 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (3, 2, {4 : 1})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, 1, {7 : 1})")

        for _ in before_and_after_flush(cql, table):

            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4 : 2} ALLOW FILTERING"),
                       row(1, 2, {4: 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c > {4 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND c > {4 : 2} ALLOW FILTERING"),
                       row(2, 1, {6: 2}),
                       row(4, 1, {7: 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b < 3 AND c <= {4 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b < 3 AND c <= {4 : 2} ALLOW FILTERING"),
                       row(1, 2, {4: 2}),
                       row(3, 2, {4: 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c >= {4 : 3} AND c <= {7 : 1}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 2 AND c >= {5 : 2} AND c <= {7 : 0} ALLOW FILTERING"),
                       row(2, 1, {6: 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, {4: 2}),
                       row(2, 1, {6: 2}))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a > 0 AND c CONTAINS KEY 4 ALLOW FILTERING"),
                       row(1, 2, {4: 2}),
                       row(3, 2, {4: 1}))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 2 AND c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(2, 1, {6: 2}))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY ? ALLOW FILTERING",
                             unset())

@pytest.mark.xfail(reason="issue #12526")
def testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndicesAndWithSets(cql, test_keyspace):
    # Test COMPACT table with clustering columns
    with create_table(cql, test_keyspace, "(a int, b int, c frozen<set<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, {6, 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 4, {4, 1})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 3, {7, 1})")

        for _ in before_and_after_flush(cql, table):

            # Checks filtering
            assertInvalidMessage(cql, table, "filtering",
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = {4, 1}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = {4, 1} ALLOW FILTERING"),
                       row(1, 4, {4, 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c > {4, 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND c > {4, 2} ALLOW FILTERING"),
                       row(1, 3, {6, 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b <= 3 AND c < {6, 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a > 0 AND b <= 3 AND c < {6, 2} ALLOW FILTERING"),
                       row(1, 2, {2, 4}),
                       row(2, 3, {1, 7}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c >= {4, 2} AND c <= {6, 4}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 0 AND c >= {4, 2} AND c <= {6, 4} ALLOW FILTERING"),
                       row(1, 2, {4, 2}),
                       row(1, 3, {6, 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a < 2 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, {4, 2}),
                       row(1, 3, {6, 2}))

            assertInvalidMessage(cql, table, "Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY 2 ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, {6, 2}))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset())

    # Test COMPACT table without clustering columns
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c frozen<set<int>>) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 1, {6, 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (3, 2, {4, 1})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, 1, {7, 1})")

        for _ in before_and_after_flush(cql, table):

            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4, 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4, 2} ALLOW FILTERING"),
                       row(1, 2, {4, 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c > {4, 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND c > {4, 2} ALLOW FILTERING"),
                       row(2, 1, {6, 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b < 3 AND c <= {4, 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a <= 4 AND b < 3 AND c <= {4, 2} ALLOW FILTERING"),
                       row(1, 2, {4, 2}),
                       row(4, 1, {1, 7}),
                       row(3, 2, {4, 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c >= {4, 3} AND c <= {7}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a < 3 AND c >= {5, 2} AND c <= {7} ALLOW FILTERING"),
                       row(2, 1, {6, 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 0 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, {4, 2}),
                       row(2, 1, {6, 2}))

            assertInvalidMessage(cql, table, "Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY 2 ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 2 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, {6, 2}))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING")
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null")
        assertInvalidMessage(cql, table, "Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset())

@pytest.mark.xfail(reason="issue #12526")
def testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndices(cql, test_keyspace):
    # Test COMPACT table with clustering columns
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY ((a, b), c)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 2, 4, 5)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 3, 6, 7)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 4, 4, 5)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 3, 7, 8)")

        # Adds tomstones
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 1, 4, 5)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 2, 7, 8)")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1 AND c = 4")
        execute(cql, table, "DELETE FROM %s WHERE a = 2 AND b = 2 AND c = 7")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4"),
                       row(1, 4, 4, 5))

            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4 AND d = 5")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4 ALLOW FILTERING"),
                       row(1, 4, 4, 5))

            assertInvalidMessage(cql, table, "IN predicates on non-primary-key columns (d) is not yet supported",
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND b = 3 AND d IN (6, 7)")

            assertInvalidMessage(cql, table, "IN predicates on non-primary-key columns (d) is not yet supported",
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND b = 3 AND d IN (6, 7) ALLOW FILTERING")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a < 2 AND c > 4 AND c <= 6 ALLOW FILTERING"),
                       row(1, 3, 6, 7))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a <= 1 AND b >= 2 AND c >= 4 AND d <= 8 ALLOW FILTERING"),
                       row(1, 3, 6, 7),
                       row(1, 4, 4, 5),
                       row(1, 2, 4, 5))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND c >= 4 AND d <= 8 ALLOW FILTERING"),
                       row(1, 3, 6, 7),
                       row(1, 4, 4, 5),
                       row(1, 2, 4, 5))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 2 AND c >= 4 AND d <= 8 ALLOW FILTERING"),
                       row(2, 3, 7, 8))

        # Checks filtering with null
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE d = null")
        assertInvalidMessage(cql, table, "Unsupported null value for column a",
                             "SELECT * FROM %s WHERE a = null ALLOW FILTERING")
        assertInvalidMessage(cql, table, "Unsupported null value for column a",
                             "SELECT * FROM %s WHERE a > null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "Unsupported unset value for column a",
                             "SELECT * FROM %s WHERE a = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "Unsupported unset value for column a",
                             "SELECT * FROM %s WHERE a > ? ALLOW FILTERING",
                             unset())

    # Test COMPACT table without clustering columns
    with create_table(cql, test_keyspace, "(a int primary key, b int, c int) WITH COMPACT STORAGE") as table:
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
            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 4 ALLOW FILTERING"),
                       row(1, 2, 4))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 2 ALLOW FILTERING"),
                       row(1, 2, 4))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b >= 2 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING"),
                       row(1, 2, 4))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b >= 2 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 2 AND b <=1 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND c >= 4 ALLOW FILTERING"),
                       row(1, 2, 4))

            assertInvalidMessage(cql, table, "IN predicates on non-primary-key columns (b) is not yet supported",
                                 "SELECT * FROM %s WHERE a = 1 AND b IN (1, 2) AND c IN (6, 7)")

            assertInvalidMessage(cql, table, "IN predicates on non-primary-key columns (c) is not yet supported",
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING")

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a >= 1 AND b >= 2 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a < 3 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a < 3 AND b >= 2 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c <=6 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(2, 1, 6),
                       row(3, 2, 4))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a) >= token(2)"),
                       row(2, 1, 6),
                       row(4, 1, 7),
                       row(3, 2, 4))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a) >= token(2) ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7),
                       row(3, 2, 4))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a) >= token(2) AND b = 1 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7))

def testFilteringOnCompactTablesWithoutIndicesAndWithMaps(cql, test_keyspace):
    # Test COMPACT table with clustering columns
    with create_table(cql, test_keyspace, "(a int, b int, c frozen<map<int, int>>, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, {6 : 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 4, {4 : 1})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 3, {7 : 1})")

        for _ in before_and_after_flush(cql, table):

            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4 : 1}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4 : 1} ALLOW FILTERING"),
                       row(1, 4, {4: 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > {4 : 2} ALLOW FILTERING"),
                       row(1, 3, {6: 2}),
                       row(2, 3, {7: 1}))

            assertInvalidMessage(cql, table, 'FILTERING',
                                 "SELECT * FROM %s WHERE b <= 3 AND c < {6 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b <= 3 AND c < {6 : 2} ALLOW FILTERING"),
                       row(1, 2, {4: 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4 : 2} AND c <= {6 : 4}")

            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE c >= {4 : 2} AND c <= {6 : 4} ALLOW FILTERING"),
                       row(1, 2, {4: 2}),
                       row(1, 3, {6: 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2")

            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, {4: 2}),
                       row(1, 3, {6: 2}))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, {6: 2}))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, {6: 2}))

        # Checks filtering with null
        # tests commented out because Scylla does support comparison with null
        # (returning nothing)
        #assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
        #                     "SELECT * FROM %s WHERE c = null")
        #assertInvalidMessage(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c = null ALLOW FILTERING")
        #assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
        #                     "SELECT * FROM %s WHERE c > null")
        #assertInvalidMessage(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c > null ALLOW FILTERING")
        #assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
        #                     "SELECT * FROM %s WHERE c CONTAINS null")
        #assertInvalidMessage(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING")
        #assertInvalidMessage(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c CONTAINS KEY null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "unset",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "unset",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "unset",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "unset",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ? ALLOW FILTERING",
                             unset())

    # Test COMPACT table without clustering columns
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c frozen<map<int, int>>) WITH COMPACT STORAGE") as table:

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 1, {6 : 2})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (3, 2, {4 : 1})")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, 1, {7 : 1})")

        for _ in before_and_after_flush(cql, table):

            # Checks filtering
            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4 : 2} ALLOW FILTERING"),
                       row(1, 2, {4: 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c > {4 : 2} ALLOW FILTERING"),
                       row(2, 1, {6: 2}),
                       row(4, 1, {7: 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= {4 : 2}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b < 3 AND c <= {4 : 2} ALLOW FILTERING"),
                       row(1, 2, {4: 2}),
                       row(3, 2, {4: 1}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4 : 3} AND c <= {7 : 1}")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c >= {5 : 2} AND c <= {7 : 0} ALLOW FILTERING"),
                       row(2, 1, {6: 2}))

            assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2")

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, {4: 2}),
                       row(2, 1, {6: 2}))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS KEY 4 ALLOW FILTERING"),
                       row(1, 2, {4: 2}),
                       row(3, 2, {4: 1}))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(2, 1, {6: 2}))

        # Checks filtering with null
        # Checks filtering with null
        # tests commented out because Scylla does support comparison with null
        # (returning nothing)
        #assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
        #                     "SELECT * FROM %s WHERE c = null")
        #assertInvalidMessage(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c = null ALLOW FILTERING")
        #assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
        #                     "SELECT * FROM %s WHERE c > null")
        #assertInvalidMessage(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c > null ALLOW FILTERING")
        #assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
        #                     "SELECT * FROM %s WHERE c CONTAINS null")
        #assertInvalidMessage(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING")
        #assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
        #                     "SELECT * FROM %s WHERE c CONTAINS KEY null")
        #assertInvalidMessage(cql, table, "Unsupported null value for column c",
        #                     "SELECT * FROM %s WHERE c CONTAINS KEY null ALLOW FILTERING")

        # Checks filtering with unset
        assertInvalidMessage(cql, table, "unset",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "unset",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "unset",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset())
        assertInvalidMessage(cql, table, "unset",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ? ALLOW FILTERING",
                             unset())

@pytest.mark.xfail(reason="issue #12526")
def testFilteringOnCompactTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, 13, 14)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, 23, 24)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, 26, 27)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, 33, 34)

        for _ in before_and_after_flush(cql, table):
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE c > 13"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27),
                       row(31, 32, 33, 34))

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE c > 13 AND c < 33"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27))

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE c > 13 AND b < 32"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27))

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE a = 21 AND c > 13 AND b < 32 ORDER BY b DESC"),
                       row(21, 25, 26, 27),
                       row(21, 22, 23, 24))

            # (NYH: note that because the partition order isn't known, we can't
            # actually check the result order here, hence I added ignoring_order.
            # this makes this test weaker).
            assert_rows_ignoring_order(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE a IN (21, 31) AND c > 13 ORDER BY b DESC"),
                       row(31, 32, 33, 34),
                       row(21, 25, 26, 27),
                       row(21, 22, 23, 24))

            # Reproduces #12526:
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE c > 13 AND d < 34"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27))

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE c > 13"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27),
                       row(31, 32, 33, 34))

    # with frozen in clustering key
    with create_table(cql, test_keyspace, "(a int, b int, c frozen<list<int>>, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, [1, 3], 14)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, [2, 3], 24)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, [2, 6], 27)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, [3, 3], 34)

        for _ in before_and_after_flush(cql, table):
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2"),
                       row(21, 22, [2, 3], 24),
                       row(21, 25, [2, 6], 27))

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND b < 25"),
                       row(21, 22, [2, 3], 24))

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 3"),
                       row(21, 22, [2, 3], 24))

            # Reproduces #12526:
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 12 AND c CONTAINS 2 AND d < 27"),
                       row(21, 22, [2, 3], 24))

    # with frozen in value
    with create_table(cql, test_keyspace, "(a int, b int, c int, d frozen<list<int>>, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, 13, [1, 4])
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, 23, [2, 4])
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, 25, [2, 6])
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, 34, [3, 4])

        for _ in before_and_after_flush(cql, table):
            # Reproduces #12526:
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE d CONTAINS 2"),
                       row(21, 22, 23, [2, 4]),
                       row(21, 25, 25, [2, 6]))

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE d CONTAINS 2 AND b < 25"),
                       row(21, 22, 23, [2, 4]))

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE d CONTAINS 2 AND d CONTAINS 4"),
                       row(21, 22, 23, [2, 4]))

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 12 AND c < 25 AND d CONTAINS 2"),
                       row(21, 22, 23, [2, 4]))

@pytest.mark.xfail(reason="issue #12526")
def testFilteringWithCounters(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 14, 11, 12, 13)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24, 21, 22, 23)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 27, 21, 25, 26)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 34, 31, 32, 33)
        execute(cql, table, "UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24, 41, 42, 43)

        for _ in before_and_after_flush(cql, table):

            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE cnt = 24"),
                       row(21, 22, 23, 24),
                       row(41, 42, 43, 24))
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 22 AND cnt = 24"),
                       row(41, 42, 43, 24))
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 10 AND b < 25 AND cnt = 24"),
                       row(21, 22, 23, 24))
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE b > 10 AND c < 25 AND cnt = 24"),
                       row(21, 22, 23, 24))
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE a = 21 AND b > 10 AND cnt > 23 ORDER BY b DESC"),
                       row(21, 25, 26, 27),
                       row(21, 22, 23, 24))
            assert_rows(executeFilteringOnly(cql, table, "SELECT * FROM %s WHERE cnt > 20 AND cnt < 30"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27),
                       row(41, 42, 43, 24))

# Check select with and without compact storage, with different column
# order. See CASSANDRA-10988
def testClusteringOrderWithSlice(cql, test_keyspace):
    # non-compound, ASC order
    with create_table(cql, test_keyspace, "(a text, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (b ASC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES ('a', 2)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES ('a', 3)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 2),
                   row("a", 3))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b DESC"),
                   row("a", 3),
                   row("a", 2))

    # non-compound, DESC order
    with create_table(cql, test_keyspace, "(a text, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES ('a', 2)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES ('a', 3)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 3),
                   row("a", 2))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2),
                   row("a", 3))

    # compound, first column DESC order
    with create_table(cql, test_keyspace, "(a text, b int, c int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 3, 5),
                   row("a", 2, 4))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2, 4),
                   row("a", 3, 5))

    # compound, mixed order
    with create_table(cql, test_keyspace, "(a text, b int, c int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (b ASC, c DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 2, 4),
                   row("a", 3, 5))

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2, 4),
                   row("a", 3, 5))

EMPTY_BYTE_BUFFER = b""

@pytest.mark.parametrize("options", ["", pytest.param(" WITH COMPACT STORAGE", marks=pytest.mark.xfail(reason="issue #12526, #12749"))])
def testEmptyRestrictionValue(cql, test_keyspace, options):
    with create_table(cql, test_keyspace, "(pk blob, c blob, v blob, PRIMARY KEY ((pk), c))" + options) as table:
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                b"foo123", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                b"foo123", b"2", b"2")

        for _ in before_and_after_flush(cql, table):
            # Scylla *does* allow lookup of an empty partition key (see
            # test_mv_empty_string_partition_key_individual()), so we
            # comment out this check
            #assertInvalidMessage(cql, table, "Key may not be empty", "SELECT * FROM %s WHERE pk = textAsBlob('');")
            #assertInvalidMessage(cql, table, "Key may not be empty", "SELECT * FROM %s WHERE pk IN (textAsBlob(''), textAsBlob('1'));")

            assertInvalidMessage(cql, table, "Key may not be empty",
                                 "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                 EMPTY_BYTE_BUFFER, b"2", b"2")

            # Test clustering columns restrictions
            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) = (textAsBlob(''));"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));"),
                       row(b"foo123", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) IN ((textAsBlob('')), (textAsBlob('1')));"),
                       row(b"foo123", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('');"),
                       row(b"foo123", b"1", b"1"),
                       row(b"foo123", b"2", b"2"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) > (textAsBlob(''));"),
                       row(b"foo123", b"1", b"1"),
                       row(b"foo123", b"2", b"2"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('');"),
                       row(b"foo123", b"1", b"1"),
                       row(b"foo123", b"2", b"2"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob(''));"),
                       row(b"foo123", b"1", b"1"),
                       row(b"foo123", b"2", b"2"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('');"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) <= (textAsBlob(''));"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('');"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) < (textAsBlob(''));"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('') AND c < textAsBlob('');"))

        if "COMPACT" in options:
            # Reproduces #12749
            assertInvalidMessage(cql, table, "Invalid empty or null value for column c",
                                 "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                 b"foo123", EMPTY_BYTE_BUFFER, b"4")
        else:
            execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    b"foo123", EMPTY_BYTE_BUFFER, b"4")

            for _ in before_and_after_flush(cql, table):
                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');"),
                           row(b"foo123", EMPTY_BYTE_BUFFER, b"4"))

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) = (textAsBlob(''));"),
                           row(b"foo123", EMPTY_BYTE_BUFFER, b"4"))

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));"),
                           row(b"foo123", EMPTY_BYTE_BUFFER, b"4"),
                           row(b"foo123", b"1", b"1"))

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) IN ((textAsBlob('')), (textAsBlob('1')));"),
                           row(b"foo123", EMPTY_BYTE_BUFFER, b"4"),
                           row(b"foo123", b"1", b"1"))

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('');"),
                           row(b"foo123", b"1", b"1"),
                           row(b"foo123", b"2", b"2"))

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) > (textAsBlob(''));"),
                           row(b"foo123", b"1", b"1"),
                           row(b"foo123", b"2", b"2"))

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('');"),
                           row(b"foo123", EMPTY_BYTE_BUFFER, b"4"),
                           row(b"foo123", b"1", b"1"),
                           row(b"foo123", b"2", b"2"))

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob(''));"),
                           row(b"foo123", EMPTY_BYTE_BUFFER, b"4"),
                           row(b"foo123", b"1", b"1"),
                           row(b"foo123", b"2", b"2"))

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('');"),
                           row(b"foo123", EMPTY_BYTE_BUFFER, b"4"))

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) <= (textAsBlob(''));"),
                           row(b"foo123", EMPTY_BYTE_BUFFER, b"4"))

                assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('');"))

                assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) < (textAsBlob(''));"))

                assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('') AND c < textAsBlob('');"))
        # Test restrictions on non-primary key value
        # Reproduces #12526:
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('') ALLOW FILTERING;"))

        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                b"foo123", b"3", EMPTY_BYTE_BUFFER)

        for _ in before_and_after_flush(cql, table):
            # Reproduces #12526:
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('') ALLOW FILTERING;"),
                       row(b"foo123", b"3", EMPTY_BYTE_BUFFER))

@pytest.mark.xfail(reason="issue #12749")
def testEmptyRestrictionValueWithMultipleClusteringColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"2", b"2")

        for _ in before_and_after_flush(cql, table):
            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('');"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 = textAsBlob('');"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) = (textAsBlob('1'), textAsBlob(''));"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1');"),
                       row(b"foo123", b"1", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 IN (textAsBlob(''), textAsBlob('1'));"),
                       row(b"foo123", b"1", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')));"),
                       row(b"foo123", b"1", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('');"),
                       row(b"foo123", b"1", b"1", b"1"),
                       row(b"foo123", b"1", b"2", b"2"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 > textAsBlob('');"),
                       row(b"foo123", b"1", b"1", b"1"),
                       row(b"foo123", b"1", b"2", b"2"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'));"),
                       row(b"foo123", b"1", b"1", b"1"),
                       row(b"foo123", b"1", b"2", b"2"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 >= textAsBlob('');"),
                       row(b"foo123", b"1", b"1", b"1"),
                       row(b"foo123", b"1", b"2", b"2"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 <= textAsBlob('');"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) <= (textAsBlob('1'), textAsBlob(''));"))

        # Reproduces #12749:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('');"),
                       row(b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('') AND c2 = textAsBlob('1');"),
                       row(b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) = (textAsBlob(''), textAsBlob('1'));"),
                       row(b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1');"),
                       row(b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4"),
                       row(b"foo123", b"1", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')));"),
                       row(b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4"),
                       row(b"foo123", b"1", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'));"),
                       row(b"foo123", b"1", b"1", b"1"),
                       row(b"foo123", b"1", b"2", b"2"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) >= (textAsBlob(''), textAsBlob('1'));"),
                       row(b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4"),
                       row(b"foo123", b"1", b"1", b"1"),
                       row(b"foo123", b"1", b"2", b"2"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) <= (textAsBlob(''), textAsBlob('1'));"),
                       row(b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) < (textAsBlob(''), textAsBlob('1'));"))

@pytest.mark.xfail(reason="issue #12749")
@pytest.mark.parametrize("options", [" WITH COMPACT STORAGE", " WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c DESC)"])
def testEmptyRestrictionValueWithOrderBy(cql, test_keyspace, options):
    orderingClause = "" if "ORDER" in options else "ORDER BY c DESC"
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
                       row(b"foo123", b"2", b"2"),
                       row(b"foo123", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('')" + orderingClause),
                       row(b"foo123", b"2", b"2"),
                       row(b"foo123", b"1", b"1"))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('')" + orderingClause))

            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('')" + orderingClause))

        # Reproduces #12749:
        assertInvalidMessage(cql, table, "Invalid empty or null value for column c",
                             "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                             b"foo123",
                             EMPTY_BYTE_BUFFER,
                             b"4")

@pytest.mark.xfail(reason="issue #12749")
@pytest.mark.parametrize("options", [" WITH COMPACT STORAGE", " WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c1 DESC, c2 DESC)"])
def testEmptyRestrictionValueWithMultipleClusteringColumnsAndOrderBy(cql, test_keyspace, options):
    orderingClause = "" if "ORDER" in options else "ORDER BY c1 DESC, c2 DESC"
    with create_table(cql, test_keyspace, "(pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))" + options) as table:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", b"foo123", b"1", b"2", b"2")
        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('')" + orderingClause),
                       row(b"foo123", b"1", b"2", b"2"),
                       row(b"foo123", b"1", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 > textAsBlob('')" + orderingClause),
                       row(b"foo123", b"1", b"2", b"2"),
                       row(b"foo123", b"1", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                       row(b"foo123", b"1", b"2", b"2"),
                       row(b"foo123", b"1", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 >= textAsBlob('')" + orderingClause),
                       row(b"foo123", b"1", b"2", b"2"),
                       row(b"foo123", b"1", b"1", b"1"))
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1')" + orderingClause),
                       row(b"foo123", b"1", b"1", b"1"),
                       row(b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')))" + orderingClause),
                       row(b"foo123", b"1", b"1", b"1"),
                       row(b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                       row(b"foo123", b"1", b"2", b"2"),
                       row(b"foo123", b"1", b"1", b"1"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) >= (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                       row(b"foo123", b"1", b"2", b"2"),
                       row(b"foo123", b"1", b"1", b"1"),
                       row(b"foo123", EMPTY_BYTE_BUFFER, b"1", b"4"))
# UpdateTest
@pytest.mark.parametrize("forceFlush", [False, True])
def testUpdate(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, value int, PRIMARY KEY (partitionKey, clustering_1))" + compactOption) as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 2, 2)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 3, 3)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, value) VALUES (1, 0, 4)")

        if forceFlush:
            nodetool.flush(cql, table)

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ?",
                           0, 1),
                   row(7))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1) = (?)", 8, 0, 2)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ?",
                           0, 2),
                   row(8))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey IN (?, ?) AND clustering_1 = ?", 9, 0, 1, 0)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ?",
                           0, 1, 0),
                   row(0, 0, 9),
                   row(1, 0, 9))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey IN ? AND clustering_1 = ?", 19, [0, 1], 0)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN ? AND clustering_1 = ?",
                           [0, 1], 0),
                   row(0, 0, 19),
                   row(1, 0, 19))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 10, 0, 1, 0)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)",
                           0, 1, 0),
                   row(0, 0, 10),
                   row(0, 1, 10))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))", 20, 0, 0, 1)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                           0, 0, 1),
                   row(0, 0, 20),
                   row(0, 1, 20))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", null, 0, 0)
        if forceFlush:
            nodetool.flush(cql, table)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                           0, 0, 1),
                   row(0, 1, 20))
        # test invalid queries. Scylla and Cassandra error messages are
        # different, so below we look for some common strings

        # missing primary key element
        assertInvalidMessage(cql, table, "partitionkey",
                             "UPDATE %s SET value = ? WHERE clustering_1 = ? ", 7, 1)

        assertInvalidMessage(cql, table, "clustering_1",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0)

        # token function
        assertInvalidMessage(cql, table, "The token function cannot be used in WHERE clauses",
                             "UPDATE %s SET value = ? WHERE token(partitionKey) = token(?) AND clustering_1 = ?",
                             7, 0, 1)

        # multiple time the same value
        # Cassandra generates an InvalidSyntax here, Scylla InvalidRequest,
        # both are reasonable.
        assertInvalidThrow(cql, table, (SyntaxException, InvalidRequest), "UPDATE %s SET value = ?, value = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1)

        # multiple time same primary key element in WHERE clause
        # This is allowed in Scylla so the test is commented out.
        #assertInvalidMessage(cql, table, "clustering_1 cannot be restricted by more than one relation if it includes an Equal",
        #                     "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_1 = ?", 7, 0, 1, 1)

        # Undefined column names
        assertInvalidMessage(cql, table, "value1",
                             "UPDATE %s SET value1 = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1)

        assertInvalidMessage(cql, table, "partitionkey1",
                             "UPDATE %s SET value = ? WHERE partitionKey1 = ? AND clustering_1 = ?", 7, 0, 1)

        assertInvalidMessage(cql, table, "clustering_3",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_3 = ?", 7, 0, 1)

        # Invalid operator in the where clause
        assertInvalidMessage(cql, table, "Only EQ and IN relation are supported on the partition key",
                             "UPDATE %s SET value = ? WHERE partitionKey > ? AND clustering_1 = ?", 7, 0, 1)

        assertInvalidMessageRE(cql, table, "Cannot use CONTAINS on non-collection column|Cannot use UPDATE with CONTAINS",
                             "UPDATE %s SET value = ? WHERE partitionKey CONTAINS ? AND clustering_1 = ?", 7, 0, 1)

        assertInvalidMessage(cql, table, "value",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND value = ?", 7, 0, 1, 3)

        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 > ?", 7, 0, 1)

@pytest.mark.parametrize("forceFlush", [False, True])
def testUpdateWithTwoClusteringColumns(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, clustering_2 int, value int, PRIMARY KEY (partitionKey, clustering_1, clustering_2))" + compactOption) as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 2, 2)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 3, 3)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 1, 1, 4)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 1, 2, 5)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (1, 0, 0, 6)")
        if forceFlush:
            nodetool.flush(cql, table)

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 1),
                   row(7))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) = (?, ?)", 8, 0, 1, 2)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 2),
                   row(8))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 9, 0, 1, 0, 0)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 0, 0),
                   row(0, 0, 0, 9),
                   row(1, 0, 0, 9))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey IN ? AND clustering_1 = ? AND clustering_2 = ?", 9, [0, 1], 0, 0)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey IN ? AND clustering_1 = ? AND clustering_2 = ?",
                           [0, 1], 0, 0),
                   row(0, 0, 0, 9),
                   row(1, 0, 0, 9))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)", 12, 0, 1, 1, 2)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)",
                           0, 1, 1, 2),
                   row(0, 1, 1, 12),
                   row(0, 1, 2, 12))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND clustering_2 IN (?, ?)", 10, 0, 1, 0, 1, 2)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND clustering_2 IN (?, ?)",
                           0, 1, 0, 1, 2),
                   row(0, 0, 1, 10),
                   row(0, 0, 2, 10),
                   row(0, 1, 1, 10),
                   row(0, 1, 2, 10))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))", 20, 0, 0, 2, 1, 2)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                           0, 0, 2, 1, 2),
                   row(0, 0, 2, 20),
                   row(0, 1, 2, 20))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", null, 0, 0, 2)
        if forceFlush:
            nodetool.flush(cql, table)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                           0, 0, 2, 1, 2),
                   row(0, 1, 2, 20))

        # test invalid queries. Scylla and Cassandra error messages are
        # different, so below we look for some common strings

        # missing primary key element
        assertInvalidMessage(cql, table, "partitionkey",
                             "UPDATE %s SET value = ? WHERE clustering_1 = ? AND clustering_2 = ?", 7, 1, 1)

        errorMsg = 'PRIMARY KEY column "clustering_2" cannot be restricted as preceding column "clustering_1" is not restricted'

        assertInvalidMessage(cql, table, errorMsg,
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_2 = ?", 7, 0, 1)

        assertInvalidMessage(cql, table, "clustering_1",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0)

        # token function
        assertInvalidMessage(cql, table, "The token function cannot be used in WHERE clauses",
                             "UPDATE %s SET value = ? WHERE token(partitionKey) = token(?) AND clustering_1 = ? AND clustering_2 = ?",
                             7, 0, 1, 1)

        # multiple time the same value
        # Cassandra generates an InvalidSyntax here, Scylla InvalidRequest,
        # both are reasonable.
        assertInvalidThrow(cql, table, (SyntaxException, InvalidRequest), "UPDATE %s SET value = ?, value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)

        # multiple time same primary key element in WHERE clause
        # Scylla allows this, so test commented out.
        #assertInvalidMessage(cql, table, "clustering_1 cannot be restricted by more than one relation if it includes an Equal",
        #                     "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND clustering_1 = ?", 7, 0, 1, 1, 1)

        # Undefined column names
        assertInvalidMessage(cql, table, "value1",
                             "UPDATE %s SET value1 = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)

        assertInvalidMessage(cql, table, "partitionkey1",
                             "UPDATE %s SET value = ? WHERE partitionKey1 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)

        assertInvalidMessage(cql, table, "clustering_3",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_3 = ?", 7, 0, 1, 1)

        # Invalid operator in the where clause
        assertInvalidMessage(cql, table, "Only EQ and IN relation are supported on the partition key",
                             "UPDATE %s SET value = ? WHERE partitionKey > ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)

        assertInvalidMessageRE(cql, table, "Cannot use CONTAINS on non-collection column|Cannot use UPDATE with CONTAINS",
                             "UPDATE %s SET value = ? WHERE partitionKey CONTAINS ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1)

        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND value = ?", 7, 0, 1, 1, 3)

        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 > ?", 7, 0, 1)

        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) > (?, ?)", 7, 0, 1, 1)

@pytest.mark.parametrize("forceFlush", [False, True])
def testUpdateWithMultiplePartitionKeyComponents(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey_1 int, partitionKey_2 int, clustering_1 int, clustering_2 int, value int, PRIMARY KEY ((partitionKey_1, partitionKey_2), clustering_1, clustering_2))" + compactOption) as table:
        execute(cql, table, "INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 1, 0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 1, 1, 1, 2)")
        execute(cql, table, "INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (1, 0, 0, 1, 3)")
        execute(cql, table, "INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (1, 1, 0, 1, 3)")
        if forceFlush:
            nodetool.flush(cql, table)

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey_1 = ? AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 0, 0, 0)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT value FROM %s WHERE partitionKey_1 = ? AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 0, 0, 0),
                   row(7))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?", 9, 0, 1, 1, 0, 1)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 1, 0, 1),
                   row(0, 1, 0, 1, 9),
                   row(1, 1, 0, 1, 9))

        execute(cql, table, "UPDATE %s SET value = ? WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 10, 0, 1, 0, 1, 0, 1)
        if forceFlush:
            nodetool.flush(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 0, 0, 0, 7),
                   row(0, 0, 0, 1, 10),
                   row(0, 1, 0, 1, 10),
                   row(0, 1, 1, 1, 2),
                   row(1, 0, 0, 1, 10),
                   row(1, 1, 0, 1, 10))

        # missing primary key element
        # There are two ways to fix the following broken request. Cassandra
        # mentions one fix in the error message - that a partition key
        # component is missing - and Scylla mentions a different fix in the
        # error message - that ALLOW FILTERING can be used. Let's just agree
        # there should be an error.
        assertInvalid(cql, table,
                             "UPDATE %s SET value = ? WHERE partitionKey_1 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 1, 1)

# AlterTest

# Test for CASSANDRA-13917
def testAlterWithCompactStaticFormat(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "column1",
                             "ALTER TABLE %s RENAME column1 TO column2")

# Test for CASSANDRA-13917
def testAlterWithCompactNonStaticFormat(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, PRIMARY KEY (a,b)) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "column1",
                             "ALTER TABLE %s RENAME column1 TO column2")

    with create_table(cql, test_keyspace, "(a int, b int, v int, PRIMARY KEY (a,b)) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "column1",
                             "ALTER TABLE %s RENAME column1 TO column2")

# CreateTest

# Test for CASSANDRA-13917
def testCreateIndextWithCompactStaticFormat(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "column1",
                             "CREATE INDEX column1_index on %s (column1)")
        assertInvalidMessage(cql, table, "value",
                             "CREATE INDEX value_index on %s (value)")

# DeleteTest

# Test for CASSANDRA-13917
def testDeleteWithCompactStaticFormat(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE") as table:
        do_testDeleteWithCompactFormat(cql, table)

    # if column1 is present, hidden column is called column2
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "column2",
                             "DELETE FROM %s WHERE a = 1 AND column2= 1")
        assertInvalidMessage(cql, table, "column2",
                             "DELETE FROM %s WHERE a = 1 AND column2 = 1 AND value1 = 1")
        assertInvalidMessage(cql, table, "column2",
                             "DELETE column2 FROM %s WHERE a = 1")

    # if value is present, hidden column is called value1
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "value1",
                             "DELETE FROM %s WHERE a = 1 AND value1 = 1")
        assertInvalidMessage(cql, table, "value1",
                             "DELETE FROM %s WHERE a = 1 AND value1 = 1 AND column1 = 1")
        assertInvalidMessage(cql, table, "value1",
                             "DELETE value1 FROM %s WHERE a = 1")

# Test for CASSANDRA-13917
# Reproduces #12815
@pytest.mark.xfail(reason="issue #12815")
def testDeleteWithCompactNonStaticFormat(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 1)")
        assert_rows(execute(cql, table, "SELECT a, b FROM %s"),
                   row(1, 1),
                   row(2, 1))
        do_testDeleteWithCompactFormat(cql, table)

    with create_table(cql, test_keyspace, "(a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, v) VALUES (1, 1, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, v) VALUES (2, 1, 4)")
        assert_rows(execute(cql, table, "SELECT a, b, v FROM %s"),
                   row(1, 1, 3),
                   row(2, 1, 4))
        do_testDeleteWithCompactFormat(cql, table)

def do_testDeleteWithCompactFormat(cql, table):
    assertInvalidMessage(cql, table, "value",
                         "DELETE FROM %s WHERE a = 1 AND value = 1")
    assertInvalidMessage(cql, table, "column1",
                         "DELETE FROM %s WHERE a = 1 AND column1= 1")
    assertInvalidMessage(cql, table, "value",
                         "DELETE FROM %s WHERE a = 1 AND value = 1 AND column1 = 1")
    assertInvalidMessage(cql, table, "value",
                         "DELETE value FROM %s WHERE a = 1")
    assertInvalidMessage(cql, table, "column1",
                         "DELETE column1 FROM %s WHERE a = 1")

# InsertTest

# Test for CASSANDRA-13917
def testInsertWithCompactStaticFormat(cql, test_keyspace):
    do_testInsertWithCompactTable(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE")
    # if column1 is present, hidden column is called column2
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, column1) VALUES (1, 1, 1, 1)")
        assertInvalidMessage(cql, table, "column2",
                             "INSERT INTO %s (a, b, c, column2) VALUES (1, 1, 1, 1)")

    # if value is present, hidden column is called value1
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, value) VALUES (1, 1, 1, 1)")
        assertInvalidMessage(cql, table, "value1",
                             "INSERT INTO %s (a, b, c, value1) VALUES (1, 1, 1, 1)")

# Test for CASSANDRA-13917
# Reproduces #12815.
# Because it currently crashes Scylla we have to skip it instead of xfail...
@pytest.mark.skip(reason="issue #12815")
def testInsertWithCompactNonStaticFormat(cql, test_keyspace):
    do_testInsertWithCompactTable(cql, test_keyspace, "(a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE")
    do_testInsertWithCompactTable(cql, test_keyspace, "(a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE")

def do_testInsertWithCompactTable(cql, test_keyspace, create):
    with create_table(cql, test_keyspace, create) as table:

        # pass correct types to the hidden columns
        assertInvalidMessage(cql, table, "column1",
                             "INSERT INTO %s (a, b, column1) VALUES (?, ?, ?)",
                             1, 1, 1, b'a')
        assertInvalidMessage(cql, table, "value",
                             "INSERT INTO %s (a, b, value) VALUES (?, ?, ?)",
                             1, 1, 1, b'a')
        assertInvalidMessage(cql, table, "column1",
                             "INSERT INTO %s (a, b, column1, value) VALUES (?, ?, ?, ?)",
                             1, 1, 1, b'a', b'b')
        assertInvalidMessage(cql, table, "value",
                             "INSERT INTO %s (a, b, value, column1) VALUES (?, ?, ?, ?)",
                             1, 1, 1, b'a', b'b')

        # pass incorrect types to the hidden columns
        assertInvalidMessage(cql, table, "value",
                             "INSERT INTO %s (a, b, value) VALUES (?, ?, ?)",
                             1, 1, 1, 1)
        assertInvalidMessage(cql, table, "column1",
                             "INSERT INTO %s (a, b, column1) VALUES (?, ?, ?)",
                             1, 1, 1, 1)
        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        # pass null to the hidden columns
        assertInvalidMessage(cql, table, "value",
                             "INSERT INTO %s (a, b, value) VALUES (?, ?, ?)",
                             1, 1, None)
        assertInvalidMessage(cql, table, "column1",
                             "INSERT INTO %s (a, b, column1) VALUES (?, ?, ?)",
                             1, 1, None)

# SelectTest

# Test for CASSANDRA-13917
def testSelectWithCompactStaticFormat(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 1, 1)")
        assert_rows(execute(cql, table, "SELECT a, b, c FROM %s"),
                   row(1, 1, 1),
                   row(2, 1, 1))
        do_testSelectWithCompactFormat(cql, table)

    # if column column1 is present, hidden column is called column2
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, column1) VALUES (1, 1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b, c, column1) VALUES (2, 1, 1, 2)")
        assert_rows(execute(cql, table, "SELECT a, b, c, column1 FROM %s"),
                   row(1, 1, 1, 1),
                   row(2, 1, 1, 2))
        assertInvalidMessage(cql, table, "column2",
                             "SELECT a, column2, value FROM %s")

    # if column value is present, hidden column is called value1
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, value) VALUES (1, 1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b, c, value) VALUES (2, 1, 1, 2)")
        assert_rows(execute(cql, table, "SELECT a, b, c, value FROM %s"),
                   row(1, 1, 1, 1),
                   row(2, 1, 1, 2))
        assertInvalidMessage(cql, table, "value1",
                             "SELECT a, value1, value FROM %s")

# Test for CASSANDRA-13917
# Reproduces #12815.
# Because it currently crashes Scylla we have to skip it instead of xfail...
@pytest.mark.skip(reason="issue #12815")
def testSelectWithCompactNonStaticFormat(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, PRIMARY KEY (a,b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 1)")
        assert_rows(execute(cql, table, "SELECT a, b FROM %s"),
                   row(1, 1),
                   row(2, 1))
        do_testSelectWithCompactFormat(cql, table)

    with create_table(cql, test_keyspace, "(a int, b int, v int, PRIMARY KEY (a,b)) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, v) VALUES (1, 1, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, v) VALUES (2, 1, 4)")
        assert_rows(execute(cql, table, "SELECT a, b, v FROM %s"),
                   row(1, 1, 3),
                   row(2, 1, 4))
        do_testSelectWithCompactFormat(cql, table)

def do_testSelectWithCompactFormat(cql, table):
    assertInvalidMessage(cql, table, "column1",
                         "SELECT column1 FROM %s")
    assertInvalidMessage(cql, table, "value",
                         "SELECT value FROM %s")
    assertInvalidMessage(cql, table, "value",
                         "SELECT value, column1 FROM %s")
    assertInvalidMessage(cql, table, "column1",
                  "SELECT * FROM %s WHERE column1 = null ALLOW FILTERING")
    assertInvalidMessage(cql, table, "value",
                  "SELECT * FROM %s WHERE value = null ALLOW FILTERING")
    assertInvalidMessage(cql, table, "column1",
                         "SELECT WRITETIME(column1) FROM %s")
    assertInvalidMessage(cql, table, "value",
                         "SELECT WRITETIME(value) FROM %s")

# UpdateTest

# Test for CASSANDRA-13917
def testUpdateWithCompactStaticFormat(cql, test_keyspace):
    do_testUpdateWithCompactFormat(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE")
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE") as table:
        assertInvalidMessage(cql, table, "column1",
                             "UPDATE %s SET b = 1 WHERE column1 = ?",
                             b'a')
        assertInvalidMessage(cql, table, "value",
                             "UPDATE %s SET b = 1 WHERE value = ?",
                             b'a')

    # if column1 is present, hidden column is called column2
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, column1) VALUES (1, 1, 1, 1)")
        execute(cql, table, "UPDATE %s SET column1 = 6 WHERE a = 1")
        assertInvalidMessage(cql, table, "column2", "UPDATE %s SET column2 = 6 WHERE a = 0")
        assertInvalidMessage(cql, table, "value", "UPDATE %s SET value = 6 WHERE a = 0")

    # if value is present, hidden column is called value1
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, value) VALUES (1, 1, 1, 1)")
        execute(cql, table, "UPDATE %s SET value = 6 WHERE a = 1")
        assertInvalidMessage(cql, table, "column1", "UPDATE %s SET column1 = 6 WHERE a = 1")
        assertInvalidMessage(cql, table, "value1", "UPDATE %s SET value1 = 6 WHERE a = 1")

# Test for CASSANDRA-13917
# Reproduces #12815.
# Because it currently crashes Scylla we have to skip it instead of xfail...
@pytest.mark.skip(reason="issue #12815")
def testUpdateWithCompactNonStaticFormat(cql, test_keyspace):
    do_testUpdateWithCompactFormat(cql, test_keyspace, "(a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE")
    do_testUpdateWithCompactFormat(cql, test_keyspace, "(a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE")

def do_testUpdateWithCompactFormat(cql, test_keyspace, create):
    with create_table(cql, test_keyspace, create) as table:
        # pass correct types to hidden columns
        assertInvalidMessage(cql, table, "column1",
                             "UPDATE %s SET column1 = ? WHERE a = 0",
                             b'a')
        assertInvalidMessage(cql, table, "value",
                             "UPDATE %s SET value = ? WHERE a = 0",
                             b'a')

        # pass incorrect types to hidden columns
        assertInvalidMessage(cql, table, "column1", "UPDATE %s SET column1 = 6 WHERE a = 0")
        assertInvalidMessage(cql, table, "value", "UPDATE %s SET value = 6 WHERE a = 0")
