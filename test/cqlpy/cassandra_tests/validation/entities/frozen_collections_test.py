# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException
from cassandra.util import OrderedMap


# Porting note: The original Java test used ByteOrderedPartitioner so could
# check the sort order of frozen collections using a partition key. I don't
# want to rely on this deprecated partitioner or any partioner at all, so
# I had to avoid testing the sort order, and use assert_rows_ignoring_order()
# instead of assert_rows(). This means this test verifies that frozen
# collections can be used as partition keys, but doesn't verify their sort
# order. We will verify their sort order in the clustering key tests below.
def do_test_partition_key_usage(cql, test_keyspace, typ, v1, v2, v3, v4):
    with create_table(cql, test_keyspace, f"(k frozen<{typ}> PRIMARY KEY, v int)") as table:
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", v1, 1)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", v2, 1)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", v3, 0)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", v4, 0)

        # overwrite with an update
        execute(cql, table, "UPDATE %s SET v=? WHERE k=?", 0, v1)
        execute(cql, table, "UPDATE %s SET v=? WHERE k=?", 0, v2)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                   [v1, 0],
                   [v2, 0],
                   [v3, 0],
                   [v4, 0])

        assert_rows_ignoring_order(execute(cql, table, "SELECT k FROM %s"),
                   [v1],
                   [v2],
                   [v3],
                   [v4])

        # The Java test had here a test for SELECT with a LIMIT, but as
        # explained above, we can't check the sort order in this test.`
        #assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s LIMIT 2"),
        #           [v1, 0],
        #           [v2, 0])

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE k=?", v3),
                   [v3, 0])

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE k=?", v1),
                   [v1, 0])

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE k IN ?", [v3, v1]),
                   [v1, 0],
                   [v3, 0])

        # The Java test had here a test for SELECT with a token(), but as
        # explained above, we can't check the sort order in this test.`
        #assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE token(k) >= token(?)", v3),
        #           [v3, 0],
        #           [v4, 0])

        # Reproduces issue #7852 (null key should be rejected):
        assert_invalid(cql, table, "INSERT INTO %s (k, v) VALUES (null, 0)")

        execute(cql, table, "DELETE FROM %s WHERE k=?", v1)
        execute(cql, table, "DELETE FROM %s WHERE k=?", v3)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                   [v2, 0],
                   [v4, 0])

def test_partition_key_usage_set(cql, test_keyspace):
    do_test_partition_key_usage(cql, test_keyspace, "set<int>", set(), {1, 2, 3}, {4, 5, 6}, {7, 8, 9})

def test_partition_key_usage_list(cql, test_keyspace):
    do_test_partition_key_usage(cql, test_keyspace, "list<int>", [], [1, 2, 3], [4, 5, 6], [7, 8, 9])

def test_partition_key_usage_map(cql, test_keyspace):
    do_test_partition_key_usage(cql, test_keyspace, "map<int, int>", {}, {1: 10, 2: 20, 3: 30}, {4: 40, 5: 50, 6: 60}, {7: 70, 8: 80, 9: 90})

def testNestedPartitionKeyUsage(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k frozen<map<set<int>, list<int>>> PRIMARY KEY, v int)") as table:
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", {}, 1)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", {frozenset({}): [1, 2, 3]}, 0)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", {frozenset({1, 2, 3}): [1, 2, 3]}, 1)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", {frozenset({4, 5, 6}): [1, 2, 3]}, 0)
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", {frozenset({7, 8, 9}): [1, 2, 3]}, 0)

        # overwrite with an update
        execute(cql, table, "UPDATE %s SET v=? WHERE k=?", 0, {})
        execute(cql, table, "UPDATE %s SET v=? WHERE k=?", 0, {frozenset({1, 2, 3}): [1, 2, 3]})

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
            [{}, 0],
            [{frozenset({}): [1, 2, 3]}, 0],
            [{frozenset({1, 2, 3}): [1, 2, 3]}, 0],
            [{frozenset({4, 5, 6}): [1, 2, 3]}, 0],
            [{frozenset({7, 8, 9}): [1, 2, 3]}, 0]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT k FROM %s"),
            [{}],
            [{frozenset({}): [1, 2, 3]}],
            [{frozenset({1, 2, 3}): [1, 2, 3]}],
            [{frozenset({4, 5, 6}): [1, 2, 3]}],
            [{frozenset({7, 8, 9}): [1, 2, 3]}]
        )

        #assertRows(execute("SELECT * FROM %s LIMIT 3"),
        #    row(map(), 0),
        #    row(map(set(), list(1, 2, 3)), 0),
        #    row(map(set(1, 2, 3), list(1, 2, 3)), 0)
        #);

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE k=?", {frozenset({4, 5, 6}): [1, 2, 3]}),
            [{frozenset({4, 5, 6}): [1, 2, 3]}, 0]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE k=?", {}),
                [{}, 0]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE k=?", {frozenset({}): [1, 2, 3]}),
                [{frozenset({}): [1, 2, 3]}, 0]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE k IN ?", [{frozenset({4, 5, 6}): [1, 2, 3]}, {}, {frozenset({}): [1, 2, 3]}]),
                   [{}, 0],
                   [{frozenset({}): [1, 2, 3]}, 0],
                   [{frozenset({4, 5, 6}): [1, 2, 3]}, 0]
        )

        #assertRows(execute("SELECT * FROM %s WHERE token(k) >= token(?)", map(set(4, 5, 6), list(1, 2, 3))),
        #    row(map(set(4, 5, 6), list(1, 2, 3)), 0),
        #    row(map(set(7, 8, 9), list(1, 2, 3)), 0)
        #);

        execute(cql, table, "DELETE FROM %s WHERE k=?", {})
        execute(cql, table, "DELETE FROM %s WHERE k=?", {frozenset({}): [1, 2, 3]})
        execute(cql, table, "DELETE FROM %s WHERE k=?", {frozenset({4, 5, 6}): [1, 2, 3]})
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
            [{frozenset({1, 2, 3}): [1, 2, 3]}, 0],
            [{frozenset({7, 8, 9}): [1, 2, 3]}, 0]
        )
def testClusteringKeyUsageSet(cql, test_keyspace):
    do_testClusteringKeyUsage(cql, test_keyspace, "set<int>", set(), {1, 2, 3}, {4, 5, 6}, {7, 8, 9})

def testClusteringKeyUsageList(cql, test_keyspace):
    do_testClusteringKeyUsage(cql, test_keyspace, "list<int>", [], [1, 2, 3], [4, 5, 6], [7, 8, 9])

def testClusteringKeyUsageMap(cql, test_keyspace):
    do_testClusteringKeyUsage(cql, test_keyspace, "map<int, int>", {},
        {1: 10, 2: 20, 3: 30}, {4: 40, 5: 50, 6: 60}, {7: 70, 8: 80, 9: 90})

def do_testClusteringKeyUsage(cql, test_keyspace, typ, v1, v2, v3, v4):
    with create_table(cql, test_keyspace, f"(a int, b frozen<{typ}>, c int, PRIMARY KEY (a,b))") as table:
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

# Reproduces issue #7868
def testClusteringKeyUsageWithReverseOrderSet(cql, test_keyspace):
    do_testClusteringKeyUsageWithReverseOrder(cql, test_keyspace, "set<int>", set(), {1, 2, 3}, {4, 5, 6}, {7, 8, 9})

# Reproduces issue #7868
def testClusteringKeyUsageWithReverseOrderList(cql, test_keyspace):
    do_testClusteringKeyUsageWithReverseOrder(cql, test_keyspace, "list<int>", [], [1, 2, 3], [4, 5, 6], [7, 8, 9])

# Reproduces issue #7868
def testClusteringKeyUsageWithReverseOrderMap(cql, test_keyspace):
    do_testClusteringKeyUsageWithReverseOrder(cql, test_keyspace, "map<int, int>", {},
        {1: 10, 2: 20, 3: 30}, {4: 40, 5: 50, 6: 60}, {7: 70, 8: 80, 9: 90})

def do_testClusteringKeyUsageWithReverseOrder(cql, test_keyspace, typ, v1, v2, v3, v4):
    with create_table(cql, test_keyspace, f"(a int, b frozen<{typ}>, c int, PRIMARY KEY (a,b)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v2, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v3, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v4, 0)

        # overwrite with an update
        execute(cql, table, "UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, v1)
        execute(cql, table, "UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, v2)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, v4, 0],
                   [0, v3, 0],
                   [0, v2, 0],
                   [0, v1, 0]
        )

        assert_rows(execute(cql, table, "SELECT b FROM %s"),
                   [v4],
                   [v3],
                   [v2],
                   [v1]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s LIMIT 2"),
                   [0, v4, 0],
                   [0, v3, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, v3),
                   [0, v3, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, v1),
                   [0, v1, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b IN ?", 0, [v3, v1]),
                   [0, v3, 0],
                   [0, v1, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ?", 0, v3),
                   [0, v4, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b >= ?", 0, v3),
                   [0, v4, 0],
                   [0, v3, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b < ?", 0, v3),
                   [0, v2, 0],
                   [0, v1, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b <= ?", 0, v3),
                   [0, v3, 0],
                   [0, v2, 0],
                   [0, v1, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, v2, v3),
                   [0, v3, 0]
        )

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=?", 0, v1)
        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=?", 0, v3)
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, v4, 0],
                   [0, v2, 0]
        )

def testNestedClusteringKeyUsage(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {}, {}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({}): [1, 2, 3]}, {}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, {}, set(), 0],
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT b FROM %s"),
                   [{}],
                   [{frozenset({}): [1, 2, 3]}],
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
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=0 ORDER BY b DESC LIMIT 4"),
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {}),
                   [0, {}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {frozenset({}): [1, 2, 3]}),
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {frozenset({1, 2, 3}): [1, 2, 3]}),
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({}): [1, 2, 3]}, {}),
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c) IN ?", 0, [tuple([{frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}]), tuple([{}, {}])]),
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
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b <= ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {}, set(), 0],
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, {frozenset({1, 2, 3}): [1, 2, 3]}, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {}, {})
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {}, {}))

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({}): [1, 2, 3]}, {})
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({}): [1, 2, 3]}, {}))

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3})
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}))

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

# Reproduces issue #7868 and #7902
def testNestedClusteringKeyUsageWithReverseOrder(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>, d int, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {}, set(), 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({}): [1, 2, 3]}, set(), 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0],
                   [0, {}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT b FROM %s"),
                   [{frozenset({7, 8, 9}): [1, 2, 3]}],
                   [{frozenset({4, 5, 6}): [1, 2, 3]}],
                   [{frozenset({1, 2, 3}): [1, 2, 3]}],
                   [{frozenset({}): [1, 2, 3]}],
                   [{}],
        )

        assert_rows(execute(cql, table, "SELECT c FROM %s"),
                   [{1, 2, 3}],
                   [{1, 2, 3}],
                   [{1, 2, 3}],
                   [set()],
                   [set()],
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s LIMIT 3"),
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=0 ORDER BY b DESC LIMIT 4"),
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {}),
                   [0, {}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {frozenset({}): [1, 2, 3]}),
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, {frozenset({1, 2, 3}): [1, 2, 3]}),
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({}): [1, 2, 3]}, {}),
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0]
        )

        # The following SELECT, with a tuple of frozen collections,
        # reproduces issue #7902:
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c) IN ?", 0, [tuple([{frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}]), tuple([{}, {}])]),
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {}, set(), 0],
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b >= ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b < ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0],
                   [0, {}, set(), 0],
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b <= ?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({}): [1, 2, 3]}, set(), 0],
                   [0, {}, set(), 0],
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, {frozenset({1, 2, 3}): [1, 2, 3]}, {frozenset({4, 5, 6}): [1, 2, 3]}),
                   [0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}, 0]
        )

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {}, {})
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {}, {}))

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({}): [1, 2, 3]}, {})
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({}): [1, 2, 3]}, {}))

        execute(cql, table, "DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3})
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}))

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [0, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}, 0],
                   [0, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}, 0],
        )


def testNormalColumnUsage(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, {}, {})
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, {frozenset({}): [99999, 999999, 99999]}, {})
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3})
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 3, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3})
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 4, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3})

        # overwrite with update
        execute(cql, table, "UPDATE %s SET b=? WHERE a=?", {frozenset({}): [1, 2, 3]}, 1)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                   [0, {}, {}],
                   [1, {frozenset({}): [1, 2, 3]}, set()],
                   [2, {frozenset({1, 2, 3}): [1, 2, 3]}, {1, 2, 3}],
                   [3, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}],
                   [4, {frozenset({7, 8, 9}): [1, 2, 3]}, {1, 2, 3}]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT b FROM %s"),
                   [{}],
                   [{frozenset({}): [1, 2, 3]}],
                   [{frozenset({1, 2, 3}): [1, 2, 3]}],
                   [{frozenset({4, 5, 6}): [1, 2, 3]}],
                   [{frozenset({7, 8, 9}): [1, 2, 3]}]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT c FROM %s"),
                   [{}],
                   [{}],
                   [{1, 2, 3}],
                   [{1, 2, 3}],
                   [{1, 2, 3}],
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=?", 3),
                   [3, {frozenset({4, 5, 6}): [1, 2, 3]}, {1, 2, 3}]
        )

        execute(cql, table, "UPDATE %s SET b=? WHERE a=?", None, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=?", 1),
                   [1, None, set()]
        )

        execute(cql, table, "UPDATE %s SET b=? WHERE a=?", {}, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=?", 1),
                   [1, {}, set()]
        )

        execute(cql, table, "UPDATE %s SET c=? WHERE a=?", None, 2)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=?", 2),
                   [2, {frozenset({1, 2, 3}): [1, 2, 3]}, None]
        )

        execute(cql, table, "UPDATE %s SET c=? WHERE a=?", {}, 2)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=?", 2),
                   [2, {frozenset({1, 2, 3}): [1, 2, 3]}, set()]
        )

        execute(cql, table, "DELETE b FROM %s WHERE a=?", 3)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=?", 3),
                   [3, None, {1, 2, 3}]
        )

        execute(cql, table, "DELETE c FROM %s WHERE a=?", 4)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=?", 4),
                   [4, {frozenset({7, 8, 9}): [1, 2, 3]}, None]
        )

def testStaticColumnUsage(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c frozen<map<set<int>, list<int>>> static, d int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, {}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, {}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, {frozenset({}): [1, 2, 3]}, 0)
        execute(cql, table, "INSERT INTO %s (a, b, d) VALUES (?, ?, ?)", 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, {frozenset({1, 2, 3}): [1, 2, 3]}, 0)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
            [0, 0, {}, 0],
            [0, 1, {}, 0],
            [1, 0, {frozenset({}): [1, 2, 3]}, 0],
            [1, 1, {frozenset({}): [1, 2, 3]}, 0],
            [2, 0, {frozenset({1, 2, 3}): [1, 2, 3]}, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND b=?", 0, 1),
            [0, 1, {}, 0]
        )

        execute(cql, table, "DELETE c FROM %s WHERE a=?", 0)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                [0, 0, None, 0],
                [0, 1, None, 0],
                [1, 0, {frozenset({}): [1, 2, 3]}, 0],
                [1, 1, {frozenset({}): [1, 2, 3]}, 0],
                [2, 0, {frozenset({1, 2, 3}): [1, 2, 3]}, 0]
        )

        execute(cql, table, "DELETE FROM %s WHERE a=?", 0)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                [1, 0, {frozenset({}): [1, 2, 3]}, 0],
                [1, 1, {frozenset({}): [1, 2, 3]}, 0],
                [2, 0, {frozenset({1, 2, 3}): [1, 2, 3]}, 0]
        )

        execute(cql, table, "UPDATE %s SET c=? WHERE a=?", {frozenset({1, 2, 3}): [1, 2, 3]}, 1)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                [1, 0, {frozenset({1, 2, 3}): [1, 2, 3]}, 0],
                [1, 1, {frozenset({1, 2, 3}): [1, 2, 3]}, 0],
                [2, 0, {frozenset({1, 2, 3}): [1, 2, 3]}, 0]
        )

def testInvalidOperations(cql, test_keyspace):
    # lists
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l frozen<list<int>>)") as table:
        assert_invalid(cql, table, "UPDATE %s SET l[?]=? WHERE k=?", 0, 0, 0)
        assert_invalid(cql, table, "UPDATE %s SET l = ? + l WHERE k=?", [0], 0)
        assert_invalid(cql, table, "UPDATE %s SET l = l + ? WHERE k=?", [4], 0)
        assert_invalid(cql, table, "UPDATE %s SET l = l - ? WHERE k=?", [3], 0)
        assert_invalid(cql, table, "DELETE l[?] FROM %s WHERE k=?", 0, 0)

    # sets
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s frozen<set<int>>)") as table:
        assert_invalid(cql, table, "UPDATE %s SET s = s + ? WHERE k=?", {0}, 0)
        assert_invalid(cql, table, "UPDATE %s SET s = s - ? WHERE k=?", {3}, 0)

    # maps
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m frozen<map<int, int>>)") as table:
        assert_invalid(cql, table, "UPDATE %s SET m[?]=? WHERE k=?", 0, 0, 0)
        assert_invalid(cql, table, "UPDATE %s SET m = m + ? WHERE k=?", {4: 4}, 0)
        assert_invalid(cql, table, "DELETE m[?] FROM %s WHERE k=?", 0, 0)

    # Cassandra uses the message "Non-frozen collections are not allowed
    # inside collections", Scylla uses the slightly different "Non-frozen user
    # types or collections are not allowed inside collections: set<set<int>>"
    with pytest.raises((InvalidRequest, ConfigurationException, SyntaxException), match="Non-frozen.* collections are not allowed inside collections"):
        with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t set<set<int>>)") as table:
            pass

    with pytest.raises((InvalidRequest, ConfigurationException, SyntaxException), match="Counters are not allowed inside collections"):
        with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t set<counter>)") as table:
            pass

    with pytest.raises((InvalidRequest, ConfigurationException, SyntaxException), match="frozen<> is only allowed on collections, tuples, and user-defined types"):
        with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t frozen<text>)") as table:
            pass

def testSecondaryIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a frozen<map<int, text>> PRIMARY KEY, b frozen<map<int, text>>)") as table:
        # for now, we don't support indexing values or keys of collections in the primary key
        # Cassandra and Scylla have slightly different error messages:
        # Cassandra talks about "the only partition key column", while Scylla
        # just says "partition key column".
        with pytest.raises(InvalidRequest, match="Cannot create secondary index on.* partition key column"):
            execute(cql, table, "CREATE INDEX ON %s (full(a))")
        with pytest.raises(InvalidRequest, match="Cannot create secondary index on.* partition key column"):
            execute(cql, table, "CREATE INDEX ON %s (keys(a))")
        # Cassandra and Scylla have slightly different error messages:
        # Cassandra says "Cannot create keys() index on frozen column b."
        # while Scylla says "Cannot create index on index_keys of frozen<map>
        # column b".
        with pytest.raises(InvalidRequest, match="Cannot create.* index on.* frozen.* b."):
            execute(cql, table, "CREATE INDEX ON %s (keys(b))")

    with create_table(cql, test_keyspace, "(a int, b frozen<list<int>>, c frozen<set<int>>, d frozen<map<int, text>>, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "CREATE INDEX ON %s (full(b))")
        execute(cql, table, "CREATE INDEX ON %s (full(c))")
        execute(cql, table, "CREATE INDEX ON %s (full(d))")

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, [1, 2, 3], {1, 2, 3}, {1: "a"})
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, [4, 5, 6], {1, 2, 3}, {1: "a"})
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, [1, 2, 3], {4, 5, 6}, {2: "b"})
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, [4, 5, 6], {4, 5, 6}, {2: "b"})

        # CONTAINS KEY doesn't work on non-maps
        assert_invalid_message(cql, table, "Cannot use CONTAINS KEY on non-map column",
                             "SELECT * FROM %s WHERE b CONTAINS KEY ?", 1)

        assert_invalid_message(cql, table, "Cannot use CONTAINS KEY on non-map column",
                             "SELECT * FROM %s WHERE b CONTAINS KEY ? ALLOW FILTERING", 1)

        assert_invalid_message(cql, table, "Cannot use CONTAINS KEY on non-map column",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ?", 1)

        # normal indexes on frozen collections don't support CONTAINS or CONTAINS KEY
        # Cassandra's and Scylla's messages are different: Cassandra has
        # "Clustering columns can only be restricted with CONTAINS with a
        # secondary index or filtering", Scylla "Cannot restrict clustering
        # columns by a CONTAINS relation without a secondary index or filtering"
        with pytest.raises(InvalidRequest, match="CONTAINS.* secondary index or filtering"):
            execute(cql, table, "SELECT * FROM %s WHERE b CONTAINS ?", 1)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE b CONTAINS ? ALLOW FILTERING", 1),
                   [0, [1, 2, 3], {1, 2, 3}, {1: "a"}],
                   [1, [1, 2, 3], {4, 5, 6}, {2: "b"}])

        assert_invalid_message(cql, table, "ALLOW FILTERING",
                             "SELECT * FROM %s WHERE d CONTAINS KEY ?", 1)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b CONTAINS ? AND d CONTAINS KEY ? ALLOW FILTERING", 1, 1),
                   [0, [1, 2, 3], {1, 2, 3}, {1: "a"}])

        # index lookup on b
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE b=?", [1, 2, 3]),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}],
            [1, [1, 2, 3], {4, 5, 6}, {2: "b"}]
        )

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE b=?", [-1]))

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND c=?", [1, 2, 3], {4, 5, 6})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b=? AND c=? ALLOW FILTERING", [1, 2, 3], {4, 5, 6}),
            [1, [1, 2, 3], {4, 5, 6}, {2: "b"}]
        )

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND c CONTAINS ?", [1, 2, 3], 5)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b=? AND c CONTAINS ? ALLOW FILTERING", [1, 2, 3], 5),
            [1, [1, 2, 3], {4, 5, 6}, {2: "b"}]
        )

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND d=?", [1, 2, 3], {1: "a"})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b=? AND d=? ALLOW FILTERING", [1, 2, 3], {1: "a"}),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}]
        )

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND d CONTAINS ?", [1, 2, 3], "a")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b=? AND d CONTAINS ? ALLOW FILTERING", [1, 2, 3], "a"),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}]
        )

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND d CONTAINS KEY ?", [1, 2, 3], 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b=? AND d CONTAINS KEY ? ALLOW FILTERING", [1, 2, 3], 1),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}]
        )

        # index lookup on c
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE c=?", {1, 2, 3}),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}],
            [0, [4, 5, 6], {1, 2, 3}, {1: "a"}]
        )

        # ordering of c should not matter
        # In Python, to control the order we need to pass a list
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE c=?", [2, 1, 3]),
                [0, [1, 2, 3], {1, 2, 3}, {1: "a"}],
                [0, [4, 5, 6], {1, 2, 3}, {1: "a"}]
        )

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE c=?", {-1}))

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND b=?", {1, 2, 3}, [1, 2, 3])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c=? AND b=? ALLOW FILTERING", {1, 2, 3}, [1, 2, 3]),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}]
        )

        # Cassandra and Scylla generate different error messages here.
        # Scylla has "Cannot restrict clustering columns by a CONTAINS
        # relation without a secondary index or filtering", Cassandra has
        # uppercase "ALLOW FILTERING"
        with pytest.raises(InvalidRequest, match=re.compile('filtering', re.IGNORECASE)):
            execute(cql, table, "SELECT * FROM %s WHERE c=? AND b CONTAINS ?", {1, 2, 3}, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c=? AND b CONTAINS ? ALLOW FILTERING", {1, 2, 3}, 1),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}]
        )

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND d = ?", {1, 2, 3}, {1: "a"})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c=? AND d = ? ALLOW FILTERING", {1, 2, 3}, {1: "a"}),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}],
            [0, [4, 5, 6], {1, 2, 3}, {1: "a"}]
        )

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND d CONTAINS ?", {1, 2, 3}, "a")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c=? AND d CONTAINS ? ALLOW FILTERING", {1, 2, 3}, "a"),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}],
            [0, [4, 5, 6], {1, 2, 3}, {1: "a"}]
        )

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND d CONTAINS KEY ?", {1, 2, 3}, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE c=? AND d CONTAINS KEY ? ALLOW FILTERING", {1, 2, 3}, 1),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}],
            [0, [4, 5, 6], {1, 2, 3}, {1: "a"}]
        )

        # index lookup on d
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d=?", {1: "a"}),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}],
            [0, [4, 5, 6], {1, 2, 3}, {1: "a"}]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d=?", {2: "b"}),
            [1, [1, 2, 3], {4, 5, 6}, {2: "b"}],
            [1, [4, 5, 6], {4, 5, 6}, {2: "b"}]
        )

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE d=?", {3: "c"}))

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE d=? AND c=?", {1: "a"}, {1, 2, 3})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d=? AND b=? ALLOW FILTERING", {1: "a"}, [1, 2, 3]),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}]
        )

        # Cassandra and Scylla generate different error messages here.
        # Scylla has "Cannot restrict clustering columns by a CONTAINS
        # relation without a secondary index or filtering", Cassandra has
        # uppercase "ALLOW FILTERING"
        with pytest.raises(InvalidRequest, match=re.compile('filtering', re.IGNORECASE)):
            execute(cql, table, "SELECT * FROM %s WHERE d=? AND b CONTAINS ?", {1: "a"}, 3)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d=? AND b CONTAINS ? ALLOW FILTERING", {1: "a"}, 3),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}]
        )

        assert_invalid_message(cql, table, "ALLOW FILTERING", "SELECT * FROM %s WHERE d=? AND b=? AND c=?", {1: "a"}, [1, 2, 3], {1, 2, 3})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d=? AND b=? AND c=? ALLOW FILTERING", {1: "a"}, [1, 2, 3], {1, 2, 3}),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d=? AND b CONTAINS ? AND c CONTAINS ? ALLOW FILTERING", {1: "a"}, 2, 2),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d CONTAINS KEY ? ALLOW FILTERING", 1),
            [0, [1, 2, 3], {1, 2, 3}, {1: "a"}],
            [0, [4, 5, 6], {1, 2, 3}, {1: "a"}]
        )

        execute(cql, table, "DELETE d FROM %s WHERE a=? AND b=?", 0, [1, 2, 3])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d=?", {1: "a"}),
            [0, [4, 5, 6], {1, 2, 3}, {1: "a"}]
        )

# Test for CASSANDRA-8302
# Also reproduces Scylla issue #7888.
def testClusteringColumnFiltering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b frozen<map<int, int>>, c int, d int, PRIMARY KEY (a,b,c))") as table:
        execute(cql, table, "CREATE INDEX c_index ON %s (c)")
        execute(cql, table, "CREATE INDEX d_index ON %s (d)")

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {0: 0, 1: 1}, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, {1: 1, 2: 2}, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, {0: 0, 1: 1}, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, {1: 1, 2: 2}, 0, 0)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE d=? AND b CONTAINS ? ALLOW FILTERING", 0, 0),
                [0, {0: 0, 1: 1}, 0, 0],
                [1, {0: 0, 1: 1}, 0, 0]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE d=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0),
                [0, {0: 0, 1: 1}, 0, 0],
                [1, {0: 0, 1: 1}, 0, 0]
        )

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND d=? AND b CONTAINS ? ALLOW FILTERING", 0, 0, 0),
                [0, {0: 0, 1: 1}, 0, 0]
        )
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND d=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0, 0),
                [0, {0: 0, 1: 1}, 0, 0]
        )

        execute(cql, table, f"DROP INDEX {test_keyspace}.d_index")

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE c=? AND b CONTAINS ? ALLOW FILTERING", 0, 0),
                [0, {0: 0, 1: 1}, 0, 0],
                [1, {0: 0, 1: 1}, 0, 0]
        )

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE c=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0),
                [0, {0: 0, 1: 1}, 0, 0],
                [1, {0: 0, 1: 1}, 0, 0]
        )

        # Scylla had bug #7888, where the following two commands failed with
        # a "marshalling error":
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND c=? AND b CONTAINS ? ALLOW FILTERING", 0, 0, 0),
                [0, {0: 0, 1: 1}, 0, 0]
        )
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND c=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0, 0),
                [0, {0: 0, 1: 1}, 0, 0]
        )

def testFrozenListInMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, m map<frozen<list<int>>, int>)") as table:
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (1, {[1, 2, 3] : 1})")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {(1, 2, 3): 1}])

        execute(cql, table, "UPDATE %s SET m[[1, 2, 3]]=2 WHERE k=1")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {(1, 2, 3): 2}])

        execute(cql, table, "UPDATE %s SET m = m + ? WHERE k=1", {(4, 5, 6): 3})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {(1, 2, 3): 2, (4, 5, 6): 3}])

        execute(cql, table, "DELETE m[[1, 2, 3]] FROM %s WHERE k = 1")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {(4, 5, 6): 3}])

def testFrozenListInSet(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, s set<frozen<list<int>>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (1, {[1, 2, 3]})")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {(1, 2, 3)}]
        )

        execute(cql, table, "UPDATE %s SET s = s + ? WHERE k=1", {(4, 5, 6)})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {(1, 2, 3), (4, 5, 6)}]
        )

        execute(cql, table, "UPDATE %s SET s = s - ? WHERE k=1", {(4, 5, 6)})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {(1, 2, 3)}]
        )

        execute(cql, table, "DELETE s[[1, 2, 3]] FROM %s WHERE k = 1")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, None]
        )

def testFrozenListInList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, l list<frozen<list<int>>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (1, [[1, 2, 3]])")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [[1, 2, 3]]]
        )

        execute(cql, table, "UPDATE %s SET l[?]=? WHERE k=1", 0, [4, 5, 6])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [[4, 5, 6]]]
        )

        execute(cql, table, "UPDATE %s SET l = ? + l WHERE k=1", [[1, 2, 3]])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [[1, 2, 3], [4, 5, 6]]]
        )

        execute(cql, table, "UPDATE %s SET l = l + ? WHERE k=1", [[7, 8, 9]])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [[1, 2, 3], [4, 5, 6], [7, 8, 9]]]
        )

        execute(cql, table, "UPDATE %s SET l = l - ? WHERE k=1", [[4, 5, 6]])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [[1, 2, 3], [7, 8, 9]]]
        )

        execute(cql, table, "DELETE l[0] FROM %s WHERE k = 1")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [[7, 8, 9]]]
        )

def testFrozenMapInMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, m map<frozen<map<int, int>>, int>)") as table:
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (1, {{1 : 1, 2 : 2} : 1})")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({1: 1, 2: 2}): 1}])

        execute(cql, table, "UPDATE %s SET m[?]=2 WHERE k=1", {1: 1, 2: 2})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({1: 1, 2: 2}): 2}])

        # Note the peculiar way in which we need to pass a map in map to the
        # CQL driver in Python :-( {{3: 3, 4: 4}: 3} is not valid python
        # because the key is not hashable... We need to use the driver's
        # OrderedMap instead, taking a list of key-value tuples.
        execute(cql, table, "UPDATE %s SET m = m + ? WHERE k=1", OrderedMap([({3: 3, 4: 4}, 3)]))
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1,
                    {freeze({1: 1, 2: 2}): 2,
                     freeze({3: 3, 4: 4}): 3}])

        execute(cql, table, "DELETE m[?] FROM %s WHERE k = 1", {1: 1, 2: 2})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({3: 3, 4: 4}): 3}])

def testFrozenMapInSet(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, s set<frozen<map<int, int>>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (1, {{1 : 1, 2 : 2}})")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({1: 1, 2: 2})}])

        # Note the peculiar way in which we need to pass a map in set to the
        # CQL driver in Python :-( {{3: 3, 4: 4}} is not valid python
        # because the item is not hashable... We can pass a list instead of
        # a set.
        execute(cql, table, "UPDATE %s SET s = s + ? WHERE k=1", [{3: 3, 4: 4}])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({1: 1, 2: 2}), freeze({3: 3, 4: 4})}])

        execute(cql, table, "UPDATE %s SET s = s - ? WHERE k=1", [{3: 3, 4: 4}])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({1: 1, 2: 2})}])

        execute(cql, table, "DELETE s[?] FROM %s WHERE k = 1", {1: 1, 2: 2})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, None])

def testFrozenMapInList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, l list<frozen<map<int, int>>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (1, [{1 : 1, 2 : 2}])")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{1: 1, 2: 2}]])

        execute(cql, table, "UPDATE %s SET l[?]=? WHERE k=1", 0, {3: 3, 4: 4})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{3: 3, 4: 4}]])

        execute(cql, table, "UPDATE %s SET l = ? + l WHERE k=1", [{1: 1, 2: 2}])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{1: 1, 2: 2}, {3: 3, 4: 4}]])

        execute(cql, table, "UPDATE %s SET l = l + ? WHERE k=1", [{5: 5, 6: 6}])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{1: 1, 2: 2}, {3: 3, 4: 4}, {5: 5, 6: 6}]])

        execute(cql, table, "UPDATE %s SET l = l - ? WHERE k=1", [{3: 3, 4: 4}])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{1: 1, 2: 2}, {5: 5, 6: 6}]])

        execute(cql, table, "DELETE l[0] FROM %s WHERE k = 1")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{5: 5, 6: 6}]])

def testFrozenSetInMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, m map<frozen<set<int>>, int>)") as table:
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (1, {{1, 2, 3} : 1})")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({1, 2, 3}): 1}])

        execute(cql, table, "UPDATE %s SET m[?]=2 WHERE k=1", {1, 2, 3})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({1, 2, 3}): 2}])

        execute(cql, table, "UPDATE %s SET m = m + ? WHERE k=1", OrderedMap([({4, 5, 6}, 3)]))
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1,
                    {freeze({1, 2, 3}): 2,
                     freeze({4, 5, 6}): 3}])

        execute(cql, table, "DELETE m[?] FROM %s WHERE k = 1", {1, 2, 3})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({4, 5, 6}): 3}])

def testFrozenSetInSet(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, s set<frozen<set<int>>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (1, {{1, 2, 3}})")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({1, 2, 3})}])

        execute(cql, table, "UPDATE %s SET s = s + ? WHERE k=1", [{4, 5, 6}])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({1, 2, 3}), freeze({4, 5, 6})}])

        execute(cql, table, "UPDATE %s SET s = s - ? WHERE k=1", [{4, 5, 6}])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, {freeze({1, 2, 3})}])

        execute(cql, table, "DELETE s[?] FROM %s WHERE k = 1", {1, 2, 3})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, None])

def testFrozenSetInList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int primary key, l list<frozen<set<int>>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (1, [{1, 2, 3}])")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{1, 2, 3}]])

        execute(cql, table, "UPDATE %s SET l[?]=? WHERE k=1", 0, {4, 5, 6})
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{4, 5, 6}]])

        execute(cql, table, "UPDATE %s SET l = ? + l WHERE k=1", [{1, 2, 3}])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{1, 2, 3}, {4, 5, 6}]])

        execute(cql, table, "UPDATE %s SET l = l + ? WHERE k=1", [{7, 8, 9}])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]])

        execute(cql, table, "UPDATE %s SET l = l - ? WHERE k=1", [{4, 5, 6}])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{1, 2, 3}, {7, 8, 9}]])

        execute(cql, table, "DELETE l[0] FROM %s WHERE k = 1")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"),
                [1, [{7, 8, 9}]])

def testUserDefinedTypes(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a set<int>, b tuple<list<int>>)") as type_name:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v frozen<{type_name}>)") as table:
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, {a: ?, b: ?})", 0, {1, 2, 3}, [[1, 2, 3]])
            assert_rows(execute(cql, table, "SELECT v.a, v.b FROM %s WHERE k=?", 0),
                [{1, 2, 3}, ([1, 2, 3],)])


# Test parsing of literal lists when the column type is reversed (CASSANDRA-15814)
def testLiteralReversedList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c frozen<list<int>>, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)") as table:
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (0, [1, 2])")
        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE k=0 AND c=[1, 2]"), [[1, 2]])

# Test parsing of literal sets when the column type is reversed (CASSANDRA-15814)
def testLiteralReversedSet(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c frozen<set<int>>, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)") as table:
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (0, {1, 2})")
        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE k=0 AND c={1, 2}"), [{1, 2}])

# Test parsing of literal maps when the column type is reversed (CASSANDRA-15814)
def testLiteralReversedMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c frozen<map<int, int>>, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)") as table:
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (0, {1:2, 3:4})")
        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE k=0 AND c={1:2, 3:4}"), [{1: 2, 3: 4}])

# The following test was not translated to Python. It doesn't make any CQL
# calls, so doesn't check anything in the server!

#    private static String clean(String classname)
#    {
#        return StringUtils.remove(classname, "org.apache.cassandra.db.marshal.");
#    }
#    @Test
#    public void testToString()
#    {
#        // set<frozen<list<int>>>
#        SetType t = SetType.getInstance(ListType.getInstance(Int32Type.instance, false), true);
#        assertEquals("SetType(FrozenType(ListType(Int32Type)))", clean(t.toString()));
#        assertEquals("SetType(ListType(Int32Type))", clean(t.toString(true)));
#
#        // frozen<set<list<int>>>
#        t = SetType.getInstance(ListType.getInstance(Int32Type.instance, false), false);
#        assertEquals("FrozenType(SetType(ListType(Int32Type)))", clean(t.toString()));
#        assertEquals("SetType(ListType(Int32Type))", clean(t.toString(true)));
#
#        // map<frozen<list<int>>, int>
#        MapType m = MapType.getInstance(ListType.getInstance(Int32Type.instance, false), Int32Type.instance, true);
#        assertEquals("MapType(FrozenType(ListType(Int32Type)),Int32Type)", clean(m.toString()));
#        assertEquals("MapType(ListType(Int32Type),Int32Type)", clean(m.toString(true)));
#
#        // frozen<map<list<int>, int>>
#        m = MapType.getInstance(ListType.getInstance(Int32Type.instance, false), Int32Type.instance, false);
#        assertEquals("FrozenType(MapType(ListType(Int32Type),Int32Type))", clean(m.toString()));
#        assertEquals("MapType(ListType(Int32Type),Int32Type)", clean(m.toString(true)));
#
#        // tuple<set<int>>
#        List<AbstractType<?>> types = new ArrayList<>();
#        types.add(SetType.getInstance(Int32Type.instance, true));
#        TupleType tuple = new TupleType(types);
#        assertEquals("TupleType(SetType(Int32Type))", clean(tuple.toString()));
#    }

def testListWithElementsBiggerThan64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l frozen<list<text>>)") as table:
        largeText = 'x' * (65536 + 10)

        execute(cql, table, "INSERT INTO %s(k, l) VALUES (0, ?)", [largeText, "v2"])
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [[largeText, "v2"]])

        # Full overwrite
        execute(cql, table, "UPDATE %s SET l = ? WHERE k = 0", ["v1", largeText])
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v1", largeText]])

        execute(cql, table, "DELETE l FROM %s WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [None])

        execute(cql, table, "INSERT INTO %s(k, l) VALUES (0, ['" + largeText + "', 'v2'])")
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [[largeText, "v2"]])

@pytest.mark.xfail(reason="fails because of long unprepared inserted, issue #7745")
def testMapsWithElementsBiggerThan64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m frozen<map<text, text>>)") as table:
        largeText = 'x' * (65536 + 10)
        largeText2 = 'y' * (65536 + 10)
        execute(cql, table, "INSERT INTO %s(k, m) VALUES (0, ?)", {largeText: "v1", "k2": largeText})
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
            [{largeText: "v1", "k2": largeText}])

        # Full overwrite
        execute(cql, table, "UPDATE %s SET m = ? WHERE k = 0", {"k5": largeText, largeText2: "v6"})
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"k5": largeText, largeText2: "v6"}])

        execute(cql, table, "DELETE m FROM %s WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"), [None])

        # The following long inline (unprepared) insert reproduces issue #7745
        execute(cql, table, "INSERT INTO %s(k, m) VALUES (0, {'" + largeText + "' : 'v1', 'k2' : '" + largeText + "'})")
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{largeText: "v1", "k2": largeText}])

@pytest.mark.xfail(reason="fails because of long unprepared inserted, issue #7745")
def testSetsWithElementsBiggerThan64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s frozen<set<text>>)") as table:
        largeText = 'x' * (65536 + 10)
        execute(cql, table, "INSERT INTO %s(k, s) VALUES (0, ?)", {largeText, "v1", "v2"})
        flush(cql, table)
        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [{largeText, "v1", "v2"}])

        # Full overwrite
        execute(cql, table, "UPDATE %s SET s = ? WHERE k = 0", {largeText, "v3"})
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [{largeText, "v3"}])

        execute(cql, table, "DELETE s FROM %s WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [None])

        # The following long inline (unprepared) insert reproduces issue #7745
        execute(cql, table, "INSERT INTO %s(k, s) VALUES (0, {'" + largeText + "', 'v1', 'v2'})")
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [{largeText, "v1", "v2"}])
