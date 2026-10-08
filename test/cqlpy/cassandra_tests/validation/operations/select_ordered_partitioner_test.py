# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1


# SELECT statement tests that require a ByteOrderedPartitioner
#
# Scylla doesn't support ByteOrderedPartitioner, and the Cassandra node we
# test against uses Murmur3Partitioner, and neither can be changed through
# CQL. So the tests below were translated only where their results don't
# depend on the order of tokens: Where a result only depends on the order of
# rows from different partitions, we check it ignoring the order; Checks
# whose results depend on the order of tokens are commented out; And tests
# where all checks depend on the order of tokens were not translated.

from ...porting import *

# For a token() call with the wrong number of arguments, Scylla's error
# message is about the token() function's number of arguments, e.g.,
# "Invalid number of arguments in call to function system.token: 1 required
# but 2 provided", instead of Cassandra's messages below.
SCYLLA_TOKEN_ARGUMENTS_MESSAGE = "Invalid number of arguments in call to function system.token"
ONLY_PARTITION_KEY_MESSAGE = re.escape("The token() function must contains only partition key components") + "|" + SCYLLA_TOKEN_ARGUMENTS_MESSAGE
ALL_OR_NONE_MESSAGE = re.escape("The token() function must be applied to all partition key components or none of them") + "|" + SCYLLA_TOKEN_ARGUMENTS_MESSAGE
# For token(b, a), where a is an int and b is a text, Scylla's error message
# is about the type mismatch ("b cannot be passed as argument 0 of function
# system.token of type int") instead of the argument order. When the types
# match, Scylla gives the same message as Cassandra.
ARGUMENTS_ORDER_MESSAGE = re.escape("The token function arguments must be in the partition key order: a, b") + "|" + re.escape("b cannot be passed as argument 0 of function system.token")

# The tests testTokenAndIndex, testFilteringOnAllPartitionKeysWithTokenRestriction,
# testFilteringOnPartitionKeyWithToken and testTokenAndCollections were not
# translated, because their results depend on the order of tokens of a
# ByteOrderedPartitioner.

def testTokenFunctionWithSingleColumnPartitionKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b text)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, 'a')")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a) >= token(?)", 0), row(0, "a"))
        # The following two checks depend on the order of tokens:
        #assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a) >= token(?) and token(a) < token(?)", 0, 1), row(0, "a"))
        #assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a) BETWEEN token(?) and token(?)", 0, 1), row(0, "a"))
        # Not translated because the Python driver refuses to send incorrect
        # parameters for prepared statements:
        #assert_invalid(cql, table, "SELECT * FROM %s WHERE token(a) > token(?)", "a")
        assert_invalid_message_re(cql, table, ONLY_PARTITION_KEY_MESSAGE,
                             "SELECT * FROM %s WHERE token(a, b) >= token(?, ?)", "b", 0)
        # Scylla does allow multiple restrictions on the same column, including
        # token(a), so the following checks are commented out. The correctness
        # of such queries is tested in
        # test_filtering.py::test_multiple_restrictions_on_same_column
        #assert_invalid_message(cql, table, "More than one restriction was found for the start bound on a",
        #                     "SELECT * FROM %s WHERE token(a) >= token(?) and token(a) >= token(?)", 0, 1)
        #assert_invalid_message(cql, table, "a cannot be restricted by more than one relation if it includes an Equal",
        #                     "SELECT * FROM %s WHERE token(a) >= token(?) and token(a) = token(?)", 0, 1)
        assert_invalid_syntax(cql, table, "SELECT * FROM %s WHERE token(a) = token(?) and token(a) IN (token(?))", 0, 1)

        #assert_invalid_message(cql, table, "More than one restriction was found for the start bound on a",
        #                     "SELECT * FROM %s WHERE token(a) > token(?) AND token(a) > token(?)", 1, 2)
        #assert_invalid_message(cql, table, "More than one restriction was found for the start bound on a",
        #                     "SELECT * FROM %s WHERE token(a) > token(?) AND token(a) BETWEEN token(?) AND token(?)", 1, 2, 3)
        #assert_invalid_message(cql, table, "More than one restriction was found for the end bound on a",
        #                     "SELECT * FROM %s WHERE token(a) <= token(?) AND token(a) < token(?)", 1, 2)
        #assert_invalid_message(cql, table, "More than one restriction was found for the end bound on a",
        #                     "SELECT * FROM %s WHERE token(a) <= token(?) AND token(a) BETWEEN token(?) AND token(?)", 1, 2, 3)
        #assert_invalid_message(cql, table, "a cannot be restricted by more than one relation if it includes an Equal",
        #                     "SELECT * FROM %s WHERE token(a) > token(?) AND token(a) = token(?)", 1, 2)
        #assert_invalid_message(cql, table, "a cannot be restricted by more than one relation if it includes an Equal",
        #                     "SELECT * FROM %s WHERE  token(a) = token(?) AND token(a) > token(?)", 1, 2)
        #assert_invalid_message(cql, table, "a cannot be restricted by more than one relation if it includes an Equal",
        #                     "SELECT * FROM %s WHERE  token(a) = token(?) AND token(a) BETWEEN token(?) AND token(?)", 1, 2, 3)

def testTokenFunctionWithPartitionKeyAndClusteringKeyArguments(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b text, PRIMARY KEY (a, b))") as table:
        assert_invalid_message_re(cql, table, ONLY_PARTITION_KEY_MESSAGE,
                             "SELECT * FROM %s WHERE token(a, b) > token(0, 'c')")

# Reproduces SCYLLADB-5153 (the BETWEEN operator).
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testTokenFunctionWithMultiColumnPartitionKey(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b text, PRIMARY KEY ((a, b)))") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, 'a')")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, 'b')")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, 'c')")

        # The following three checks depend on the order of tokens:
        #assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a, b) > token(?, ?)", 0, "a"),
        #           row(0, "b"),
        #           row(0, "c"))
        #assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a, b) > token(?, ?) and token(a, b) < token(?, ?)", 0, "a", 0, "d"),
        #           row(0, "b"),
        #           row(0, "c"))
        #assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a, b) between token(?, ?) and token(?, ?)", 0, "b", 0, "d"),
        #           row(0, "b"),
        #           row(0, "c"))
        assert_invalid_message_re(cql, table, ALL_OR_NONE_MESSAGE,
                             "SELECT * FROM %s WHERE token(a) > token(?) and token(b) > token(?)", 0, "a")
        assert_invalid_message_re(cql, table, ALL_OR_NONE_MESSAGE,
                             "SELECT * FROM %s WHERE token(a) > token(?, ?) and token(a) < token(?, ?) and token(b) > token(?, ?) ",
                             0, "a", 0, "d", 0, "a")
        assert_invalid_message_re(cql, table, ALL_OR_NONE_MESSAGE,
                             "SELECT * FROM %s WHERE token(a) BETWEEN token(?, ?) AND token(?, ?) and token(b) > token(?, ?) ",
                             0, "a", 0, "d", 0, "a")
        assert_invalid_message_re(cql, table, ARGUMENTS_ORDER_MESSAGE,
                             "SELECT * FROM %s WHERE token(b, a) > token(0, 'c')")
        assert_invalid_message_re(cql, table, ARGUMENTS_ORDER_MESSAGE,
                             "SELECT * FROM %s WHERE token(b, a) BETWEEN token(0, 'c') AND token(0, 'f')")
        assert_invalid_message_re(cql, table, ALL_OR_NONE_MESSAGE,
                             "SELECT * FROM %s WHERE token(a, b) > token(?, ?) and token(b) < token(?, ?)", 0, "a", 0, "a")
        assert_invalid_message_re(cql, table, ALL_OR_NONE_MESSAGE,
                             "SELECT * FROM %s WHERE token(a) > token(?, ?) and token(b) > token(?, ?)", 0, "a", 0, "a")
        assert_invalid_message_re(cql, table, ALL_OR_NONE_MESSAGE,
                             "SELECT * FROM %s WHERE token(a) > token(?, ?) and token(b) > token(?, ?)", 0, "a", 0, "a")

# The tests testSingleColumnPartitionKeyWithTokenNonTokenRestrictionsMix and
# testMultiColumnPartitionKeyWithTokenNonTokenRestrictionsMix were not
# translated, because their results depend on the order of tokens of a
# ByteOrderedPartitioner.

def testMultiColumnPartitionKeyWithIndexAndTokenNonTokenRestrictionsMix(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key((a, b)))") as table:
        execute(cql, table, "CREATE INDEX ON %s(b)")
        execute(cql, table, "CREATE INDEX ON %s(c)")

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, 0);")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 1, 1);")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 2, 2);")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 0, 3);")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1, 4);")

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE b = ?;", 1),
                   row(0, 1, 1),
                   row(1, 1, 4))

        # The following three checks depend on the order of tokens:
        #assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a, b) > token(?, ?) AND b = ?;", 0, 0, 1),
        #           row(0, 1, 1),
        #           row(1, 1, 4))
        #assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b = ? AND token(a, b) > token(?, ?);", 1, 0, 0),
        #           row(0, 1, 1),
        #           row(1, 1, 4))
        #assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b = ? AND token(a, b) > token(?, ?) and c = ? ALLOW FILTERING;", 1, 0, 0, 4),
        #           row(1, 1, 4))

def testTokenFunctionWithCompoundPartitionAndClusteringCols(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY ((a, b), c, d))") as table:
        # just test that the queries don't error
        execute(cql, table, "SELECT * FROM %s WHERE token(a, b) > token(0, 0) AND c > 10 ALLOW FILTERING;")
        execute(cql, table, "SELECT * FROM %s WHERE c > 10 AND token(a, b) > token(0, 0) ALLOW FILTERING;")
        execute(cql, table, "SELECT * FROM %s WHERE token(a, b) > token(0, 0) AND (c, d) > (0, 0) ALLOW FILTERING;")
        execute(cql, table, "SELECT * FROM %s WHERE (c, d) > (0, 0) AND token(a, b) > token(0, 0) ALLOW FILTERING;")

# Test undefined columns
# migrated from cql_tests.py:TestCQL.undefined_column_handling_test()
def testUndefinedColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v1 int, v2 int,)") as table:
        execute(cql, table, "INSERT INTO %s (k, v1, v2) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (k, v1) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (k, v1, v2) VALUES (2, 2, 2)")

        assert_rows_ignoring_order(execute(cql, table, "SELECT v2 FROM %s"), row(0), row(null), row(2))

        rows = getRows(execute(cql, table, "SELECT v2 FROM %s WHERE k = 1"))
        assert 1 == len(rows)
        assert rows[0][0] is None

# Check table with only a PK (#4361),
# migrated from cql_tests.py:TestCQL.only_pk_test()
def testPrimaryKeyOnly(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, PRIMARY KEY (k, c))") as table:
        for k in range(2):
            for c in range(2):
                execute(cql, table, "INSERT INTO %s (k, c) VALUES (?, ?)", k, c)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 0),
                   row(0, 1),
                   row(1, 0),
                   row(1, 1))

# Migrated from cql_tests.py:TestCQL.composite_index_with_pk_test()
def testCompositeIndexWithPK(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(blog_id int, time1 int, time2 int, author text, content text, PRIMARY KEY (blog_id, time1, time2))") as table:
        execute(cql, table, "CREATE INDEX ON %s(author)")

        execute(cql, table, "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)", 1, 0, 0, "foo", "bar1")
        execute(cql, table, "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)", 1, 0, 1, "foo", "bar2")
        execute(cql, table, "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)", 2, 1, 0, "foo", "baz")
        execute(cql, table, "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)", 3, 0, 1, "gux", "qux")

        assert_rows_ignoring_order(execute(cql, table, "SELECT blog_id, content FROM %s WHERE author='foo'"),
                   row(1, "bar1"),
                   row(1, "bar2"),
                   row(2, "baz"))

        assert_rows(execute(cql, table, "SELECT blog_id, content FROM %s WHERE time1 > 0 AND author='foo' ALLOW FILTERING"),
                   row(2, "baz"))

        assert_rows(execute(cql, table, "SELECT blog_id, content FROM %s WHERE time1 = 1 AND author='foo' ALLOW FILTERING"),
                   row(2, "baz"))

        assert_rows(execute(cql, table, "SELECT blog_id, content FROM %s WHERE time1 = 1 AND time2 = 0 AND author='foo' ALLOW FILTERING"),
                   row(2, "baz"))

        assert_empty(execute(cql, table, "SELECT content FROM %s WHERE time1 = 1 AND time2 = 1 AND author='foo' ALLOW FILTERING"))

        assert_empty(execute(cql, table, "SELECT content FROM %s WHERE time1 = 1 AND time2 > 0 AND author='foo' ALLOW FILTERING"))

        assert_invalid(cql, table, "SELECT content FROM %s WHERE time2 >= 0 AND author='foo'")

        assert_invalid(cql, table, "SELECT blog_id, content FROM %s WHERE time1 > 0 AND author='foo'")
        assert_invalid(cql, table, "SELECT blog_id, content FROM %s WHERE time1 = 1 AND author='foo'")
        assert_invalid(cql, table, "SELECT blog_id, content FROM %s WHERE time1 = 1 AND time2 = 0 AND author='foo'")
        assert_invalid(cql, table, "SELECT content FROM %s WHERE time1 = 1 AND time2 = 1 AND author='foo'")
        assert_invalid(cql, table, "SELECT content FROM %s WHERE time1 = 1 AND time2 > 0 AND author='foo'")

# The test testLimitBug (testing LIMIT bugs from 4579, migrated from
# cql_tests.py:TestCQL.limit_bugs_test()) was not translated, because which
# rows a LIMIT returns from a multi-partition scan depends on the order of
# tokens of a ByteOrderedPartitioner.

# Test for #4612 bug and more generally order by when multiple C* rows are queried
# migrated from cql_tests.py:TestCQL.order_by_multikey_test()
def testOrderByMultikey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(my_id varchar, col1 int, col2 int, value varchar, PRIMARY KEY (my_id, col1, col2))") as table:
        execute(cql, table, "INSERT INTO %s (my_id, col1, col2, value) VALUES ( 'key1', 1, 1, 'a');")
        execute(cql, table, "INSERT INTO %s (my_id, col1, col2, value) VALUES ( 'key2', 3, 3, 'a');")
        execute(cql, table, "INSERT INTO %s (my_id, col1, col2, value) VALUES ( 'key3', 2, 2, 'b');")
        execute(cql, table, "INSERT INTO %s (my_id, col1, col2, value) VALUES ( 'key4', 2, 1, 'b');")

        # Cassandra refuses to page a query with both ORDER BY and IN on the
        # partition key, so we need to disable paging (the original Java test
        # doesn't page its queries).
        assert_rows(execute_without_paging(cql, table, "SELECT col1 FROM %s WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                   row(1), row(2), row(3))

        assert_rows(execute_without_paging(cql, table, "SELECT col1, value, my_id, col2 FROM %s WHERE my_id in('key3', 'key4') ORDER BY col1, col2"),
                   row(2, "b", "key4", 1), row(2, "b", "key3", 2))

        assert_invalid(cql, table, "SELECT col1 FROM %s ORDER BY col1")
        assert_invalid(cql, table, "SELECT col1 FROM %s WHERE my_id > 'key1' ORDER BY col1")

# Migrated from cql_tests.py:TestCQL.composite_index_collections_test()
def testIndexOnCompositeWithCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(blog_id int, time1 int, time2 int, author text, content set<text>, PRIMARY KEY (blog_id, time1, time2))") as table:
        execute(cql, table, "CREATE INDEX ON %s (author)")

        execute(cql, table, "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, { 'bar1', 'bar2' })", 1, 0, 0, "foo")
        execute(cql, table, "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, { 'bar2', 'bar3' })", 1, 0, 1, "foo")
        execute(cql, table, "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, { 'baz' })", 2, 1, 0, "foo")
        execute(cql, table, "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, { 'qux' })", 3, 0, 1, "gux")

        assert_rows_ignoring_order(execute(cql, table, "SELECT blog_id, content FROM %s WHERE author='foo'"),
                   row(1, {"bar1", "bar2"}),
                   row(1, {"bar2", "bar3"}),
                   row(2, {"baz"}))

# Migrated from cql_tests.py:TestCQL.truncate_clean_cache_test()
def testTruncateWithCaching(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v1 int, v2 int) WITH CACHING = { 'keys': 'ALL', 'rows_per_partition': 'ALL' };") as table:
        for i in range(3):
            execute(cql, table, "INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)", i, i, i * 2)

        assert_rows_ignoring_order(execute(cql, table, "SELECT v1, v2 FROM %s WHERE k IN (0, 1, 2)"),
                   row(0, 0),
                   row(1, 2),
                   row(2, 4))

        execute(cql, table, "TRUNCATE %s")

        assert_empty(execute(cql, table, "SELECT v1, v2 FROM %s WHERE k IN (0, 1, 2)"))

# Migrated from cql_tests.py:TestCQL.range_key_ordered_test()
def testRangeKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY)") as table:
        execute(cql, table, "INSERT INTO %s (k) VALUES (-1)")
        execute(cql, table, "INSERT INTO %s (k) VALUES ( 0)")
        execute(cql, table, "INSERT INTO %s (k) VALUES ( 1)")

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                   row(0),
                   row(1),
                   row(-1))

        assert_invalid(cql, table, "SELECT * FROM %s WHERE k >= -1 AND k < 1")

def testTokenFunctionWithInvalidColumnNames(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY ((a, b), c))") as table:
        # Cassandra's error message is "Undefined column name e", Scylla's is
        # "Unrecognized name e", so we only check the common part "name e".
        assert_invalid_message(cql, table, "name e", "SELECT * FROM %s WHERE token(a, e) = token(0, 0)")
        assert_invalid_message(cql, table, "name e", "SELECT * FROM %s WHERE token(a, e) > token(0, 1)")
        # Here, Cassandra says "Undefined column name e" but Scylla gives
        # a clearer error message about the real cause: "Aliases aren't
        # allowed in the WHERE clause (name: 'e')".
        assert_invalid(cql, table, "SELECT b AS e FROM %s WHERE token(a, e) = token(0, 0)")
        assert_invalid(cql, table, "SELECT b AS e FROM %s WHERE token(a, e) > token(0, 1)")
