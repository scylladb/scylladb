# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException
from cassandra.query import UNSET_VALUE

# Test for the validation bug of CASSANDRA-4706,
# migrated from cql_tests.py:TestCQL.validate_counter_regular_test()
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testRegularCounters(cql, test_keyspace):
    # The Cassandra and Scylla error messages are different: Cassandra says
    # "Cannot mix counter and non counter columns in the same table", Scylla
    # "Cannot add a non counter column (things) in a counter column family".
    # Let's just check the common phrase "non counter".
    # Cassandra changed the exception it throws on dropping a nonexistent keyspace.
    # FIXME: Cassandra throws InvalidRequest here, but Scylla uses
    # ConfigurationException. We shouldn't have done that... But I consider
    # this difference to be so trivial I didn't want to consider this a bug.
    # Maybe we should reconsider, and not allow ConfigurationException...
    assert_invalid_throw_message(cql, test_keyspace + "." + unique_name(),
                                 "non counter",
                                 (InvalidRequest, ConfigurationException),
                                 "CREATE TABLE %s (id bigint PRIMARY KEY, count counter, things set<text>)")

# Migrated from cql_tests.py:TestCQL.collection_counter_test()
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCountersOnCollections(cql, test_keyspace):
    assert_invalid_throw(cql, test_keyspace + "." + unique_name(),
                         InvalidRequest,
                         "CREATE TABLE %s (k int PRIMARY KEY, l list<counter>)")

    assert_invalid_throw(cql, test_keyspace + "." + unique_name(),
                         InvalidRequest,
                         "CREATE TABLE %s (k int PRIMARY KEY, s set<counter>)")

    assert_invalid_throw(cql, test_keyspace + "." + unique_name(),
                         InvalidRequest,
                         "CREATE TABLE %s (k int PRIMARY KEY, m map<text, counter>)")

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCounterUpdatesWithUnset(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, c counter)") as table:
        # set up
        execute(cql, table, "UPDATE %s SET c = c + 1 WHERE k = 10")
        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE k = 10"), [1])
        # increment
        execute(cql, table, "UPDATE %s SET c = c + ? WHERE k = 10", 1)
        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE k = 10"), [2])
        execute(cql, table, "UPDATE %s SET c = c + ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE k = 10"), [2]) # no change to the counter value
        # decrement
        execute(cql, table, "UPDATE %s SET c = c - ? WHERE k = 10", 1)
        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE k = 10"), [1])
        execute(cql, table, "UPDATE %s SET c = c - ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT c FROM %s WHERE k = 10"), [1]) # no change to the counter value


@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCounterFiltering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, a counter)") as table:
        for i in range(10):
            execute(cql, table, "UPDATE %s SET a = a + ? WHERE k = ?", i, i)

        execute(cql, table, "UPDATE %s SET a = a + ? WHERE k = ?", 6, 10)

        # GT
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a > ? ALLOW FILTERING", 5),
                                [6, 6], [7, 7], [8, 8], [9, 9], [10, 6])

        # GTE
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a >= ? ALLOW FILTERING", 6),
                                [6, 6], [7, 7], [8, 8], [9, 9], [10, 6])

        # LT
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a < ? ALLOW FILTERING", 3),
                                [0,0], [1, 1], [2, 2])

        # LTE
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a <= ? ALLOW FILTERING", 3),
                                [0,0], [1, 1], [2, 2], [3, 3])

        # EQ
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE a = ? ALLOW FILTERING", 6),
                                [6, 6], [10, 6])

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCounterFilteringWithNull(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, a counter, b counter)") as table:
        execute(cql, table, "UPDATE %s SET a = a + ? WHERE k = ?", 1, 1)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a > ? ALLOW FILTERING", 0), [1, 1, None])
        # GT
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE b > ? ALLOW FILTERING", 1))
        # GTE
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE b >= ? ALLOW FILTERING", 1))
        # LT
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE b < ? ALLOW FILTERING", 1))
        # LTE
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE b <= ? ALLOW FILTERING", 1))
        # EQ
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE b = ? ALLOW FILTERING", 1))
        # with null
        assert_invalid_message(cql, table, "Invalid null value for counter increment/decrement",
                             "SELECT * FROM %s WHERE b = null ALLOW FILTERING")

# Test for the validation bug of CASSANDRA-9395.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testProhibitReversedCounterAsPartOfPrimaryKey(cql, test_keyspace):
    # The Cassandra message and Scylla message differ slightly -
    # counter type is not supported for PRIMARY KEY column 'a'"
    # counter type is not supported for PRIMARY KEY part a
    # respectively.
    assert_invalid_throw_message(cql, test_keyspace + "." + unique_name(),
                                 "counter type is not supported for PRIMARY KEY",
                                 InvalidRequest,
                                 "CREATE TABLE %s (a counter, b int, PRIMARY KEY (b, a)) WITH CLUSTERING ORDER BY (a desc);")

# Check that a counter batch works as intended
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testCounterBatch(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid int, url text, total counter, PRIMARY KEY (userid, url))") as table:
        # Ensure we handle updates to the same CQL row in the same partition properly
        execute(cql, table, "BEGIN UNLOGGED BATCH " +
                "UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'; " +
                "APPLY BATCH; ")
        assert_rows(execute(cql, table, "SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"), [3])

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
