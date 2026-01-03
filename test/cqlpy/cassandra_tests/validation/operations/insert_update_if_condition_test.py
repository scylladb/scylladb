# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 1737efb050e1da9576d47287ebc6f1cc3073a8a0
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Before importing porting.py, enable pytest's nice assertion rewrite in
# its functions such as assert_rows():
import pytest
pytest.register_assert_rewrite('test.cqlpy.cassandra_tests.porting')
from ...porting import *

from cassandra.util import Duration
# The "Duration" type is composed of three separate integers, counting
# months, days and nanoseconds. Other units are composed from these basic
# units - e.g., a year is 12 months, and a week is 7 days. Composing units
# like seconds, minutes and hours, from nanoseconds is long and ugly, so
# the following shortcuts are useful:
s = 1000000000  # nanoseconds per second

# As explained in kb/lwt-differences.rst, Scylla is different from Cassandra
# in that it always returns the full version of the old row, even if the old
# row didn't exist (so it's all NULLs) or if the condition failed - but in
# those cases Cassandra only returns the success boolean and not the whole
# row. For better or for worse, we decided that we want to keep have this
# incompatibility with Cassandra, so needed to modify the test to support it.
@pytest.fixture(scope="module")
def is_scylla(cql):
    names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    yield any('scylla' in name for name in names)

# InsertUpdateIfConditionCollectionsTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670)
# Any changes here check if they apply to the other classes
# - InsertUpdateIfConditionStaticsTest
# - InsertUpdateIfConditionCollectionsTest
# - InsertUpdateIfConditionTest

# Migrated from cql_tests.py:TestCQL.cas_simple_test()
def testSimpleCas(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(tkn int, consumed boolean, PRIMARY KEY (tkn))") as table:
        for i in range(10):
            execute(cql, table, "INSERT INTO %s (tkn, consumed) VALUES (?, FALSE)", i)

            assertRows(execute(cql, table, "UPDATE %s SET consumed = TRUE WHERE tkn = ? IF consumed = ?", i, false), row(true, false) if is_scylla else row(true))
            assertRows(execute(cql, table, "UPDATE %s SET consumed = TRUE WHERE tkn = ? IF consumed = ?", i, false), row(false, true))


# Migrated from cql_tests.py:TestCQL.conditional_update_test()
def testConditionalUpdate(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v1 int, v2 text, v3 int)") as table:
        # ScyllaDB's and Cassandra's error message here are slightly different
        assertInvalidMessageRE(cql, table, "unset.*value",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = ?", unset())

        # Shouldn't apply

        assertRows(execute(cql, table, "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = ?", 4), row(false, None) if is_scylla else row(false))
        assertRows(execute(cql, table, "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 IN (?, ?)", 1, 2), row(false, None) if is_scylla else row(false))
        assertRows(execute(cql, table, "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 IN ?", [1, 2]), row(false, None) if is_scylla else row(false))
        assertRows(execute(cql, table, "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS"), row(false, None, None, None, None) if is_scylla else row(false))

        # Should apply
        assertRows(execute(cql, table, "INSERT INTO %s (k, v1, v2) VALUES (0, 2, 'foo') IF NOT EXISTS"), row(true, None, None, None, None) if is_scylla else row(true))

        # Shouldn't apply
        assertRows(execute(cql, table, "INSERT INTO %s (k, v1, v2) VALUES (0, 5, 'bar') IF NOT EXISTS"), row(false, 0, 2, "foo", null))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, 2, "foo", null))

        # Shouldn't apply
        assertRows(execute(cql, table, "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = ?", 4), row(false, 2))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, 2, "foo", null))

        # Should apply (note: we want v2 before v1 in the statement order to exercise #5786)
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 = ?", 2), row(true, 2) if is_scylla else row(true))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar', v1 = 2 WHERE k = 0 IF v1 IN (?, ?)", 2, 3), row(true, 3) if is_scylla else row(true))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 IN ?", [2, 3]), row(true, 2) if is_scylla else row(true))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 IN ?", [1, null, 3]), row(true, 3) if is_scylla else row(true))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF EXISTS"), row(true, 0, 3, 'bar', None) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, 3, "bar", null))

        # Shouldn't apply, only one condition is ok
        assertRows(execute(cql, table, "UPDATE %s SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = ? AND v2 = ?", 3, "foo"), row(false, 3, "bar"))
        assertRows(execute(cql, table, "UPDATE %s SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'foo'"), row(false, 3, "bar"))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, 3, "bar", null))

        # Should apply
        assertRows(execute(cql, table, "UPDATE %s SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = ? AND v2 = ?", 3, "bar"), row(true, 3, 'bar') if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, 5, "foobar", null))

        # Shouldn't apply
        assertRows(execute(cql, table, "DELETE v2 FROM %s WHERE k = 0 IF v1 = ?", 3), row(false, 5))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, 5, "foobar", null))

        # Shouldn't apply
        assertRows(execute(cql, table, "DELETE v2 FROM %s WHERE k = 0 IF v1 = ?", null), row(false, 5))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, 5, "foobar", null))

        # Should apply
        assertRows(execute(cql, table, "DELETE v2 FROM %s WHERE k = 0 IF v1 = ?", 5), row(true, 5) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, 5, null, null))

        # Shouldn't apply
        assertRows(execute(cql, table, "DELETE v1 FROM %s WHERE k = 0 IF v3 = ?", 4), row(false, null))

        # Should apply
        assertRows(execute(cql, table, "DELETE v1 FROM %s WHERE k = 0 IF v3 = ?", null), row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, null, null, null))

        # Should apply
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k = 0 IF v1 = ?", null), row(true, null) if is_scylla else row(true))
        assertEmpty(execute(cql, table, "SELECT * FROM %s"))

        # Shouldn't apply
        assertRows(execute(cql, table, "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS"), row(false, null, null, null, null) if is_scylla else row(false))

        # Should apply
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k = 0"))
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k = 0 IF v1 IN (?)", null), row(true, null) if is_scylla else row(true))

    with create_table(cql, test_keyspace, "(k int, c int, v1 text, PRIMARY KEY (k, c))") as table:
        assertInvalidMessage(cql, table, "IN on the clustering key columns is not supported with conditional updates",
                             "UPDATE %s SET v1 = 'A' WHERE k = 0 AND c IN () IF EXISTS")
        assertInvalidMessage(cql, table, "IN on the clustering key columns is not supported with conditional updates",
                             "UPDATE %s SET v1 = 'A' WHERE k = 0 AND c IN (1, 2) IF EXISTS")

        # Due to issue #13586, ScyllaDB is missing support for CONTAINS in
        # LWT expressions, so this check failed with a syntax error instead
        # of an error message. We have tests for this syntax already in
        # insert_update_if_condition_collections_test.py so let's not
        # repeat it here.
        #assertInvalidMessage(cql, table, "Cannot use CONTAINS on non-collection column v1", "UPDATE %s SET v1 = 'B' WHERE k = 0 IF v1 CONTAINS 'A'")

# Migrated from cql_tests.py:TestCQL.non_eq_conditional_update_test()
def testNonEqConditionalUpdate(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v1 int, v2 text, v3 int)") as table:
        # non-EQ conditions
        execute(cql, table, "INSERT INTO %s (k, v1, v2) VALUES (0, 2, 'foo')")

        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 < ?", 3), row(true, 2) if is_scylla else row(true))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 <= ?", 3), row(true, 2) if is_scylla else row(true))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 > ?", 1), row(true, 2) if is_scylla else row(true))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 >= ?", 1), row(true, 2) if is_scylla else row(true))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 != ?", 1), row(true, 2) if is_scylla else row(true))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 != ?", 2), row(false, 2))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN (?, ?, ?)", 0, 1, 2), row(true, 2) if is_scylla else row(true))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN ?", [142, 276]), row(false, 2))
        assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN ()"), row(false, 2))
        # Cassandra since commit https://github.com/apache/cassandra/commit/70e33d96e1f1236788afb50c1f02fbc64d760281
        # for CASSANDRA-12981, allows unset() values in IN and they are 
        # skipped. ScyllaDB doesn't allow this - see discussion in #13659.
        #assertRows(execute(cql, table, "UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN (?, ?)", unset(), 1), row(false, 2))

        # ScyllaDB and Cassandra have different error messages here
        assertInvalidMessageRE(cql, table, "unset.*value",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 < ?", unset())
        assertInvalidMessageRE(cql, table, "unset.*value",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 <= ?", unset())
        assertInvalidMessageRE(cql, table, "unset.*value",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 > ?", unset())
        assertInvalidMessageRE(cql, table, "unset.*value",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 >= ?", unset())
        assertInvalidMessageRE(cql, table, "unset.*value",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 != ?", unset())

# Migrated from cql_tests.py:TestCQL.conditional_delete_test()
def testConditionalDelete(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v1 int,)") as table:
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k=1 IF EXISTS"), row(false, null, null) if is_scylla else row(false))

        execute(cql, table, "INSERT INTO %s (k, v1) VALUES (1, 2)")
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k=1 IF EXISTS"), row(true, 1, 2) if is_scylla else row(true))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k=1"))
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k=1 IF EXISTS"), row(false, null, null) if is_scylla else row(false))

        execute(cql, table, "UPDATE %s USING TTL 1 SET v1=2 WHERE k=1")
        time.sleep(1.01)
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k=1 IF EXISTS"), row(false, null, null) if is_scylla else row(false))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k=1"))

        execute(cql, table, "INSERT INTO %s (k, v1) VALUES (2, 2) USING TTL 1")
        time.sleep(1.01)
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k=2 IF EXISTS"), row(false, null, null) if is_scylla else row(false))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k=2"))

        execute(cql, table, "INSERT INTO %s (k, v1) VALUES (3, 2)")
        assertRows(execute(cql, table, "DELETE v1 FROM %s WHERE k=3 IF EXISTS"), row(true, 3, 2) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k=3"), row(3, null))
        assertRows(execute(cql, table, "DELETE v1 FROM %s WHERE k=3 IF EXISTS"), row(true, 3, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k=3 IF EXISTS"), row(true, 3, null) if is_scylla else row(true))

        execute(cql, table, "INSERT INTO %s (k, v1) VALUES (4, 2)")
        execute(cql, table, "UPDATE %s USING TTL 1 SET v1=2 WHERE k=4")
        time.sleep(1.01)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k=4"), row(4, null))
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k=4 IF EXISTS"), row(true, 4, null) if is_scylla else row(true))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k=4"))

    # static columns
    with create_table(cql, test_keyspace, "(k text, s text static, i int, v text, PRIMARY KEY (k, i) )") as table:
        execute(cql, table, "INSERT INTO %s (k, s, i, v) VALUES ('k', 's', 0, 'v')")
        assertRows(execute(cql, table, "DELETE v FROM %s WHERE k='k' AND i=0 IF EXISTS"), row(true, 'k', 0, 's', 'v') if is_scylla else row(true))
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k='k' AND i=0 IF EXISTS"), row(true, 'k', 0, 's', null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row("k", null, "s", null))
        assertRows(execute(cql, table, "DELETE v FROM %s WHERE k='k' AND i=0 IF s = 'z'"), row(false, "s"))
        assertRows(execute(cql, table, "DELETE v FROM %s WHERE k='k' AND i=0 IF v = 'z'"), row(false, null) if is_scylla else row(false))
        # ScyllaDB returns the static column first, Cassandra returns
        # delete column first. Since both also say which columns they
        # return, neither is more correct than the other.
        assertRows(execute(cql, table, "DELETE v FROM %s WHERE k='k' AND i=0 IF v = 'z' AND s = 'z'"), row(false, "s", null) if is_scylla else row(false, null, "s"))
        assertRows(execute(cql, table, "DELETE v FROM %s WHERE k='k' AND i=0 IF EXISTS"), row(false, null, null, null, null) if is_scylla else row(false))
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k='k' AND i=0 IF EXISTS"), row(false, null, null, null, null) if is_scylla else row(false))

        # CASSANDRA-6430
        assertInvalidMessage(cql, table, "DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns",
                             "DELETE FROM %s WHERE k = 'k' IF EXISTS")
        assertInvalidMessage(cql, table, "DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns",
                             "DELETE FROM %s WHERE k = 'k' IF v = ?", "foo")
        # The error message in ScyllaDB and Cassandra are a bit different:
        assertInvalidMessageRE(cql, table, "[mM]issing.* k",
                             "DELETE FROM %s WHERE i = 0 IF EXISTS")

        assertInvalidMessage(cql, table, "Invalid INTEGER constant (0) for \"k\" of type text",
                             "DELETE FROM %s WHERE k = 0 AND i > 0 IF EXISTS")
        assertInvalidMessage(cql, table, "Invalid INTEGER constant (0) for \"k\" of type text",
                             "DELETE FROM %s WHERE k = 0 AND i > 0 IF v = 'foo'")
        assertInvalidMessage(cql, table, "DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns",
                             "DELETE FROM %s WHERE k = 'k' AND i > 0 IF EXISTS")
        assertInvalidMessage(cql, table, "DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns",
                             "DELETE FROM %s WHERE k = 'k' AND i > 0 IF v = 'foo'")
        assertInvalidMessage(cql, table, "IN on the clustering key columns is not supported with conditional deletions",
                             "DELETE FROM %s WHERE k = 'k' AND i IN (0, 1) IF v = 'foo'")
        assertInvalidMessage(cql, table, "IN on the clustering key columns is not supported with conditional deletions",
                             "DELETE FROM %s WHERE k = 'k' AND i IN () IF v = 'foo'")
        assertInvalidMessage(cql, table, "IN on the clustering key columns is not supported with conditional deletions",
                             "DELETE FROM %s WHERE k = 'k' AND i IN (0, 1) IF EXISTS")
        assertInvalidMessage(cql, table, "IN on the clustering key columns is not supported with conditional deletions",
                             "DELETE FROM %s WHERE k = 'k' AND i IN () IF EXISTS")

        # The error message in ScyllaDB and Cassandra are a bit different:
        assertInvalidMessageRE(cql, table, "unset.*value",
                             "DELETE FROM %s WHERE k = 'k' AND i = 0 IF v = ?", unset())

    with create_table(cql, test_keyspace, "(k int, s int static, i int, v text, PRIMARY KEY (k, i) )") as table:
        execute(cql, table, "INSERT INTO %s (k, s, i, v) VALUES ( 1, 1, 2, '1')")
        assertRows(execute(cql, table, "DELETE v FROM %s WHERE k = 1 AND i = 2 IF s != ?", 1), row(false, 1))
        assertRows(execute(cql, table, "DELETE v FROM %s WHERE k = 1 AND i = 2 IF s = ?", 1), row(true, 1) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k = 1 AND i = 2"), row(1, 2, 1, null))

        assertRows(execute(cql, table, "DELETE FROM %s WHERE  k = 1 AND i = 2 IF s != ?", 1), row(false, 1))
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k = 1 AND i = 2 IF s = ?", 1), row(true, 1) if is_scylla else row(true))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k = 1 AND i = 2"))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"), row(1, null, 1, null))

    with create_table(cql, test_keyspace, "(k int, i int, v1 int, v2 int, s int static, PRIMARY KEY (k, i) )") as table:
        execute(cql, table, "INSERT INTO %s (k, i, v1, v2, s) VALUES (?, ?, ?, ?, ?)",
                1, 1, 1, 1, 1)
        assertRows(execute(cql, table, "DELETE v1 FROM %s WHERE k = 1 AND i = 1 IF EXISTS"),
                   row(true, 1, 1, 1, 1, 1) if is_scylla else row(true))
        assertRows(execute(cql, table, "DELETE v2 FROM %s WHERE k = 1 AND i = 1 IF EXISTS"),
                   row(true, 1, 1, 1, null, 1) if is_scylla else row(true))
        assertRows(execute(cql, table, "DELETE FROM %s WHERE k = 1 AND i = 1 IF EXISTS"),
                   row(true, 1, 1, 1, null, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "select * from %s"),
                   row(1, null, 1, null, null))
        assertRows(execute(cql, table, "DELETE v1 FROM %s WHERE k = 1 AND i = 1 IF EXISTS"),
                   row(false, null, null, null, null, null) if is_scylla else row(false))
        assertRows(execute(cql, table, "DELETE v1 FROM %s WHERE k = 1 AND i = 1 IF s = 5"),
                   row(false, 1))
        assertRows(execute(cql, table, "DELETE v1 FROM %s WHERE k = 1 AND i = 1 IF v1 = 1 AND v2 = 1"),
                   row(false, null, null) if is_scylla else row(false))
        # ScyllaDB returns the static column first, Cassandra returns
        # deleted column first. Since both also say which columns they
        # return, neither is more correct than the other.
        assertRows(execute(cql, table, "DELETE v1 FROM %s WHERE k = 1 AND i = 1 IF v1 = 1 AND v2 = 1 AND s = 1"),
                   row(false, 1, null, null) if is_scylla else row(false, null, null, 1))
        assertRows(execute(cql, table, "DELETE v1 FROM %s WHERE k = 1 AND i = 5 IF s = 1"),
                   row(true, 1) if is_scylla else row(true))

# Migrated from cql_tests.py:TestCQL.cas_and_ttl_test()
def testCasAndTTL(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v int, lock boolean)") as table:
        execute(cql, table, "INSERT INTO %s (k, v, lock) VALUES (0, 0, false)")
        execute(cql, table, "UPDATE %s USING TTL 1 SET lock=true WHERE k=0")

        time.sleep(1.01)
        assertRows(execute(cql, table, "UPDATE %s SET v = 1 WHERE k = 0 IF lock = null"),
                   row(true, null) if is_scylla else row(true))

# Test for 7499,
# migrated from cql_tests.py:TestCQL.cas_and_list_index_test()
def testCasAndListIndex(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v text, l list<text>)") as table:
        execute(cql, table, "INSERT INTO %s (k, v, l) VALUES(0, 'foobar', ['foi', 'bar'])")

        assertRows(execute(cql, table, "UPDATE %s SET l[0] = 'foo' WHERE k = 0 IF v = 'barfoo'"), row(false, "foobar"))
        assertRows(execute(cql, table, "UPDATE %s SET l[0] = 'foo' WHERE k = 0 IF v = 'foobar'"), row(true, "foobar") if is_scylla else row(true))

        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, ["foo", "bar"], "foobar"))


# Migrated from cql_tests.py:TestCQL.conditional_ddl_keyspace_test()
def testDropCreateKeyspaceIfNotExists(cql, test_keyspace):
        keyspace = unique_name()
        # create and confirm
        execute(cql, test_keyspace, "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} and durable_writes = true ")
        assertRows(execute(cql, test_keyspace, "select durable_writes from system_schema.keyspaces where keyspace_name = ?",
                           keyspace),
                   row(true))

        # unsuccessful create since it's already there, confirm settings don't change
        execute(cql, test_keyspace, "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} and durable_writes = false ")

        assertRows(execute(cql, test_keyspace, "select durable_writes from system_schema.keyspaces where keyspace_name = ?",
                           keyspace),
                   row(true))

        # drop and confirm
        execute(cql, test_keyspace, "DROP KEYSPACE IF EXISTS " + keyspace)

        assertEmpty(execute(cql, test_keyspace, "select * from system_schema.keyspaces where keyspace_name = ?",
                            keyspace))

# Migrated from cql_tests.py:TestCQL.conditional_ddl_table_test()
def testDropCreateTableIfNotExists(cql, test_keyspace):
    tableName = unique_name()
    fullTableName = test_keyspace + '.' + tableName

    # try dropping when doesn't exist
    execute(cql, test_keyspace, "DROP TABLE IF EXISTS " + fullTableName)

     # create and confirm
    execute(cql, test_keyspace, "CREATE TABLE IF NOT EXISTS " + fullTableName + " (id text PRIMARY KEY, value1 blob) with comment = 'foo'")

    assertRows(execute(cql, test_keyspace, "select comment from system_schema.tables where keyspace_name = ? and table_name = ?", test_keyspace, tableName),
           row("foo"))

    # unsuccessful create since it's already there, confirm settings don't change
    execute(cql, test_keyspace, "CREATE TABLE IF NOT EXISTS " + fullTableName + " (id text PRIMARY KEY, value2 blob)with comment = 'bar'")

    assertRows(execute(cql, test_keyspace, "select comment from system_schema.tables where keyspace_name = ? and table_name = ?", test_keyspace, tableName),
           row("foo"))

    # drop and confirm
    execute(cql, test_keyspace, "DROP TABLE IF EXISTS " + fullTableName)

    assertEmpty(execute(cql, test_keyspace, "select * from system_schema.tables where keyspace_name = ? and table_name = ?", test_keyspace, tableName))

# Migrated from cql_tests.py:TestCQL.conditional_ddl_index_test()
def testDropCreateIndexIfNotExists(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY, value1 text, value2 blob) with comment = 'foo'") as table:
        # try dropping when doesn't exist
        cql.execute(f"DROP INDEX IF EXISTS {test_keyspace}.myindex")

        # create and confirm
        execute(cql, table, f"CREATE INDEX IF NOT EXISTS myindex ON %s (value1)")

        # unsuccessful create since it's already there
        execute(cql, table, "CREATE INDEX IF NOT EXISTS myindex ON %s (value1)")

        # drop and confirm
        execute(cql, test_keyspace, "DROP INDEX IF EXISTS %s.myindex")

        _, tableName = table.split('.')
        rows = getRows(execute(cql, table, "select index_name from system.\"IndexInfo\" where table_name = ?", tableName))
        assertEquals(0, len(rows))

# Migrated from cql_tests.py:TestCQL.conditional_ddl_type_test()
def testDropCreateTypeIfNotExists(cql, test_keyspace):
    mytype = unique_name()

    # try dropping when doesn't exist
    cql.execute(f"DROP TYPE IF EXISTS {test_keyspace}.{mytype}")

    # create and confirm
    cql.execute(f"CREATE TYPE IF NOT EXISTS {test_keyspace}.{mytype} (somefield int)")
    assertRows(execute(cql, test_keyspace, "SELECT type_name from system_schema.types where keyspace_name = ? and type_name = ?",
                           test_keyspace, mytype),
                   row(mytype))

    # unsuccessful create since it 's already there
    # TODO: confirm this create attempt doesn't alter type field from int to blob
    cql.execute(f"CREATE TYPE IF NOT EXISTS {test_keyspace}.{mytype} (somefield blob)")

    # drop and confirm
    cql.execute(f"DROP TYPE IF EXISTS {test_keyspace}.{mytype}")
    assertEmpty(execute(cql, test_keyspace, "SELECT type_name from system_schema.types where keyspace_name = ? and type_name = ?",
                            test_keyspace, mytype))

def testConditionalUpdatesWithNonExistingValues(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(a int, b int, s int static, d text, PRIMARY KEY (a, b))") as table:
        assertRows(execute(cql, table, "UPDATE %s SET s = 1 WHERE a = 1 IF s = NULL"),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT a, s, d FROM %s WHERE a = 1"),
                   row(1, 1, null))

        assertRows(execute(cql, table, "UPDATE %s SET s = 2 WHERE a = 2 IF s IN (10,20,NULL)"),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT a, s, d FROM %s WHERE a = 2"),
                   row(2, 2, null))

        assertRows(execute(cql, table, "UPDATE %s SET s = 4 WHERE a = 4 IF s != 4"),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT a, s, d FROM %s WHERE a = 4"),
                   row(4, 4, null))

        # rejected: IN doesn't contain null
        assertRows(execute(cql, table, "UPDATE %s SET s = 3 WHERE a = 3 IF s IN ?", [10,20,30]),
                   row(false, null) if is_scylla else row(false))
        assertEmpty(execute(cql, table, "SELECT a, s, d FROM %s WHERE a = 3"))

        # rejected: comparing number with NULL always returns false
        for operator in [">", "<", ">=", "<=", "="]:
            assertRows(execute(cql, table, "UPDATE %s SET s = 50 WHERE a = 5 IF s " + operator + " ?", 3),
                       row(false, null) if is_scylla else row(false))
            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = 5"))

def testConditionalUpdatesWithNullValues(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(a int, b int, s int static, d int, PRIMARY KEY (a, b))") as table:
        # pre-populate, leave out static column
        for i in range(1,6):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", i, 1)
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", i, 2)

        assertRows(execute(cql, table, "UPDATE %s SET s = 100 WHERE a = 1 IF s = NULL"),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT a, b, s, d FROM %s WHERE a = 1"),
                   row(1, 1, 100, null),
                   row(1, 2, 100, null))

        assertRows(execute(cql, table, "UPDATE %s SET s = 200 WHERE a = 2 IF s IN (10,20,NULL)"),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT a, b, s, d FROM %s WHERE a = 2"),
                   row(2, 1, 200, null),
                   row(2, 2, 200, null))

        # rejected: IN doesn't contain null
        assertRows(execute(cql, table, "UPDATE %s SET s = 30 WHERE a = 3 IF s IN ?", [10,20,30]),
                   row(false, null))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 3"),
                   row(3, 1, null, null),
                   row(3, 2, null, null))

        assertRows(execute(cql, table, "UPDATE %s SET s = 400 WHERE a = 4 IF s IN (10,20,NULL)"),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 4"),
                   row(4, 1, 400, null),
                   row(4, 2, 400, null))

        # rejected: comparing number with NULL always returns false
        for operator in [">", "<", ">=", "<=", "="]:
            assertRows(execute(cql, table, "UPDATE %s SET s = 50 WHERE a = 5 IF s " + operator + " 3"),
                       row(false, null))
            assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 5"),
                       row(5, 1, null, null),
                       row(5, 2, null, null))

        assertRows(execute(cql, table, "UPDATE %s SET s = 500 WHERE a = 5 IF s != 5"),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT a, b, s, d FROM %s WHERE a = 5"),
                   row(5, 1, 500, null),
                   row(5, 2, 500, null))

    # Similar test, although with two static columns to test limits
    with create_table(cql, test_keyspace, "(a int, b int, s1 int static, s2 int static, d int, PRIMARY KEY (a, b))") as table:
        for i in range(1,6):
            for j in range(5):
                execute(cql, table, "INSERT INTO %s (a, b, d) VALUES (?, ?, ?)", i, j, i + j)

        assertRows(execute(cql, table, "UPDATE %s SET s2 = 100 WHERE a = 1 IF s1 = NULL"),
                   row(true, null) if is_scylla else row(true))

        execute(cql, table, "INSERT INTO %s (a, b, s1) VALUES (?, ?, ?)", 2, 2, 2)
        assertRows(execute(cql, table, "UPDATE %s SET s1 = 100 WHERE a = 2 IF s2 = NULL"),
                   row(true, null) if is_scylla else row(true))

        execute(cql, table, "INSERT INTO %s (a, b, s1) VALUES (?, ?, ?)", 2, 2, 2)
        assertRows(execute(cql, table, "UPDATE %s SET s1 = 100 WHERE a = 2 IF s2 = NULL"),
                   row(true, null) if is_scylla else row(true))

def testConditionalUpdatesWithNullValuesWithBatch(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(a int, b int, s int static, d text, PRIMARY KEY (a, b))") as table:
        # pre-populate, leave out static column
        for i in range(1,7):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", i, i)

        # applied: null is indistiguishable from empty value, lwt condition is executed before INSERT
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, d) values (2, 2, 'a');\n"
                           + "UPDATE %s SET s = 2 WHERE a = 2 IF s = null;\n"
                           + "APPLY BATCH"),
            # See issue #27955 discussing this difference
            [row(true, 2, 2, null), row(true, 2, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 2"),
                   row(2, 2, 2, "a"))

        # rejected: comparing number with null value always returns false
        for operator in [">", "<", ">=", "<=", "="]:
            assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                               + "INSERT INTO %s (a, b, s, d) values (3, 3, 40, 'a');\n"
                               + "UPDATE %s SET s = 30 WHERE a = 3 IF s " + operator + " 5;\n"
                               + "APPLY BATCH"),
                # See issue #27955 discussing this difference
                [row(false, 3, 3, null), row(false, 3, null, null)] if is_scylla else [row(false, 3, 3, null)])
            assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 3"),
                       row(3, 3, null, null))

        # applied: lwt condition is executed before INSERT, update is applied after it
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s, d) values (4, 4, 4, 'a');\n"
                           + "UPDATE %s SET s = 5 WHERE a = 4 IF s = null;\n"
                           + "APPLY BATCH"),
            # See issue #27955 discussing this difference
            [row(true, 4, 4, null), row(true, 4, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 4"),
                   row(4, 4, 5, "a"))

        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s, d) values (5, 5, 5, 'a');\n"
                           + "UPDATE %s SET s = 6 WHERE a = 5 IF s IN (1,2,null);\n"
                           + "APPLY BATCH"),
            # See issue #27955 discussing this difference
            [row(true, 5, 5, null), row(true, 5, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 5"),
                   row(5, 5, 6, "a"))

        # rejected: IN doesn't contain null
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s, d) values (6, 6, 70, 'a');\n"
                           + "UPDATE %s SET s = 60 WHERE a = 6 IF s IN (1,2,3);\n"
                           + "APPLY BATCH"),
            # See issue #27955 discussing this difference
            [row(false, 6, 6, null), row(false, 6, null, null)] if is_scylla else [row(false, 6, 6, null)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 6"),
                   row(6, 6, null, null))

def testConditionalUpdatesWithNonExistingValuesWithBatch(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(a int, b int, s int static, d text, PRIMARY KEY (a, b))") as table:
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, d) values (2, 2, 'a');\n"
                           + "UPDATE %s SET s = 2 WHERE a = 2 IF s = null;\n"
                           + "APPLY BATCH"),
            # See issue #27955 discussing this difference
            [row(true, null, null, null), row(true, null, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 2"),
                   row(2, 2, 2, "a"))

        # applied: lwt condition is executed before INSERT, update is applied after it
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s, d) values (4, 4, 4, 'a');\n"
                           + "UPDATE %s SET s = 5 WHERE a = 4 IF s = null;\n"
                           + "APPLY BATCH"),
            # See issue #27955 discussing this difference
            [row(true, null, null, null), row(true, null, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 4"),
                   row(4, 4, 5, "a")); # Note that the update wins because 5 > 4 (we have a timestamp tie, so values are used)

        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s, d) values (5, 5, 5, 'a');\n"
                           + "UPDATE %s SET s = 6 WHERE a = 5 IF s IN (1,2,null);\n"
                           + "APPLY BATCH"),
            # See issue #27955 discussing this difference
            [row(true, null, null, null), row(true, null, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 5"),
                   row(5, 5, 6, "a")); # Same as above

        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s, d) values (7, 7, 7, 'a');\n"
                           + "UPDATE %s SET s = 8 WHERE a = 7 IF s != 7;\n"
                           + "APPLY BATCH"),
            # See issue #27955 discussing this difference
            [row(true, null, null, null), row(true, null, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 7"),
                   row(7, 7, 8, "a")); # Same as above

        # rejected: comparing number with non-existing value always returns false
        for operator in [">", "<", ">=", "<=", "="]:
            assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                               + "INSERT INTO %s (a, b, s, d) values (3, 3, 3, 'a');\n"
                               + "UPDATE %s SET s = 3 WHERE a = 3 IF s " + operator + " 5;\n"
                               + "APPLY BATCH"),
                # See issue #27955 discussing this difference
                [row(false, null, null, null), row(false, null, null, null)] if is_scylla else [row(false)])
            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = 3"))

        # rejected: IN doesn't contain null
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s, d) values (6, 6, 6, 'a');\n"
                           + "UPDATE %s SET s = 7 WHERE a = 6 IF s IN (1,2,3);\n"
                           + "APPLY BATCH"),
            # See issue #27955 discussing this difference
            [row(false, null, null, null), row(false, null, null, null)] if is_scylla else [row(false)])
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = 6"))

def testConditionalDeleteWithNullValues(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b))") as table:
        for i in range(1, 6):
            execute(cql, table, "INSERT INTO %s (a, b, s1, s2, v) VALUES (?, ?, ?, ?, ?)", i, i, i, null, i)

        assertRows(execute(cql, table, "DELETE s1 FROM %s WHERE a = 1 IF s2 = ?", null),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 1"),
                   row(1, 1, null, null, 1))

        # rejected: IN doesn't contain null
        assertRows(execute(cql, table, "DELETE s1 FROM %s WHERE a = 2 IF s2 IN ?", [10,20,30]),
                   row(false, null))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 2"),
                   row(2, 2, 2, null, 2))

        assertRows(execute(cql, table, "DELETE s1 FROM %s WHERE a = 3 IF s2 IN (?, ?, ?)", null, 20, 30),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 3"),
                   row(3, 3, null, null, 3))

        assertRows(execute(cql, table, "DELETE s1 FROM %s WHERE a = 4 IF s2 != ?", 4),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 4"),
                   row(4, 4, null, null, 4))

        # rejected: comparing number with NULL always returns false
        for operator in [">", "<", ">=", "<=", "="]:
            assertRows(execute(cql, table, "DELETE s1 FROM %s WHERE a = 5 IF s2 " + operator + " ?", 3),
                       row(false, null))
            assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 5"),
                       row(5, 5, 5, null, 5))

def testConditionalDeletesWithNonExistingValuesWithBatch(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b))") as table:
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s1, v) values (2, 2, 2, 2);\n"
                           + "DELETE s1 FROM %s WHERE a = 2 IF s2 = null;\n"
                           + "APPLY BATCH"),
            # See issue #27955 discussing this difference
           [row(true, null, null, null), row(true, null, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 2"),
                   row(2, 2, null, null, 2))

        # rejected: comparing number with non-existing value always returns false
        for operator in [">", "<", ">=", "<=", "="]:
            assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                               + "INSERT INTO %s (a, b, s1, v) values (3, 3, 3, 3);\n"
                               + "DELETE s1 FROM %s WHERE a = 3 IF s2 " + operator + " 5;\n"
                               + "APPLY BATCH"),
                # See issue #27955 discussing this difference
                [row(false, null, null, null), row(false, null, null, null)] if is_scylla else [row(false)])
            assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = 3"))

        # rejected: IN doesn't contain null
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s1, v) values (6, 6, 6, 6);\n"
                           + "DELETE s1 FROM %s WHERE a = 6 IF s2 IN (1,2,3);\n"
                           + "APPLY BATCH"),
                # See issue #27955 discussing this difference
                [row(false, null, null, null), row(false, null, null, null)] if is_scylla else [row(false)])
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = 6"))

        # Note that on equal timestamp, tombstone wins so the DELETE wins
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s1, v) values (4, 4, 4, 4);\n"
                           + "DELETE s1 FROM %s WHERE a = 4 IF s2 = null;\n"
                           + "APPLY BATCH"),
                # See issue #27955 discussing this difference
                [row(true, null, null, null), row(true, null, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 4"),
                   row(4, 4, null, null, 4))

        # Note that on equal timestamp, tombstone wins so the DELETE wins
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s1, v) VALUES (5, 5, 5, 5);\n"
                           + "DELETE s1 FROM %s WHERE a = 5 IF s1 IN (1,2,null);\n"
                           + "APPLY BATCH"),
                # See issue #27955 discussing this difference
                [row(true, null, null, null), row(true, null, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 5"),
                   row(5, 5, null, null, 5))

        # Note that on equal timestamp, tombstone wins so the DELETE wins
        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "INSERT INTO %s (a, b, s1, v) values (7, 7, 7, 7);\n"
                           + "DELETE s1 FROM %s WHERE a = 7 IF s2 != 7;\n"
                           + "APPLY BATCH"),
                # See issue #27955 discussing this difference
                [row(true, null, null, null), row(true, null, null, null)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 7"),
                   row(7, 7, null, null, 7))

# Test for CASSANDRA-12060, using a table without clustering.
def testMultiExistConditionOnSameRowNoClustering(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v1 text, v2 text)") as table:
        ## Multiple inserts on the same row with not exist conditions
        assert_rows_list(execute(cql, table, "BEGIN BATCH "
                           + "INSERT INTO %s (k, v1) values (0, 'foo') IF NOT EXISTS; "
                           + "INSERT INTO %s (k, v2) values (0, 'bar') IF NOT EXISTS; "
                           + "APPLY BATCH"),
                   [row(true, null, null, null), row(true, null, null, null)] if is_scylla else [row(true)])

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k = 0"), row(0, "foo", "bar"))

        # Same, but both insert on the same column: doing so would almost surely be a user error, but that's the
        # original case reported in #12867, so being thorough.
        assert_rows_list(execute(cql, table, "BEGIN BATCH "
                           + "INSERT INTO %s (k, v1) values (1, 'foo') IF NOT EXISTS; "
                           + "INSERT INTO %s (k, v1) values (1, 'bar') IF NOT EXISTS; "
                           + "APPLY BATCH"),
                   [row(true, null, null, null), row(true, null, null, null)] if is_scylla else [row(true)])

        # As all statement gets the same timestamp, the biggest value ends up winning, so that's "foo"
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"), row(1, "foo", null))

        # Multiple deletes on the same row with exists conditions (note that this is somewhat none-sensical, one of the
        # deletes is redundant, we're just checking it doesn't break something)
        assert_rows_list(execute(cql, table, "BEGIN BATCH "
                           + "DELETE FROM %s WHERE k = 0 IF EXISTS; "
                           + "DELETE FROM %s WHERE k = 0 IF EXISTS; "
                           + "APPLY BATCH"),
                   [row(true, 0, 'foo', 'bar'), row(true, 0, 'foo', 'bar')] if is_scylla else [row(true)])

        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k = 0"))

        # Validate we can't mix different type of conditions however
        # Scylla does allow these things, as explained in lwt-differences.rst
        #assertInvalidMessage(cql, table, "Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row",
        #                     "BEGIN BATCH "
        #                   + "INSERT INTO %s (k, v1) values (1, 'foo') IF NOT EXISTS; "
        #                   + "DELETE FROM %s WHERE k = 1 IF EXISTS; "
        #                   + "APPLY BATCH")

        #assertInvalidMessage(cql, table, "Cannot mix IF conditions and IF NOT EXISTS for the same row",
        #                     "BEGIN BATCH "
        #                     + "INSERT INTO %s (k, v1) values (1, 'foo') IF NOT EXISTS; "
        #                     + "UPDATE %s SET v2 = 'bar' WHERE k = 1 IF v1 = 'foo'; "
        #                     + "APPLY BATCH")

# Test for CASSANDRA-12060, using a table with clustering.
def testMultiExistConditionOnSameRowClustering(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(k int, t int, v1 text, v2 text, PRIMARY KEY (k,t))") as table:
        # Multiple inserts on the same row with not exist conditions
        assert_rows_list(execute(cql, table, "BEGIN BATCH "
                           + "INSERT INTO %s (k, t, v1) values (0, 0, 'foo') IF NOT EXISTS; "
                           + "INSERT INTO %s (k, t, v2) values (0, 0, 'bar') IF NOT EXISTS; "
                           + "APPLY BATCH"),
                   [row(true, null, null, null, null), row(true, null, null, null, null)] if is_scylla else [row(true)])

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k = 0"), row(0, 0, "foo", "bar"))

        # Same, but both insert on the same column: doing so would almost surely be a user error, but that's the
        # original case reported in #12867, so being thorough.
        assert_rows_list(execute(cql, table, "BEGIN BATCH "
                           + "INSERT INTO %s (k, t, v1) values (1, 0, 'foo') IF NOT EXISTS; "
                           + "INSERT INTO %s (k, t, v1) values (1, 0, 'bar') IF NOT EXISTS; "
                           + "APPLY BATCH"),
                   [row(true, null, null, null, null), row(true, null, null, null, null)] if is_scylla else [row(true)])

        # As all statement gets the same timestamp, the biggest value ends up winning, so that's "foo"
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"), row(1, 0, "foo", null))

        # Multiple deletes on the same row with exists conditions (note that this is somewhat none-sensical, one of the
        # deletes is redundant, we're just checking it doesn't break something)
        assert_rows_list(execute(cql, table, "BEGIN BATCH "
                           + "DELETE FROM %s WHERE k = 0 AND t = 0 IF EXISTS; "
                           + "DELETE FROM %s WHERE k = 0 AND t = 0 IF EXISTS; "
                           + "APPLY BATCH"),
                   [row(true, 0, 0, 'foo', 'bar'), row(true, 0, 0, 'foo', 'bar')] if is_scylla else [row(true)])

        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE k = 0"))

        # Validate we can't mix different type of conditions however
        # Scylla does allow these things, as explained in lwt-differences.rst
        #assertInvalidMessage(cql, table, "Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row",
        #                     "BEGIN BATCH "
        #                     + "INSERT INTO %s (k, t, v1) values (1, 0, 'foo') IF NOT EXISTS; "
        #                     + "DELETE FROM %s WHERE k = 1 AND t = 0 IF EXISTS; "
        #                     + "APPLY BATCH")

        #assertInvalidMessage(cql, table, "Cannot mix IF conditions and IF NOT EXISTS for the same row",
        #                     "BEGIN BATCH "
        #                     + "INSERT INTO %s (k, t, v1) values (1, 0, 'foo') IF NOT EXISTS; "
        #                     + "UPDATE %s SET v2 = 'bar' WHERE k = 1 AND t = 0 IF v1 = 'foo'; "
        #                     + "APPLY BATCH")

def testConditionalOnDurationColumns(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v int, d duration)") as table:
        # Cassandra and Scylla print different messages: Cassandra prints
        # "Slice conditions ( > ) are not supported on durations", Scylla
        # prints "Duration type is unordered for d".
        assertInvalidMessageRE(cql, table, r"Slice conditions \( > \) are not supported on durations|Duration type is unordered for d",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d > 1s")
        assertInvalidMessageRE(cql, table, r"Slice conditions \( >= \) are not supported on durations|Duration type is unordered for d",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d >= 1s")
        assertInvalidMessageRE(cql, table, r"Slice conditions \( <= \) are not supported on durations|Duration type is unordered for d",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d <= 1s")
        assertInvalidMessageRE(cql, table, r"Slice conditions \( < \) are not supported on durations|Duration type is unordered for d",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d < 1s")

        execute(cql, table, "INSERT INTO %s (k, v, d) VALUES (1, 1, 2s)")

        assertRows(execute(cql, table, "UPDATE %s SET v = 4 WHERE k = 1 IF d = 1s"), row(false, Duration(0, 0, 2*s)))
        assertRows(execute(cql, table, "UPDATE %s SET v = 3 WHERE k = 1 IF d = 2s"), row(true, Duration(0, 0, 2*s)) if is_scylla else row(true))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"), row(1, Duration(0, 0, 2*s), 3))

        assertRows(execute(cql, table, "UPDATE %s SET d = 10s WHERE k = 1 IF d != 2s"), row(false, Duration(0, 0, 2*s)))
        assertRows(execute(cql, table, "UPDATE %s SET v = 6 WHERE k = 1 IF d != 1s"), row(true, Duration(0, 0, 2*s)) if is_scylla else row(true))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"), row(1, Duration(0, 0, 2*s), 6))

        assertRows(execute(cql, table, "UPDATE %s SET v = 5 WHERE k = 1 IF d IN (1s, 5s)"), row(false, Duration(0, 0, 2*s)))
        assertRows(execute(cql, table, "UPDATE %s SET d = 10s WHERE k = 1 IF d IN (1s, 2s)"), row(true, Duration(0, 0, 2*s)) if is_scylla else row(true))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k = 1"), row(1, Duration(0, 0, 10*s), 6))
