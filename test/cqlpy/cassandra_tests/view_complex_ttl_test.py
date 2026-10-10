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
from ..test_materialized_view_old import clock

# The original Java test class is parameterized to run each test with each of
# the CQL protocol versions which Cassandra supports. We use only the Python
# driver's default protocol version.

# Translation of ViewAbstractParameterizedTest.updateViewWithFlush(). The
# Java updateView() waits for asynchronous view updates; on a single node
# both Scylla and Cassandra apply them synchronously, so we just execute().
def updateViewWithFlush(cql, table, query, flush, *args):
    execute(cql, table, query, *args)
    if flush:
        nodetool.flush_keyspace(cql, table.split('.')[0])

def testUpdateColumnInViewPKWithTTLWithFlush(cql, test_keyspace, clock):
    # CASSANDRA-13657
    updateColumnInViewPKWithTTL(cql, test_keyspace, clock, True)

def testUpdateColumnInViewPKWithTTLWithoutFlush(cql, test_keyspace, clock):
    # CASSANDRA-13657
    updateColumnInViewPKWithTTL(cql, test_keyspace, clock, False)

def updateColumnInViewPKWithTTL(cql, test_keyspace, clock, flush):
    # CASSANDRA-13657 if base column used in view pk is ttled, then view row is considered dead
    with create_table(cql, test_keyspace, "(k int primary key, a int, b int)") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)") as view, \
             nodetool.no_autocompaction_context(cql, view):

            updateViewWithFlush(cql, table, "UPDATE %s SET a = 1 WHERE k = 1;", flush)

            assert_rows(execute(cql, table, "SELECT * from %s"), row(1, 1, None))
            assert_rows(execute(cql, view, "SELECT * from %s"), row(1, 1, None))

            updateViewWithFlush(cql, table, "DELETE a FROM %s WHERE k = 1", flush)

            assert_empty(execute(cql, table, "SELECT * from %s"))
            assert_empty(execute(cql, view, "SELECT * from %s"))

            updateViewWithFlush(cql, table, "INSERT INTO %s (k) VALUES (1);", flush)

            assert_rows(execute(cql, table, "SELECT * from %s"), row(1, None, None))
            assert_empty(execute(cql, view, "SELECT * from %s"))

            updateViewWithFlush(cql, table, "UPDATE %s USING TTL 5 SET a = 10 WHERE k = 1;", flush)

            assert_rows(execute(cql, table, "SELECT * from %s"), row(1, 10, None))
            assert_rows(execute(cql, view, "SELECT * from %s"), row(10, 1, None))

            updateViewWithFlush(cql, table, "UPDATE %s SET b = 100 WHERE k = 1;", flush)

            assert_rows(execute(cql, table, "SELECT * from %s"), row(1, 10, 100))
            assert_rows(execute(cql, view, "SELECT * from %s"), row(10, 1, 100))

            # The Java test sleeps 5000 ms here.
            clock.jump(6)

            # 'a' is TTL of 5 and removed.
            assert_rows(execute(cql, table, "SELECT * from %s"), row(1, None, 100))
            assert_empty(execute(cql, view, "SELECT * from %s"))
            assert_empty(execute(cql, view, "SELECT * from %s WHERE k = ? AND a = ?", 1, 10))

            updateViewWithFlush(cql, table, "DELETE b FROM %s WHERE k=1", flush)

            assert_rows(execute(cql, table, "SELECT * from %s"), row(1, None, None))
            assert_empty(execute(cql, view, "SELECT * from %s"))

            updateViewWithFlush(cql, table, "DELETE FROM %s WHERE k=1;", flush)

            assert_empty(execute(cql, table, "SELECT * from %s"))
            assert_empty(execute(cql, view, "SELECT * from %s"))

def testUnselectedColumnsTTLWithFlush(cql, test_keyspace, clock):
    # CASSANDRA-13127
    unselectedColumnsTTL(cql, test_keyspace, clock, True)

def testUnselectedColumnsTTLWithoutFlush(cql, test_keyspace, clock):
    # CASSANDRA-13127
    unselectedColumnsTTL(cql, test_keyspace, clock, False)

def unselectedColumnsTTL(cql, test_keyspace, clock, flush):
    # CASSANDRA-13127 not ttled unselected column in base should keep view row alive
    with create_table(cql, test_keyspace, "(p int, c int, v int, primary key(p, c))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT p, c FROM %s " +
                                     "WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)") as view, \
             nodetool.no_autocompaction_context(cql, view):

            updateViewWithFlush(cql, table, "INSERT INTO %s (p, c) VALUES (0, 0) USING TTL 3;", flush)

            updateViewWithFlush(cql, table, "UPDATE %s USING TTL 1000 SET v = 0 WHERE p = 0 and c = 0;", flush)

            assert_rows_ignoring_order(execute(cql, view, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0))

            # The Java test sleeps 3000 ms here.
            clock.jump(4)

            r = execute(cql, table, "SELECT v, ttl(v) from %s WHERE c = ? AND p = ?", 0, 0).one()
            assert r[0] == 0, "row should have value of 0"
            assert r[1] < 1000, "row should have ttl less than 1000"
            assert_rows_ignoring_order(execute(cql, view, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0))

            updateViewWithFlush(cql, table, "DELETE FROM %s WHERE p = 0 and c = 0;", flush)
            assert_empty(execute(cql, view, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))

            updateViewWithFlush(cql, table, "INSERT INTO %s (p, c) VALUES (0, 0) ", flush)
            assert_rows_ignoring_order(execute(cql, view, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0))

            # already have a live row, no need to apply the unselected cell ttl
            updateViewWithFlush(cql, table, "UPDATE %s USING TTL 3 SET v = 0 WHERE p = 0 and c = 0;", flush)
            assert_rows_ignoring_order(execute(cql, view, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0))

            updateViewWithFlush(cql, table, "INSERT INTO %s (p, c) VALUES (1, 1) USING TTL 3", flush)
            assert_rows_ignoring_order(execute(cql, view, "SELECT * from %s WHERE c = ? AND p = ?", 1, 1), row(1, 1))

            # The Java test sleeps 4000 ms here.
            clock.jump(4)

            assert_rows_ignoring_order(execute(cql, view, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0))
            assert_empty(execute(cql, view, "SELECT * from %s WHERE c = ? AND p = ?", 1, 1))

            # unselected should keep view row alive
            updateViewWithFlush(cql, table, "UPDATE %s SET v = 0 WHERE p = 1 and c = 1;", flush)
            assert_rows_ignoring_order(execute(cql, view, "SELECT * from %s WHERE c = ? AND p = ?", 1, 1), row(1, 1))

def testBaseTTLWithSameTimestampTest(cql, test_keyspace, clock):
    # CASSANDRA-13127 when liveness timestamp tie, greater localDeletionTime should win if both are expiring.
    with create_table(cql, test_keyspace, "(p int, c int, v int, primary key(p, c))") as table:
        execute(cql, table, "INSERT INTO %s (p, c, v) VALUES (0, 0, 0) using timestamp 1;")

        nodetool.flush_keyspace(cql, test_keyspace)

        execute(cql, table, "INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING TTL 3 and timestamp 1;")

        nodetool.flush_keyspace(cql, test_keyspace)

        # The Java test sleeps 4000 ms here.
        clock.jump(4)

        assert_empty(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))

        # reversed order
        execute(cql, table, "truncate %s;")

        execute(cql, table, "INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING TTL 3 and timestamp 1;")

        nodetool.flush_keyspace(cql, test_keyspace)

        execute(cql, table, "INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING timestamp 1;")

        nodetool.flush_keyspace(cql, test_keyspace)

        # The Java test sleeps 4000 ms here.
        clock.jump(4)

        assert_empty(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))
