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

# The original Java test class is parameterized to run each test with each of
# the CQL protocol versions which Cassandra supports. We use only the Python
# driver's default protocol version.

def flushIf(cql, keyspace, flush):
    if flush:
        nodetool.flush_keyspace(cql, keyspace)

def testCommutativeRowDeletionFlush(cql, test_keyspace):
    # CASSANDRA-13409
    commutativeRowDeletion(cql, test_keyspace, True)

def testCommutativeRowDeletionWithoutFlush(cql, test_keyspace):
    # CASSANDRA-13409
    commutativeRowDeletion(cql, test_keyspace, False)

def commutativeRowDeletion(cql, test_keyspace, flush):
    # CASSANDRA-13409 new update should not resurrect previous deleted data in view
    with create_table(cql, test_keyspace, "(p int primary key, v1 int, v2 int)") as table:
        with create_view(cql, table, "create materialized view %s as select * from %s " +
                                     "where p is not null and v1 is not null primary key (v1, p)") as view, \
             nodetool.no_autocompaction_context(cql, view):

            # sstable-1, Set initial values TS=1
            execute(cql, table, "Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 1;")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v2, WRITETIME(v2) from %s WHERE v1 = ? AND p = ?", 1, 3), row(3, 1))
            # sstable-2
            execute(cql, table, "Delete from %s using timestamp 2 where p = 3;")
            flushIf(cql, test_keyspace, flush)

            assert_empty(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s"))
            # sstable-3
            execute(cql, table, "Insert into %s (p, v1) values (3, 1) using timestamp 3;")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, None, None))
            # sstable-4
            execute(cql, table, "UPdate %s using timestamp 4 set v1 = 2 where p = 3;")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(2, 3, None, None))
            # sstable-5
            execute(cql, table, "UPdate %s using timestamp 5 set v1 = 1 where p = 3;")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, None, None))

            # The Java test now compacts only the view's sstables 2, 4 and 5,
            # with Cassandra's internal forceUserDefinedCompaction(), and
            # checks that 3 sstables remain. We can't compact specific
            # sstables through CQL or the REST API, so this step was not
            # translated.

            # regular tombstone should be retained after compaction
            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, None, None))

def testComplexTimestampDeletionTestWithFlush(cql, test_keyspace):
    complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(cql, test_keyspace, True)
    complexTimestampWithbasePKColumnsInViewPKDeletionTest(cql, test_keyspace, True)

def testComplexTimestampDeletionTestWithoutFlush(cql, test_keyspace):
    complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(cql, test_keyspace, False)
    complexTimestampWithbasePKColumnsInViewPKDeletionTest(cql, test_keyspace, False)

def complexTimestampWithbasePKColumnsInViewPKDeletionTest(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(p1 int, p2 int, v1 int, v2 int, primary key(p1, p2))") as table:
        with create_view(cql, table, "create materialized view %s as select * from %s " +
                                     "where p1 is not null and p2 is not null primary key (p2, p1)") as view, \
             nodetool.no_autocompaction_context(cql, view):

            # Set initial values TS=1
            execute(cql, table, "Insert into %s (p1, p2, v1, v2) values (1, 2, 3, 4) using timestamp 1;")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, v2, WRITETIME(v2) from %s WHERE p1 = ? AND p2 = ?", 1, 2),
                                       row(3, 4, 1))
            # remove row/mv TS=2
            execute(cql, table, "Delete from %s using timestamp 2 where p1 = 1 and p2 = 2;")
            flushIf(cql, test_keyspace, flush)
            # view are empty
            assert_empty(execute(cql, view, "SELECT * FROM %s"))
            # insert PK with TS=3
            execute(cql, table, "Insert into %s (p1, p2) values (1, 2) using timestamp 3;")
            flushIf(cql, test_keyspace, flush)
            # deleted column in MV remained dead
            assert_rows_ignoring_order(execute(cql, view, "SELECT * FROM %s"), row(2, 1, None, None))

            nodetool.compact(cql, view)
            assert_rows_ignoring_order(execute(cql, view, "SELECT * FROM %s"), row(2, 1, None, None))

            # reset values
            execute(cql, table, "Insert into %s (p1, p2, v1, v2) values (1, 2, 3, 4) using timestamp 10;")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, v2, WRITETIME(v2) from %s WHERE p1 = ? AND p2 = ?", 1, 2),
                                       row(3, 4, 10))

            execute(cql, table, "UPDATE %s using timestamp 20 SET v2 = 5 WHERE p1 = 1 and p2 = 2")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, v2, WRITETIME(v2) from %s WHERE p1 = ? AND p2 = ?", 1, 2),
                                       row(3, 5, 20))

            execute(cql, table, "DELETE FROM %s using timestamp 10 WHERE p1 = 1 and p2 = 2")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, v2, WRITETIME(v2) from %s WHERE p1 = ? AND p2 = ?", 1, 2),
                                       row(None, 5, 20))

def complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(p int primary key, v1 int, v2 int)") as table:
        with create_view(cql, table, "create materialized view %s as select * from %s " +
                                     "where p is not null and v1 is not null primary key (v1, p)") as view, \
             nodetool.no_autocompaction_context(cql, view):

            # Set initial values TS=1
            execute(cql, table, "Insert into %s (p, v1, v2) values (3, 1, 5) using timestamp 1;")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v2, WRITETIME(v2) from %s WHERE v1 = ? AND p = ?", 1, 3), row(5, 1))
            # remove row/mv TS=2
            execute(cql, table, "Delete from %s using timestamp 2 where p = 3;")
            flushIf(cql, test_keyspace, flush)
            # view are empty
            assert_empty(execute(cql, view, "SELECT * FROM %s"))
            # insert PK with TS=3
            execute(cql, table, "Insert into %s (p, v1) values (3, 1) using timestamp 3;")
            flushIf(cql, test_keyspace, flush)
            # deleted column in MV remained dead
            assert_rows_ignoring_order(execute(cql, view, "SELECT * FROM %s"), row(1, 3, None))

            # insert values TS=2, it should be considered dead due to previous tombstone
            execute(cql, table, "Insert into %s (p, v1, v2) values (3, 1, 5) using timestamp 2;")
            flushIf(cql, test_keyspace, flush)
            # deleted column in MV remained dead
            assert_rows_ignoring_order(execute(cql, view, "SELECT * FROM %s"), row(1, 3, None))
            assert_rows_ignoring_order(execute(cql, view, "SELECT * from %s limit 1"), row(1, 3, None))

            # insert values TS=2, it should be considered dead due to previous tombstone
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET v2 = ? WHERE p = ?", 4, 3)
            flushIf(cql, test_keyspace, flush)

            assert_rows(execute(cql, table, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, 4, 3))

            nodetool.compact(cql, view)
            assert_rows(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, 4, 3))
            assert_rows(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s limit 1"), row(1, 3, 4, 3))

# The test testNoBatchlogCleanupForLocalMutations was not translated, because
# it checks the number of sstables of Cassandra's internal system.batches
# table, which can't be checked through CQL.
