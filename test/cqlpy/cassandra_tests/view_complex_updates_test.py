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

# Cassandra's error message for dropping a base column of a view is "Cannot
# drop column v on base table t with materialized views". Scylla's is
# "Cannot drop column v from base table ks.t: materialized view mv needs this
# column".
DROP_COLUMN_ERROR = "Cannot drop column {} (on|from) base table"

def flushIf(cql, keyspace, flush):
    if flush:
        nodetool.flush_keyspace(cql, keyspace)

def testUpdateColumnNotInViewWithFlush(cql, test_keyspace, clock):
    updateColumnNotInView(cql, test_keyspace, clock, True)

def testUpdateColumnNotInViewWithoutFlush(cql, test_keyspace, clock):
    # CASSANDRA-13127
    updateColumnNotInView(cql, test_keyspace, clock, False)

def updateColumnNotInView(cql, test_keyspace, clock, flush):
    # CASSANDRA-13127: if base column not selected in view are alive, then pk of view row should be alive
    with create_table(cql, test_keyspace, "(p int, c int, v1 int, v2 int, primary key(p, c))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT p, c from %s " +
                                     "WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)") as mv, \
             nodetool.no_autocompaction_context(cql, mv):

            execute(cql, table, "UPDATE %s USING TIMESTAMP 0 SET v1 = 1 WHERE p = 0 AND c = 0")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, 1, None))
            assert_rows_ignoring_order(execute(cql, mv, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0))

            execute(cql, table, "DELETE v1 FROM %s USING TIMESTAMP 1 WHERE p = 0 AND c = 0")
            flushIf(cql, test_keyspace, flush)

            assert_empty(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))
            assert_empty(execute(cql, mv, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))

            # shadowed by tombstone
            execute(cql, table, "UPDATE %s USING TIMESTAMP 1 SET v1 = 1 WHERE p = 0 AND c = 0")
            flushIf(cql, test_keyspace, flush)

            assert_empty(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))
            assert_empty(execute(cql, mv, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))

            execute(cql, table, "UPDATE %s USING TIMESTAMP 2 SET v2 = 1 WHERE p = 0 AND c = 0")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, None, 1))
            assert_rows_ignoring_order(execute(cql, mv, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0))

            execute(cql, table, "DELETE v1 FROM %s USING TIMESTAMP 3 WHERE p = 0 AND c = 0")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, None, 1))
            assert_rows_ignoring_order(execute(cql, mv, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0))

            execute(cql, table, "DELETE v2 FROM %s USING TIMESTAMP 4 WHERE p = 0 AND c = 0")
            flushIf(cql, test_keyspace, flush)

            assert_empty(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))
            assert_empty(execute(cql, mv, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))

            execute(cql, table, "UPDATE %s USING TTL 3 SET v2 = 1 WHERE p = 0 AND c = 0")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, None, 1))
            assert_rows_ignoring_order(execute(cql, mv, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0))

            # The Java test sleeps 3 seconds here.
            clock.jump(3)

            assert_empty(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))
            assert_empty(execute(cql, mv, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0))

            execute(cql, table, "UPDATE %s SET v2 = 1 WHERE p = 0 AND c = 0")
            flushIf(cql, test_keyspace, flush)

            assert_rows_ignoring_order(execute(cql, table, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, None, 1))
            assert_rows_ignoring_order(execute(cql, mv, "SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0))

            assert_invalid_message_re(cql, table, DROP_COLUMN_ERROR.format("v2"), "ALTER TABLE %s DROP v2")
            # // drop unselected base column, unselected metadata should be removed, thus view row is dead
            # updateView("ALTER TABLE %s DROP v2");
            # assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
            # assertRowsIgnoringOrder(executeView("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
            # assertRowsIgnoringOrder(execute("SELECT * from %s"));
            # assertRowsIgnoringOrder(executeView("SELECT * from %s"));

def testPartialUpdateWithUnselectedCollectionsWithFlush(cql, test_keyspace):
    partialUpdateWithUnselectedCollections(cql, test_keyspace, True)

def testPartialUpdateWithUnselectedCollectionsWithoutFlush(cql, test_keyspace):
    partialUpdateWithUnselectedCollections(cql, test_keyspace, False)

def partialUpdateWithUnselectedCollections(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(k int, c int, a int, b int, l list<int>, s set<int>, m map<int,int>, PRIMARY KEY (k, c))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT a, b, c, k from %s " +
                                     "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)") as mv, \
             nodetool.no_autocompaction_context(cql, mv):

            execute(cql, table, "UPDATE %s SET l=l+[1,2,3] WHERE k = 1 AND c = 1")
            flushIf(cql, test_keyspace, flush)
            assert_rows(execute(cql, mv, "SELECT * from %s"), row(1, 1, None, None))

            execute(cql, table, "UPDATE %s SET l=l-[1,2] WHERE k = 1 AND c = 1")
            flushIf(cql, test_keyspace, flush)
            assert_rows(execute(cql, mv, "SELECT * from %s"), row(1, 1, None, None))

            execute(cql, table, "UPDATE %s SET b=3 WHERE k=1 AND c=1")
            flushIf(cql, test_keyspace, flush)
            assert_rows(execute(cql, mv, "SELECT * from %s"), row(1, 1, None, 3))

            execute(cql, table, "UPDATE %s SET b=null, l=l-[3], s=s-{3} WHERE k = 1 AND c = 1")
            if flush:
                nodetool.flush_keyspace(cql, test_keyspace)
                nodetool.compact(cql, mv)
            assert_empty(execute(cql, table, "SELECT k,c,a,b from %s"))
            assert_empty(execute(cql, mv, "SELECT * from %s"))

            execute(cql, table, "UPDATE %s SET m=m+{3:3}, l=l-[1], s=s-{2} WHERE k = 1 AND c = 1")
            flushIf(cql, test_keyspace, flush)
            assert_rows_ignoring_order(execute(cql, table, "SELECT k,c,a,b from %s"), row(1, 1, None, None))
            assert_rows_ignoring_order(execute(cql, mv, "SELECT * from %s"), row(1, 1, None, None))

            assert_invalid_message_re(cql, table, DROP_COLUMN_ERROR.format("m"), "ALTER TABLE %s DROP m")
            # executeNet(version, "ALTER TABLE %s DROP m");
            # ks.getColumnFamilyStore(mv).forceMajorCompaction();
            # assertRowsIgnoringOrder(execute("SELECT k,c,a,b from %s WHERE k = 1 AND c = 1"));
            # assertRowsIgnoringOrder(executeView("SELECT * from %s WHERE k = 1 AND c = 1"));
            # assertRowsIgnoringOrder(execute("SELECT k,c,a,b from %s"));
            # assertRowsIgnoringOrder(executeView("SELECT * from %s"));

def testUpdateWithColumnTimestampSmallerThanPkWithFlush(cql, test_keyspace):
    updateWithColumnTimestampSmallerThanPk(cql, test_keyspace, True)

def testUpdateWithColumnTimestampSmallerThanPkWithoutFlush(cql, test_keyspace):
    updateWithColumnTimestampSmallerThanPk(cql, test_keyspace, False)

def updateWithColumnTimestampSmallerThanPk(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(p int primary key, v1 int, v2 int)") as table:
        with create_view(cql, table, "create materialized view %s as select * from %s " +
                                     "where p is not null and v1 is not null primary key (v1, p)") as mv, \
             nodetool.no_autocompaction_context(cql, mv):

            # reset value
            execute(cql, table, "Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 6;")
            flushIf(cql, test_keyspace, flush)
            assert_rows_ignoring_order(execute(cql, mv, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, 3, 6))
            # increase pk's timestamp to 20
            execute(cql, table, "Insert into %s (p) values (3) using timestamp 20;")
            flushIf(cql, test_keyspace, flush)
            assert_rows_ignoring_order(execute(cql, mv, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, 3, 6))
            # change v1's to 2 and remove existing view row with ts7
            execute(cql, table, "UPdate %s using timestamp 7 set v1 = 2 where p = 3;")
            flushIf(cql, test_keyspace, flush)
            assert_rows_ignoring_order(execute(cql, mv, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(2, 3, 3, 6))
            assert_rows_ignoring_order(execute(cql, mv, "SELECT v1, p, v2, WRITETIME(v2) from %s" + " limit 1"), row(2, 3, 3, 6))
            # change v1's to 1 and remove existing view row with ts8
            execute(cql, table, "UPdate %s using timestamp 8 set v1 = 1 where p = 3;")
            flushIf(cql, test_keyspace, flush)
            assert_rows_ignoring_order(execute(cql, mv, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, 3, 6))

def testUpdateWithColumnTimestampBiggerThanPkWithFlush(cql, test_keyspace):
    # CASSANDRA-11500
    updateWithColumnTimestampBiggerThanPk(cql, test_keyspace, True)

def testUpdateWithColumnTimestampBiggerThanPkWithoutFlush(cql, test_keyspace):
    # CASSANDRA-11500
    updateWithColumnTimestampBiggerThanPk(cql, test_keyspace, False)

def updateWithColumnTimestampBiggerThanPk(cql, test_keyspace, flush):
    # CASSANDRA-11500 able to shadow old view row with column ts greater tahn pk's ts and re-insert the view row
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, a int, b int)") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * from %s " +
                                     "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)") as mv, \
             nodetool.no_autocompaction_context(cql, mv):
            execute(cql, table, "DELETE FROM %s USING TIMESTAMP 0 WHERE k = 1;")
            flushIf(cql, test_keyspace, flush)
            # sstable-1, Set initial values TS=1
            execute(cql, table, "INSERT INTO %s(k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 1;")
            flushIf(cql, test_keyspace, flush)
            assert_rows_ignoring_order(execute(cql, mv, "SELECT k,a,b from %s"), row(1, 1, 1))
            execute(cql, table, "UPDATE %s USING TIMESTAMP 10 SET b = 2 WHERE k = 1;")
            assert_rows_ignoring_order(execute(cql, mv, "SELECT k,a,b from %s"), row(1, 1, 2))
            flushIf(cql, test_keyspace, flush)
            assert_rows_ignoring_order(execute(cql, mv, "SELECT k,a,b from %s"), row(1, 1, 2))
            execute(cql, table, "UPDATE %s USING TIMESTAMP 2 SET a = 2 WHERE k = 1;")
            assert_rows_ignoring_order(execute(cql, mv, "SELECT k,a,b from %s"), row(1, 2, 2))
            flushIf(cql, test_keyspace, flush)
            nodetool.compact(cql, mv)
            assert_rows_ignoring_order(execute(cql, mv, "SELECT k,a,b from %s"), row(1, 2, 2))
            assert_rows_ignoring_order(execute(cql, mv, "SELECT k,a,b from %s limit 1"), row(1, 2, 2))
            execute(cql, table, "UPDATE %s USING TIMESTAMP 11 SET a = 1 WHERE k = 1;")
            flushIf(cql, test_keyspace, flush)
            assert_rows_ignoring_order(execute(cql, mv, "SELECT k,a,b from %s"), row(1, 1, 2))
            assert_rows_ignoring_order(execute(cql, table, "SELECT k,a,b from %s"), row(1, 1, 2))

            # set non-key base column as tombstone, view row is removed with shadowable
            execute(cql, table, "UPDATE %s USING TIMESTAMP 12 SET a = null WHERE k = 1;")
            flushIf(cql, test_keyspace, flush)
            assert_empty(execute(cql, mv, "SELECT k,a,b from %s"))
            assert_rows_ignoring_order(execute(cql, table, "SELECT k,a,b from %s"), row(1, None, 2))

            # column b should be alive
            execute(cql, table, "UPDATE %s USING TIMESTAMP 13 SET a = 1 WHERE k = 1;")
            flushIf(cql, test_keyspace, flush)
            assert_rows_ignoring_order(execute(cql, mv, "SELECT k,a,b from %s"), row(1, 1, 2))
            assert_rows_ignoring_order(execute(cql, table, "SELECT k,a,b from %s"), row(1, 1, 2))

            assert_invalid_message_re(cql, table, DROP_COLUMN_ERROR.format("a"), "ALTER TABLE %s DROP a")
