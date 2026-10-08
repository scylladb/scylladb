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

def flushIf(cql, keyspace, flush):
    if flush:
        nodetool.flush_keyspace(cql, keyspace)

def testRegularColumnTimestampUpdates(cql, test_keyspace):
    # Regression test for CASSANDRA-10910
    with create_table(cql, test_keyspace, "(" +
                      "k int PRIMARY KEY, " +
                      "c int, " +
                      "val int)") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE k IS NOT NULL AND c IS NOT NULL " +
                                     "PRIMARY KEY (k,c)") as view:

            execute(cql, table, "UPDATE %s SET c = ?, val = ? WHERE k = ?", 0, 0, 0)
            execute(cql, table, "UPDATE %s SET val = ? WHERE k = ?", 1, 0)
            execute(cql, table, "UPDATE %s SET c = ? WHERE k = ?", 1, 0)
            assert_rows(execute(cql, view, "SELECT c, k, val FROM %s"), row(1, 0, 1))

            execute(cql, table, "TRUNCATE %s")

            execute(cql, table, "UPDATE %s USING TIMESTAMP 1 SET c = ?, val = ? WHERE k = ?", 0, 0, 0)
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE k = ?", 1, 0)
            execute(cql, table, "UPDATE %s USING TIMESTAMP 2 SET val = ? WHERE k = ?", 1, 0)
            execute(cql, table, "UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE k = ?", 2, 0)
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET val = ? WHERE k = ?", 2, 0)

            assert_rows(execute(cql, view, "SELECT c, k, val FROM %s"), row(2, 0, 2))
            assert_rows(execute(cql, view, "SELECT c, k, val FROM %s limit 1"), row(2, 0, 2))

def testcomplexTimestampUpdateTestWithFlush(cql, test_keyspace):
    complexTimestampUpdateTest(cql, test_keyspace, True)

def testcomplexTimestampUpdateTestWithoutFlush(cql, test_keyspace):
    complexTimestampUpdateTest(cql, test_keyspace, False)

def complexTimestampUpdateTest(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY (a, b))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL " +
                                     "PRIMARY KEY (c, a, b)") as view, \
             nodetool.no_autocompaction_context(cql, view):

            #Set initial values TS=0, leaving e null and verify view
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (0, 0, 1, 0) USING TIMESTAMP 0")
            assert_rows(execute(cql, view, "SELECT d from %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0))

            #update c's timestamp TS=2
            execute(cql, table, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0)
            assert_rows(execute(cql, view, "SELECT d from %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0))

            flushIf(cql, test_keyspace, flush)

            # change c's value and TS=3, tombstones c=1 and adds c=0 record
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? and b = ? ", 0, 0, 0)
            flushIf(cql, test_keyspace, flush)
            assert_empty(execute(cql, view, "SELECT d from %s WHERE c = ? and a = ? and b = ?", 1, 0, 0))

            if flush:
                nodetool.compact(cql, view)
                nodetool.flush_keyspace(cql, test_keyspace)

            #change c's value back to 1 with TS=4, check we can see d
            execute(cql, table, "UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0)
            if flush:
                nodetool.compact(cql, view)
                nodetool.flush_keyspace(cql, test_keyspace)

            assert_rows(execute(cql, view, "SELECT d,e from %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, None))

            #Add e value @ TS=1
            execute(cql, table, "UPDATE %s USING TIMESTAMP 1 SET e = ? WHERE a = ? and b = ? ", 1, 0, 0)
            assert_rows(execute(cql, view, "SELECT d,e from %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, 1))

            flushIf(cql, test_keyspace, flush)

            #Change d value @ TS=2
            execute(cql, table, "UPDATE %s USING TIMESTAMP 2 SET d = ? WHERE a = ? and b = ? ", 2, 0, 0)
            assert_rows(execute(cql, view, "SELECT d from %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(2))

            flushIf(cql, test_keyspace, flush)

            #Change d value @ TS=3
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? and b = ? ", 1, 0, 0)
            assert_rows(execute(cql, view, "SELECT d from %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(1))

            #Tombstone c
            execute(cql, table, "DELETE FROM %s WHERE a = ? and b = ?", 0, 0)
            assert_empty(execute(cql, view, "SELECT d from %s"))

            #Add back without D
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, 1)")

            #Make sure D doesn't pop back in.
            assert_rows(execute(cql, view, "SELECT d from %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(None))

            #New partition
            # insert a row with timestamp 0
            execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 0", 1, 0, 0, 0, 0)

            # overwrite pk and e with timestamp 1, but don't overwrite d
            execute(cql, table, "INSERT INTO %s (a, b, c, e) VALUES (?, ?, ?, ?) USING TIMESTAMP 1", 1, 0, 0, 0)

            # delete with timestamp 0 (which should only delete d)
            execute(cql, table, "DELETE FROM %s USING TIMESTAMP 0 WHERE a = ? AND b = ?", 1, 0)
            assert_rows(execute(cql, view, "SELECT a, b, c, d, e from %s WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                        row(1, 0, 0, None, 0))

            execute(cql, table, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? AND b = ?", 1, 1, 0)
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? AND b = ?", 0, 1, 0)
            assert_rows(execute(cql, view, "SELECT a, b, c, d, e from %s WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                        row(1, 0, 0, None, 0))

            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? AND b = ?", 0, 1, 0)
            assert_rows(execute(cql, view, "SELECT a, b, c, d, e from %s WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                        row(1, 0, 0, 0, 0))

def testttlTest(cql, test_keyspace, clock):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "c int," +
                      "d int," +
                      "PRIMARY KEY (a, b))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)") as view:

            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 3", 1, 1, 1, 1)

            # The Java test sleeps 1 second here.
            clock.jump(1)
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 2)

            # The Java test sleeps 5 seconds here.
            clock.jump(5)
            results = list(execute(cql, view, "SELECT d FROM %s WHERE c = 2 AND a = 1 AND b = 1"))
            assert len(results) == 1
            assert results[0][0] is None, "There should be a null result given back due to ttl expiry"

def testttlExpirationTest(cql, test_keyspace, clock):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "c int," +
                      "d int," +
                      "PRIMARY KEY (a, b))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)") as view:

            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 3", 1, 1, 1, 1)

            # The Java test sleeps 4 seconds here.
            clock.jump(4)
            assert_empty(execute(cql, view, "SELECT * FROM %s WHERE c = 1 AND a = 1 AND b = 1"))

def testconflictingTimestampTest(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "c int," +
                      "PRIMARY KEY (a, b))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)") as view:

            for i in range(50):
                execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP 1", 1, 1, i)

            mvRows = execute(cql, view, "SELECT c FROM %s")
            rows = list(execute(cql, table, "SELECT c FROM %s"))
            assert len(rows) == 1, "There should be exactly one row in base"
            expected = rows[0].c
            assert_rows(mvRows, row(expected))

def testCreateMvWithTTL(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int PRIMARY KEY, " +
                      "c int, " +
                      "val int) WITH default_time_to_live = 60") as table:

        # Must NOT include "default_time_to_live" for Materialized View creation
        # (Scylla's message is "Cannot set or alter default_time_to_live...")
        with pytest.raises(InvalidRequest, match="Cannot set (or alter )?default_time_to_live for a materialized view"):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                         "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c) WITH default_time_to_live = 30"):
                pass

def testAlterMvWithNoZeroTTL(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int PRIMARY KEY, " +
                      "c int, " +
                      "val int) WITH default_time_to_live = 60") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)") as view:

            # Must NOT include "default_time_to_live" on alter Materialized View
            # (Note that, like the Java test, this test doesn't fail if the
            # ALTER succeeds - only checks the message if it fails.)
            try:
                cql.execute("ALTER MATERIALIZED VIEW " + view + " WITH default_time_to_live = 30")
            except Exception as e:
                # Make sure the message is clear. See CASSANDRA-16960
                # Cassandra's message is "Forbidden default_time_to_live
                # detected for a materialized view. Data in a materialized view
                # always expire at the same time than the corresponding data in
                # the parent table. default_time_to_live must be set to zero,
                # see CASSANDRA-12868 for more information". Scylla's begins
                # "Cannot set or alter default_time_to_live for a materialized
                # view", and continues with the same explanation.
                assert re.search("(Forbidden default_time_to_live detected|Cannot set or alter default_time_to_live) for a materialized view. " +
                                 "Data in a materialized view always expire at the same time than the corresponding " +
                                 "data in the parent table", str(e))

def testMvWithZeroTTL(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int PRIMARY KEY, " +
                      "c int, " +
                      "val int) WITH default_time_to_live = 60") as table:
        # Should not fail if TTL equal to 0 is provided while altering materialized view
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c) WITH default_time_to_live = 0"):
            pass

def testAlterMvWithZeroTTL(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int PRIMARY KEY, " +
                      "c int, " +
                      "val int) WITH default_time_to_live = 60") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)") as view:
            # Should not fail if TTL equal to 0 is provided while altering materialized view
            cql.execute("ALTER MATERIALIZED VIEW " + view + " WITH default_time_to_live = 0")
