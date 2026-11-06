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

def testUnselectedColumnWithExpiredLivenessInfoWithFlush(cql, test_keyspace, clock):
    unselectedColumnWithExpiredLivenessInfo(cql, test_keyspace, clock, True)

def testUnselectedColumnWithExpiredLivenessInfoWithoutFlush(cql, test_keyspace, clock):
    unselectedColumnWithExpiredLivenessInfo(cql, test_keyspace, clock, False)

def unselectedColumnWithExpiredLivenessInfo(cql, test_keyspace, clock, flush):
    with create_table(cql, test_keyspace, "(k int, c int, a int, b int, PRIMARY KEY(k, c))") as table:
        with create_view(cql, table, "create materialized view %s as select k,c,b from %s " +
                                     "where c is not null and k is not null primary key (c, k)") as view, \
             nodetool.no_autocompaction_context(cql, view):

            # sstable-1, Set initial values TS=1
            updateViewWithFlush(cql, table, "UPDATE %s SET a = 1 WHERE k = 1 AND c = 1;", flush)

            assert_rows_ignoring_order(execute(cql, table, "SELECT * from %s WHERE k = 1 AND c = 1;"),
                                       row(1, 1, 1, None))
            assert_rows_ignoring_order(execute(cql, view, "SELECT k,c,b from %s WHERE k = 1 AND c = 1;"),
                                       row(1, 1, None))

            # sstable-2
            updateViewWithFlush(cql, table, "INSERT INTO %s(k,c) VALUES(1,1) USING TTL 5", flush)

            assert_rows_ignoring_order(execute(cql, table, "SELECT * from %s WHERE k = 1 AND c = 1;"),
                                       row(1, 1, 1, None))
            assert_rows_ignoring_order(execute(cql, view, "SELECT k,c,b from %s WHERE k = 1 AND c = 1;"),
                                       row(1, 1, None))

            # The Java test sleeps 5001 ms here.
            clock.jump(6)

            assert_rows_ignoring_order(execute(cql, table, "SELECT * from %s WHERE k = 1 AND c = 1;"),
                                       row(1, 1, 1, None))
            assert_rows_ignoring_order(execute(cql, view, "SELECT k,c,b from %s WHERE k = 1 AND c = 1;"),
                                       row(1, 1, None))

            # sstable-3
            updateViewWithFlush(cql, table, "Update %s set a = null where k = 1 AND c = 1;", flush)

            assert_empty(execute(cql, table, "SELECT * from %s WHERE k = 1 AND c = 1;"))
            assert_empty(execute(cql, view, "SELECT k,c,b from %s WHERE k = 1 AND c = 1;"))

            # sstable-4
            updateViewWithFlush(cql, table, "Update %s USING TIMESTAMP 1 set b = 1 where k = 1 AND c = 1;", flush)

            assert_rows_ignoring_order(execute(cql, table, "SELECT * from %s WHERE k = 1 AND c = 1;"),
                                       row(1, 1, None, 1))
            assert_rows_ignoring_order(execute(cql, view, "SELECT k,c,b from %s WHERE k = 1 AND c = 1;"),
                                       row(1, 1, 1))

# The Java test also checks, after each major compaction, the number of the
# view's live sstables - one while the view has a dead row whose tombstone
# can't be garbage-collected yet, and zero after gc_grace_seconds. The number
# of sstables can't be checked through CQL, so these checks were not
# translated, and only the view's contents are checked.
def testStrictLivenessTombstone(cql, test_keyspace, clock):
    with create_table(cql, test_keyspace, "(p int primary key, v1 int, v2 int)") as table:
        with create_view(cql, table, "create materialized view %s as select * from %s " +
                                     "where p is not null and v1 is not null primary key (v1, p) " +
                                     "with gc_grace_seconds=5") as view, \
             nodetool.no_autocompaction_context(cql, view):

            execute(cql, table, "Insert into %s (p, v1, v2) values (1, 1, 1)")
            assert_rows_ignoring_order(execute(cql, view, "SELECT p, v1, v2 from %s"), row(1, 1, 1))

            execute(cql, table, "Update %s set v1 = null WHERE p = 1")
            nodetool.flush_keyspace(cql, test_keyspace)
            assert_empty(execute(cql, view, "SELECT p, v1, v2 from %s"))

            nodetool.compact(cql, view) # before gc grace second, strict-liveness tombstoned dead row remains
            # assertEquals(1, cfs.getLiveSSTables().size());

            # The Java test sleeps 6000 ms here.
            clock.jump(6)
            # assertEquals(1, cfs.getLiveSSTables().size()); // no auto compaction.

            nodetool.compact(cql, view) # after gc grace second, no data left
            # assertEquals(0, cfs.getLiveSSTables().size());

            execute(cql, table, "Update %s using ttl 5 set v1 = 1 WHERE p = 1")
            nodetool.flush_keyspace(cql, test_keyspace)
            assert_rows_ignoring_order(execute(cql, view, "SELECT p, v1, v2 from %s"), row(1, 1, 1))

            nodetool.compact(cql, view) # before ttl+gc_grace_second, strict-liveness ttled dead row remains
            # assertEquals(1, cfs.getLiveSSTables().size());
            assert_rows_ignoring_order(execute(cql, view, "SELECT p, v1, v2 from %s"), row(1, 1, 1))

            # The Java test sleeps 5500 ms here.
            clock.jump(6) # after expired, before gc_grace_second
            nodetool.compact(cql, view) # before ttl+gc_grace_second, strict-liveness ttled dead row remains
            # assertEquals(1, cfs.getLiveSSTables().size());
            assert_empty(execute(cql, view, "SELECT p, v1, v2 from %s"))

            # The Java test sleeps 5500 ms here.
            clock.jump(6) # after expired + gc_grace_second
            # assertEquals(1, cfs.getLiveSSTables().size()); // no auto compaction.

            nodetool.compact(cql, view) # after gc grace second, no data left
            # assertEquals(0, cfs.getLiveSSTables().size());
