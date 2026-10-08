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

def testCellTombstoneAndShadowableTombstonesWithFlush(cql, test_keyspace):
    cellTombstoneAndShadowableTombstones(cql, test_keyspace, True)

def testCellTombstoneAndShadowableTombstonesWithoutFlush(cql, test_keyspace):
    cellTombstoneAndShadowableTombstones(cql, test_keyspace, False)

def cellTombstoneAndShadowableTombstones(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(p int primary key, v1 int, v2 int)") as table:
        with create_view(cql, table, "create materialized view %s as select * from %s " +
                                     "where p is not null and v1 is not null primary key (v1, p)") as view, \
             nodetool.no_autocompaction_context(cql, view):

            # sstable 1, Set initial values TS=1
            execute(cql, table, "Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 1")

            if flush:
                nodetool.flush_keyspace(cql, test_keyspace)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v2, WRITETIME(v2) from %s WHERE v1 = ? AND p = ?", 1, 3), row(3, 1))
            # sstable 2
            execute(cql, table, "UPdate %s using timestamp 2 set v2 = null where p = 3")

            if flush:
                nodetool.flush_keyspace(cql, test_keyspace)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v2, WRITETIME(v2) from %s WHERE v1 = ? AND p = ?", 1, 3),
                                       row(None, None))
            # sstable 3
            execute(cql, table, "UPdate %s using timestamp 3 set v1 = 2 where p = 3")

            if flush:
                nodetool.flush_keyspace(cql, test_keyspace)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(2, 3, None, None))
            # sstable 4
            execute(cql, table, "UPdate %s using timestamp 4 set v1 = 1 where p = 3")

            if flush:
                nodetool.flush_keyspace(cql, test_keyspace)

            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, None, None))

            # The Java test now compacts only the view's sstables 2 and 3,
            # with Cassandra's internal forceUserDefinedCompaction(). We can't
            # compact specific sstables through CQL or the REST API, so this
            # step was not translated.

            # cell-tombstone in sstable 4 is not compacted away, because the shadowable tombstone is shadowed by new row.
            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, None, None))
            assert_rows_ignoring_order(execute(cql, view, "SELECT v1, p, v2, WRITETIME(v2) from %s limit 1"), row(1, 3, None, None))
