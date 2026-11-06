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

def testExpiredLivenessLimitWithFlush(cql, test_keyspace):
    # CASSANDRA-13883
    expiredLivenessLimit(cql, test_keyspace, True)

def testExpiredLivenessLimitWithoutFlush(cql, test_keyspace):
    # CASSANDRA-13883
    expiredLivenessLimit(cql, test_keyspace, False)

def expiredLivenessLimit(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, a int, b int)") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)") as mv1, \
             create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)") as mv2, \
             nodetool.no_autocompaction_context(cql, mv1, mv2):

            for i in range(1, 101):
                execute(cql, table, "INSERT INTO %s(k, a, b) VALUES (?, ?, ?)", i, i, i)
            for i in range(1, 101):
                if i % 50 == 0:
                    continue
                # create expired liveness
                execute(cql, table, "DELETE a FROM %s WHERE k = ?", i)

            if flush:
                nodetool.flush(cql, mv1)
                nodetool.flush(cql, mv2)

            for view in [mv1, mv2]:
                # paging
                assert 1 == len(list(execute_with_paging(cql, view, "SELECT k,a,b FROM %s limit 1", 1)))
                assert 2 == len(list(execute_with_paging(cql, view, "SELECT k,a,b FROM %s limit 2", 1)))
                assert 2 == len(list(execute_with_paging(cql, view, "SELECT k,a,b FROM %s", 1)))
                assert_rows(execute_with_paging(cql, view, "SELECT k,a,b FROM %s ", 1),
                            row(50, 50, 50),
                            row(100, 100, 100))
                # limit
                assert 1 == len(list(execute(cql, view, "SELECT k,a,b FROM %s limit 1")))
                assert_rows_ignoring_order(execute(cql, view, "SELECT k,a,b FROM %s limit 2"),
                                           row(50, 50, 50),
                                           row(100, 100, 100))
