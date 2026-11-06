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

def testCompactionOfDeletedRowWithTtl(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, a int, b int, c int, primary key(k, a)) with default_time_to_live=6000") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT k,a,b FROM %s WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)") as view:
            execute(cql, table, "UPDATE %s SET c=2 WHERE k=1 AND a=1")
            flush(cql, view)
            assert_rows(execute(cql, table, "SELECT k,a,b,c FROM %s"), row(1, 1, None, 2))
            assert_rows(execute(cql, view, "SELECT k,a,b FROM %s"), row(1, 1, None))

            compact(cql, view)

            assert_rows(execute(cql, table, "SELECT k,a,b,c FROM %s"), row(1, 1, None, 2))
            assert_rows(execute(cql, view, "SELECT k,a,b FROM %s"), row(1, 1, None))

            execute(cql, table, "DELETE c FROM %s WHERE k=1 AND a=1")
            flush(cql, view)

            assert_empty(execute(cql, table, "SELECT k,a,b,c FROM %s"))
            assert_empty(execute(cql, view, "SELECT k,a,b FROM %s"))

            compact(cql, view)

            assert_empty(execute(cql, table, "SELECT k,a,b,c FROM %s"))
            assert_empty(execute(cql, view, "SELECT k,a,b FROM %s"))
