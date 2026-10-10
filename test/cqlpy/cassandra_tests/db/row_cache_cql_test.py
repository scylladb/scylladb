# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of RowCacheCQLTest.java from Cassandra's
# test/unit/org/apache/cassandra/db directory.

from ..porting import *

# The Java test first sets the row cache's capacity to 1 MB, through
# Cassandra's internal CacheService. We can't do that, and don't need to:
# Scylla's row cache is always enabled.
def test7636(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(p1 bigint, c1 int, v int, PRIMARY KEY (p1, c1)) WITH caching = { 'keys': 'NONE', 'rows_per_partition': 'ALL' }") as table:
        execute(cql, table, "INSERT INTO %s (p1, c1, v) VALUES (?, ?, ?)", 123, 10, 12)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE p1 = ? and c1 > ?", 123, 1000))
        res = list(execute(cql, table, "SELECT * FROM %s WHERE p1 = ? and c1 > ?", 123, 0))
        assert len(res) == 1
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE p1 = ? and c1 > ?", 123, 1000))

# Test for CASSANDRA-13482
def testPartialCache(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, v1 int, v2 int, primary key (pk, ck1))" +
                      "WITH CACHING = { 'keys': 'ALL', 'rows_per_partition': '1' }") as table:
        assertEmpty(execute(cql, table, "select * from %s where pk = 10000"))

        execute(cql, table, "DELETE FROM %s WHERE pk = 1 AND ck1 = 0")
        execute(cql, table, "DELETE FROM %s WHERE pk = 1 AND ck1 = 1")
        execute(cql, table, "DELETE FROM %s WHERE pk = 1 AND ck1 = 2")
        execute(cql, table, "INSERT INTO %s (pk, ck1, v1, v2) VALUES (1, 1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (pk, ck1, v1, v2) VALUES (1, 2, 2, 2)")
        execute(cql, table, "INSERT INTO %s (pk, ck1, v1, v2) VALUES (1, 3, 3, 3)")
        execute(cql, table, "DELETE FROM %s WHERE pk = 1 AND ck1 = 2")
        execute(cql, table, "DELETE FROM %s WHERE pk = 1 AND ck1 = 3")
        execute(cql, table, "INSERT INTO %s (pk, ck1, v1, v2) VALUES (1, 4, 4, 4)")
        execute(cql, table, "INSERT INTO %s (pk, ck1, v1, v2) VALUES (1, 5, 5, 5)")

        assertRows(execute(cql, table, "select * from %s where pk = 1"),
                   row(1, 1, 1, 1),
                   row(1, 4, 4, 4),
                   row(1, 5, 5, 5))
        assertRows(execute(cql, table, "select * from %s where pk = 1 LIMIT 1"),
                   row(1, 1, 1, 1))

        assertRows(execute(cql, table, "select * from %s where pk = 1 and ck1 >=2"),
                   row(1, 4, 4, 4),
                   row(1, 5, 5, 5))
        assertRows(execute(cql, table, "select * from %s where pk = 1 and ck1 >=2 LIMIT 1"),
                   row(1, 4, 4, 4))

        assertRows(execute(cql, table, "select * from %s where pk = 1 and ck1 >=2"),
                   row(1, 4, 4, 4),
                   row(1, 5, 5, 5))
        assertRows(execute(cql, table, "select * from %s where pk = 1 and ck1 >=2 LIMIT 1"),
                   row(1, 4, 4, 4))

def testPartialCacheWithStatic(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, s int static, v1 int, primary key (pk, ck1))" +
                      "WITH CACHING = { 'keys': 'ALL', 'rows_per_partition': '1' }") as table:
        assertEmpty(execute(cql, table, "select * from %s where pk = 10000"))

        execute(cql, table, "INSERT INTO %s (pk, s) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (pk, ck1, v1) VALUES (1, 2, 2)")
        execute(cql, table, "INSERT INTO %s (pk, ck1, v1) VALUES (1, 3, 3)")

        execute(cql, table, "DELETE FROM %s WHERE pk = 2 AND ck1 = 0")
        execute(cql, table, "DELETE FROM %s WHERE pk = 2 AND ck1 = 1")
        execute(cql, table, "DELETE FROM %s WHERE pk = 3 AND ck1 = 2")
        execute(cql, table, "INSERT INTO %s (pk, s) VALUES (2, 2)")
        execute(cql, table, "INSERT INTO %s (pk, ck1, v1) VALUES (2, 1, 1)")
        execute(cql, table, "INSERT INTO %s (pk, ck1, v1) VALUES (2, 2, 2)")
        execute(cql, table, "INSERT INTO %s (pk, ck1, v1) VALUES (2, 3, 3)")

        assertRows(execute(cql, table, "select * from %s WHERE pk = 1"),
                   row(1, 2, 1, 2),
                   row(1, 3, 1, 3))

        assertRows(execute(cql, table, "select * from %s WHERE pk = 2"),
                   row(2, 1, 2, 1),
                   row(2, 2, 2, 2),
                   row(2, 3, 2, 3))
