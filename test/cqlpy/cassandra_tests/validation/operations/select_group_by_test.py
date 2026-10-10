# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2023-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ...porting import *
from datetime import datetime, timezone
from cassandra.util import Date, Time, uuid_from_time

@pytest.mark.xfail(reason="Issue #2060, #13109")
def testGroupByWithoutPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, primary key (a, b, c, d))") as table:

        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (2, 2, 3, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (2, 4, 3, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (3, 3, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (4, 8, 2, 12, 24)")

        # Makes sure that we have some tombstones
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2 AND d = 12")
        execute(cql, table, "DELETE FROM %s WHERE a = 3")

        # Range queries
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a"),
                   row(1, 2, 6, 4, 24),
                   row(2, 2, 6, 2, 12),
                   row(4, 8, 24, 1, 24))

        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b"),
                   row(1, 2, 6, 2, 12),
                   row(1, 4, 12, 2, 24),
                   row(2, 2, 6, 1, 6),
                   row(2, 4, 12, 1, 12),
                   row(4, 8, 24, 1, 24))

        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE b = 2 GROUP BY a, b ALLOW FILTERING"),
                   row(1, 2, 6, 2, 12),
                   row(2, 2, 6, 1, 6))

        # Reproduces #12477:
        assertEmpty(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE b IN () GROUP BY a, b ALLOW FILTERING"))

        # Range queries without aggregates
        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12))

        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12))

        # Range queries with wildcard
        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        # Range query with LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b LIMIT 2"),
                   row(1, 2, 6, 2, 12),
                   row(1, 4, 12, 2, 24))

        # Range queries with PER PARTITION LIMIT
        # Reproduces #5363:
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 6, 2, 12),
                   row(2, 2, 6, 1, 6),
                   row(4, 8, 24, 1, 24))

        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 4, 24),
                   row(2, 2, 6, 2, 12),
                   row(4, 8, 24, 1, 24))

        # Range query with PER PARTITION LIMIT and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2"),
                   row(1, 2, 6, 2, 12),
                   row(2, 2, 6, 1, 6))

        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 4, 24),
                   row(2, 2, 6, 2, 12),
                   row(4, 8, 24, 1, 24))

        # Range queries without aggregates and with LIMIT
        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c LIMIT 3"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6))

        # Reproduces 5362:
        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b LIMIT 3"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3))

        # Range queries with wildcard and with LIMIT
        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b, c LIMIT 3"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12))

        # Reproduces 5362:
        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b LIMIT 3"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6))

        # Range queries without aggregates and with PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12))

        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3),
                   row(2, 2, 3, 3),
                   row(4, 8, 2, 12))

        # Range queries with wildcard and with PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3, 6),
                   row(2, 2, 3, 3, 6),
                   row(4, 8, 2, 12, 24))

        # Range queries without aggregates, with PER PARTITION LIMIT and LIMIT
        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(2, 2, 3, 3))

        # Range queries with wildcard, with PER PARTITION LIMIT and LIMIT
        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(2, 2, 3, 3, 6))

        # Range query with DISTINCT
        assertRows(execute(cql, table, "SELECT DISTINCT a, count(a)FROM %s GROUP BY a"),
                   row(1, 1),
                   row(2, 1),
                   row(4, 1))

        # Reproduces #12479:
        assertInvalidMessage(cql, table, "Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s GROUP BY a, b")

        # Range query with DISTINCT and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT DISTINCT a, count(a)FROM %s GROUP BY a LIMIT 2"),
                   row(1, 1),
                   row(2, 1))

        # Reproduces #12479:
        assertInvalidMessage(cql, table, "Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s GROUP BY a, b LIMIT 2")

        # Range query with ORDER BY
        assertInvalidMessage(cql, table, "ORDER BY is only supported when the partition key is restricted by an EQ or an IN",
                             "SELECT a, b, c, count(b), max(e) FROM %s GROUP BY a, b ORDER BY b DESC, c DESC")

        # Single partition queries
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(1, 4, 12, 2, 24))

        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY b, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(1, 4, 12, 2, 24))

        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY a, b, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12))

        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY a, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12))

        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12))

        # Single partition queries without aggregates
        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6))

        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6))

        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6))

        # Reproduces #2060:
        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 and token(a) = token(1) GROUP BY b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6))

        # Single partition queries with wildcard
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12))

        # Single partition queries with DISTINCT
        assertRows(execute(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 1))

        # Reproduces #12479:
        assertInvalidMessage(cql, table, "Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a, b")

        # Single partition queries with LIMIT
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 10"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(1, 4, 12, 2, 24))

        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12))

        assertRows(execute(cql, table, "SELECT count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 1"),
                   row(1, 6))

        # Single partition queries with PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 10"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(1, 4, 12, 2, 24))

        # Reproduces #5363:
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12))

        assertRows(execute(cql, table, "SELECT count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 1"),
                   row(1, 6))

        # Single partition queries without aggregates and with LIMIT
        # Reproduces #5362:
        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6))

        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1"),
                   row(1, 2, 1, 3))

        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6))

        # Single partition queries with wildcard and with LIMIT
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1"),
                   row(1, 2, 1, 3, 6))

        # Single partition queries without aggregates and with PER PARTITION LIMIT
        # Reproduces #5363:
        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6))

        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3))

        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6))

        # Single partition queries with wildcard and with PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3, 6))

        # Single partition queries with ORDER BY
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC"),
                   row(1, 4, 24, 2, 24),
                   row(1, 2, 12, 1, 12),
                   row(1, 2, 6, 1, 6))

        # Single partition queries with ORDER BY and PER PARTITION LIMIT
        # Reproduces #5363:
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC PER PARTITION LIMIT 1"),
                   row(1, 4, 24, 2, 24))

        # Single partition queries with ORDER BY and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 2"),
                   row(1, 4, 24, 2, 24),
                   row(1, 2, 12, 1, 12))

        # Multi-partitions queries
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(1, 4, 12, 2, 24),
                   row(2, 2, 6, 1, 6),
                   row(2, 4, 12, 1, 12),
                   row(4, 8, 24, 1, 24))

        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) AND b = 2 GROUP BY a, b, c"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(2, 2, 6, 1, 6))

        # Multi-partitions queries without aggregates
        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b"),
                   row(1, 2, 1, 3),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12))

        assertRows(execute(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                   row(1, 2, 1, 3),
                   row(1, 2, 2, 6),
                   row(1, 4, 2, 6),
                   row(2, 2, 3, 3),
                   row(2, 4, 3, 6),
                   row(4, 8, 2, 12))

        # Multi-partitions with wildcard
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b"),
                   row(1, 2, 1, 3, 6),
                   row(1, 4, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        # Multi-partitions query with DISTINCT
        assertRows(execute(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a"),
                   row(1, 1),
                   row(2, 1),
                   row(4, 1))

        # Reproduces #12479:
        assertInvalidMessage(cql, table, "Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                             "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b")

        # Multi-partitions query with DISTINCT and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a LIMIT 2"),
                   row(1, 1),
                   row(2, 1))

        # Multi-partitions queries with PER PARTITION LIMIT
        # Reproduces #5363:
        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 1"),
                   row(1, 2, 6, 1, 6),
                   row(2, 2, 6, 1, 6),
                   row(4, 8, 24, 1, 24))

        assertRows(execute(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 6, 1, 6),
                   row(1, 2, 12, 1, 12),
                   row(2, 2, 6, 1, 6),
                   row(2, 4, 12, 1, 12),
                   row(4, 8, 24, 1, 24))

        # Multi-partitions with wildcard and PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 3, 6),
                   row(1, 2, 2, 6, 12),
                   row(2, 2, 3, 3, 6),
                   row(2, 4, 3, 6, 12),
                   row(4, 8, 2, 12, 24))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 3, 6),
                   row(2, 2, 3, 3, 6),
                   row(4, 8, 2, 12, 24))

        # Multi-partitions queries with ORDER BY
        # Reproduces #13109:
        assertRows(execute_without_paging(cql, table, "SELECT a, b, c, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC"),
                   row(4, 8, 2, 1, 24),
                   row(2, 4, 3, 1, 12),
                   row(1, 4, 2, 2, 24),
                   row(2, 2, 3, 1, 6),
                   row(1, 2, 2, 2, 12))

        assertRows(execute_without_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c ORDER BY b DESC, c DESC"),
                   row(4, 8, 2, 12),
                   row(2, 4, 3, 6),
                   row(1, 4, 2, 12),
                   row(2, 2, 3, 3),
                   row(1, 2, 2, 6),
                   row(1, 2, 1, 3))

        # Multi-partitions queries with ORDER BY and LIMIT
        # Reproduces #5361:
        assertRows(execute_without_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 3"),
                   row(4, 8, 2, 12),
                   row(2, 4, 3, 6),
                   row(1, 4, 2, 12))

        # Multi-partitions with wildcard, ORDER BY and LIMIT
        # Reproduces #5361:
        assertRows(execute_without_paging(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 3"),
                   row(4, 8, 2, 12, 24),
                   row(2, 4, 3, 6, 12),
                   row(1, 4, 2, 12, 24))

        # Invalid queries
        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, e")

        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY c")

        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, c, b")

        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, a")

        assertInvalidMessage(cql, table, "Group by",
                             "SELECT a, b, c, d FROM %s WHERE token(a) = token(1) GROUP BY b, c")

        assertInvalidMessage(cql, table, "clustering1",
                             "SELECT a, b as clustering1, max(c) FROM %s WHERE a = 1 GROUP BY a, clustering1")

        assertInvalidMessage(cql, table, "z",
                             "SELECT a, b, max(c) FROM %s WHERE a = 1 GROUP BY a, b, z")

    # Test with composite partition key
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, primary key ((a, b), c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 1, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 3, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)")

        assertInvalidMessage(cql, table, "partition key",
                             "SELECT a, b, max(d) FROM %s GROUP BY a")

        assertRows(execute(cql, table, "SELECT a, b, max(d) FROM %s GROUP BY a, b"),
                   row(1, 2, 12),
                   row(1, 1, 12))

        assertRows(execute(cql, table, "SELECT a, b, max(d) FROM %s WHERE a = 1 AND b = 1 GROUP BY b"),
                   row(1, 1, 12))

    # Test with table without clustering key
    with create_table(cql, test_keyspace, "(a int primary key, b int, c int)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (3, 12, 24)")

        assertInvalidMessage(cql, table, "order",
                             "SELECT a, max(c) FROM %s WHERE a = 1 GROUP BY a, a")

def testGroupByWithoutPagingWithDeletions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, primary key (a, b, c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 9, 18)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 9, 18)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 9, 18)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 12, 24)")

        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 2 AND c = 1 AND d = 12")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 2 AND c = 2 AND d = 9")

        assertRows(execute(cql, table, "SELECT a, b, c, count(b), max(d) FROM %s GROUP BY a, b, c"),
                   row(1, 2, 1, 3, 9),
                   row(1, 2, 2, 3, 12),
                   row(1, 2, 3, 4, 12))

def testGroupByWithRangeNamesQueryWithoutPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, primary key (a, b, c))") as table:
        for i in range(1,5):
            for j in range(1,5):
                for k in range(1,5):
                    execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, k, i + j)

        # Makes sure that we have some tombstones
        execute(cql, table, "DELETE FROM %s WHERE a = 3")

        # Range queries
        assertRows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        assertRows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        assertRows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(1, 2, 3, 2, 3),
                   row(2, 1, 3, 2, 3),
                   row(2, 2, 4, 2, 4),
                   row(4, 1, 5, 2, 5),
                   row(4, 2, 6, 2, 6))

        # Range queries with LIMIT
        assertRows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a LIMIT 5 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        assertRows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(1, 2, 3, 2, 3),
                   row(2, 1, 3, 2, 3))

        # Range queries with PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        # Reproduces #5363:
        assertRows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        # Range queries with PER PARTITION LIMIT and LIMIT
        assertRows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 5 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3),
                   row(4, 1, 5, 2, 5))

        # Reproduces #5363:
        assertRows(execute(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 2, 2, 2),
                   row(2, 1, 3, 2, 3))

@pytest.mark.xfail(reason="Issue #13109")
def testGroupByWithStaticColumnsWithoutPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, s int static, d int, primary key (a, b, c))") as table:
        # ------------------------------------
        # Test with non static columns empty
        # ------------------------------------
        execute(cql, table, "UPDATE %s SET s = 1 WHERE a = 1")
        execute(cql, table, "UPDATE %s SET s = 2 WHERE a = 2")
        execute(cql, table, "UPDATE %s SET s = 3 WHERE a = 4")

        # Range queries
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a"),
                   row(1, None, 1, 0, 1),
                   row(2, None, 2, 0, 1),
                   row(4, None, 3, 0, 1))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b"),
                   row(1, None, 1, 0, 1),
                   row(2, None, 2, 0, 1),
                   row(4, None, 3, 0, 1))

        # Range query without aggregates
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s GROUP BY a, b"),
                   row(1, None, 1),
                   row(2, None, 2),
                   row(4, None, 3))

        # Range query with wildcard
        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b"),
                   row(1, None, None, 1, None),
                   row(2, None, None, 2, None),
                   row(4, None, None, 3, None ))

        # Range query with LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b LIMIT 2"),
                   row(1, None, 1, 0, 1),
                   row(2, None, 2, 0, 1))

        # Range queries with PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a PER PARTITION LIMIT 2"),
                   row(1, None, 1, 0, 1),
                   row(2, None, 2, 0, 1),
                   row(4, None, 3, 0, 1))

        # Range query with DISTINCT
        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s GROUP BY a"),
                   row(1, 1, 1),
                   row(2, 2, 1),
                   row(4, 3, 1))

        # Range queries with DISTINCT and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s GROUP BY a LIMIT 2"),
                   row(1, 1, 1),
                   row(2, 2, 1))

        # Single partition queries
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, None, 1, 0, 1))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, None, 1, 0, 1))

        # Single partition query without aggregates
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, None, 1))

        # Single partition query with wildcard
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, None, None, 1, None))

        # Single partition query with LIMIT
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a, b LIMIT 2"),
                   row(1, None, 1, 0, 1))

        # Single partition query with PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 2"),
                   row(1, None, 1, 0, 1))

        # Single partition query with DISTINCT
        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 1, 1))

        # Multi-partitions queries
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, None, 1, 0, 1),
                   row(2, None, 2, 0, 1),
                   row(4, None, 3, 0, 1))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, None, 1, 0, 1),
                   row(2, None, 2, 0, 1),
                   row(4, None, 3, 0, 1))

        # Multi-partitions query without aggregates
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, None, 1),
                   row(2, None, 2),
                   row(4, None, 3))

        # Multi-partitions query with wildcard
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, None, None, 1, None),
                   row(2, None, None, 2, None),
                   row(4, None, None, 3, None))

        # Multi-partitions query with LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 2"),
                   row(1, None, 1, 0, 1),
                   row(2, None, 2, 0, 1))

        # Multi-partitions query with PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 2"),
                   row(1, None, 1, 0, 1),
                   row(2, None, 2, 0, 1),
                   row(4, None, 3, 0, 1))

        # Multi-partitions queries with DISTINCT
        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, 1, 1),
                   row(2, 2, 1),
                   row(4, 3, 1))

        # Multi-partitions with DISTINCT and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"),
                   row(1, 1, 1),
                   row(2, 2, 1))

        # ------------------------------------
        # Test with some non static columns empty
        # ------------------------------------
        execute(cql, table, "UPDATE %s SET s = 3 WHERE a = 3")
        execute(cql, table, "DELETE s FROM %s WHERE a = 4")

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 2, 1, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 2, 2, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 3, 2, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 4, 2, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 4, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 2, 3, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 4, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (4, 8, 2, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (5, 8, 2, 12)")

        # Makes sure that we have some tombstones
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2")
        execute(cql, table, "DELETE FROM %s WHERE a = 5")

        # Range queries
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a"),
                   row(1, 2, 1, 4, 4),
                   row(2, 2, 2, 2, 2),
                   row(4, 8, None, 1, 0),
                   row(3, None, 3, 0, 1))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b"),
                   row(1, 2, 1, 2, 2),
                   row(1, 4, 1, 2, 2),
                   row(2, 2, 2, 1, 1),
                   row(2, 4, 2, 1, 1),
                   row(4, 8, None, 1, 0),
                   row(3, None, 3, 0, 1))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE b = 2 GROUP BY a, b ALLOW FILTERING"),
                   row(1, 2, 1, 2, 2),
                   row(2, 2, 2, 1, 1))

        # Range queries without aggregates
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s GROUP BY a"),
                   row(1, 2, 1),
                   row(2, 2, 2),
                   row(4, 8, None),
                   row(3, None, 3))

        assertRows(execute(cql, table, "SELECT a, b, s FROM %s GROUP BY a, b"),
                   row(1, 2, 1),
                   row(1, 4, 1),
                   row(2, 2, 2),
                   row(2, 4, 2),
                   row(4, 8, None),
                   row(3, None, 3))

        # Range queries with wildcard
        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a"),
                   row(1, 2, 1, 1, 3),
                   row(2, 2, 3, 2, 3),
                   row(4, 8, 2, None, 12),
                   row(3, None, None, 3, None))

        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b"),
                   row(1, 2, 1, 1, 3),
                   row(1, 4, 2, 1, 12),
                   row(2, 2, 3, 2, 3),
                   row(2, 4, 3, 2, 6),
                   row(4, 8, 2, None, 12),
                   row(3, None, None, 3, None))

        # Range query with LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a LIMIT 2"),
                   row(1, 2, 1, 4, 4),
                   row(2, 2, 2, 2, 2))

        # Range query with PER PARTITION LIMIT
        # Reproduces #5363:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 2, 2),
                   row(2, 2, 2, 1, 1),
                   row(4, 8, None, 1, 0),
                   row(3, None, 3, 0, 1))

        # Range query with PER PARTITION LIMIT and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 3"),
                   row(1, 2, 1, 2, 2),
                   row(2, 2, 2, 1, 1),
                   row(4, 8, None, 1, 0))

        # Range queries without aggregates and with LIMIT
        # Reproduces #5362:
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s GROUP BY a LIMIT 2"),
                   row(1, 2, 1),
                   row(2, 2, 2))

        assertRows(execute(cql, table, "SELECT a, b, s FROM %s GROUP BY a, b LIMIT 10"),
                   row(1, 2, 1),
                   row(1, 4, 1),
                   row(2, 2, 2),
                   row(2, 4, 2),
                   row(4, 8, None),
                   row(3, None, 3))

        # Range queries with wildcard and with LIMIT
        # Reproduces #5362:
        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a LIMIT 2"),
                   row(1, 2, 1, 1, 3),
                   row(2, 2, 3, 2, 3))

        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b LIMIT 10"),
                   row(1, 2, 1, 1, 3),
                   row(1, 4, 2, 1, 12),
                   row(2, 2, 3, 2, 3),
                   row(2, 4, 3, 2, 6),
                   row(4, 8, 2, None, 12),
                   row(3, None, None, 3, None))

        # Range queries without aggregates and with PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1),
                   row(2, 2, 2),
                   row(4, 8, None),
                   row(3, None, 3))

        # Range queries with wildcard and with PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 1, 3),
                   row(2, 2, 3, 2, 3),
                   row(4, 8, 2, None, 12),
                   row(3, None, None, 3, None))

        # Range queries without aggregates, with PER PARTITION LIMIT and with LIMIT
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2"),
                   row(1, 2, 1),
                   row(2, 2, 2))

        # Range queries with wildcard, PER PARTITION LIMIT and LIMIT
        assertRows(execute(cql, table, "SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2"),
                   row(1, 2, 1, 1, 3),
                   row(2, 2, 3, 2, 3))

        # Range query with DISTINCT
        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s GROUP BY a"),
                   row(1, 1, 1, 1),
                   row(2, 2, 1, 1),
                   row(4, None, 1, 0),
                   row(3, 3, 1, 1))

        # Range query with DISTINCT and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s GROUP BY a LIMIT 2"),
                   row(1, 1, 1, 1),
                   row(2, 2, 1, 1))

        # Range query with ORDER BY
        assertInvalidMessage(cql, table, "ORDER BY is only supported when the partition key is restricted by an EQ or an IN",
                             "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a ORDER BY b DESC, c DESC")

        # Single partition queries
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 2, 1, 4, 4))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 3 GROUP BY a, b"),
                   row(3, None, 3, 0, 1))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 AND b = 2 GROUP BY a, b"),
                   row(2, 2, 2, 1, 1))

        # Single partition queries without aggregates
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 2, 1))

        assertRows(execute(cql, table, "SELECT a, b, s FROM %s WHERE a = 4 GROUP BY a, b"),
                   row(4, 8, None))

        # Single partition queries with wildcard
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 2, 1, 1, 3))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 4 GROUP BY a, b"),
                   row(4, 8, 2, None, 12))

        # Single partition query with LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b LIMIT 1"),
                   row(2, 2, 2, 1, 1))

        # Single partition query with PER PARTITION LIMIT
        # Reproduces #5363:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(2, 2, 2, 1, 1))

        # Single partition queries without aggregates and with LIMIT
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s WHERE a = 2 GROUP BY a, b LIMIT 1"),
                   row(2, 2, 2))

        assertRows(execute(cql, table, "SELECT a, b, s FROM %s WHERE a = 2 GROUP BY a, b LIMIT 2"),
                   row(2, 2, 2),
                   row(2, 4, 2))

        # Single partition queries with DISTINCT
        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a = 2 GROUP BY a"),
                   row(2, 2, 1, 1))

        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a = 4 GROUP BY a"),
                   row(4, None, 1, 0))

        # Single partition query with ORDER BY
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC"),
                   row(2, 4, 2, 1, 1),
                   row(2, 2, 2, 1, 1))

        # Single partition queries with ORDER BY and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 1"),
                   row(2, 4, 2, 1, 1))

        # Single partition queries with ORDER BY and PER PARTITION LIMIT
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC PER PARTITION LIMIT 1"),
                  row(2, 4, 2, 1, 1))

        # Multi-partitions queries
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, 2, 1, 4, 4),
                   row(2, 2, 2, 2, 2),
                   row(3, None, 3, 0, 1),
                   row(4, 8, None, 1, 0))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, 2, 1, 2, 2),
                   row(1, 4, 1, 2, 2),
                   row(2, 2, 2, 1, 1),
                   row(2, 4, 2, 1, 1),
                   row(3, None, 3, 0, 1),
                   row(4, 8, None, 1, 0))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) AND b = 2 GROUP BY a, b"),
                   row(1, 2, 1, 2, 2),
                   row(2, 2, 2, 1, 1))

        # Multi-partitions queries without aggregates
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, 2, 1),
                   row(2, 2, 2),
                   row(3, None, 3),
                   row(4, 8, None))

        assertRows(execute(cql, table, "SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, 2, 1),
                   row(1, 4, 1),
                   row(2, 2, 2),
                   row(2, 4, 2),
                   row(3, None, 3),
                   row(4, 8, None))

        # Multi-partitions queries with wildcard
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, 2, 1, 1, 3),
                   row(2, 2, 3, 2, 3),
                   row(3, None, None, 3, None),
                   row(4, 8, 2, None, 12))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, 2, 1, 1, 3),
                   row(1, 4, 2, 1, 12),
                   row(2, 2, 3, 2, 3),
                   row(2, 4, 3, 2, 6),
                   row(3, None, None, 3, None),
                   row(4, 8, 2, None, 12))

        # Multi-partitions query with LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"),
                   row(1, 2, 1, 4, 4),
                   row(2, 2, 2, 2, 2))

        # Multi-partitions query with PER PARTITION LIMIT
        # Reproduces #5363:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 2, 2),
                   row(2, 2, 2, 1, 1),
                   row(3, None, 3, 0, 1),
                   row(4, 8, None, 1, 0))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 2, 2),
                   row(1, 4, 1, 2, 2),
                   row(2, 2, 2, 1, 1),
                   row(2, 4, 2, 1, 1),
                   row(3, None, 3, 0, 1),
                   row(4, 8, None, 1, 0))

        # Multi-partitions queries with PER PARTITION LIMIT and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 3"),
                   row(1, 2, 1, 2, 2),
                   row(2, 2, 2, 1, 1),
                   row(3, None, 3, 0, 1))

        assertRows(execute(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 4 LIMIT 3"),
                   row(1, 2, 1, 2, 2),
                   row(1, 4, 1, 2, 2),
                   row(2, 2, 2, 1, 1))

        # Multi-partitions queries without aggregates and with LIMIT
        # Reproduces #5362:
        assertRows(execute(cql, table, "SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"),
                   row(1, 2, 1),
                   row(2, 2, 2))

        assertRows(execute(cql, table, "SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 10"),
                   row(1, 2, 1),
                   row(1, 4, 1),
                   row(2, 2, 2),
                   row(2, 4, 2),
                   row(3, None, 3),
                   row(4, 8, None))

        # Multi-partitions query with DISTINCT
        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, 1, 1, 1),
                   row(2, 2, 1, 1),
                   row(3, 3, 1, 1),
                   row(4, None, 1, 0))

        # Multi-partitions query with DISTINCT and LIMIT
        # Reproduces #5361:
        assertRows(execute(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"),
                   row(1, 1, 1, 1),
                   row(2, 2, 1, 1))

        # Multi-partitions query with ORDER BY
        # Reproduces #13109:
        assertRows(execute_without_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC"),
                   row(4, 8, None, 1, 0),
                   row(1, 4, 1, 2, 2),
                   row(2, 4, 2, 1, 1),
                   row(2, 2, 2, 1, 1),
                   row(1, 2, 1, 2, 2))

        # Multi-partitions queries with ORDER BY and LIMIT
        # Reproduces #5361:
        assertRows(execute_without_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 2"),
                   row(4, 8, None, 1, 0),
                   row(1, 4, 1, 2, 2))

def testGroupByWithPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, primary key (a, b, c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (2, 2, 3, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (2, 4, 3, 6, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (3, 3, 2, 12, 24)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (4, 8, 2, 12, 24)")

        # Makes sure that we have some tombstones
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2 AND d = 12")
        execute(cql, table, "DELETE FROM %s WHERE a = 3")

        for pageSize in range(1, 10):
            # Range queries
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a", pageSize),
                          row(1, 2, 6, 4, 24),
                          row(2, 2, 6, 2, 12),
                          row(4, 8, 24, 1, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b", pageSize),
                          row(1, 2, 6, 2, 12),
                          row(1, 4, 12, 2, 24),
                          row(2, 2, 6, 1, 6),
                          row(2, 4, 12, 1, 12),
                          row(4, 8, 24, 1, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s", pageSize),
                          row(1, 2, 6, 7, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE b = 2 GROUP BY a, b ALLOW FILTERING",
                                               pageSize),
                          row(1, 2, 6, 2, 12),
                          row(2, 2, 6, 1, 6))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE b = 2 ALLOW FILTERING",
                                               pageSize),
                          row(1, 2, 6, 3, 12))

            # Range queries without aggregates
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c", pageSize),
                          row(1, 2, 1, 3),
                          row(1, 2, 2, 6),
                          row(1, 4, 2, 6),
                          row(2, 2, 3, 3),
                          row(2, 4, 3, 6),
                          row(4, 8, 2, 12))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b", pageSize),
                          row(1, 2, 1, 3),
                          row(1, 4, 2, 6),
                          row(2, 2, 3, 3),
                          row(2, 4, 3, 6),
                          row(4, 8, 2, 12))

            # Range queries with wildcard
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b, c", pageSize),
                          row(1, 2, 1, 3, 6),
                          row(1, 2, 2, 6, 12),
                          row(1, 4, 2, 6, 12),
                          row(2, 2, 3, 3, 6),
                          row(2, 4, 3, 6, 12),
                          row(4, 8, 2, 12, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b", pageSize),
                          row(1, 2, 1, 3, 6),
                          row(1, 4, 2, 6, 12),
                          row(2, 2, 3, 3, 6),
                          row(2, 4, 3, 6, 12),
                          row(4, 8, 2, 12, 24))

            # Range query with LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b LIMIT 2",
                                               pageSize),
                          row(1, 2, 6, 2, 12),
                          row(1, 4, 12, 2, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s LIMIT 2",
                                               pageSize),
                          row(1, 2, 6, 7, 24))

            # Range queries with PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 3", pageSize),
                          row(1, 2, 6, 2, 12),
                          row(1, 4, 12, 2, 24),
                          row(2, 2, 6, 1, 6),
                          row(2, 4, 12, 1, 12),
                          row(4, 8, 24, 1, 24))

            # Reproduces #5363:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                          row(1, 2, 6, 2, 12),
                          row(2, 2, 6, 1, 6),
                          row(4, 8, 24, 1, 24))

            # Range query with PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2", pageSize),
                          row(1, 2, 6, 2, 12),
                          row(2, 2, 6, 1, 6))

            # Range query without aggregates and with PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2", pageSize),
                          row(1, 2, 1, 3),
                          row(1, 2, 2, 6),
                          row(2, 2, 3, 3),
                          row(2, 4, 3, 6),
                          row(4, 8, 2, 12))

            # Range queries without aggregates and with LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c LIMIT 3", pageSize),
                          row(1, 2, 1, 3),
                          row(1, 2, 2, 6),
                          row(1, 4, 2, 6))

            # Reproduces #5362:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b LIMIT 3", pageSize),
                          row(1, 2, 1, 3),
                          row(1, 4, 2, 6),
                          row(2, 2, 3, 3))

            # Range query without aggregates, with PER PARTITION LIMIT and with LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3", pageSize),
                          row(1, 2, 1, 3),
                          row(1, 2, 2, 6),
                          row(2, 2, 3, 3))

            # Range queries with wildcard and with LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b, c LIMIT 3", pageSize),
                          row(1, 2, 1, 3, 6),
                          row(1, 2, 2, 6, 12),
                          row(1, 4, 2, 6, 12))

            # Reproduces #5362:
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b LIMIT 3", pageSize),
                          row(1, 2, 1, 3, 6),
                          row(1, 4, 2, 6, 12),
                          row(2, 2, 3, 3, 6))

            # Range queries with wildcard and with PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2", pageSize),
                          row(1, 2, 1, 3, 6),
                          row(1, 2, 2, 6, 12),
                          row(2, 2, 3, 3, 6),
                          row(2, 4, 3, 6, 12),
                          row(4, 8, 2, 12, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                          row(1, 2, 1, 3, 6),
                          row(2, 2, 3, 3, 6),
                          row(4, 8, 2, 12, 24))

            # Range queries with wildcard, with PER PARTITION LIMIT and LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3", pageSize),
                          row(1, 2, 1, 3, 6),
                          row(1, 2, 2, 6, 12),
                          row(2, 2, 3, 3, 6))

            # Range query with DISTINCT
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, count(a)FROM %s GROUP BY a", pageSize),
                          row(1, 1),
                          row(2, 1),
                          row(4, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, count(a)FROM %s", pageSize),
                          row(1, 3))

            # Range query with DISTINCT and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, count(a)FROM %s GROUP BY a LIMIT 2", pageSize),
                          row(1, 1),
                          row(2, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, count(a)FROM %s LIMIT 2", pageSize),
                          row(1, 3))

            # Range query with ORDER BY
            assertInvalidMessage(cql, table, "ORDER BY is only supported when the partition key is restricted by an EQ or an IN",
                                 "SELECT a, b, c, count(b), max(e) FROM %s GROUP BY a, b ORDER BY b DESC, c DESC")

            # Single partition queries
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c",
                                               pageSize),
                          row(1, 2, 6, 1, 6),
                          row(1, 2, 12, 1, 12),
                          row(1, 4, 12, 2, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1", pageSize),
                          row(1, 2, 6, 4, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY a, b, c",
                                               pageSize),
                          row(1, 2, 6, 1, 6),
                          row(1, 2, 12, 1, 12))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2",
                                               pageSize),
                          row(1, 2, 6, 2, 12))

            # Single partition queries without aggregates
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b", pageSize),
                          row(1, 2, 1, 3),
                          row(1, 4, 2, 6))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c", pageSize),
                          row(1, 2, 1, 3),
                          row(1, 2, 2, 6),
                          row(1, 4, 2, 6))

            # Single partition queries with wildcard
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c", pageSize),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12),
                       row(1, 4, 2, 6, 12))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b", pageSize),
                       row(1, 2, 1, 3, 6),
                       row(1, 4, 2, 6, 12))

            # Single partition query with DISTINCT
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a",
                                               pageSize),
                          row(1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a",
                                               pageSize),
                          row(1, 1))

            # Single partition queries with LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 10",
                                               pageSize),
                          row(1, 2, 6, 1, 6),
                          row(1, 2, 12, 1, 12),
                          row(1, 4, 12, 2, 24))

            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2",
                                               pageSize),
                          row(1, 2, 6, 1, 6),
                          row(1, 2, 12, 1, 12))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 LIMIT 2",
                                               pageSize),
                          row(1, 2, 6, 4, 24))

            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 1",
                                               pageSize),
                          row(1, 6))

            # Single partition query with PER PARTITION LIMIT
            # Reproduces #5363:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2",
                                               pageSize),
                          row(1, 2, 6, 1, 6),
                          row(1, 2, 12, 1, 12))

            # Single partition queries without aggregates and with LIMIT
            # Reproduces #5362:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 2",
                                               pageSize),
                          row(1, 2, 1, 3),
                          row(1, 4, 2, 6))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1",
                                               pageSize),
                          row(1, 2, 1, 3))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2",
                                               pageSize),
                          row(1, 2, 1, 3),
                          row(1, 2, 2, 6))

            # Single partition queries with wildcard and with LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2", pageSize),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1", pageSize),
                       row(1, 2, 1, 3, 6))

            # Single partition queries with wildcard and with PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2", pageSize),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                       row(1, 2, 1, 3, 6))

            # Single partition queries with ORDER BY
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC",
                                               pageSize),
                          row(1, 4, 24, 2, 24),
                          row(1, 2, 12, 1, 12),
                          row(1, 2, 6, 1, 6))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 ORDER BY b DESC, c DESC",
                                               pageSize),
                          row(1, 4, 24, 4, 24))

            # Single partition queries with ORDER BY and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 2",
                                               pageSize),
                          row(1, 4, 24, 2, 24),
                          row(1, 2, 12, 1, 12))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 ORDER BY b DESC, c DESC LIMIT 2",
                                               pageSize),
                          row(1, 4, 24, 4, 24))

            # Single partition queries with ORDER BY and PER PARTITION LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC PER PARTITION LIMIT 2",
                                               pageSize),
                          row(1, 4, 24, 2, 24),
                          row(1, 2, 12, 1, 12))

            # Multi-partitions queries
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c",
                                               pageSize),
                          row(1, 2, 6, 1, 6),
                          row(1, 2, 12, 1, 12),
                          row(1, 4, 12, 2, 24),
                          row(2, 2, 6, 1, 6),
                          row(2, 4, 12, 1, 12),
                          row(4, 8, 24, 1, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4)",
                                               pageSize),
                          row(1, 2, 6, 7, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) AND b = 2 GROUP BY a, b, c",
                                               pageSize),
                          row(1, 2, 6, 1, 6),
                          row(1, 2, 12, 1, 12),
                          row(2, 2, 6, 1, 6))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) AND b = 2",
                                               pageSize),
                          row(1, 2, 6, 3, 12))

            # Multi-partitions queries with PER PARTITION LIMIT
            # Reproduces #5362:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 2",
                                               pageSize),
                          row(1, 2, 6, 1, 6),
                          row(1, 2, 12, 1, 12),
                          row(2, 2, 6, 1, 6),
                          row(2, 4, 12, 1, 12),
                          row(4, 8, 24, 1, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 1",
                                               pageSize),
                          row(1, 2, 6, 1, 6),
                          row(2, 2, 6, 1, 6),
                          row(4, 8, 24, 1, 24))

            # Multi-partitions queries without aggregates
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b",
                                               pageSize),
                          row(1, 2, 1, 3),
                          row(1, 4, 2, 6),
                          row(2, 2, 3, 3),
                          row(2, 4, 3, 6),
                          row(4, 8, 2, 12))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c",
                                               pageSize),
                          row(1, 2, 1, 3),
                          row(1, 2, 2, 6),
                          row(1, 4, 2, 6),
                          row(2, 2, 3, 3),
                          row(2, 4, 3, 6),
                          row(4, 8, 2, 12))

            # Multi-partitions with wildcard
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c", pageSize),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12),
                       row(1, 4, 2, 6, 12),
                       row(2, 2, 3, 3, 6),
                       row(2, 4, 3, 6, 12),
                       row(4, 8, 2, 12, 24))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b", pageSize),
                       row(1, 2, 1, 3, 6),
                       row(1, 4, 2, 6, 12),
                       row(2, 2, 3, 3, 6),
                       row(2, 4, 3, 6, 12),
                       row(4, 8, 2, 12, 24))

            # Multi-partitions queries with DISTINCT
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a",
                                               pageSize),
                          row(1, 1),
                          row(2, 1),
                          row(4, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4)",
                                               pageSize),
                          row(1, 3))

            # Multi-partitions query with DISTINCT and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 1),
                          row(2, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) LIMIT 2",
                                               pageSize),
                          row(1, 3))

def testGroupByWithRangeNamesQueryWithPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, primary key (a, b, c))") as table:
        for i in range(1,5):
            for j in range(1,5):
                for k in range(1,5):
                    execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, k, i + j)

        # Makes sure that we have some tombstones
        execute(cql, table, "DELETE FROM %s WHERE a = 3")

        for pageSize in range(1, 2):   # NYH: this one-iteration loop was in the original test!
            # Range queries
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a ALLOW FILTERING", pageSize),
                          row(1, 1, 2, 2, 2),
                          row(2, 1, 3, 2, 3),
                          row(4, 1, 5, 2, 5))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b ALLOW FILTERING", pageSize),
                          row(1, 1, 2, 2, 2),
                          row(2, 1, 3, 2, 3),
                          row(4, 1, 5, 2, 5))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b ALLOW FILTERING", pageSize),
                          row(1, 1, 2, 2, 2),
                          row(1, 2, 3, 2, 3),
                          row(2, 1, 3, 2, 3),
                          row(2, 2, 4, 2, 4),
                          row(4, 1, 5, 2, 5),
                          row(4, 2, 6, 2, 6))

            # Range queries with LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a LIMIT 5 ALLOW FILTERING", pageSize),
                          row(1, 1, 2, 2, 2),
                          row(2, 1, 3, 2, 3),
                          row(4, 1, 5, 2, 5))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING", pageSize),
                          row(1, 1, 2, 2, 2),
                          row(2, 1, 3, 2, 3),
                          row(4, 1, 5, 2, 5))

            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING", pageSize),
                          row(1, 1, 2, 2, 2),
                          row(1, 2, 3, 2, 3),
                          row(2, 1, 3, 2, 3))

            # Range queries with PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 2, 2, 2),
                          row(2, 1, 3, 2, 3),
                          row(4, 1, 5, 2, 5))

            # Reproduces #5363:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 ALLOW FILTERING", pageSize),
                          row(1, 1, 2, 2, 2),
                          row(2, 1, 3, 2, 3),
                          row(4, 1, 5, 2, 5))

            # Range queries with PER PARTITION LIMIT and LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 5 ALLOW FILTERING", pageSize),
                          row(1, 1, 2, 2, 2),
                          row(2, 1, 3, 2, 3),
                          row(4, 1, 5, 2, 5))

            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 2, 2, 2),
                          row(2, 1, 3, 2, 3))

def testGroupByWithStaticColumnsWithPaging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, s int static, d int, primary key (a, b, c))") as table:
        # ------------------------------------
        # Test with non static columns empty
        # ------------------------------------
        execute(cql, table, "UPDATE %s SET s = 1 WHERE a = 1")
        execute(cql, table, "UPDATE %s SET s = 2 WHERE a = 2")
        execute(cql, table, "UPDATE %s SET s = 3 WHERE a = 4")

        for pageSize in range(1,10):
            # Range queries
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a", pageSize),
                          row(1, None, 1, 0, 1),
                          row(2, None, 2, 0, 1),
                          row(4, None, 3, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b", pageSize),
                          row(1, None, 1, 0, 1),
                          row(2, None, 2, 0, 1),
                          row(4, None, 3, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s", pageSize),
                          row(1, None, 1, 0, 3))

            # Range query without aggregates
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s GROUP BY a, b", pageSize),
                          row(1, None, 1),
                          row(2, None, 2),
                          row(4, None, 3))

            # Range query with wildcard
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b", pageSize),
                       row(1, None, None, 1, None),
                       row(2, None, None, 2, None),
                       row(4, None, None, 3, None))

            # Range query with LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b LIMIT 2",
                                               pageSize),
                          row(1, None, 1, 0, 1),
                          row(2, None, 2, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s LIMIT 2", pageSize),
                          row(1, None, 1, 0, 3))

            # Range query with PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a PER PARTITION LIMIT 2", pageSize),
                          row(1, None, 1, 0, 1),
                          row(2, None, 2, 0, 1),
                          row(4, None, 3, 0, 1))

            # Range query with PER PARTITION LIMIT and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a PER PARTITION LIMIT 2 LIMIT 2", pageSize),
                          row(1, None, 1, 0, 1),
                          row(2, None, 2, 0, 1))

            # Range queries with DISTINCT
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s GROUP BY a", pageSize),
                          row(1, 1, 1),
                          row(2, 2, 1),
                          row(4, 3, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s ", pageSize),
                          row(1, 1, 3))

            # Range queries with DISTINCT and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s GROUP BY a LIMIT 2", pageSize),
                          row(1, 1, 1),
                          row(2, 2, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s LIMIT 2", pageSize),
                          row(1, 1, 3))

            # Single partition queries
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a",
                                               pageSize),
                          row(1, None, 1, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a, b",
                                               pageSize),
                          row(1, None, 1, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1", pageSize),
                          row(1, None, 1, 0, 1))

            # Single partition query without aggregates
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s WHERE a = 1 GROUP BY a, b", pageSize),
                          row(1, None, 1))

            # Single partition query with wildcard
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a, b", pageSize),
                       row(1, None, None, 1, None))

            # Single partition queries with LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a, b LIMIT 2",
                                               pageSize),
                          row(1, None, 1, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 LIMIT 2",
                                               pageSize),
                          row(1, None, 1, 0, 1))


            # Single partition queries with DISTINCT
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s WHERE a = 1 GROUP BY a",
                                               pageSize),
                          row(1, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s WHERE a = 1", pageSize),
                          row(1, 1, 1))

            # Multi-partitions queries
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a",
                                               pageSize),
                          row(1, None, 1, 0, 1),
                          row(2, None, 2, 0, 1),
                          row(4, None, 3, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b",
                                               pageSize),
                          row(1, None, 1, 0, 1),
                          row(2, None, 2, 0, 1),
                          row(4, None, 3, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4)",
                                               pageSize),
                          row(1, None, 1, 0, 3))

            # Multi-partitions query without aggregates
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b",
                                               pageSize),
                          row(1, None, 1),
                          row(2, None, 2),
                          row(4, None, 3))

            # Multi-partitions query with LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 2",
                                               pageSize),
                          row(1, None, 1, 0, 1),
                          row(2, None, 2, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) LIMIT 2",
                                               pageSize),
                          row(1, None, 1, 0, 3))

            # Multi-partitions query with PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a PER PARTITION LIMIT 2",
                                               pageSize),
                          row(1, None, 1, 0, 1),
                          row(2, None, 2, 0, 1),
                          row(4, None, 3, 0, 1))

            # Multi-partitions query with PER PARTITION LIMIT and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a PER PARTITION LIMIT 2 LIMIT 2",
                                               pageSize),
                          row(1, None, 1, 0, 1),
                          row(2, None, 2, 0, 1))

            # Multi-partitions queries with DISTINCT
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a",
                                               pageSize),
                          row(1, 1, 1),
                          row(2, 2, 1),
                          row(4, 3, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4)",
                                               pageSize),
                          row(1, 1, 3))

            # Multi-partitions queries with DISTINCT and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 1, 1),
                          row(2, 2, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4) LIMIT 2",
                                               pageSize),
                          row(1, 1, 3))

        # ------------------------------------
        # Test with non static columns
        # ------------------------------------
        execute(cql, table, "UPDATE %s SET s = 3 WHERE a = 3")
        execute(cql, table, "DELETE s FROM %s WHERE a = 4")

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 2, 1, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 2, 2, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 3, 2, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 4, 2, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 4, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 2, 3, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, 4, 3, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (4, 8, 2, 12)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (5, 8, 2, 12)")

        # Makes sure that we have some tombstones
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2")
        execute(cql, table, "DELETE FROM %s WHERE a = 5")

        for pageSize in range(1,10):
            # Range queries
            # Reproduces #21267
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a", pageSize),
                          row(1, 2, 1, 4, 4),
                          row(2, 2, 2, 2, 2),
                          row(4, 8, None, 1, 0),
                          row(3, None, 3, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b", pageSize),
                          row(1, 2, 1, 2, 2),
                          row(1, 4, 1, 2, 2),
                          row(2, 2, 2, 1, 1),
                          row(2, 4, 2, 1, 1),
                          row(4, 8, None, 1, 0),
                          row(3, None, 3, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s", pageSize),
                          row(1, 2, 1, 7, 7))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE b = 2 GROUP BY a, b ALLOW FILTERING",
                                               pageSize),
                          row(1, 2, 1, 2, 2),
                          row(2, 2, 2, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE b = 2 ALLOW FILTERING",
                                               pageSize),
                          row(1, 2, 1, 3, 3))

            # Range queries without aggregates
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s GROUP BY a", pageSize),
                          row(1, 2, 1),
                          row(2, 2, 2),
                          row(4, 8, None),
                          row(3, None, 3))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s GROUP BY a, b", pageSize),
                          row(1, 2, 1),
                          row(1, 4, 1),
                          row(2, 2, 2),
                          row(2, 4, 2),
                          row(4, 8, None),
                          row(3, None, 3))

            # Range queries with wildcard
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(2, 2, 3, 2, 3),
                       row(4, 8, 2, None, 12),
                       row(3, None, None, 3, None))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(1, 4, 2, 1, 12),
                       row(2, 2, 3, 2, 3),
                       row(2, 4, 3, 2, 6),
                       row(4, 8, 2, None, 12),
                       row(3, None, None, 3, None))

            # Range query with LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 2, 1, 4, 4),
                          row(2, 2, 2, 2, 2))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s LIMIT 2", pageSize),
                          row(1, 2, 1, 7, 7))

            # Range queries without aggregates and with LIMIT
            # Reproduces #5362:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s GROUP BY a LIMIT 2", pageSize),
                          row(1, 2, 1),
                          row(2, 2, 2))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s GROUP BY a, b LIMIT 10", pageSize),
                          row(1, 2, 1),
                          row(1, 4, 1),
                          row(2, 2, 2),
                          row(2, 4, 2),
                          row(4, 8, None),
                          row(3, None, 3))

            # Range queries with wildcard and with LIMIT
            # Reproduces #5362:
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a LIMIT 2", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(2, 2, 3, 2, 3))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b LIMIT 10", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(1, 4, 2, 1, 12),
                       row(2, 2, 3, 2, 3),
                       row(2, 4, 3, 2, 6),
                       row(4, 8, 2, None, 12),
                       row(3, None, None, 3, None))

            # Range queries with PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 2", pageSize),
                          row(1, 2, 1, 2, 2),
                          row(1, 4, 1, 2, 2),
                          row(2, 2, 2, 1, 1),
                          row(2, 4, 2, 1, 1),
                          row(4, 8, None, 1, 0),
                          row(3, None, 3, 0, 1))

            # Reproduces #5363:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                          row(1, 2, 1, 2, 2),
                          row(2, 2, 2, 1, 1),
                          row(4, 8, None, 1, 0),
                          row(3, None, 3, 0, 1))

            # Range queries with wildcard and PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(2, 2, 3, 2, 3),
                       row(4, 8, 2, None, 12),
                       row(3, None, None, 3, None))

            # Range queries with PER PARTITION LIMIT and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 3", pageSize),
                          row(1, 2, 1, 2, 2),
                          row(1, 4, 1, 2, 2),
                          row(2, 2, 2, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 3", pageSize),
                          row(1, 2, 1, 2, 2),
                          row(2, 2, 2, 1, 1),
                          row(4, 8, None, 1, 0))

            # Range queries with wildcard, PER PARTITION LIMIT and LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(2, 2, 3, 2, 3))

            # Range query without aggregates and with PER PARTITION LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                          row(1, 2, 1),
                          row(2, 2, 2),
                          row(4, 8, None),
                          row(3, None, 3))

            # Range queries with DISTINCT
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s GROUP BY a", pageSize),
                          row(1, 1, 1, 1),
                          row(2, 2, 1, 1),
                          row(4, None, 1, 0),
                          row(3, 3, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s", pageSize),
                          row(1, 1, 4, 3))

            # Range queries with DISTINCT and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 1, 1, 1),
                          row(2, 2, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s LIMIT 2", pageSize),
                          row(1, 1, 4, 3))

            # Single partition queries
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a",
                                               pageSize),
                          row(1, 2, 1, 4, 4))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 3 GROUP BY a, b",
                                               pageSize),
                          row(3, None, 3, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 3",
                                               pageSize),
                          row(3, None, 3, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 AND b = 2 GROUP BY a, b",
                                               pageSize),
                          row(2, 2, 2, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 AND b = 2",
                                               pageSize),
                          row(2, 2, 2, 1, 1))

            # Single partition queries without aggregates
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s WHERE a = 1 GROUP BY a", pageSize),
                          row(1, 2, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s WHERE a = 4 GROUP BY a, b", pageSize),
                          row(4, 8, None))

            # Single partition queries with wildcard
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 1 GROUP BY a", pageSize),
                       row(1, 2, 1, 1, 3))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a = 4 GROUP BY a, b", pageSize),
                       row(4, 8, 2, None, 12))

            # Single partition queries with LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b LIMIT 1",
                                               pageSize),
                          row(2, 2, 2, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 LIMIT 1",
                                               pageSize),
                          row(2, 2, 2, 2, 2))

            # Single partition queries without aggregates and with LIMIT
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s WHERE a = 2 GROUP BY a, b LIMIT 1", pageSize),
                          row(2, 2, 2))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s WHERE a = 2 GROUP BY a, b LIMIT 2", pageSize),
                          row(2, 2, 2),
                          row(2, 4, 2))

            # Single partition queries with DISTINCT
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a = 2 GROUP BY a",
                                               pageSize),
                          row(2, 2, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a = 4 GROUP BY a",
                                               pageSize),
                          row(4, None, 1, 0))

            # Single partition queries with ORDER BY
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC",
                                               pageSize),
                          row(2, 4, 2, 1, 1),
                          row(2, 2, 2, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 ORDER BY b DESC, c DESC",
                                               pageSize),
                          row(2, 4, 2, 2, 2))

            # Single partition queries with ORDER BY and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 1",
                                               pageSize),
                          row(2, 4, 2, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 ORDER BY b DESC, c DESC LIMIT 2",
                                               pageSize),
                          row(2, 4, 2, 2, 2))

            # Multi-partitions queries
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a",
                                               pageSize),
                          row(1, 2, 1, 4, 4),
                          row(2, 2, 2, 2, 2),
                          row(3, None, 3, 0, 1),
                          row(4, 8, None, 1, 0))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b",
                                               pageSize),
                          row(1, 2, 1, 2, 2),
                          row(1, 4, 1, 2, 2),
                          row(2, 2, 2, 1, 1),
                          row(2, 4, 2, 1, 1),
                          row(3, None, 3, 0, 1),
                          row(4, 8, None, 1, 0))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4)",
                                               pageSize),
                          row(1, 2, 1, 7, 7))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) AND b = 2 GROUP BY a, b",
                                               pageSize),
                          row(1, 2, 1, 2, 2),
                          row(2, 2, 2, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) AND b = 2",
                                               pageSize),
                          row(1, 2, 1, 3, 3))

            # Multi-partitions queries without aggregates
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a", pageSize),
                          row(1, 2, 1),
                          row(2, 2, 2),
                          row(3, None, 3),
                          row(4, 8, None))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b",
                                               pageSize),
                          row(1, 2, 1),
                          row(1, 4, 1),
                          row(2, 2, 2),
                          row(2, 4, 2),
                          row(3, None, 3),
                          row(4, 8, None))

            # Multi-partitions queries with wildcard
            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(2, 2, 3, 2, 3),
                       row(3, None, None, 3, None),
                       row(4, 8, 2, None, 12))

            assertRowsNet(execute_with_paging(cql, table, "SELECT * FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(1, 4, 2, 1, 12),
                       row(2, 2, 3, 2, 3),
                       row(2, 4, 3, 2, 6),
                       row(3, None, None, 3, None),
                       row(4, 8, 2, None, 12))

            # Multi-partitions queries with LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 2, 1, 4, 4),
                          row(2, 2, 2, 2, 2))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) LIMIT 2",
                                               pageSize),
                          row(1, 2, 1, 7, 7))

            # Multi-partitions queries without aggregates and with LIMIT
            # Reproduces #5362:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 2, 1),
                          row(2, 2, 2))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 10",
                                               pageSize),
                          row(1, 2, 1),
                          row(1, 4, 1),
                          row(2, 2, 2),
                          row(2, 4, 2),
                          row(3, None, 3),
                          row(4, 8, None))

            # Multi-partitions queries with PER PARTITION LIMIT
            # Reproduces #5363:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1",
                                               pageSize),
                          row(1, 2, 1, 2, 2),
                          row(2, 2, 2, 1, 1),
                          row(3, None, 3, 0, 1),
                          row(4, 8, None, 1, 0))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 3",
                                               pageSize),
                          row(1, 2, 1, 2, 2),
                          row(1, 4, 1, 2, 2),
                          row(2, 2, 2, 1, 1),
                          row(2, 4, 2, 1, 1),
                          row(3, None, 3, 0, 1),
                          row(4, 8, None, 1, 0))

            # Multi-partitions queries with PER PARTITION LIMIT and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 3",
                                               pageSize),
                          row(1, 2, 1, 2, 2),
                          row(2, 2, 2, 1, 1),
                          row(3, None, 3, 0, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 3 LIMIT 10",
                                               pageSize),
                          row(1, 2, 1, 2, 2),
                          row(1, 4, 1, 2, 2),
                          row(2, 2, 2, 1, 1),
                          row(2, 4, 2, 1, 1),
                          row(3, None, 3, 0, 1),
                          row(4, 8, None, 1, 0))

            # Multi-partitions queries with DISTINCT
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a",
                                               pageSize),
                          row(1, 1, 1, 1),
                          row(2, 2, 1, 1),
                          row(3, 3, 1, 1),
                          row(4, None, 1, 0))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4)",
                                               pageSize),
                          row(1, 1, 4, 3))

            # Multi-partitions query with DISTINCT and LIMIT
            # Reproduces #5361:
            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 1, 1, 1),
                          row(2, 2, 1, 1))

            assertRowsNet(execute_with_paging(cql, table, "SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4) LIMIT 2",
                                               pageSize),
                          row(1, 1, 4, 3))

# The following tests check GROUP BY on time ranges, using the floor()
# function, which Cassandra added in Cassandra 5.0 (CASSANDRA-11871).
# Reproduces SCYLLADB-5211 (the floor() function and grouping by time ranges)

# Helpers for the values the Java test builds, all in UTC:
def toTimestamp(s):
    # s looks like "2016-09-27 16:10:00 UTC". The Python driver returns
    # timestamps as naive datetime objects in UTC.
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S UTC")

def toTimeUUID(s):
    return uuid_from_time(datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc))

def toDate(s):
    return Date(s)

def toTime(s):
    return Time(s)

compactOptions = ["", " WITH COMPACT STORAGE"]

@pytest.mark.xfail(reason="SCYLLADB-5211")
def testGroupByTimeRangesWithTimestamTypeAndWithoutPaging(cql, test_keyspace, compact_storage):
    for compactOption in compactOptions:
        with create_table(cql, test_keyspace, "(pk int, time timestamp, v int, primary key (pk, time))" + compactOption) as table:
            # The Java test also checks here that invalid durations or starting
            # times are rejected; we moved these checks to
            # testGroupByTimeRangesWithInvalidArguments (see there).

            assertInvalidMessage(cql, table, "Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                                 "SELECT pk, floor(time, 2h, '2016-09-01'), v FROM %s GROUP BY floor(time, 2h, '2016-09-01')")

            assertInvalidMessage(cql, table, "Only monotonic functions are supported in the GROUP BY clause. Got: system.floor : (timestamp, duration(constant), timestamp) -> timestamp",
                                 "SELECT pk, floor(time, 2h, time), v FROM %s GROUP BY pk, floor(time, 2h, time)")

            execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:10:00 UTC', 1)")
            execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:12:00 UTC', 2)")
            execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:14:00 UTC', 3)")
            execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:15:00 UTC', 4)")
            execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:21:00 UTC', 5)")
            execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:22:00 UTC', 6)")
            execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:26:00 UTC', 7)")
            execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:26:20 UTC', 8)")
            execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (2, '2016-09-27 16:26:20 UTC', 10)")
            execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (2, '2016-09-27 16:30:00 UTC', 11)")

            # Test prepared statement
            assertRows(execute(cql, table, "SELECT pk, floor(time, 5m, ?), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m, ?)",
                               toTimestamp("2016-09-27 00:00:00 UTC"),
                               toTimestamp("2016-09-27 00:00:00 UTC")),
                       row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                       row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1),
                       row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2),
                       row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                       row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1),
                       row(2, toTimestamp("2016-09-27 16:30:00 UTC"), 11, 11, 1))

            for startingTime in ["", ", '2016-09-27 UTC'"]:
                assertRows(execute(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m" + startingTime + ")"),
                           row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                           row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1),
                           row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2),
                           row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                           row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1),
                           row(2, toTimestamp("2016-09-27 16:30:00 UTC"), 11, 11, 1))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m" + startingTime + ") LIMIT 2"),
                           row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                           row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m" + startingTime + ") PER PARTITION LIMIT 1"),
                           row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                           row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, floor(time, 5m" + startingTime + ") ORDER BY time DESC"),
                           row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                           row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2),
                           row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1),
                           row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, floor(time, 5m" + startingTime + ") ORDER BY time DESC LIMIT 2"),
                           row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                           row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2))

def insertTimeUUIDRows(cql, table):
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (?, ?, ?)", 1, toTimeUUID("2016-09-27 16:10:00"), 1)
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (?, ?, ?)", 1, toTimeUUID("2016-09-27 16:12:00"), 2)
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (?, ?, ?)", 1, toTimeUUID("2016-09-27 16:14:00"), 3)
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (?, ?, ?)", 1, toTimeUUID("2016-09-27 16:15:00"), 4)
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (?, ?, ?)", 1, toTimeUUID("2016-09-27 16:21:00"), 5)
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (?, ?, ?)", 1, toTimeUUID("2016-09-27 16:22:00"), 6)
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (?, ?, ?)", 1, toTimeUUID("2016-09-27 16:26:00"), 7)
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (?, ?, ?)", 1, toTimeUUID("2016-09-27 16:26:20"), 8)
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (?, ?, ?)", 2, toTimeUUID("2016-09-27 16:26:00"), 10)
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (?, ?, ?)", 2, toTimeUUID("2016-09-27 16:30:00"), 11)

@pytest.mark.xfail(reason="SCYLLADB-5211")
def testGroupByTimeRangesWithTimeUUIDAndWithoutPaging(cql, test_keyspace, compact_storage):
    for compactOption in compactOptions:
        with create_table(cql, test_keyspace, "(pk int, time timeuuid, v int, primary key (pk, time))" + compactOption) as table:
            # The Java test also checks here that invalid durations or starting
            # times are rejected; we moved these checks to
            # testGroupByTimeRangesWithInvalidArguments (see there).
            insertTimeUUIDRows(cql, table)

            for startingTime in ["", ", '2016-09-27 UTC'"]:
                assertRows(execute(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m" + startingTime + ")"),
                           row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                           row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1),
                           row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2),
                           row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                           row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1),
                           row(2, toTimestamp("2016-09-27 16:30:00 UTC"), 11, 11, 1))

                # The Java test uses the new function name to_timestamp(),
                # which Scylla doesn't support yet (SCYLLADB-5141). Cassandra
                # still supports the old name toTimestamp(), so we use it.
                assertRows(execute(cql, table, "SELECT pk, floor(toTimestamp(time), 5m" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(toTimestamp(time), 5m" + startingTime + ")"),
                           row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                           row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1),
                           row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2),
                           row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                           row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1),
                           row(2, toTimestamp("2016-09-27 16:30:00 UTC"), 11, 11, 1))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m" + startingTime + ") LIMIT 2"),
                           row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                           row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m" + startingTime + ") PER PARTITION LIMIT 1"),
                           row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                           row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, floor(time, 5m" + startingTime + ") ORDER BY time DESC"),
                           row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                           row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2),
                           row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1),
                           row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, floor(time, 5m" + startingTime + ") ORDER BY time DESC LIMIT 2"),
                           row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                           row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2))

def insertDateRows(cql, table):
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27', 1)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-28', 2)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-29', 3)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-30', 4)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-10-01', 5)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-10-04', 6)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-10-20', 7)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-11-27', 8)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (2, '2016-11-01', 10)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (2, '2016-11-02', 11)")

@pytest.mark.xfail(reason="SCYLLADB-5211")
def testGroupByTimeRangesWithDateTypeAndWithoutPaging(cql, test_keyspace, compact_storage):
    for compactOption in compactOptions:
        with create_table(cql, test_keyspace, "(pk int, time date, v int, primary key (pk, time))" + compactOption) as table:
            # The Java test also checks here that invalid durations or starting
            # times are rejected; we moved these checks to
            # testGroupByTimeRangesWithInvalidArguments (see there).
            insertDateRows(cql, table)

            for startingTime in ["", ", '2016-06-01'"]:
                assertRows(execute(cql, table, "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 1mo" + startingTime + ")"),
                           row(1, toDate("2016-09-01"), 1, 4, 4),
                           row(1, toDate("2016-10-01"), 5, 7, 3),
                           row(1, toDate("2016-11-01"), 8, 8, 1),
                           row(2, toDate("2016-11-01"), 10, 11, 2))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 1mo" + startingTime + ") LIMIT 2"),
                           row(1, toDate("2016-09-01"), 1, 4, 4),
                           row(1, toDate("2016-10-01"), 5, 7, 3))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 1mo" + startingTime + ") PER PARTITION LIMIT 1"),
                           row(1, toDate("2016-09-01"), 1, 4, 4),
                           row(2, toDate("2016-11-01"), 10, 11, 2))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, floor(time, 1mo" + startingTime + ") ORDER BY time DESC"),
                           row(1, toDate("2016-11-01"), 8, 8, 1),
                           row(1, toDate("2016-10-01"), 5, 7, 3),
                           row(1, toDate("2016-09-01"), 1, 4, 4))

                assertRows(execute(cql, table, "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, floor(time, 1mo" + startingTime + ") ORDER BY time DESC LIMIT 2"),
                           row(1, toDate("2016-11-01"), 8, 8, 1),
                           row(1, toDate("2016-10-01"), 5, 7, 3))

def insertDateTimeRows(cql, table):
    execute(cql, table, "INSERT INTO %s (pk, date, time, v) VALUES (1, '2016-09-27', '16:10:00', 1)")
    execute(cql, table, "INSERT INTO %s (pk, date, time, v) VALUES (1, '2016-09-27', '16:12:00', 2)")
    execute(cql, table, "INSERT INTO %s (pk, date, time, v) VALUES (1, '2016-09-27', '16:14:00', 3)")
    execute(cql, table, "INSERT INTO %s (pk, date, time, v) VALUES (1, '2016-09-27', '16:15:00', 4)")
    execute(cql, table, "INSERT INTO %s (pk, date, time, v) VALUES (1, '2016-09-27', '16:21:00', 5)")
    execute(cql, table, "INSERT INTO %s (pk, date, time, v) VALUES (1, '2016-09-27', '16:22:00', 6)")
    execute(cql, table, "INSERT INTO %s (pk, date, time, v) VALUES (1, '2016-09-27', '16:26:00', 7)")
    execute(cql, table, "INSERT INTO %s (pk, date, time, v) VALUES (1, '2016-09-27', '16:26:20', 8)")
    execute(cql, table, "INSERT INTO %s (pk, date, time, v) VALUES (1, '2016-09-28', '16:26:20', 9)")
    execute(cql, table, "INSERT INTO %s (pk, date, time, v) VALUES (1, '2016-09-28', '16:26:30', 10)")

@pytest.mark.xfail(reason="SCYLLADB-5211")
def testGroupByTimeRangesWithTimeTypeAndWithoutPaging(cql, test_keyspace, compact_storage):
    for compactOption in compactOptions:
        with create_table(cql, test_keyspace, "(pk int, date date, time time, v int, primary key (pk, date, time))" + compactOption) as table:
            # The Java test also checks here that invalid durations or starting
            # times are rejected; we moved these checks to
            # testGroupByTimeRangesWithInvalidArguments (see there).
            insertDateTimeRows(cql, table)

            assertInvalidMessage(cql, table, "Functions are only supported on the last element of the GROUP BY clause",
                                 "SELECT pk, floor(date, 1w), time, min(v), max(v), count(v) FROM %s GROUP BY pk, floor(date, 1w), time")

            assertRows(execute(cql, table, "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s GROUP BY pk, date, floor(time, 5m)"),
                       row(1, toDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3),
                       row(1, toDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1),
                       row(1, toDate("2016-09-27"), toTime("16:20:00"), 5, 6, 2),
                       row(1, toDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2),
                       row(1, toDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2))

            assertRows(execute(cql, table, "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s GROUP BY pk, date, floor(time, 5m) LIMIT 2"),
                       row(1, toDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3),
                       row(1, toDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1))

            assertRows(execute(cql, table, "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, date, floor(time, 5m) ORDER BY date DESC, time DESC"),
                       row(1, toDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2),
                       row(1, toDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2),
                       row(1, toDate("2016-09-27"), toTime("16:20:00"), 5, 6, 2),
                       row(1, toDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1),
                       row(1, toDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3))

            assertRows(execute(cql, table, "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, date, floor(time, 5m) ORDER BY date DESC, time DESC LIMIT 2"),
                       row(1, toDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2),
                       row(1, toDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2))

def insertTimestampRows(cql, table):
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:10:00 UTC', 1)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:12:00 UTC', 2)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:14:00 UTC', 3)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:15:00 UTC', 4)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:21:00 UTC', 5)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:22:00 UTC', 6)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:26:00 UTC', 7)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:26:20 UTC', 8)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (2, '2016-09-27 16:26:20 UTC', 10)")
    execute(cql, table, "INSERT INTO %s (pk, time, v) VALUES (2, '2016-09-27 16:30:00 UTC', 11)")

# The rows that the paging tests on timestamp and timeuuid columns expect -
# they are the same for both types.
def assertTimeRangesWithPaging(cql, table, pageSize, startingTime):
    assertRowsNet(execute_with_paging(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m" + startingTime + ")", pageSize),
                  row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                  row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1),
                  row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2),
                  row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                  row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1),
                  row(2, toTimestamp("2016-09-27 16:30:00 UTC"), 11, 11, 1))

    assertRowsNet(execute_with_paging(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m" + startingTime + ") LIMIT 2", pageSize),
                  row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                  row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1))

    assertRowsNet(execute_with_paging(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m" + startingTime + ") PER PARTITION LIMIT 1", pageSize),
                  row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3),
                  row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1))

    assertRowsNet(execute_with_paging(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, floor(time, 5m" + startingTime + ") ORDER BY time DESC", pageSize),
                  row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                  row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2),
                  row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1),
                  row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3))

    assertRowsNet(execute_with_paging(cql, table, "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, floor(time, 5m" + startingTime + ") ORDER BY time DESC LIMIT 2", pageSize),
                  row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2),
                  row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2))

@pytest.mark.xfail(reason="SCYLLADB-5211")
def testGroupByTimeRangesWithTimestampTypeAndPaging(cql, test_keyspace, compact_storage):
    for compactOption in compactOptions:
        with create_table(cql, test_keyspace, "(pk int, time timestamp, v int, primary key (pk, time))" + compactOption) as table:
            insertTimestampRows(cql, table)

            for pageSize in range(1, 10):
                for startingTime in ["", ", '2016-09-27 UTC'"]:
                    assertTimeRangesWithPaging(cql, table, pageSize, startingTime)

@pytest.mark.xfail(reason="SCYLLADB-5211")
def testGroupByTimeRangesWithTimeUUIDAndPaging(cql, test_keyspace, compact_storage):
    for compactOption in compactOptions:
        with create_table(cql, test_keyspace, "(pk int, time timeuuid, v int, primary key (pk, time))" + compactOption) as table:
            insertTimeUUIDRows(cql, table)

            for pageSize in range(1, 10):
                for startingTime in ["", ", '2016-09-27 UTC'"]:
                    assertTimeRangesWithPaging(cql, table, pageSize, startingTime)

@pytest.mark.xfail(reason="SCYLLADB-5211")
def testGroupByTimeRangesWithDateTypeAndPaging(cql, test_keyspace, compact_storage):
    for compactOption in compactOptions:
        with create_table(cql, test_keyspace, "(pk int, time date, v int, primary key (pk, time))" + compactOption) as table:
            insertDateRows(cql, table)

            for pageSize in range(1, 10):
                for startingTime in ["", ", '2016-06-01'"]:
                    assertRowsNet(execute_with_paging(cql, table, "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 1mo" + startingTime + ")", pageSize),
                                  row(1, toDate("2016-09-01"), 1, 4, 4),
                                  row(1, toDate("2016-10-01"), 5, 7, 3),
                                  row(1, toDate("2016-11-01"), 8, 8, 1),
                                  row(2, toDate("2016-11-01"), 10, 11, 2))

                    assertRowsNet(execute_with_paging(cql, table, "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 1mo" + startingTime + ") LIMIT 2", pageSize),
                                  row(1, toDate("2016-09-01"), 1, 4, 4),
                                  row(1, toDate("2016-10-01"), 5, 7, 3))

                    assertRowsNet(execute_with_paging(cql, table, "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 1mo" + startingTime + ") PER PARTITION LIMIT 1", pageSize),
                                  row(1, toDate("2016-09-01"), 1, 4, 4),
                                  row(2, toDate("2016-11-01"), 10, 11, 2))

                    assertRowsNet(execute_with_paging(cql, table, "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, floor(time, 1mo" + startingTime + ") ORDER BY time DESC", pageSize),
                                  row(1, toDate("2016-11-01"), 8, 8, 1),
                                  row(1, toDate("2016-10-01"), 5, 7, 3),
                                  row(1, toDate("2016-09-01"), 1, 4, 4))

                    assertRowsNet(execute_with_paging(cql, table, "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, floor(time, 1mo" + startingTime + ") ORDER BY time DESC LIMIT 2", pageSize),
                                  row(1, toDate("2016-11-01"), 8, 8, 1),
                                  row(1, toDate("2016-10-01"), 5, 7, 3))

@pytest.mark.xfail(reason="SCYLLADB-5211")
def testGroupByTimeRangesWithTimeTypeAndPaging(cql, test_keyspace, compact_storage):
    for compactOption in compactOptions:
        with create_table(cql, test_keyspace, "(pk int, date date, time time, v int, primary key (pk, date, time))" + compactOption) as table:
            insertDateTimeRows(cql, table)

            for pageSize in range(1, 10):
                assertRowsNet(execute_with_paging(cql, table, "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s GROUP BY pk, date, floor(time, 5m)", pageSize),
                              row(1, toDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3),
                              row(1, toDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1),
                              row(1, toDate("2016-09-27"), toTime("16:20:00"), 5, 6, 2),
                              row(1, toDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2),
                              row(1, toDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2))

                assertRowsNet(execute_with_paging(cql, table, "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s GROUP BY pk, date, floor(time, 5m) LIMIT 2", pageSize),
                              row(1, toDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3),
                              row(1, toDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1))

                assertRowsNet(execute_with_paging(cql, table, "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, date, floor(time, 5m) ORDER BY date DESC, time DESC", pageSize),
                              row(1, toDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2),
                              row(1, toDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2),
                              row(1, toDate("2016-09-27"), toTime("16:20:00"), 5, 6, 2),
                              row(1, toDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1),
                              row(1, toDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3))

                assertRowsNet(execute_with_paging(cql, table, "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s WHERE pk = 1 GROUP BY pk, date, floor(time, 5m) ORDER BY date DESC, time DESC LIMIT 2", pageSize),
                              row(1, toDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2),
                              row(1, toDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2))

# The checks, from the four testGroupByTimeRanges*WithoutPaging tests, that
# floor() in a GROUP BY rejects invalid durations and starting times. Over
# the network, Cassandra fails these requests with a ReadFailure - the
# InvalidRequestException is thrown while reading, on the replica - instead
# of an InvalidRequest error (the Java test doesn't notice this, because
# CQLTester executes the requests internally) - see CASSANDRA-21739. So this
# test is marked cassandra_bug.
@pytest.mark.xfail(reason="SCYLLADB-5211")
def testGroupByTimeRangesWithInvalidArguments(cql, test_keyspace, cassandra_bug):
    for type in ["timestamp", "timeuuid"]:
        with create_table(cql, test_keyspace, "(pk int, time " + type + ", v int, primary key (pk, time))") as table:
            if type == "timestamp":
                insertTimestampRows(cql, table)
            else:
                insertTimeUUIDRows(cql, table)
            for startingTime in ["", ", '2016-09-27 UTC'"]:
                # Checks with duration lower than precisions
                assertInvalidMessage(cql, table, "The floor cannot be computed for the 10us duration as precision is below 1 millisecond",
                                     "SELECT pk, floor(time, 10us" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 10us" + startingTime + ")")

                # Checks with a negative duration
                assertInvalidMessage(cql, table, "Negative durations are not supported by the floor function",
                                     "SELECT pk, floor(time, 10us" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, -5m" + startingTime + ")")

            # Checks with start time is greater than the timestamp
            assertInvalidMessage(cql, table, "The floor function starting time is greater than the provided time",
                                 "SELECT pk, floor(time, 5m, '2016-10-27'), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 5m, '2016-10-27')")

    with create_table(cql, test_keyspace, "(pk int, time date, v int, primary key (pk, time))") as table:
        insertDateRows(cql, table)
        for startingTime in ["", ", '2016-06-01'"]:
            # Checks with duration lower than precisions
            assertInvalidMessage(cql, table, "The floor on date values cannot be computed for the 1h duration as precision is below 1 day",
                                 "SELECT pk, floor(time, 1h" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 1h" + startingTime + ")")

            # Checks with a negative duration
            assertInvalidMessage(cql, table, "Negative durations are not supported by the floor function",
                                 "SELECT pk, floor(time, -1mo" + startingTime + "), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, -1mo" + startingTime + ")")

        # Checks with start time is greater than the timestamp
        assertInvalidMessage(cql, table, "The floor function starting time is greater than the provided time",
                             "SELECT pk, floor(time, 1mo, '2017-01-01'), min(v), max(v), count(v) FROM %s GROUP BY pk, floor(time, 1mo, '2017-01-01')")

    with create_table(cql, test_keyspace, "(pk int, date date, time time, v int, primary key (pk, date, time))") as table:
        insertDateTimeRows(cql, table)
        # Checks with duration greater than dayprecisions
        assertInvalidMessage(cql, table, "For time values, the floor can only be computed for durations smaller that a day",
                             "SELECT pk, date, floor(time, 10d), min(v), max(v), count(v) FROM %s GROUP BY pk, date, floor(time, 10d)")

        # Checks with a negative duration
        assertInvalidMessage(cql, table, "Negative durations are not supported by the floor function",
                             "SELECT pk, date, floor(time, -10m), min(v), max(v), count(v) FROM %s GROUP BY pk, date, floor(time, -10m)")
