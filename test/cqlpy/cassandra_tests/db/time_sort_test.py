# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of TimeSortTest.java from Cassandra's
# test/unit/org/apache/cassandra/db directory.

from ..porting import *

def testMixedSources(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 0, 100, 0, 100)
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 0, 0, 1, 0)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b >= ? LIMIT 1000", 0, 10), row(0, 100, 0))

def testTimeSort(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        for i in range(900, 1000):
            for j in range(8):
                execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", i, j * 2, 0, j * 2)

        validateTimeSort(cql, table)
        flush(cql, table)
        validateTimeSort(cql, table)

        # interleave some new data to test memtable + sstable
        for j in range(4):
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 900, j * 2 + 1, 1, j * 2 + 1)

        # and some overwrites
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 900, 0, 2, 100)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 900, 10, 2, 100)

        # verify
        results = list(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b >= ? LIMIT 1000", 900, 0))
        assert len(results) == 12
        for j in range(8):
            assert results[j].b == j

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b IN (?, ?)", 900, 0, 10),
                   row(900, 0, 2),
                   row(900, 10, 2))

def validateTimeSort(cql, table):
    for i in range(900, 1000):
        for j in range(0, 8, 3):
            results = list(execute(cql, table, "SELECT writetime(c) AS wt FROM %s WHERE a = ? AND b >= ? LIMIT 1000", i, j * 2))
            assert len(results) == 8 - j
            k = j
            for r in results:
                assert r.wt == k * 2
                k += 1
