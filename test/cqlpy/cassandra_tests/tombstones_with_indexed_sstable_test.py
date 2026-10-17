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
from cassandra.concurrent import execute_concurrent_with_args
import random
import string

# The tests testTombstoneBoundariesInIndexCached and
# testTombstoneBoundariesInIndexNotCached were not translated, because they
# inspect Cassandra's internal sstable index entries to place range deletions
# exactly on an index block boundary (reproducing CASSANDRA-11158).

def testActiveTombstoneInIndexCached(cql, test_keyspace):
    activeTombstoneInIndex(cql, test_keyspace, "ALL")

def testActiveTombstoneInIndexNotCached(cql, test_keyspace):
    activeTombstoneInIndex(cql, test_keyspace, "NONE")

# The Java test runs the following thousands of INSERT and SELECT statements
# one after another. To make the test faster, we run them concurrently.
def insertRows(cql, table, column, timestamp, rows, text):
    stmt = cql.prepare(f"INSERT INTO {table}(k, t, {column}) VALUES (?, ?, ?) USING TIMESTAMP {timestamp}")
    execute_concurrent_with_args(cql, stmt, [(0, i, text) for i in range(rows)], concurrency=100, raise_on_first_error=True)

def activeTombstoneInIndex(cql, test_keyspace, cacheKeys):
    ROWS = 1000
    VALUE_LENGTH = 100

    # On a single-node Scylla, tombstone_gc's default "repair" mode makes
    # tombstones purgeable as soon as the second of their deletion has
    # passed. So this test, with its descending LIMIT 1 queries, also
    # reproduces SCYLLADB-5187 (the row cache lost purgeable range tombstones
    # in reversed reads, so later reads returned deleted data).
    with create_table(cql, test_keyspace, "(k int, t int, v1 text, v2 text, v3 text, v4 text, PRIMARY KEY (k, t)) WITH caching = { 'keys' : '" + cacheKeys + "' }") as table:
        text = makeRandomString(VALUE_LENGTH)

        # Write a large-enough partition to be indexed.
        insertRows(cql, table, "v1", 1, ROWS, text)
        # Add v2 that should survive part of the deletion we later insert
        insertRows(cql, table, "v2", 3, ROWS, text)
        flush(cql, table)

        # Now delete parts of this partition, but add enough new data to make sure the deletion spans index blocks
        minDeleted1 = ROWS // 10
        maxDeleted1 = 5 * ROWS // 10
        execute(cql, table, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = 0 AND t >= ? AND t < ?", minDeleted1, maxDeleted1)

        # Delete again to make a boundary
        minDeleted2 = 4 * ROWS // 10
        maxDeleted2 = 9 * ROWS // 10
        execute(cql, table, "DELETE FROM %s USING TIMESTAMP 4 WHERE k = 0 AND t >= ? AND t < ?", minDeleted2, maxDeleted2)

        # Add v3 surviving that deletion too and also ensuring the two deletions span index blocks
        insertRows(cql, table, "v3", 5, ROWS, text)
        flush(cql, table)

        # test deletions worked
        verifyExpectedActiveTombstoneRows(cql, table, ROWS, text, minDeleted1, minDeleted2, maxDeleted2)

        # Test again compacted. This is much easier to pass and doesn't actually test active tombstones in index
        compact(cql, table)
        verifyExpectedActiveTombstoneRows(cql, table, ROWS, text, minDeleted1, minDeleted2, maxDeleted2)

def verifyExpectedActiveTombstoneRows(cql, table, ROWS, text, minDeleted1, minDeleted2, maxDeleted2):
    assert_row_count(execute(cql, table, "SELECT t FROM %s WHERE k = ? AND v1 = ? ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted1))
    assert_row_count(execute(cql, table, "SELECT t FROM %s WHERE k = ? AND v1 = ? ORDER BY t DESC ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted1))
    assert_row_count(execute(cql, table, "SELECT t FROM %s WHERE k = ? AND v2 = ? ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted2))
    assert_row_count(execute(cql, table, "SELECT t FROM %s WHERE k = ? AND v2 = ? ORDER BY t DESC ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted2))
    assert_row_count(execute(cql, table, "SELECT t FROM %s WHERE k = ? AND v3 = ? ALLOW FILTERING", 0, text), ROWS)
    assert_row_count(execute(cql, table, "SELECT t FROM %s WHERE k = ? AND v3 = ? ORDER BY t DESC ALLOW FILTERING", 0, text), ROWS)
    # test index yields the correct active deletions
    ascending = cql.prepare(f"SELECT v1,v2,v3 FROM {table} WHERE k = ? AND t >= ? LIMIT 1")
    descending = cql.prepare(f"SELECT v1,v2,v3 FROM {table} WHERE k = ? AND t <= ? ORDER BY t DESC LIMIT 1")
    args = [(0, i) for i in range(ROWS)]
    for stmt in [ascending, descending]:
        results = execute_concurrent_with_args(cql, stmt, args, concurrency=100, raise_on_first_error=True)
        for i, (success, result) in enumerate(results):
            v1Expected = text if i < minDeleted1 or i >= maxDeleted2 else None
            v2Expected = text if i < minDeleted2 or i >= maxDeleted2 else None
            assert_rows(result, row(v1Expected, v2Expected, text))

def makeRandomString(length):
    # Note that the original Java function only sets every second character
    # (the rest are null characters), probably by mistake. We do the same.
    chars = ['\0'] * length
    i = 0
    while i < length:
        chars[i] = random.choice(string.ascii_lowercase)
        i += 2
    return ''.join(chars)
