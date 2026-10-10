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

def testqueryIndexedSSTableTest(cql, test_keyspace):
    # That test reproduces the bug from CASSANDRA-10903 and the fact we have a static column is
    # relevant to that reproduction in particular as it forces a slightly different code path that
    # if there wasn't a static.

    ROWS = 1000
    VALUE_LENGTH = 100

    with create_table(cql, test_keyspace, "(k int, t int, s text static, v text, PRIMARY KEY (k, t))") as table:
        # We create a partition that is big enough that the underlying sstable will be indexed
        # For that, we use a large-ish number of row, and a value that isn't too small.
        text = makeRandomString(VALUE_LENGTH)
        # The Java test runs these INSERTs one after another. To make the
        # test faster, we run them concurrently.
        stmt = cql.prepare(f"INSERT INTO {table}(k, t, v) VALUES (?, ?, ?)")
        execute_concurrent_with_args(cql, stmt, [(0, i, text + str(i)) for i in range(ROWS)], concurrency=100, raise_on_first_error=True)

        flush(cql, table)
        compact(cql, table)

        # The original Java test has here a sanity check that we're reading
        # from an indexed sstable, which inspects Cassandra's internal sstable
        # index entries, so it was not translated.

        assert_row_count(execute(cql, table, "SELECT s FROM %s WHERE k = ?", 0), ROWS)
        assert_row_count(execute(cql, table, "SELECT s FROM %s WHERE k = ? ORDER BY t DESC", 0), ROWS)

        assert_row_count(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k = ?", 0), 1)
        assert_row_count(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k = ? ORDER BY t DESC", 0), 1)
