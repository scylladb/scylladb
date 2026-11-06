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
from cassandra.query import SimpleStatement
import random

# A random string of printable ASCII characters. We map random bytes to
# characters with bytes.translate(), because building the string character by
# character, like the Java code does, takes seconds in Python.
SOME_TEXT_CHARS = bytes(32 + b % 95 for b in range(256))
def someText(length):
    return random.randbytes(length).translate(SOME_TEXT_CHARS).decode()

def testpagingOnRegularColumn(cql, test_keyspace):
    # The Java test writes a 20 MB partition of 100*100 rows with two 1 KB
    # values each, so that the partition has hundreds of index blocks in the
    # sstable (this test was added together with CASSANDRA-11206's support
    # for large partitions), and then reads it with pages of 3 rows. On
    # Scylla, reading these 10,000 rows 3 at a time takes seconds, so on
    # Scylla we write the same 20 MB with 20*20 rows of two 25 KB values,
    # which still has hundreds of index blocks (of Scylla's default
    # column_index_size_in_kb, 64 KB) but needs 25 times fewer pages.
    if is_scylla(cql):
        C1S, C2S, TEXT_LENGTH, FLUSH_EVERY = 20, 20, 25 * 1024, 6
    else:
        C1S, C2S, TEXT_LENGTH, FLUSH_EVERY = 100, 100, 1024, 30
    with create_table(cql, test_keyspace, "(" +
                    " k1 int," +
                    " c1 int," +
                    " c2 int," +
                    " v1 text," +
                    " v2 text," +
                    " v3 text," +
                    " v4 text," +
                    "PRIMARY KEY (k1, c1, c2))") as table:
        # The Java test runs these INSERTs one after another. To make the
        # test faster, we run each c1's INSERTs concurrently.
        stmt = cql.prepare(f"INSERT INTO {table} (k1, c1, c2, v1, v2, v3, v4) VALUES (?, ?, ?, ?, ?, ?, ?)")
        for c1 in range(C1S):
            execute_concurrent_with_args(cql, stmt, [(1, c1, c2, str(c1), str(c2), someText(TEXT_LENGTH), someText(TEXT_LENGTH)) for c2 in range(C2S)],
                                         concurrency=100, raise_on_first_error=True)
            if c1 % FLUSH_EVERY == 0:
                flush(cql, table)
        flush(cql, table)

        stmt = SimpleStatement(f"SELECT c1, c2, v1, v2 FROM {table} WHERE k1 = 1", fetch_size=3)
        it = iter(cql.execute(stmt))
        for c1 in range(C1S):
            for c2 in range(C2S):
                row = next(it, None)
                assert row is not None
                msg = f"On {c1},{c2}"
                assert row[0] == c1, msg
                assert row[1] == c2, msg
                assert row[2] == str(c1), msg
                assert row[3] == str(c2), msg
        assert next(it, None) is None

        for c1 in range(C1S):
            stmt = SimpleStatement(f"SELECT c1, c2, v1, v2 FROM {table} WHERE k1 = 1 AND c1 = %s", fetch_size=3)
            it = iter(cql.execute(stmt, [c1]))
            for c2 in range(C2S):
                row = next(it, None)
                assert row is not None
                msg = f"Within {c1} on {c2}"
                assert row[0] == c1, msg
                assert row[1] == c2, msg
                assert row[2] == str(c1), msg
                assert row[3] == str(c2), msg
            assert next(it, None) is None
