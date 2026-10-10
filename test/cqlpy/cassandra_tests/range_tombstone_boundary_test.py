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

# The original Java test sets Cassandra's column_index_size to 1 KiB through
# Cassandra's internal configuration API, so that the 30 rows written by each
# test span about 3 index blocks in the sstable, and the range tombstones are
# in different blocks. We can't change this configuration through CQL, so
# with the default configuration the rows are probably in a single index
# block, but the tests still check the same results.
# The Java test also runs each test with Cassandra's "cursor compaction"
# enabled and disabled, also through the internal configuration API, which
# we can't do.

TABLE_OPTIONS = " WITH compression = {'enabled': 'false'} AND compaction = {'class': 'LeveledCompactionStrategy', 'enabled': false}"

def insertRows(cql, table):
    # 30 rows with ~100-byte values → ~3 index blocks at 1 KiB each
    pad = "x" * 80
    stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1")
    for i in range(30):
        cql.execute(stmt, ["p", f"r{i:03d}", pad + str(i)])

# Reproduces #8948 (the compression option 'enabled')
@pytest.mark.xfail(reason="#8948")
def testOpenRangeTombstoneInLastBlock(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk text, ck text, v1 text, PRIMARY KEY (pk, ck))" + TABLE_OPTIONS) as table:
        insertRows(cql, table)

        flush(cql, table)

        # Open-ended delete: removes rows >= 27 (last block)
        execute(cql, table, "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = ? AND ck >= ?", "p", "r027")

        flush(cql, table)
        compact(cql, table)

        range = list(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND ck >= ?", "p", "r023"))
        assert len(range) == 4, "range select [r023, r029] should return 4 rows (23, 24, 25, 26)"

        range = list(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND ck >= ?", "p", "r027"))
        assert len(range) == 0

# Reproduces #8948 (the compression option 'enabled')
@pytest.mark.xfail(reason="#8948")
def testOpenRangeTombstoneInMiddleBlock(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk text, ck text, v1 text, PRIMARY KEY (pk, ck))" + TABLE_OPTIONS) as table:
        insertRows(cql, table)

        flush(cql, table)

        # Open-ended delete: removes rows >= 15 (middle block)
        execute(cql, table, "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = ? AND ck >= ?", "p", "r015")

        flush(cql, table)
        compact(cql, table)

        range = list(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND ck >= ?", "p", "r012"))
        assert len(range) == 3

        range = list(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND ck >= ?", "p", "r023"))
        assert len(range) == 0

# Reproduces #8948 (the compression option 'enabled')
@pytest.mark.xfail(reason="#8948")
def testMidBlockRangeTombstoneInLastBlock(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk text, ck text, v1 text, PRIMARY KEY (pk, ck))" + TABLE_OPTIONS) as table:
        insertRows(cql, table)

        flush(cql, table)

        # Mid-block delete: removes rows 24-25 (last block)
        execute(cql, table, "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = ? AND ck >= ? AND ck <= ?", "p", "r024", "r025")

        flush(cql, table)
        compact(cql, table)

        range = list(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND ck >= ?", "p", "r023"))
        assert len(range) == 5, "range select [r023, r029] should return 5 rows (23, 26, 27, 28, 29)"
