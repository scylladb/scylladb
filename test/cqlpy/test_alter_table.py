# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for alter table statement

import time
import pytest

from util import new_test_table, unique_name
from nodetool import flush

# Test checks only case of preparing `ALTER TABLE ... DROP ... USING TIMESTAMP ?` statement.
# It is `scylla_only` because Cassandra doesn't allow to prepare the statement with ? as the timestamp.
# More tests about `ALTER TABLE ... DROP ...` are in cassandra_tests/validation/operation/alter_test.py
def testDropColumnWithTimestampPrepared(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "id int, c1 int, v1 int, todrop int, PRIMARY KEY (id, c1)") as table:
        prepared = cql.prepare(f"ALTER TABLE {table} DROP todrop USING TIMESTAMP ?")
        for i in range(5):
            cql.execute(f"INSERT INTO {table} (id, c1, v1, todrop) VALUES (1, {i}, {i}, {i}) USING TIMESTAMP {10000 * i}")

        # It's safer to flush the table now
        # Flushing is not necessary, if we are sure the table will be always in memory.
        # But if the table is flushed just after dropping the column, all data from the column would be lost.
        # (the data isn't part of schema at that point, so it's not saved to sstable during flush)
        flush(cql, table)
        cql.execute(prepared, [20000])
        cql.execute(f"ALTER TABLE {table} ADD todrop int")
        cql.execute(f"INSERT INTO {table} (id, c1, v1, todrop) VALUES (1, 100, 100, 100) USING TIMESTAMP 30000")
        
        result = set(cql.execute(f"SELECT id, c1, v1, todrop FROM {table}"))
        assert result == set([
                (1, 0, 0, None),
                (1, 1, 1, None),
                (1, 2, 2, None),
                (1, 3, 3, 3),
                (1, 4, 4, 4),
                (1, 100, 100, 100)
        ])

# We allow to prepare alter table statement when the schema doesn't exist yet
def testDropColumnWithTimestampPreparedNonExistingSchema(scylla_only, cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    
    prepared = cql.prepare(f"ALTER TABLE {table} DROP todrop USING TIMESTAMP ?")
    cql.execute(f"CREATE TABLE {table} (id int, c1 int, v1 int, todrop int, PRIMARY KEY (id, c1))")

    try:
        for i in range(5):
            cql.execute(f"INSERT INTO {table} (id, c1, v1, todrop) VALUES (1, {i}, {i}, {i}) USING TIMESTAMP {10000 * i}")

        # It's safer to flush the table now
        # Flushing is not necessary, if we are sure the table will be always in memory.
        # But if the table is flushed just after dropping the column, all data from the column would be lost.
        # (the data isn't part of schema at that point, so it's not saved to sstable during flush)
        flush(cql, table)
        cql.execute(prepared, [20000])
        cql.execute(f"ALTER TABLE {table} ADD todrop int")
        cql.execute(f"INSERT INTO {table} (id, c1, v1, todrop) VALUES (1, 100, 100, 100) USING TIMESTAMP 30000")
        
        result = set(cql.execute(f"SELECT id, c1, v1, todrop FROM {table}"))
        assert result == set([
                (1, 0, 0, None),
                (1, 1, 1, None),
                (1, 2, 2, None),
                (1, 3, 3, 3),
                (1, 4, 4, 4),
                (1, 100, 100, 100)
        ])
    finally:
        cql.execute(f"DROP TABLE {table}")
