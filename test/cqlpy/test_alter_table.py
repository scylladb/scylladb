# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for alter table statement

import re
import pytest

from cassandra.protocol import ConfigurationException
from .util import new_test_table, unique_name
from .nodetool import flush

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

@pytest.mark.parametrize("percentile", ["-1.1", "-1", "-0", "0", "+0", "100", "+100", "101", "+101", "+101.1"])
def test_invalid_percentile_speculative_retry_values(cql, test_keyspace, percentile):
    """
    In test_invalid_percentile_speculative_retry_values, we verify that invalid values for the
    PERCENTILE option in the `speculative_retry` setting are properly rejected. According to the
    documentation (https://enterprise.docs.scylladb.com/stable/cql/ddl.html#speculative-retry-options),
    the valid range for PERCENTILE is between 0.0 and 100.0.

    This test ensures that the system correctly rejects invalid inputs such as negative values
    or values exceeding the maximum. Additionally, it verifies the correct handling of valid
    but less common formats, such as values with a "+" sign.

    See issue #21825.
    """

    # For negative values and zero, Cassandra returns a shortened error message compared to ScyllaDB.
    # Therefore, a regular expression is used to match both formats of the error message.
    message = (
        f"Invalid value {re.escape(percentile)}PERCENTILE "
        r"for (?:PERCENTILE option|option) 'speculative_retry'"
        r"(?:\: must be between \(0\.0 and 100\.0\))?"
    )
    with new_test_table(cql, test_keyspace, "id UUID PRIMARY KEY, value TEXT") as table:
        with pytest.raises(ConfigurationException, match=message):
            cql.execute(f"ALTER TABLE {table} WITH speculative_retry = '{percentile}PERCENTILE'")
