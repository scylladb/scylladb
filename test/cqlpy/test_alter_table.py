# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for alter table statement

import re
import pytest

from cassandra.protocol import ConfigurationException
from .util import is_scylla, new_test_table, unique_name
from .nodetool import flush, compact

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

def test_valid_percentile_speculative_retry_values(cql, test_keyspace):
    """
    In test_valid_percentile_speculative_retry_values, we verify that valid values for the
    PERCENTILE option in the `speculative_retry` setting are accepted without errors. According to the
    documentation (https://enterprise.docs.scylladb.com/stable/cql/ddl.html#speculative-retry-options),
    the valid range for PERCENTILE is between 0.0 and 100.0.

    This test ensures that the system correctly accepts boundary values such as 0 and 100,
    including variations with a "+" sign, which are less common but still valid.

    See issue #26369.
    """

    with new_test_table(cql, test_keyspace, "id UUID PRIMARY KEY, value TEXT") as table:
        for percentile in ["-0", "0", "+0", "0.1", "0.01", "0.001", "99", "99.9", "99.999", "100", "+100"]:
            cql.execute(f"ALTER TABLE {table} WITH speculative_retry = '{percentile}PERCENTILE'")

def test_invalid_percentile_speculative_retry_values(cql, test_keyspace):
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

    with new_test_table(cql, test_keyspace, "id UUID PRIMARY KEY, value TEXT") as table:
        for percentile in ["-1.1", "-1", "-0.1", "-0.01", "-0.001", "100.1", "100.01", "100.001", "101", "+101", "+101.1", "dog"]:
            percentile = percentile.upper()

            # For negative values and zero, Cassandra returns a shortened error message compared to ScyllaDB.
            # Therefore, a regular expression is used to match both formats of the error message.
            message = (
                f"(?:"
                f"Invalid value {re.escape(percentile)}PERCENTILE "
                r"for (?:PERCENTILE option|option) 'speculative_retry'"
                r"(?:\: must be between \(0\.0 and 100\.0\))?"
                f"|"
                f"cannot convert {re.escape(percentile)}PERCENTILE to speculative_retry"
                f")"
            )
            with pytest.raises(ConfigurationException, match=message):
                cql.execute(f"ALTER TABLE {table} WITH speculative_retry = '{percentile}PERCENTILE'")

def test_invalid_speculative_retry_values(cql, test_keyspace):
    """
    Verify that speculative_retry values that match none of the supported
    formats (ALWAYS, NONE, XPERCENTILE, Yms) are rejected with a proper
    configuration error, including values shorter than the "ms" and
    "PERCENTILE" suffixes and non-finite or negative numbers.
    """

    with new_test_table(cql, test_keyspace, "id UUID PRIMARY KEY, value TEXT") as table:
        for value in ["", "x", "ms", "percentile", "dog",
                      "nanPERCENTILE", "infPERCENTILE", "-infPERCENTILE",
                      "nanms", "infms", "-1ms"]:
            with pytest.raises(ConfigurationException):
                cql.execute(f"ALTER TABLE {table} WITH speculative_retry = '{value}'")

def test_valid_speculative_retry_values(cql, test_keyspace):
    """
    Verify that all supported speculative_retry formats are accepted,
    case-insensitively. On Scylla, also verify the canonical form the value
    is normalized to in the schema tables; Cassandra normalizes to different
    spellings (e.g. 99p), so that part of the check is Scylla-only.
    """

    with new_test_table(cql, test_keyspace, "id UUID PRIMARY KEY, value TEXT") as table:
        ks, cf = table.split('.')
        for value, canonical in [('NONE', 'NONE'), ('none', 'NONE'), ('ALWAYS', 'ALWAYS'),
                                 ('200ms', '200.00ms'), ('0ms', '0.00ms'),
                                 ('99PERCENTILE', '99.0PERCENTILE'), ('99.0percentile', '99.0PERCENTILE')]:
            cql.execute(f"ALTER TABLE {table} WITH speculative_retry = '{value}'")
            if is_scylla(cql):
                r = list(cql.execute(f"SELECT speculative_retry FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{cf}'"))
                assert len(r) == 1
                assert r[0].speculative_retry == canonical

def test_prepared_alter_table_options_validated_on_every_execute(cql, test_keyspace):
    """
    A prepared ALTER TABLE with invalid options must be rejected on
    every execution. Validation caches the compaction class in the
    prepared statement after the first run, and that cache must not
    skip the remaining checks on the second run. The same for views is
    in test_materialized_view.py.
    """
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY") as table:
        alter = cql.prepare(f"ALTER TABLE {table} WITH compaction = {{'class': 'LeveledCompactionStrategy'}}"
                            " AND min_index_interval = 0")
        for _ in range(2):
            with pytest.raises(ConfigurationException, match="min_index_interval"):
                cql.execute(alter)

# The following tests check what happens when a dropped column is re-added
# with a different kind - static instead of regular, or vice versa (issue
# #9930) - while an sstable still holds cells written to the old column
# before it was dropped. Those cells belong to the old column, so they
# should be ignored, and must not corrupt other columns or break reads.
# The tests read with BYPASS CACHE, because the row cache converts its data
# to the new schema correctly, so only reading the sstable shows the
# problem. This is also why they are scylla_only.

# Re-adding a dropped static column as a regular column must not attribute
# the old static column's value to another static column. Reproduces #9930,
# where s2 was read as 700 instead of 70. The test then writes the new
# regular column s to a second sstable, checks that it is read correctly,
# and runs a major compaction, which merges the two sstables into a new one
# written with the new schema, and checks that this doesn't write the wrong
# value - with #9930, the new sstable stored s2=700, making the corruption
# permanent.
def test_readd_static_column_as_regular(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c int, s int static, s2 int static, v int, PRIMARY KEY (p, c)') as table:
        cql.execute(f'INSERT INTO {table} (p, c, s, s2, v) VALUES (1, 1, 700, 70, 8)')
        flush(cql, table)
        cql.execute(f'ALTER TABLE {table} DROP s')
        cql.execute(f'ALTER TABLE {table} ADD s int')
        assert list(cql.execute(f'SELECT p, c, s, s2, v FROM {table} WHERE p = 1 BYPASS CACHE')) == [(1, 1, None, 70, 8)]
        cql.execute(f'INSERT INTO {table} (p, c, s) VALUES (1, 2, 5)')
        flush(cql, table)
        expected = [(1, 1, None, 70, 8), (1, 2, 5, 70, None)]
        assert list(cql.execute(f'SELECT p, c, s, s2, v FROM {table} WHERE p = 1 BYPASS CACHE')) == expected
        compact(cql, table)
        assert list(cql.execute(f'SELECT p, c, s, s2, v FROM {table} WHERE p = 1 BYPASS CACHE')) == expected

# Re-adding a dropped regular column as a static column must not break
# reading the old sstable. Reproduces #9930, where every read of the
# partition failed with an internal error ("column id 5 >= 5"). The test
# then writes the new static column v to a second sstable, checks that it
# is read correctly, and also after a major compaction.
def test_readd_regular_column_as_static(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c int, s int static, v int, v2 int, PRIMARY KEY (p, c)') as table:
        cql.execute(f'INSERT INTO {table} (p, c, s, v, v2) VALUES (1, 1, 7, 800, 80)')
        flush(cql, table)
        cql.execute(f'ALTER TABLE {table} DROP v')
        cql.execute(f'ALTER TABLE {table} ADD v int static')
        assert list(cql.execute(f'SELECT p, c, s, v, v2 FROM {table} BYPASS CACHE')) == [(1, 1, 7, None, 80)]
        cql.execute(f'INSERT INTO {table} (p, v) VALUES (1, 6)')
        flush(cql, table)
        expected = [(1, 1, 7, 6, 80)]
        assert list(cql.execute(f'SELECT p, c, s, v, v2 FROM {table} BYPASS CACHE')) == expected
        compact(cql, table)
        assert list(cql.execute(f'SELECT p, c, s, v, v2 FROM {table} BYPASS CACHE')) == expected
