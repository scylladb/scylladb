# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for alter table statement

import re
import pytest

from cassandra.protocol import ConfigurationException, InvalidRequest
from .util import config_value_context, is_scylla, new_test_table, new_type, unique_name
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

# The following tests check re-adding a dropped column with a different type
# (issues #24305 and #24290). A client may still hold a statement prepared
# before the DROP, and send values serialized for the dropped column's type.
# So the new type must be able to interpret the old type's values - the same
# rule as for altering the type of an existing column.

# Re-adding a dropped column with an incompatible type must be rejected,
# because a stale prepared statement could store nonsense in it. For example,
# if a bigint column is re-added as text, the bigint 1094861636, serialized
# as the bytes 00 00 00 00 41 42 43 44, is valid UTF-8 and would be stored as
# the string '\x00\x00\x00\x00ABCD'. Similarly, if a text column is re-added
# as int, the string 'abcd' would be stored as the int 1633837924.
# Reproduces #24290.
def test_readd_column_with_incompatible_type(cql, test_keyspace):
    for old_type, new_type in [('bigint', 'text'), ('text', 'int')]:
        with new_test_table(cql, test_keyspace, f'p int PRIMARY KEY, x {old_type}') as table:
            cql.execute(f'ALTER TABLE {table} DROP x')
            with pytest.raises(InvalidRequest, match='incompatible with previously dropped column'):
                cql.execute(f'ALTER TABLE {table} ADD x {new_type}')

# The configuration option allow_readd_column_with_incompatible_type is an
# emergency override, allowing to re-add a dropped column with an
# incompatible type anyway - e.g., when restoring a backup of a table in which
# an older version allowed this. The error message without it mentions it.
# The test also demonstrates why this is normally refused: a statement
# prepared before the DROP stores nonsense in the re-added column - the bigint
# 1094861636, serialized as the bytes 00 00 00 00 41 42 43 44, is stored as
# the string '\x00\x00\x00\x00ABCD'.
def test_readd_column_with_incompatible_type_override(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, x bigint') as table:
        bound = cql.prepare(f'INSERT INTO {table} (p, x) VALUES (?, ?)').bind([1, 1094861636])
        cql.execute(f'ALTER TABLE {table} DROP x')
        with pytest.raises(InvalidRequest, match='allow_readd_column_with_incompatible_type'):
            cql.execute(f'ALTER TABLE {table} ADD x text')
        with config_value_context(cql, 'allow_readd_column_with_incompatible_type', 'true'):
            cql.execute(f'ALTER TABLE {table} ADD x text')
        cql.execute(bound)
        assert list(cql.execute(f'SELECT p, x FROM {table}')) == [(1, '\x00\x00\x00\x00ABCD')]

# Re-adding a dropped non-frozen collection column as a non-frozen collection
# of an incompatible type is refused by an older check (#6276), which the
# configuration option allow_readd_column_with_incompatible_type doesn't
# override. So the error message shouldn't suggest this option, and setting
# it doesn't allow this.
def test_readd_nonfrozen_collection_with_incompatible_type(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, x list<int>') as table:
        cql.execute(f'ALTER TABLE {table} DROP x')
        with pytest.raises(InvalidRequest) as e:
            cql.execute(f'ALTER TABLE {table} ADD x list<text>')
        assert 'allow_readd_column_with_incompatible_type' not in str(e.value)
        with config_value_context(cql, 'allow_readd_column_with_incompatible_type', 'true'):
            with pytest.raises(InvalidRequest):
                cql.execute(f'ALTER TABLE {table} ADD x list<text>')

# Re-adding a dropped column with a different type is allowed if the new type
# can interpret the old type's values. Then a statement prepared before the
# DROP keeps working correctly: here, its ascii value is a valid text value
# with the same meaning.
def test_readd_column_with_compatible_type(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, x ascii') as table:
        bound = cql.prepare(f'INSERT INTO {table} (p, x) VALUES (?, ?)').bind([1, 'hello'])
        cql.execute(f'ALTER TABLE {table} DROP x')
        cql.execute(f'ALTER TABLE {table} ADD x text')
        cql.execute(bound)
        assert list(cql.execute(f'SELECT p, x FROM {table}')) == [(1, 'hello')]

# Re-adding a dropped column with the same frozen user type must be allowed.
# The type of a dropped column is stored with its user types replaced by
# tuples, so this checks that the new type is compared to it correctly.
# Cassandra has exactly this bug: it rejects this ALTER because it compares
# the user type to the tuple type that replaced it (CASSANDRA-21761).
def test_readd_column_with_same_frozen_udt(cql, test_keyspace, cassandra_bug):
    with new_type(cql, test_keyspace, '(a int, b text)') as udt:
        with new_test_table(cql, test_keyspace, f'p int PRIMARY KEY, x frozen<{udt}>') as table:
            cql.execute(f'ALTER TABLE {table} DROP x')
            cql.execute(f'ALTER TABLE {table} ADD x frozen<{udt}>')

# Like test_readd_column_with_same_frozen_udt, with a non-frozen user type.
# It is scylla_only because Cassandra doesn't allow dropping a column of a
# non-frozen user type.
def test_readd_column_with_same_nonfrozen_udt(cql, test_keyspace, scylla_only):
    with new_type(cql, test_keyspace, '(a int, b text)') as udt:
        with new_test_table(cql, test_keyspace, f'p int PRIMARY KEY, x {udt}') as table:
            cql.execute(f'ALTER TABLE {table} DROP x')
            cql.execute(f'ALTER TABLE {table} ADD x {udt}')

# Re-adding a dropped non-frozen collection column with the same type must be
# allowed also when the collection contains a user type. Reproduces #7936,
# where this was refused with "Cannot add a collection with the name ...
# because a collection with the same name and a different type has already
# been used in the past": the check compared the collection type with the
# type recorded for the dropped column, in which the user type is replaced
# by a tuple. Cassandra has a similar bug (CASSANDRA-21761).
def test_readd_collection_column_with_same_udt(cql, test_keyspace, cassandra_bug):
    with new_type(cql, test_keyspace, '(a int, b text)') as udt:
        for t in [f'map<text, frozen<{udt}>>', f'list<frozen<{udt}>>', f'set<frozen<{udt}>>']:
            with new_test_table(cql, test_keyspace, f'p int PRIMARY KEY, x {t}') as table:
                cql.execute(f'ALTER TABLE {table} DROP x')
                cql.execute(f'ALTER TABLE {table} ADD x {t}')

# Re-adding a dropped int column as blob, which can hold any value, is
# allowed. The value written to the int column before the DROP belongs to the
# dropped column, so it must not reappear as a blob. The test reads with
# BYPASS CACHE, to read the value from the sstable. It is scylla_only
# because Cassandra doesn't allow re-adding an int column as blob.
def test_readd_int_column_as_blob(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, x int, y int') as table:
        cql.execute(f'INSERT INTO {table} (p, x, y) VALUES (1, 5, 6)')
        flush(cql, table)
        cql.execute(f'ALTER TABLE {table} DROP x')
        cql.execute(f'ALTER TABLE {table} ADD x blob')
        assert list(cql.execute(f'SELECT p, x, y FROM {table} BYPASS CACHE')) == [(1, None, 6)]
