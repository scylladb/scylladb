# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for alter table statement

from test import DEBUG_MODES
import re
from collections import Counter

import pytest

from cassandra.protocol import ConfigurationException, InvalidRequest
from .util import is_scylla, new_named_table, new_test_table, unique_name
from .nodetool import flush, flush_all

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

# In Scylla, a dropped column's name can be re-used for a column of a
# different kind: a dropped set<text> column can come back as an int
# (CASSANDRA-7744), and a dropped int column as a list<int>. The data of the
# dropped column must not show up in the new one. But a dropped collection
# cannot come back as a collection with a different element type.
# Cassandra rejects all of these re-adds as incompatible with the dropped
# column's type, so this test is scylla_only.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_readd_dropped_column_with_other_kind(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int PRIMARY KEY, v set<text>") as table:
        cql.execute(f"INSERT INTO {table} (k, v) VALUES (0, {{'f'}})")
        flush_all(cql)
        cql.execute(f"ALTER TABLE {table} DROP v")
        cql.execute(f"ALTER TABLE {table} ADD v int")
        assert list(cql.execute(f"SELECT k, v FROM {table}")) == [(0, None)]
        cql.execute(f"INSERT INTO {table} (k, v) VALUES (1, 3)")
        assert set(cql.execute(f"SELECT k, v FROM {table}")) == {(0, None), (1, 3)}
    with new_test_table(cql, test_keyspace, "k text PRIMARY KEY, v int") as table:
        cql.execute(f"INSERT INTO {table} (k, v) VALUES ('k', 10)")
        flush_all(cql)
        cql.execute(f"ALTER TABLE {table} DROP v")
        assert list(cql.execute(f"SELECT * FROM {table}")) == [('k',)]
        cql.execute(f"ALTER TABLE {table} ADD v list<int>")
        assert list(cql.execute(f"SELECT * FROM {table}")) == [('k', None)]
        cql.execute(f"INSERT INTO {table} (k, v) VALUES ('k', [8])")
        assert list(cql.execute(f"SELECT * FROM {table}")) == [('k', [8])]
        cql.execute(f"ALTER TABLE {table} DROP v")
        with pytest.raises(InvalidRequest):
            cql.execute(f"ALTER TABLE {table} ADD v list<text>")

# CREATE TABLE and ALTER TABLE accept legacy and obsolete options
# (read_repair_chance, the JSON-string form of caching, an empty
# sstable_compression, compaction sub-options), and apply the ones that
# still mean something. Cassandra 4.0 removed read_repair_chance, so this
# is scylla_only.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_legacy_table_options(scylla_only, cql, test_keyspace):
    def options(table):
        ks, cf = table.split('.')
        return cql.execute("SELECT comment, gc_grace_seconds, bloom_filter_fp_chance, compaction, caching "
                           f"FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{cf}'").one()
    with new_test_table(cql, test_keyspace, "k int PRIMARY KEY, c int",
            """WITH comment = 'My comment'
               AND read_repair_chance = 0.5
               AND dclocal_read_repair_chance = 0.5
               AND gc_grace_seconds = 4
               AND bloom_filter_fp_chance = 0.01
               AND compaction = { 'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 10 }
               AND compression = { 'sstable_compression' : '' }
               AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'""") as table:
        o = options(table)
        assert (o.comment, o.gc_grace_seconds, o.bloom_filter_fp_chance) == ('My comment', 4, 0.01)
        assert o.compaction['class'].endswith('LeveledCompactionStrategy')
        assert o.compaction['sstable_size_in_mb'] == '10'
        assert o.caching['keys'] == 'ALL' and o.caching['rows_per_partition'] == 'ALL'
        cql.execute(f"""ALTER TABLE {table}
            WITH comment = 'other comment'
             AND read_repair_chance = 0.3
             AND dclocal_read_repair_chance = 0.3
             AND gc_grace_seconds = 100
             AND bloom_filter_fp_chance = 0.1
             AND compaction = {{ 'class' : 'SizeTieredCompactionStrategy', 'min_sstable_size' : 42 }}
             AND compression = {{ 'sstable_compression' : 'SnappyCompressor' }}
             AND caching = '{{"keys":"NONE","rows_per_partition":"ALL"}}'""")
        o = options(table)
        assert (o.comment, o.gc_grace_seconds, o.bloom_filter_fp_chance) == ('other comment', 100, 0.1)
        assert o.compaction['class'].endswith('SizeTieredCompactionStrategy')
        assert o.compaction['min_sstable_size'] == '42'
        assert o.caching['keys'] == 'NONE' and o.caching['rows_per_partition'] == 'ALL'

# ALTER TABLE ... ALTER <column> TYPE blob turns a text column into a blob,
# for a regular column, the partition key, a clustering column, and the
# elements of a set. The existing data is then read back as the raw UTF-8
# bytes. Cassandra removed ALTER ... TYPE (CASSANDRA-12443), so this is
# scylla_only. See #4041.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_alter_column_type_text_to_blob(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k text, c text, s set<text>, v text, PRIMARY KEY (k, c)") as table:
        # A non-ASCII character makes the change to bytes visible.
        x = "ɸ"
        b = x.encode()
        cql.execute(f"INSERT INTO {table} (k, c, s, v) VALUES ('{x}', '{x}', {{'{x}'}}, '{x}')")
        assert list(cql.execute(f"SELECT k, c, s, v FROM {table}")) == [(x, x, {x}, x)]
        cql.execute(f"ALTER TABLE {table} ALTER v TYPE blob")
        assert list(cql.execute(f"SELECT k, c, s, v FROM {table}")) == [(x, x, {x}, b)]
        cql.execute(f"ALTER TABLE {table} ALTER k TYPE blob")
        assert list(cql.execute(f"SELECT k, c, s, v FROM {table}")) == [(b, x, {x}, b)]
        cql.execute(f"ALTER TABLE {table} ALTER c TYPE blob")
        assert list(cql.execute(f"SELECT k, c, s, v FROM {table}")) == [(b, b, {x}, b)]
        cql.execute(f"ALTER TABLE {table} ALTER s TYPE set<blob>")
        assert list(cql.execute(f"SELECT k, c, s, v FROM {table}")) == [(b, b, {b}, b)]

# ALTER TABLE ... DROP is not allowed on a COMPACT STORAGE table.
# Scylla-only: Cassandra 4.0 and later no longer accept WITH COMPACT STORAGE.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_drop_column_compact_storage(scylla_only, cql, test_keyspace, compact_storage):
    with new_test_table(cql, test_keyspace, "key int PRIMARY KEY, c1 int, c2 int", "WITH COMPACT STORAGE") as table:
        with pytest.raises(InvalidRequest, match="Cannot drop columns from a"):
            cql.execute(f"ALTER TABLE {table} DROP c1")

# Ported from scylla-dtest schema_test.py::TestSchema::test_drop_column_queries.
# Drop a column whose values are still in the memtable and re-add it: the old
# values stay hidden and the new ones are returned. Unlike the drop tests in
# cassandra_tests/.../alter_test.py, there is no explicit USING TIMESTAMP and
# nothing was flushed before the drop.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_drop_column_queries(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "key int PRIMARY KEY, c1 int, c2 int") as table:
        # insert some data.
        cql.execute(f"INSERT INTO {table} (key, c1, c2) VALUES (0, 1, 2)")
        cql.execute(f"INSERT INTO {table} (key, c1, c2) VALUES (1, 2, 3)")
        cql.execute(f"INSERT INTO {table} (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        cql.execute(f"ALTER TABLE {table} DROP c1")
        cql.execute(f"ALTER TABLE {table} ADD c1 int")

        # add another row.
        cql.execute(f"INSERT INTO {table} (key, c1, c2) VALUES (3, 4, 5)")

        # test that old (pre-drop) c1 values aren't returned and new ones are.
        assert Counter(cql.execute(f"SELECT c1 FROM {table}")) == Counter([(None,), (None,), (None,), (4,)])
        assert Counter(cql.execute(f"SELECT * FROM {table}")) == Counter([(0, None, 2), (1, None, 3), (2, None, 4), (3, 4, 5)])
        assert list(cql.execute(f"SELECT c1 FROM {table} WHERE key = 0")) == [(None,)]
        assert list(cql.execute(f"SELECT c1 FROM {table} WHERE key = 3")) == [(4,)]

# Every schema change of a table adds one new version to
# system.scylla_table_schema_history, holding the table's columns at that
# version.
def test_schema_history_alter_table(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int, int_col int") as table:
        ks, cf = table.split('.')
        cf_id = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{cf}'").one().id
        seen_versions = set()
        def new_version_columns():
            rows = list(cql.execute(f"SELECT * FROM system.scylla_table_schema_history WHERE cf_id = {cf_id}"))
            new_versions = {r.schema_version for r in rows} - seen_versions
            assert len(new_versions) == 1
            version = new_versions.pop()
            seen_versions.add(version)
            return {(r.column_name, r.type) for r in rows if r.schema_version == version and r.kind == 'regular'}

        columns = new_version_columns()
        assert columns == {('v', 'int'), ('int_col', 'int')}
        for alter, added, removed in [
                ("ALTER v TYPE varint", {('v', 'varint')}, {('v', 'int')}),
                ("ADD (v2 int, v3 int)", {('v2', 'int'), ('v3', 'int')}, set()),
                ("ALTER int_col TYPE varint", {('int_col', 'varint')}, {('int_col', 'int')}),
                ("DROP int_col", set(), {('int_col', 'varint')}),
                ("ADD int_col bigint", {('int_col', 'bigint')}, set()),
                ("DROP (int_col, v)", set(), {('int_col', 'bigint'), ('v', 'varint')})]:
            cql.execute(f"ALTER TABLE {table} {alter}")
            new_columns = new_version_columns()
            assert new_columns - columns == added, alter
            assert columns - new_columns == removed, alter
            columns = new_columns

# Tests ported from scylla-dtest's cql_additional_tests.py. Like the dtests,
# they run in a keyspace with vnodes, and keep the dtests' table names.

# A row written by UPDATE reads back with the new columns as nulls after
# ALTER TABLE ADD of a list and then of a map (CASSANDRA-5232).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_alter_add_collections_to_existing_row(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "t1", "id int PRIMARY KEY, t text") as table:
        cql.execute(f"UPDATE {table} SET t = '111' WHERE id = 1;")
        cql.execute(f"ALTER TABLE {table} ADD l list<text>;")
        assert list(cql.execute(f"SELECT * FROM {table};")) == [(1, None, "111")]
        cql.execute(f"ALTER TABLE {table} ADD m map<int, text>;")
        assert list(cql.execute(f"SELECT * FROM {table};")) == [(1, None, None, "111")]

# A static column cannot be added to a table without clustering columns.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_alter_add_static_without_clustering(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "foo", "bar int, PRIMARY KEY (bar)") as table:
        with pytest.raises(InvalidRequest):
            cql.execute(f"ALTER TABLE {table} ADD bar2 text static")

# ALTER TABLE ADD of several columns at once; a row using them can be
# written and read.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_alter_add_multiple_columns(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "foo", "bar int, PRIMARY KEY (bar)") as table:
        cql.execute(f"ALTER TABLE {table} ADD (c text, d int)")
        cql.execute(f"INSERT INTO {table} (bar, c, d) VALUES (1, 'hello', 100)")
        assert list(cql.execute(f"SELECT bar, c, d FROM {table}")) == [(1, "hello", 100)]
