# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for compaction strategy validation
#############################################################################

import pytest
from .util import config_value_context, new_test_table, new_materialized_view, unique_name
from cassandra.protocol import ConfigurationException

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int PRIMARY KEY, b int", "WITH compaction = { 'class' : 'SizeTieredCompactionStrategy' }") as table:
        yield table

# NOTE: The following tests which use this assert_throws() all try to 
# check the specific wording of the error text, and it sometimes differs
# between Scylla and Cassandra - so we need to allow both: msg is a regular
# expression, so you can use the "|" character to allow two options.
def assert_throws(cql, table1, msg, cmd):
    with pytest.raises(ConfigurationException, match=msg):
        cql.execute(cmd.replace('%s', table1))

def test_common_options(cql, table1):
    assert_throws(cql, table1, r"tombstone_threshold value \(-0.4\) must be between 0.0 and 1.0|tombstone_threshold must be greater than 0, but was -0.400000", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'tombstone_threshold' : -0.4 }")
    assert_throws(cql, table1, r"tombstone_threshold value \(5.5\) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'tombstone_threshold' : 5.5 }")
    assert_throws(cql, table1, r"tombstone_compaction_interval value \(-7000ms\) must be positive", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'tombstone_compaction_interval' : -7 }")
    assert_throws(cql, table1, r"unchecked_tombstone_compaction value \(maybe\) must be \"true\" or \"false\"|'unchecked_tombstone_compaction' should be either 'true' or 'false', not 'maybe'", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'unchecked_tombstone_compaction' : 'maybe' }")
    assert_throws(cql, table1, r"enabled value \(certainly\) must be \"true\" or \"false\"|enabled should either be 'true' or 'false', not certainly", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'enabled' : 'certainly' }")

def test_size_tiered_compaction_strategy_options(cql, table1):
    assert_throws(cql, table1, r"min_sstable_size value \(-1\) must be non negative|min_sstable_size must be non negative: -1", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_sstable_size' : -1 }")
    assert_throws(cql, table1, r"bucket_low value \(0\) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'bucket_low' : 0.0 }")
    assert_throws(cql, table1, r"bucket_low value \(1.3\) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'bucket_low' : 1.3 }")
    assert_throws(cql, table1, r"bucket_high value \(0.7\) must be greater than 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'bucket_high' : 0.7 }")
    assert_throws(cql, table1, r"min_threshold value \(1\) must be bigger or equal to 2", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 1 }")

# In ScyllaDB, SizeTieredCompactionStrategy is deprecated and is merely an alias
# of IncrementalCompactionStrategy: a table that asks for STCS keeps the name in
# its schema but is compacted by ICS, and takes the ICS options - including the
# ICS-only ones.
def test_size_tiered_is_alias_of_incremental(cql, test_keyspace, table1, scylla_only):
    assert_throws(cql, table1, r"space_amplification_goal value \(2.2\) must be greater than 1.0 and less than or equal to 2.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'space_amplification_goal' : 2.2 }")
    # The one STCS option ICS doesn't have is accepted, and ignored, so that a
    # schema dumped from an older version can still be replayed as-is, and the
    # class name is kept in the schema.
    with new_test_table(cql, test_keyspace, "a int PRIMARY KEY, b int",
                        "WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'cold_reads_to_omit' : 0.5 }") as table:
        desc = cql.execute(f"DESCRIBE TABLE {table}").one().create_statement
        assert 'SizeTieredCompactionStrategy' in desc, f"Expected SizeTieredCompactionStrategy in DESCRIBE output but got: {desc}"
        # Altering an unrelated property must not trip over the strategy it carries.
        cql.execute(f"ALTER TABLE {table} WITH comment = 'still alterable'")
        cql.execute(f"ALTER TABLE {table} WITH gc_grace_seconds = 1234")

# Naming the deprecated SizeTieredCompactionStrategy returns a CQL warning, on
# each of the four statements that can name it. It is only a warning by default,
# for backward compatibility; the guardrail below turns it into an error.
DEPRECATED = 'SizeTieredCompactionStrategy is deprecated'
STCS = "{ 'class' : 'SizeTieredCompactionStrategy' }"

def assert_warns(res):
    warnings = res.response_future.warnings or []
    assert any(DEPRECATED in w for w in warnings), f"Expected a deprecation warning, got: {warnings}"

def test_size_tiered_table_warns(cql, test_keyspace, scylla_only):
    table = test_keyspace + '.' + unique_name()
    try:
        assert_warns(cql.execute(f"CREATE TABLE {table} (a int PRIMARY KEY, b int) WITH compaction = {STCS}"))
        assert_warns(cql.execute(f"ALTER TABLE {table} WITH compaction = {STCS}"))
    finally:
        cql.execute(f"DROP TABLE IF EXISTS {table}")

def test_size_tiered_view_warns(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
        mv = test_keyspace + '.' + unique_name()
        try:
            assert_warns(cql.execute(f"CREATE MATERIALIZED VIEW {mv} AS SELECT * FROM {table} "
                                     f"WHERE b is not null and a is not null PRIMARY KEY (b, a) "
                                     f"WITH compaction = {STCS}"))
            assert_warns(cql.execute(f"ALTER MATERIALIZED VIEW {mv} WITH compaction = {STCS}"))
        finally:
            cql.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv}")

# With allow_deprecated_size_tiered_compaction_strategy turned off, naming the
# deprecated strategy is an error instead of a warning.
def test_size_tiered_guardrail(cql, test_keyspace, scylla_only):
    with config_value_context(cql, 'allow_deprecated_size_tiered_compaction_strategy', 'false'):
        with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
            with pytest.raises(ConfigurationException, match=DEPRECATED):
                cql.execute(f"ALTER TABLE {table} WITH compaction = {STCS}")
            with new_materialized_view(cql, table, '*', 'b, a', 'b is not null and a is not null') as mv:
                with pytest.raises(ConfigurationException, match=DEPRECATED):
                    cql.execute(f"ALTER MATERIALIZED VIEW {mv} WITH compaction = {STCS}")
        with pytest.raises(ConfigurationException, match=DEPRECATED):
            with new_test_table(cql, test_keyspace, "a int PRIMARY KEY, b int", f"WITH compaction = {STCS}"):
                pass

# The guardrail must also hold when the ALTER was prepared before the table
# existed: at prepare time the statement can't see a schema to check against, so
# it has to be enforced where the statement is applied.
def test_size_tiered_guardrail_when_prepared_before_create(cql, test_keyspace, scylla_only):
    with config_value_context(cql, 'allow_deprecated_size_tiered_compaction_strategy', 'false'):
        table = test_keyspace + '.' + unique_name()
        prepared = cql.prepare(f"ALTER TABLE {table} WITH compaction = {STCS}")
        cql.execute(f"CREATE TABLE {table} (a int PRIMARY KEY, b int)")
        try:
            with pytest.raises(ConfigurationException, match=DEPRECATED):
                cql.execute(prepared)
        finally:
            cql.execute(f"DROP TABLE {table}")

def test_time_window_compaction_strategy_options(cql, table1):
    assert_throws(cql, table1, "Invalid window unit SECONDS for compaction_window_unit|SECONDS is not valid for compaction_window_unit", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'compaction_window_unit' : 'SECONDS' }")
    assert_throws(cql, table1, r"compaction_window_size value \(-8\) must be greater than 1|-8 must be greater than 1 for compaction_window_size", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'compaction_window_size' : -8 }")
    assert_throws(cql, table1, "Invalid timestamp resolution SECONDS for timestamp_resolution", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'timestamp_resolution' : 'SECONDS' }")
    assert_throws(cql, table1, r"enable_optimized_twcs_queries value \(no\) must be \"true\" or \"false\"", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'enable_optimized_twcs_queries' : 'no' }")
    assert_throws(cql, table1, r"max_threshold value \(1\) must be bigger or equal to 2", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'max_threshold' : 1 }")

def test_leveled_compaction_strategy_options(cql, table1):
    assert_throws(cql, table1, r"sstable_size_in_mb value \(-5\) must be positive|sstable_size_in_mb must be larger than 0, but was -5", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : -5 }")

def test_incremental_compaction_strategy_options(cql, table1, scylla_only):
    assert_throws(cql, table1, r"min_sstable_size value \(-1\) must be non negative", "ALTER TABLE %s WITH compaction = { 'class' : 'IncrementalCompactionStrategy', 'min_sstable_size' : -1 }")
    assert_throws(cql, table1, r"bucket_low value \(0\) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'IncrementalCompactionStrategy', 'bucket_low' : 0.0 }")
    assert_throws(cql, table1, r"bucket_low value \(1.3\) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'IncrementalCompactionStrategy', 'bucket_low' : 1.3 }")
    assert_throws(cql, table1, r"bucket_high value \(0.7\) must be greater than 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'IncrementalCompactionStrategy', 'bucket_high' : 0.7 }")
    assert_throws(cql, table1, r"space_amplification_goal value \(2.2\) must be greater than 1.0 and less than or equal to 2.0", "ALTER TABLE %s WITH compaction = { 'class' : 'IncrementalCompactionStrategy', 'space_amplification_goal' : 2.2 }")
    assert_throws(cql, table1, r"min_threshold value \(1\) must be bigger or equal to 2", "ALTER TABLE %s WITH compaction = { 'class' : 'IncrementalCompactionStrategy', 'min_threshold' : 1 }")

# Reproducer for https://github.com/scylladb/scylladb/issues/SCYLLADB-1353
# When compaction is disabled via 'enabled': 'false', DESCRIBE should still
# show the actual compaction strategy class, not NullCompactionStrategy.
def test_describe_shows_real_strategy_when_disabled(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int PRIMARY KEY, b int",
                        "WITH compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '160'}") as table:
        # Disable compaction
        cql.execute(f"ALTER TABLE {table} WITH compaction = {{'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '160', 'enabled': 'false'}}")
        # DESCRIBE should show LeveledCompactionStrategy, not NullCompactionStrategy
        desc = cql.execute(f"DESCRIBE TABLE {table}").one().create_statement
        assert 'LeveledCompactionStrategy' in desc, f"Expected LeveledCompactionStrategy in DESCRIBE output but got: {desc}"
        assert 'NullCompactionStrategy' not in desc, f"NullCompactionStrategy should not appear in DESCRIBE output but got: {desc}"

def test_not_allowed_options(cql, table1):
    def scylla_error(**kwargs):
        template = "Invalid compaction strategy options {{{}}} for chosen strategy type"
        # TODO: remove the old old_options
        # {fmt} formats map like  {k1: v1, k2: v2}, while existing operator<<
        # formatter formats like {{k1, v1}, {k2, v2}}, so cater both formats
        # before switching to {fmt}'s formatter.
        old_options = ', '.join(f"{{{k}, {v}}}" for k, v in kwargs.items())
        options = ', '.join(f"\"{k}\": \"{v}\"" for k, v in kwargs.items())
        return '|'.join([template.format(old_options), template.format(options)])

    assert_throws(cql, table1, rf"{scylla_error(abc=-54.54)}|Properties specified \[abc\] are not understood by SizeTieredCompactionStrategy", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'abc' : -54.54 }")
    assert_throws(cql, table1, rf"{scylla_error(dog=3)}||Properties specified \[dog\] are not understood by TimeWindowCompactionStrategy", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'dog' : 3 }")
    assert_throws(cql, table1, rf"{scylla_error(compaction_window_size=4)}|Properties specified \[compaction_window_size\] are not understood by LeveledCompactionStrategy", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'compaction_window_size' : 4 }")
    assert_throws(cql, table1, rf"{scylla_error(cold_reads_to_omit=0.5)}|Properties specified \[cold_reads_to_omit\] are not understood by IncrementalCompactionStrategy", "ALTER TABLE %s WITH compaction = { 'class' : 'IncrementalCompactionStrategy', 'cold_reads_to_omit' : 0.5 }")
