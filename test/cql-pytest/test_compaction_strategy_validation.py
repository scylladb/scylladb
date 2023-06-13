# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for compaction strategy validation
#############################################################################

from cassandra_tests.porting import create_keyspace, create_table, execute, assert_invalid_throw_message, ConfigurationException

def assert_throws(cql, msg, cmd):
    with create_keyspace(cql, "replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }") as ks:
        with create_table(cql, ks, "(a int PRIMARY KEY, b int) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy' }") as table:
            assert_invalid_throw_message(cql, table, msg, ConfigurationException, cmd)

def test_common_options(cql):
    assert_throws(cql, "tombstone_threshold value (-0.4) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'tombstone_threshold' : -0.4 }")
    assert_throws(cql, "tombstone_threshold value (5.5) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'tombstone_threshold' : 5.5 }")
    assert_throws(cql, "tombstone_compaction_interval value (-7000ms) must be positive", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'tombstone_compaction_interval' : -7 }")

def test_size_tiered_compaction_strategy_options(cql):
    assert_throws(cql, "min_sstable_size value (-1) must be non negative", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_sstable_size' : -1 }")
    assert_throws(cql, "bucket_low value (0) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'bucket_low' : 0.0 }")
    assert_throws(cql, "bucket_low value (1.3) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'bucket_low' : 1.3 }")
    assert_throws(cql, "bucket_high value (0.7) must be greater than 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'bucket_high' : 0.7 }")
    assert_throws(cql, "cold_reads_to_omit value (-8.1) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'cold_reads_to_omit' : -8.1 }")
    assert_throws(cql, "cold_reads_to_omit value (3.5) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'cold_reads_to_omit' : 3.5 }")
    assert_throws(cql, "min_threshold value (1) must be bigger or equal to 2", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 1 }")

def test_time_window_compaction_strategy_options(cql):
    assert_throws(cql, "Invalid window unit SECONDS for compaction_window_unit", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'compaction_window_unit' : 'SECONDS' }")
    assert_throws(cql, "compaction_window_size value (-8) must be greater than 1", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'compaction_window_size' : -8 }")
    assert_throws(cql, "Invalid timestamp resolution SECONDS for timestamp_resolution", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'timestamp_resolution' : 'SECONDS' }")
    assert_throws(cql, "enable_optimized_twcs_queries value (no) must be \"true\" or \"false\"", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'enable_optimized_twcs_queries' : 'no' }")
    assert_throws(cql, "max_threshold value (1) must be bigger or equal to 2", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'max_threshold' : 1 }")

def test_leveled_compaction_strategy_options(cql):
    assert_throws(cql, "sstable_size_in_mb value (-5) must be positive", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : -5 }")

def test_not_allowed_options(cql):
    assert_throws(cql, "Invalid compaction strategy options {{abc, -54.54}} for chosen strategy type", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'abc' : -54.54 }")
    assert_throws(cql, "Invalid compaction strategy options {{dog, 3}} for chosen strategy type", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'dog' : 3 }")
    assert_throws(cql, "Invalid compaction strategy options {{compaction_window_size, 4}} for chosen strategy type", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'compaction_window_size' : 4 }")
