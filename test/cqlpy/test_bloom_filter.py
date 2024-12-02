# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import rest_api
import nodetool
from util import new_test_table, config_value_context
from cassandra.protocol import ConfigurationException

# Disable component memory reclamation by setting the threshold to max value
@pytest.fixture(scope="module")
def disable_component_memory_reclaim(cql):
    with config_value_context(cql, 'components_memory_reclaim_threshold', '1'):
        yield

# Test inserts `N` rows into table, flushes it 
# and tries to read `M` non-existing keys.
# Then bloom filter's false-positive ratio is checked.
@pytest.mark.parametrize("N,M,fp_chance", [(500, 1000, 0.1)])
def test_bloom_filter(scylla_only, cql, test_keyspace, disable_component_memory_reclaim, N, M, fp_chance):
    def run_test(cql, test_keyspace, N, M, fp_chance):
        with new_test_table(cql, test_keyspace, "a int PRIMARY KEY",
            f"WITH bloom_filter_fp_chance = {fp_chance}") as table:

            stmt = cql.prepare(f"INSERT INTO {table} (a) VALUES(?)")
            for k in range(N):
                cql.execute(stmt, [k])
            nodetool.flush(cql, table)

            read_stmt = cql.prepare(f"SELECT * FROM {table} WHERE a = ? BYPASS CACHE")
            for k in range(N, N+M):
                cql.execute(read_stmt, [k])

            fp = rest_api.get_column_family_metric(cql,
              "bloom_filter_false_positives", table)
            ratio = fp / M

            assert ratio <= fp_chance * 1.15
            return ratio >= fp_chance * 0.7

    # fast mode: if efficiency is the one expected, test succeeds.
    #   if lower, test fails.
    #   if higher, test enter slow mode which retries with higher number of keys, to handle
    #       scenario where keys were split into many SSTables (e.g. many compaction groups).
    if run_test(cql, test_keyspace, N, M, fp_chance) == False:
        assert run_test(cql, test_keyspace, 50000, M, fp_chance)
            
# Test very small bloom_filter_fp_chance settings.
# The Cassandra documentation suggests that bloom_filter_fp_chance can be set
# to anything between 0 and 1, and the Datastax documentation even goes further
# and explains that 0 means "the largest possible Bloom filter".
# But in practice, there is a minimal false-positive chance that the Bloom
# filter can possibly achieve and Cassandra refuses lower settings (see
# CASSANDRA-11920) and Scylla should do the same instead of crashing much
# later during a memtable flush as it did in issue #11524.
@pytest.mark.parametrize("fp_chance", [1e-5, 0])
def test_small_bloom_filter_fp_chance(cql, test_keyspace, fp_chance):
    with pytest.raises(ConfigurationException):
        with new_test_table(cql, test_keyspace, 'a int PRIMARY KEY', f'WITH bloom_filter_fp_chance = {fp_chance}') as table:
            cql.execute(f'INSERT INTO {table} (a) VALUES (1)')
            # In issue #11524, Scylla used to crash during this flush after the
            # table creation succeeded above.
            nodetool.flush(cql, table)

# Check that bloom_filter_fp_chance outside [0, 1] (i.e., > 1 or < 0)
# is, unsurprisingly, forbidden.
@pytest.mark.parametrize("fp_chance", [-0.1, 1.1])
def test_invalid_bloom_filter_fp_chance(cql, test_keyspace, fp_chance):
    with pytest.raises(ConfigurationException):
        with new_test_table(cql, test_keyspace, 'a int PRIMARY KEY', f'WITH bloom_filter_fp_chance = {fp_chance}') as table:
            pass
