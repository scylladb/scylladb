# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the aggregated_metrics cluster config option (SCYLLADB-4254): whether a table reports
# per-table metrics aggregated across shards, overriding enable_node_aggregated_table_metrics.

import time

import pytest
from cassandra.protocol import InvalidRequest

from .util import new_test_keyspace, new_test_table, new_secondary_index, new_materialized_view, ScyllaMetrics

@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

# Only the aggregated branch of table::set_metrics() registers this metric, as test clusters
# run with enable_keyspace_column_family_metrics=false.
AGGREGATED_METRIC = 'scylla_column_family_live_sstable'

def has_aggregated_metric(cql, table):
    ks, cf = table.split(".")
    return ScyllaMetrics.query(cql).get(AGGREGATED_METRIC, labels={'cf': cf, 'ks': ks}) is not None

# A change reaches the tables once the node's cluster config cache refreshes.
def wait_for_aggregated_metric(cql, table, expected):
    deadline = time.time() + 30
    while has_aggregated_metric(cql, table) != expected:
        assert time.time() < deadline, f"{table}: aggregated metrics still {not expected}"
        time.sleep(0.1)

def test_default_follows_yaml(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key") as table:
        assert has_aggregated_metric(cql, table)

def test_create_table_with_option(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key", " WITH aggregated_metrics = false") as table:
        assert "aggregated_metrics = false" in cql.execute(f"DESC TABLE {table}").one().create_statement
        wait_for_aggregated_metric(cql, table, False)

def test_alter_table_takes_effect(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key") as table:
        cql.execute(f"ALTER TABLE {table} WITH aggregated_metrics = false")
        wait_for_aggregated_metric(cql, table, False)
        cql.execute(f"ALTER TABLE {table} WITH aggregated_metrics = null")
        wait_for_aggregated_metric(cql, table, True)

# A table inherits the keyspace value unless it sets its own.
def test_keyspace_scope(cql):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        with new_test_table(cql, ks, "p int primary key") as inherits, \
             new_test_table(cql, ks, "p int primary key", " WITH aggregated_metrics = true") as overrides:
            cql.execute(f"ALTER KEYSPACE {ks} WITH aggregated_metrics = false")
            wait_for_aggregated_metric(cql, inherits, False)
            assert has_aggregated_metric(cql, overrides)

# Secondary index and materialized views follow their base table, at creation and on a later change.
def test_index_follows_base(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key, v int", " WITH aggregated_metrics = false") as table:
        wait_for_aggregated_metric(cql, table, False)
        with new_secondary_index(cql, table, "v") as idx:
            idx_table = f"{idx}_index"
            assert not has_aggregated_metric(cql, idx_table)
            cql.execute(f"ALTER TABLE {table} WITH aggregated_metrics = true")
            wait_for_aggregated_metric(cql, idx_table, True)

def test_materialized_view_follows_base(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key, v int", " WITH aggregated_metrics = false") as table:
        wait_for_aggregated_metric(cql, table, False)
        with new_materialized_view(cql, table, "*", "v, p", "v is not null and p is not null") as mv:
            assert not has_aggregated_metric(cql, mv)
            cql.execute(f"ALTER TABLE {table} WITH aggregated_metrics = true")
            wait_for_aggregated_metric(cql, mv, True)

def test_materialized_view_rejects_option(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key, v int") as table:
        with new_materialized_view(cql, table, "*", "v, p", "v is not null and p is not null") as mv:
            with pytest.raises(InvalidRequest, match="aggregated_metrics"):
                cql.execute(f"ALTER MATERIALIZED VIEW {mv} WITH aggregated_metrics = false")

# A CDC log table is a table of its own: it doesn't follow its base, but can be altered directly.
def test_cdc_log_has_own_setting(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key, v int", " WITH aggregated_metrics = false AND cdc = {'enabled': true}") as table:
        log_table = f"{table}_scylla_cdc_log"
        wait_for_aggregated_metric(cql, table, False)
        assert has_aggregated_metric(cql, log_table)
        cql.execute(f"ALTER TABLE {log_table} WITH aggregated_metrics = false")
        wait_for_aggregated_metric(cql, log_table, False)
