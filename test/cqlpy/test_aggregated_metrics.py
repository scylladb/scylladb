# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the per-table `aggregated_metrics` WITH-clause property
# (SCYLLADB-4254): lets an operator override, per table, whether
# table::set_metrics() registers the node-aggregated metric group,
# independent of the enable_node_aggregated_table_metrics config default.

import pytest

from .util import new_test_table, ScyllaMetrics

# Scylla-only: this property doesn't exist in Cassandra.
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass


def test_create_table_with_aggregated_metrics_true(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key",
                         extra=" WITH aggregated_metrics = true"):
        pass

def test_create_table_with_aggregated_metrics_false(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key",
                         extra=" WITH aggregated_metrics = false"):
        pass

def test_alter_table_aggregated_metrics(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key") as table:
        cql.execute(f"ALTER TABLE {table} WITH aggregated_metrics = true")
        cql.execute(f"ALTER TABLE {table} WITH aggregated_metrics = false")

# scylla_column_family_live_sstable is registered by the node-aggregated
# branch of table::set_metrics() (replica/table.cc). Test clusters run with
# enable_keyspace_column_family_metrics=false (the config default), so the
# per-shard-detailed branch never fires here: this metric's presence for a
# given table is exactly the aggregated-branch on/off signal. (Its "__" -
# prefixed node_table_metrics label is Prometheus-internal and isn't part of
# the exposed line, so we don't match on it.)
AGGREGATED_METRIC = 'scylla_column_family_live_sstable'

def _has_aggregated_metric(cql, table):
    ks_name, table_name = table.split(".")
    return ScyllaMetrics.query(cql).get(AGGREGATED_METRIC, labels={'cf': table_name, 'ks': ks_name}) is not None

# By default (aggregated_metrics unset, enable_keyspace_column_family_metrics
# config default False, enable_node_aggregated_table_metrics config default
# True) a new table falls back to the aggregated branch, so the aggregated
# metric is present.
def test_aggregated_metrics_default_matches_global_default(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key") as table:
        assert _has_aggregated_metric(cql, table)

# aggregated_metrics = false must disable the aggregated metric group for
# that table specifically, overriding the (enabled-by-default) global config.
def test_aggregated_metrics_false_disables_aggregated_group(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key",
                         extra=" WITH aggregated_metrics = false") as table:
        assert not _has_aggregated_metric(cql, table)

# aggregated_metrics = true is observably identical to the (default-enabled)
# global config for this table.
def test_aggregated_metrics_true_enables_aggregated_group(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key",
                         extra=" WITH aggregated_metrics = true") as table:
        assert _has_aggregated_metric(cql, table)

# ALTER TABLE must take effect immediately (replica/table.cc:set_schema()
# re-registers metrics when the effective aggregated_metrics value changes),
# not just at table creation.
def test_alter_table_aggregated_metrics_takes_effect(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key") as table:
        assert _has_aggregated_metric(cql, table)
        cql.execute(f"ALTER TABLE {table} WITH aggregated_metrics = false")
        assert not _has_aggregated_metric(cql, table)
        cql.execute(f"ALTER TABLE {table} WITH aggregated_metrics = true")
        assert _has_aggregated_metric(cql, table)
