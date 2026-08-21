# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from .util import new_test_table, ScyllaMetrics

# Test that executing CQL requests with client-provided timestamps
# updates the client timestamp drift histogram metric.
# The Python driver sends client timestamps by default (protocol v3+),
# so any normal query should contribute to this metric.
# Reproduces: https://scylladb.atlassian.net/browse/SCYLLADB-1946
def test_client_timestamp_drift_metric(cql, test_keyspace, scylla_only):
    schema = 'k int, v int, primary key (k)'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Get metrics before executing queries
        metrics_before = ScyllaMetrics.query(cql)
        count_before = metrics_before.get('scylla_transport_cql_client_timestamp_drift_histogram_count')

        # Execute some queries which carry client timestamps
        for i in range(10):
            cql.execute(f"INSERT INTO {table} (k, v) VALUES ({i}, {i})")

        # Check that the metric was updated
        metrics_after = ScyllaMetrics.query(cql)
        count_after = metrics_after.get('scylla_transport_cql_client_timestamp_drift_histogram_count')

        # The count should have increased by at least the number of queries we executed
        before = count_before if count_before is not None else 0
        after = count_after if count_after is not None else 0
        assert after >= before + 10, \
            f"Expected at least 10 new samples, got {after - before}"
