# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import logging
import math

import cassandra.concurrent

from .util import new_test_table, ScyllaMetrics

logger = logging.getLogger(__name__)


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


# Run read_func() and check that the number of reads it reports is reflected in
# the scylla_database_total_reads metric of the user-facing read classes.
# read_func() is expected to perform the reads and return how many it issued.
def check_reads_counted(cql, read_func):
    # A user connection starts in the sl:driver scheduling group and only migrates to
    # sl:default once the server detects it as a user connection, so reads issued by the
    # session may be accounted under either class. get() sums both for us.
    classes = {"sl:default", "sl:driver"}

    initial_reads = ScyllaMetrics.query(cql).get("scylla_database_total_reads", labels={"class": classes})
    if initial_reads is None:
        # Versions before 2025.1 merge these metrics into the "user" class
        classes = {"user"}
        initial_reads = ScyllaMetrics.query(cql).get("scylla_database_total_reads", labels={"class": classes})

    total_count = read_func()

    final_reads = ScyllaMetrics.query(cql).get("scylla_database_total_reads", labels={"class": classes})

    added = final_reads - initial_reads
    min_count = total_count
    # The driver itself issues side-reads that get counted in the same metric classes: the
    # control connection polls system.local/system.peers for schema agreement, and a schema
    # change makes it re-read the system_schema tables. Their number depends on timing, not on
    # how many reads the test issued, so `added` can exceed total_count by a few tens of reads.
    # A 10% overshoot window absorbs that as long as total_count is large enough - which is why
    # the callers below issue well over a thousand reads. An upper bound is still worth keeping
    # so a regression that causes far more reads than expected doesn't go unnoticed.
    max_count = total_count * 1.1
    assert min_count <= added <= max_count, \
        f"Expected additional reads to be in the [{min_count}, {max_count}] range, but metrics show {added} reads"


# Following scylladb/scylla:0c6bbc8 queries are now classified by its initiator, so here is a
# small test that aims to ensure that when a user runs queries, they will be marked as user
# initiated (here we are checking the `scylla_database_total_reads` metric under class="user"
# or the sl:default/sl:driver scheduling group classes).
def test_total_reads_user(cql, test_keyspace, scylla_only):
    schema = "c1 int, c2 int, primary key (c1)"
    with new_test_table(cql, test_keyspace, schema) as table:
        keys = range(100)
        cassandra.concurrent.execute_concurrent_with_args(
            cql, cql.prepare(f"INSERT INTO {table} (c1, c2) VALUES (?, ?)"),
            [(k, k) for k in keys], concurrency=32)

        select_stmt = cql.prepare(f"SELECT * FROM {table} WHERE c1=?")
        # Read every key several times: the 10% overshoot allowed by check_reads_counted() has
        # to stay comfortably above the driver's own background reads, whose number doesn't
        # shrink with the number of reads the test issues.
        reads_per_key = 16
        params = [(k,) for k in keys for _ in range(reads_per_key)]

        def read_func():
            cassandra.concurrent.execute_concurrent_with_args(cql, select_stmt, params, concurrency=32)
            return len(params)

        check_reads_counted(cql, read_func)


# Same principle as test_total_reads_user, but read a system table instead,
# and check that the reads are still classified as "user" reads.
def test_total_reads_system(cql, scylla_only):
    all_rows = list(cql.execute("SELECT keyspace_name, table_name, column_name FROM system_schema.columns"))

    # As in test_total_reads_user, issue plenty of reads so that the 10% overshoot allowed by
    # check_reads_counted() stays well above the driver's own background reads.
    rounds = math.ceil(1000 / len(all_rows))
    params = [(r.keyspace_name, r.table_name, r.column_name) for _ in range(rounds) for r in all_rows]

    select_stmt = cql.prepare(
        "SELECT * FROM system_schema.columns WHERE keyspace_name=? AND table_name=? AND column_name=?")

    def read_func():
        cassandra.concurrent.execute_concurrent_with_args(cql, select_stmt, params, concurrency=32)
        return len(params)

    check_reads_counted(cql, read_func)
