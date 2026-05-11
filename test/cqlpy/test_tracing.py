# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for CQL tracing functionality, including basic tracing and
# slow query logging.
#
# Ported from scylla-dtest/cql_tracing_test.py (SCYLLADB-1920).
#############################################################################

import time
import uuid

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from .rest_api import get_request, post_request
from .util import new_test_table


def _wait_for_tracing(cql, query, timeout=30):
    """Execute a query and poll system_traces until the trace is available.
    Returns the trace_id (UUID) once events are visible."""
    result = cql.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ONE), trace=True)
    trace = result.response_future.get_query_trace(max_wait=timeout)
    assert trace is not None, "Trace was not returned"
    trace_id = trace.trace_id

    # Poll for events — they're written asynchronously
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = list(cql.execute(
            SimpleStatement(
                f"SELECT activity FROM system_traces.events WHERE session_id = {trace_id}",
                consistency_level=ConsistencyLevel.ONE)))
        if rows:
            return trace_id
        time.sleep(0.5)
    pytest.fail(f"Trace events for session {trace_id} did not appear within {timeout}s")


def test_tracing_simple(cql, test_keyspace):
    """Test basic CQL tracing: INSERT and SELECT produce trace sessions and events.

    Ported from scylla-dtest cql_tracing_test.py::test_tracing_simple.
    """
    with new_test_table(cql, test_keyspace,
                        "userid uuid PRIMARY KEY, firstname text, lastname text, age int") as table:
        user_id = uuid.UUID('550e8400-e29b-41d4-a716-446655440000')

        # INSERT with tracing
        insert_q = f"INSERT INTO {table} (userid, firstname, lastname, age) VALUES ({user_id}, 'Frodo', 'Baggins', 32)"
        trace_id = _wait_for_tracing(cql, insert_q)

        # Verify session exists
        sessions = list(cql.execute(
            SimpleStatement(
                f"SELECT * FROM system_traces.sessions WHERE session_id = {trace_id}",
                consistency_level=ConsistencyLevel.ONE)))
        assert len(sessions) >= 1, "No trace session found"

        # SELECT with tracing
        select_q = f"SELECT firstname, lastname FROM {table} WHERE userid = {user_id}"
        result = cql.execute(SimpleStatement(select_q, consistency_level=ConsistencyLevel.ONE), trace=True)
        rows = list(result)
        assert len(rows) == 1
        assert rows[0].firstname == 'Frodo'
        assert rows[0].lastname == 'Baggins'

        trace = result.response_future.get_query_trace(max_wait=30)
        assert trace is not None, "SELECT trace was not returned"

        # Verify events exist for the SELECT trace
        events = []
        deadline = time.time() + 30
        while time.time() < deadline:
            events = list(cql.execute(
                SimpleStatement(
                    f"SELECT activity FROM system_traces.events WHERE session_id = {trace.trace_id}",
                    consistency_level=ConsistencyLevel.ONE)))
            if events:
                break
            time.sleep(0.5)
        assert len(events) > 0, "No trace events found for SELECT query"


def test_slow_query_tracing_fast(cql, test_keyspace, scylla_only):
    """Test that in fast slow-query tracing mode, no new events are written to
    system_traces.events, but node_slow_log and sessions are populated.

    Ported from scylla-dtest cql_tracing_test.py::test_fast_slow_query_tracing[enabled].
    """
    # Save original slow query config
    original = get_request(cql, 'storage_service/slow_query')

    try:
        # Snapshot the number of events before the test so we can verify
        # no new events are written in fast mode (previous tests may have
        # left events in the table).
        events_before = len(list(cql.execute(
            SimpleStatement(
                "SELECT session_id FROM system_traces.events",
                consistency_level=ConsistencyLevel.ONE))))

        # Enable slow query tracing in fast mode with very low threshold
        post_request(cql, 'storage_service/slow_query?enable=true&threshold=0&fast=true')

        with new_test_table(cql, test_keyspace,
                            "pk int PRIMARY KEY, v text") as table:
            # Generate some load
            stmt = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
            for i in range(500):
                cql.execute(stmt, [i, f"value_{i}"])

            # Also do some reads to generate more traced queries
            for i in range(100):
                cql.execute(f"SELECT * FROM {table} WHERE pk = {i}")

            # Wait for traces to be flushed
            deadline = time.time() + 30
            slow_log_populated = False
            while time.time() < deadline:
                slow_log = list(cql.execute(
                    SimpleStatement(
                        "SELECT * FROM system_traces.node_slow_log",
                        consistency_level=ConsistencyLevel.ONE)))
                if slow_log:
                    slow_log_populated = True
                    break
                time.sleep(1)

            assert slow_log_populated, "system_traces.node_slow_log is empty after slow query tracing"

            # sessions should be populated
            sessions = list(cql.execute(
                SimpleStatement(
                    "SELECT * FROM system_traces.sessions",
                    consistency_level=ConsistencyLevel.ONE)))
            assert len(sessions) > 0, "system_traces.sessions is empty"

            # In fast mode, no new events should have been written
            events_after = len(list(cql.execute(
                SimpleStatement(
                    "SELECT session_id FROM system_traces.events",
                    consistency_level=ConsistencyLevel.ONE))))
            assert events_after == events_before, \
                f"system_traces.events grew from {events_before} to {events_after} rows in fast mode"
    finally:
        # Restore original config; use try/except to avoid masking the
        # original test failure if the REST call fails.
        try:
            if original:
                enable = str(original.get('enable', 'false')).lower()
                threshold = original.get('threshold', 500000)
                fast = str(original.get('fast', 'false')).lower()
                post_request(cql, f'storage_service/slow_query?enable={enable}&threshold={threshold}&fast={fast}')
            else:
                post_request(cql, 'storage_service/slow_query?enable=false')
        except Exception:
            pass


def test_slow_query_tracing_full(cql, test_keyspace, scylla_only):
    """Test that in non-fast slow-query tracing mode, system_traces.events,
    node_slow_log, and sessions are all populated.

    Ported from scylla-dtest cql_tracing_test.py::test_fast_slow_query_tracing[disabled].
    """
    original = get_request(cql, 'storage_service/slow_query')

    try:
        # Enable slow query tracing in non-fast mode with very low threshold
        post_request(cql, 'storage_service/slow_query?enable=true&threshold=0&fast=false')

        with new_test_table(cql, test_keyspace,
                            "pk int PRIMARY KEY, v text") as table:
            stmt = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
            for i in range(500):
                cql.execute(stmt, [i, f"value_{i}"])

            for i in range(100):
                cql.execute(f"SELECT * FROM {table} WHERE pk = {i}")

            # Wait for traces to be flushed
            deadline = time.time() + 30
            slow_log_populated = False
            while time.time() < deadline:
                slow_log = list(cql.execute(
                    SimpleStatement(
                        "SELECT * FROM system_traces.node_slow_log",
                        consistency_level=ConsistencyLevel.ONE)))
                if slow_log:
                    slow_log_populated = True
                    break
                time.sleep(1)

            assert slow_log_populated, "system_traces.node_slow_log is empty"

            sessions = list(cql.execute(
                SimpleStatement(
                    "SELECT * FROM system_traces.sessions",
                    consistency_level=ConsistencyLevel.ONE)))
            assert len(sessions) > 0, "system_traces.sessions is empty"

            # In non-fast mode, events should also be populated
            events = list(cql.execute(
                SimpleStatement(
                    "SELECT * FROM system_traces.events",
                    consistency_level=ConsistencyLevel.ONE)))
            assert len(events) > 0, \
                f"system_traces.events should be non-empty in non-fast mode"
    finally:
        try:
            if original:
                enable = str(original.get('enable', 'false')).lower()
                threshold = original.get('threshold', 500000)
                fast = str(original.get('fast', 'false')).lower()
                post_request(cql, f'storage_service/slow_query?enable={enable}&threshold={threshold}&fast={fast}')
            else:
                post_request(cql, 'storage_service/slow_query?enable=false')
        except Exception:
            pass
