# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the rows a traced query leaves in the system_traces tables: its
# events, its session record and the time index entry of that, and, with
# slow query logging on, its slow query log record and the time index entry
# of that. The trace keyspace backend builds these rows as mutations rather
# than with INSERT statements, so every column and the TTL of every cell is
# checked, not just the activity the driver reads back.

import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from .rest_api import post_request
from .util import new_test_table

# The TTL of the records of a traced query, tracing::ttl_by_type() for a query
TRACE_TTL = 86400


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    """The table the traced queries of the tests read, with one row"""
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int") as table:
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (1, 1)")
        yield table


def traced_select(cql, table):
    """Runs a traced SELECT on the table and returns the trace the driver read back"""
    rf = cql.execute_async(SimpleStatement(f"SELECT * FROM {table} WHERE p = 1"), trace=True)
    rf.result()
    trace = rf.get_query_trace(max_wait=30)
    assert trace.duration is not None, "no session row"
    return trace


def read_trace_rows(cql, query, params=None, expected=1, timeout=30):
    """Reads trace rows, waiting for the backend to write them out. Reads at CL ONE: the trace
    keyspace has a replication factor of 2, which a one node test cluster cannot read at a
    quorum, and the records are written at CL ANY."""
    stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.ONE)
    deadline = time.time() + timeout
    while True:
        rows = list(cql.execute(stmt, params))
        if len(rows) >= expected or time.time() > deadline:
            return rows
        time.sleep(0.1)


def assert_trace_ttl(ttls, row):
    for ttl in ttls:
        assert TRACE_TTL - 120 <= ttl <= TRACE_TTL, row


def test_trace_events(cql, table1, scylla_only):
    trace = traced_select(cql, table1)
    assert len(trace.events) > 0, "no event rows"

    rows = read_trace_rows(cql,
        "SELECT event_id, activity, source, source_elapsed, thread, scylla_parent_id, scylla_span_id,"
        " TTL(activity), TTL(source), TTL(source_elapsed), TTL(thread), TTL(scylla_parent_id), TTL(scylla_span_id)"
        f" FROM system_traces.events WHERE session_id = {trace.trace_id}",
        expected=len(trace.events))
    # Events written after the driver read the trace back (e.g. by the replica side of the read,
    # running on another shard) are not in trace.events, so it may have fewer.
    assert len(rows) >= len(trace.events)
    assert {e.description for e in trace.events} <= {row.activity for row in rows}
    for row in rows:
        assert row.event_id is not None and row.activity and row.source is not None, row
        assert row.source_elapsed is not None and row.thread, row
        assert row.scylla_parent_id is not None and row.scylla_span_id is not None, row
        assert_trace_ttl(row[7:], row)


def test_trace_session(cql, table1, scylla_only):
    trace = traced_select(cql, table1)

    rows = read_trace_rows(cql,
        "SELECT command, client, coordinator, duration, parameters, request, started_at, request_size, response_size, username,"
        " TTL(command), TTL(request), TTL(started_at), TTL(username)"
        f" FROM system_traces.sessions WHERE session_id = {trace.trace_id}")
    assert len(rows) == 1
    row = rows[0]
    assert row.command == "QUERY", row
    assert row.client is not None and row.coordinator is not None, row
    assert row.duration is not None and row.duration > 0, row
    assert row.parameters and "query" in row.parameters and table1 in row.parameters["query"], row
    assert row.request and row.started_at is not None, row
    assert row.request_size is not None and row.response_size is not None, row
    assert row.username, row
    assert_trace_ttl(row[10:], row)

    # The time index is keyed by the minute the query started in
    minute = row.started_at.replace(second=0, microsecond=0)
    rows = read_trace_rows(cql,
        "SELECT session_id FROM system_traces.sessions_time_idx WHERE minute = %s AND started_at = %s AND session_id = %s",
        (minute, row.started_at, trace.trace_id))
    assert len(rows) == 1, "no sessions_time_idx entry"


def test_slow_query_log(cql, table1, scylla_only):
    # Every query is a slow one with a threshold of zero
    post_request(cql, f"storage_service/slow_query?enable=true&threshold=0&ttl={TRACE_TTL}")
    try:
        trace = traced_select(cql, table1)

        rows = read_trace_rows(cql,
            "SELECT start_time, node_ip, shard, session_id, date, command, duration, parameters, source_ip, table_names, username,"
            " TTL(date), TTL(command), TTL(duration), TTL(source_ip), TTL(username)"
            f" FROM system_traces.node_slow_log WHERE session_id = {trace.trace_id} ALLOW FILTERING")
        assert len(rows) == 1, "no slow query log row"
        row = rows[0]
        assert row.start_time is not None and row.node_ip is not None and row.shard is not None, row
        assert row.date is not None and row.duration is not None and row.duration >= 0, row
        assert row.command and table1 in row.command, row
        assert row.parameters and "query" in row.parameters, row
        assert row.source_ip is not None and row.username, row
        assert row.table_names == {table1}, row
        assert_trace_ttl(row[11:], row)

        minute = row.date.replace(second=0, microsecond=0)
        rows = read_trace_rows(cql,
            "SELECT start_time, node_ip, shard, TTL(start_time), TTL(node_ip), TTL(shard) FROM system_traces.node_slow_log_time_idx"
            " WHERE minute = %s AND started_at = %s AND session_id = %s",
            (minute, row.date, trace.trace_id))
        assert len(rows) == 1, "no node_slow_log_time_idx entry"
        idx = rows[0]
        assert idx.start_time == row.start_time and idx.node_ip == row.node_ip and idx.shard == row.shard, idx
        assert_trace_ttl(idx[3:], idx)
    finally:
        post_request(cql, "storage_service/slow_query?enable=false")


def test_slow_query_log_without_ttl(cql, table1, scylla_only):
    # A slow query log TTL of 0 means the records never expire, as with an INSERT ... USING TTL 0,
    # rather than them expiring right away.
    post_request(cql, "storage_service/slow_query?enable=true&threshold=0&ttl=0")
    try:
        trace = traced_select(cql, table1)

        rows = read_trace_rows(cql,
            "SELECT date, command, duration, TTL(date), TTL(command), TTL(duration)"
            f" FROM system_traces.node_slow_log WHERE session_id = {trace.trace_id} ALLOW FILTERING")
        assert len(rows) == 1, "no slow query log row"
        row = rows[0]
        assert row.date is not None and row.command and row.duration is not None, row
        assert row[3:] == (None, None, None), row

        minute = row.date.replace(second=0, microsecond=0)
        rows = read_trace_rows(cql,
            "SELECT start_time, TTL(start_time) FROM system_traces.node_slow_log_time_idx"
            " WHERE minute = %s AND started_at = %s AND session_id = %s",
            (minute, row.date, trace.trace_id))
        assert len(rows) == 1, "no node_slow_log_time_idx entry"
        assert rows[0].start_time is not None and rows[0][1] is None, rows[0]
    finally:
        post_request(cql, f"storage_service/slow_query?enable=false&ttl={TRACE_TTL}")
