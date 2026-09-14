# -*- coding: utf-8 -*-
# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Tests for batch operations
#############################################################################
from cassandra import InvalidRequest
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import SyntaxException
from .util import new_test_table
from .rest_api import scylla_inject_error


import pytest


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int primary key, t text") as table:
        yield table


def generate_big_batch(table, size_in_kb):
    statements = [f"INSERT INTO {table} (k, t) VALUES ({idx}, '{'x' * 743}')" for idx in range(size_in_kb)]
    return "BEGIN BATCH\n" + "\n".join(statements) + "\n APPLY BATCH\n"


@pytest.mark.xfail(reason="Scylla does not send warnings for batch: https://github.com/scylladb/scylla/issues/10196")
def test_warnings_are_returned_in_response(cql, table1):
    """Verifies if response contains warning messages.
    Because warning message threshold is different for Scylla and Cassandra,
    test tries several sizes until warning appears.
    """
    for size_in_kb in [10, 129, 256]:
        over_sized_batch = generate_big_batch(table1, size_in_kb)
        response_future = cql.execute_async(over_sized_batch)
        response_future.result()
        if response_future.warnings:
            break
    else:
        pytest.fail('Oversized batch did not generate a warning')

    # example message for Cassandra:
    # 'Batch for [cql_test_1647006065554.cql_test_1647006065623] is of size 7590,
    # exceeding specified threshold of 5120 by 2470.'
    assert "exceeding specified" in response_future.warnings[0]


def test_error_is_raised_for_batch_size_above_threshold(cql, table1):
    """Verifies cql returns error for batch that exceeds size provided in batch_size_fail_threshold_in_kb setting
    from scylla.yaml."""
    with pytest.raises(InvalidRequest, match="Batch too large"):
        cql.execute(generate_big_batch(table1, 1025))

# Test checks unexpected errors handling in CQL server.
#
# The original problem was that std::bad_alloc exception occurred while parsing a large batch request.
# This exception was caught by try/catch in cql_server::connection::process_request_one and
# an attempt was made to construct the error response message via make_error function.
# This attempt failed since the error message contained entire query and exceeded the limit of 64K
# in cql_server::response::write_string, causing "Value too large" exception to be thrown.
# This new exception reached the general handler in cql_server::connection::process_request, where
# it was just logged and no information about the problem was sent to the client.
# As a result, the client received a timeout exception after a while and
# no other information about the cause of the error.
#
# It is quite difficult to reproduce OOM in a test, so we use error injection instead.
# Passing injection_key in the body of the request ensures that the exception will be
# thrown only for this test request and will not affect other requests that
# the driver may send in the background.
def test_batch_with_error(cql, table1):
    injection_key = 'query_processor-parse_statement-test_failure'
    with scylla_inject_error(cql, injection_key, one_shot=False):
        # exceptions::exception_code::SERVER_ERROR, it gets converted to NoHostAvailable by the driver
        with pytest.raises(NoHostAvailable, match="Value too large"):
            cql.execute(generate_big_batch(table1, 100) + injection_key)

# A large textual batch is parsed and prepared with yields in between
# (SCYLLADB-219). Run a batch of many statements as an unprepared QUERY and as
# a PREPARE followed by an EXECUTE, and check that every statement took effect.
def test_large_batch_query(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int primary key, t text") as table:
        n = 1000
        inserts = "\n".join(f"INSERT INTO {table} (k, t) VALUES ({i}, 'q')" for i in range(n))
        cql.execute("BEGIN UNLOGGED BATCH\n" + inserts + "\nAPPLY BATCH")
        assert cql.execute(f"SELECT COUNT(*) FROM {table}").one()[0] == n

def test_large_batch_prepared(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int primary key, t text") as table:
        n = 1000
        inserts = "\n".join(f"INSERT INTO {table} (k, t) VALUES (?, ?)" for _ in range(n))
        prepared = cql.prepare("BEGIN UNLOGGED BATCH\n" + inserts + "\nAPPLY BATCH")
        cql.execute(prepared, [v for i in range(n) for v in (i, 'p')])
        assert cql.execute(f"SELECT COUNT(*) FROM {table}").one()[0] == n

# A syntax error at the end of a large textual batch is reported as usual,
# though such a batch is parsed in a thread that yields (SCYLLADB-219).
def test_large_batch_syntax_error(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int primary key, t text") as table:
        inserts = "\n".join(f"INSERT INTO {table} (k, t) VALUES ({i}, 'q')" for i in range(1000))
        with pytest.raises(SyntaxException):
            cql.execute("BEGIN UNLOGGED BATCH\n" + inserts + "\nAPPLY")
