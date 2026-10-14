# -*- coding: utf-8 -*-
# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Tests for batch operations
#############################################################################
from cassandra import InvalidRequest
from cassandra.cluster import NoHostAvailable
from cassandra.query import BatchStatement
from .util import new_test_table, new_test_keyspace, unique_name
from .rest_api import scylla_inject_error


import pytest


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int primary key, t text") as table:
        yield table


def generate_big_batch(table, size_in_kb):
    statements = [f"INSERT INTO {table} (k, t) VALUES ({idx}, '{'x' * 743}')" for idx in range(size_in_kb)]
    return "BEGIN BATCH\n" + "\n".join(statements) + "\n APPLY BATCH\n"


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


# Like test_warnings_are_returned_in_response, but sends the batch as the CQL
# protocol's BATCH message (as drivers usually do), instead of as a "BEGIN
# BATCH ... APPLY BATCH" statement in a QUERY message.
# Reproduces #10196.
def test_warnings_are_returned_in_response_to_batch_message(cql, table1):
    insert = cql.prepare(f'INSERT INTO {table1} (k, t) VALUES (?, ?)')
    for size_in_kb in [10, 129, 256]:
        batch = BatchStatement()
        for idx in range(size_in_kb):
            batch.add(insert, [idx, 'x' * 743])
        response_future = cql.execute_async(batch)
        response_future.result()
        if response_future.warnings:
            break
    else:
        pytest.fail('Oversized batch did not generate a warning')
    assert 'exceeding specified' in response_future.warnings[0]


# The oversized-batch warning lists every table the batch writes to, so a
# batch over a few hundred long-named tables gets a warning longer than 65535
# bytes, the most a CQL string can hold. The batch must still succeed and
# return the warning; Cassandra truncates it and ends it with " [truncated]".
# This test passes on Cassandra 5, but Cassandra 6 fails to send any string
# longer than 32767 bytes, so the client gets no response, and times out
# (CASSANDRA-21763).
# This is a slow test - it creates 303 tables, taking about 5 seconds in a dev
# build and 50 in a debug build - because we have no other way to create a
# very long warning: no other warning includes text of arbitrary length, and
# this one lists every table only once, with names of at most 192 characters.
# So it is marked tier2, which test/cqlpy/run and CI's per-PR runs skip - even
# when the test is named explicitly. To run it, pass "-m tier2" to test/cqlpy/run.
@pytest.mark.tier2
def test_warning_longer_than_64k_is_returned_in_response(cql, cassandra_bug):
    with new_test_keyspace(cql, "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        prefix = unique_name()
        table_name_len = 192
        n_tables = 65535 // (len(ks) + len('.') + table_name_len + len(', ')) + 1
        tables = [f'{ks}.' + f'{prefix}_{i:04d}_'.ljust(table_name_len, 'x') for i in range(n_tables)]
        for table in tables:
            cql.execute(f'CREATE TABLE {table} (k int PRIMARY KEY, t text)')
        for size_in_kb in [10, 256]:
            value = 'x' * (size_in_kb * 1024 // n_tables)
            # A distinct key per table: Cassandra merges same-key writes in a keyspace
            # into one mutation, and doesn't check the size of a single-mutation batch.
            batch = 'BEGIN BATCH\n' + '\n'.join(
                f"INSERT INTO {table} (k, t) VALUES ({i}, '{value}')" for i, table in enumerate(tables)) + '\nAPPLY BATCH\n'
            response_future = cql.execute_async(batch)
            response_future.result()
            if response_future.warnings:
                break
        else:
            pytest.fail('Oversized batch did not generate a warning')
        assert ks in response_future.warnings[0]
        assert response_future.warnings[0].endswith(' [truncated]')


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
