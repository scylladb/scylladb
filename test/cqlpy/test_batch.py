# -*- coding: utf-8 -*-
# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Tests for batch operations
#############################################################################
from cassandra import InvalidRequest
from cassandra.cluster import NoHostAvailable
from .util import new_test_table, unique_key_int
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

#############################################################################
# The following tests were translated from scylla-dtest's batch_test.py.
#############################################################################

@pytest.fixture(scope='module')
def clicks(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'userid int, url text, total counter, PRIMARY KEY (userid, url)') as table:
        yield table

@pytest.fixture(scope='module')
def users(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'id int PRIMARY KEY, firstname text, lastname text') as table:
        yield table

# Test that a counter batch accepts counter updates, and applies them.
# Translated from batch_test.py::TestBatch::test_counter_batch_accepts_counter_mutations.
def test_counter_batch_accepts_counter_mutations(cql, clicks):
    user1 = unique_key_int()
    user2 = unique_key_int()
    cql.execute(f"""
        BEGIN COUNTER BATCH
        UPDATE {clicks} SET total = total + 1 WHERE userid = {user1} AND url = 'http://foo.com'
        UPDATE {clicks} SET total = total + 1 WHERE userid = {user1} AND url = 'http://bar.com'
        UPDATE {clicks} SET total = total + 1 WHERE userid = {user2} AND url = 'http://baz.com'
        APPLY BATCH
    """)
    assert sorted(cql.execute(f'SELECT userid, url, total FROM {clicks} WHERE userid IN ({user1}, {user2})')) == [
        (user1, 'http://bar.com', 1), (user1, 'http://foo.com', 1), (user2, 'http://baz.com', 1)]

# Test that a logged or unlogged batch accepts regular (non-counter) writes,
# and applies them.
# Translated from batch_test.py::TestBatch tests
# test_logged_batch_accepts_regular_mutations and
# test_unlogged_batch_accepts_regular_mutations.
@pytest.mark.parametrize('batch_type', ['', 'UNLOGGED'])
def test_batch_accepts_regular_mutations(cql, users, batch_type):
    id1 = unique_key_int()
    id2 = unique_key_int()
    cql.execute(f"""
        BEGIN {batch_type} BATCH
        INSERT INTO {users} (id, firstname, lastname) VALUES ({id1}, 'Jack', 'Sparrow')
        INSERT INTO {users} (id, firstname, lastname) VALUES ({id2}, 'Will', 'Turner')
        APPLY BATCH
    """)
    assert sorted(cql.execute(f'SELECT * FROM {users} WHERE id IN ({id1}, {id2})')) == sorted([
        (id1, 'Jack', 'Sparrow'), (id2, 'Will', 'Turner')])

# Test that counter updates and regular writes can't be mixed in one batch,
# and that a logged batch can't contain counter updates. Each kind of batch
# has its own error message. For a logged batch, both errors apply: Scylla
# reports that a logged batch can't contain counter updates, and Cassandra
# reports that counter updates and regular writes can't be mixed.
# Translated from batch_test.py::TestBatch tests
# test_counter_batch_rejects_regular_mutations,
# test_logged_batch_rejects_counter_mutations and
# def_unlogged_batch_rejects_counter_mutations (which never ran, because
# its name doesn't begin with "test").
@pytest.mark.parametrize('batch_type,message', [
    ('COUNTER', 'Cannot include non-counter statement in a counter batch'),
    ('', 'Cannot include a counter statement in a logged batch|Counter and non-counter mutations cannot exist in the same batch'),
    ('UNLOGGED', 'Counter and non-counter mutations cannot exist in the same batch')])
def test_batch_rejects_mixed_mutations(cql, clicks, users, batch_type, message):
    with pytest.raises(InvalidRequest, match=message):
        cql.execute(f"""
            BEGIN {batch_type} BATCH
            UPDATE {clicks} SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'
            INSERT INTO {users} (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            APPLY BATCH
        """)

# Test that a logged batch can't contain counter updates, also when it
# contains nothing else.
# Added to the translation of batch_test.py::TestBatch::test_logged_batch_rejects_counter_mutations
# above, because for that test's batch, which also has a regular write,
# Cassandra reports a different error.
def test_logged_batch_rejects_counter_mutations(cql, clicks):
    with pytest.raises(InvalidRequest, match='Cannot include a counter statement in a logged batch'):
        cql.execute(f"""
            BEGIN BATCH
            UPDATE {clicks} SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'
            APPLY BATCH
        """)

# Test that a logged batch writing to a table with gc_grace_seconds = 0
# returns a warning to the client, because the gc_grace_seconds is used as
# the TTL of the batchlog entries, so they may expire before being replayed.
# Translated from batch_test.py::TestBatch::test_logged_batch_gcgs_below_threshold_single_table,
# which looked for the warning in the log instead.
@pytest.mark.xfail(reason='issue #2483')
def test_logged_batch_gc_grace_seconds_0_warning(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'id int PRIMARY KEY, firstname text, lastname text', 'WITH gc_grace_seconds = 0') as table:
        response_future = cql.execute_async(f"""
            BEGIN BATCH
            INSERT INTO {table} (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO {table} (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)
        response_future.result()
        assert response_future.warnings == [
            f'Executing a LOGGED BATCH on table [{table}], configured with a gc_grace_seconds of 0. '
            'The gc_grace_seconds is used to TTL batchlog entries, so setting gc_grace_seconds too low '
            'on tables involved in an atomic batch might cause batchlog entries to expire before being replayed.']

# Like test_logged_batch_gc_grace_seconds_0_warning, with a batch writing to
# two tables with gc_grace_seconds = 0: one warning names both tables.
# Translated from batch_test.py::TestBatch::test_logged_batch_gcgs_below_threshold_multi_table.
@pytest.mark.xfail(reason='issue #2483')
def test_logged_batch_gc_grace_seconds_0_warning_two_tables(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'id int PRIMARY KEY, firstname text, lastname text', 'WITH gc_grace_seconds = 0') as table1, \
         new_test_table(cql, test_keyspace, 'userid int, url text, PRIMARY KEY (userid, url)', 'WITH gc_grace_seconds = 0') as table2:
        response_future = cql.execute_async(f"""
            BEGIN BATCH
            INSERT INTO {table1} (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO {table2} (userid, url) VALUES (1, 'Will')
            APPLY BATCH
        """)
        response_future.result()
        assert response_future.warnings is not None and len(response_future.warnings) == 1
        warning = response_future.warnings[0]
        # The order of the two tables in the warning is unspecified
        assert warning in [f'Executing a LOGGED BATCH on tables [{t1}, {t2}], configured with a gc_grace_seconds of 0. '
                           'The gc_grace_seconds is used to TTL batchlog entries, so setting gc_grace_seconds too low '
                           'on tables involved in an atomic batch might cause batchlog entries to expire before being replayed.'
                           for t1, t2 in [(table1, table2), (table2, table1)]]

# Test that an unlogged batch writing to a table with gc_grace_seconds = 0
# doesn't return a warning: an unlogged batch doesn't use the batchlog.
# Translated from batch_test.py::TestBatch::test_unlogged_batch_gcgs_below_threshold_should_not_print_warning.
# The dtest looked for a message in the log which neither Scylla nor
# Cassandra ever print, so it could never fail; this test checks that the
# response has no warnings at all.
def test_unlogged_batch_gc_grace_seconds_0_no_warning(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'id int PRIMARY KEY, firstname text, lastname text', 'WITH gc_grace_seconds = 0') as table:
        response_future = cql.execute_async(f"""
            BEGIN UNLOGGED BATCH
            INSERT INTO {table} (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO {table} (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)
        response_future.result()
        assert not response_future.warnings
