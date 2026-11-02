# -*- coding: utf-8 -*-
# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Tests for batch operations
#############################################################################
import time

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.cluster import NoHostAvailable
from cassandra.query import BatchStatement, SimpleStatement
from .util import new_test_table, unique_key_int
from . import rest_api
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
# Translated from scylla-dtest's batch_test.py::TestBatch::test_counter_batch_accepts_counter_mutations.
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
# Translated from scylla-dtest's batch_test.py::TestBatch tests
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
# Translated from scylla-dtest's batch_test.py::TestBatch tests
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
# Added to the translation of scylla-dtest's batch_test.py::TestBatch::test_logged_batch_rejects_counter_mutations
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
# Translated from scylla-dtest's batch_test.py::TestBatch::test_logged_batch_gcgs_below_threshold_single_table,
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
# Translated from scylla-dtest's batch_test.py::TestBatch::test_logged_batch_gcgs_below_threshold_multi_table.
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
# Translated from scylla-dtest's batch_test.py::TestBatch::test_unlogged_batch_gcgs_below_threshold_should_not_print_warning.
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

# The following tests were translated from scylla-dtest's
# cassandra_batch_test.py. They are similar to cassandra_tests'
# testBatchTTLConditionalInteraction and
# testBatchStaticTTLConditionalInteraction, but those send each batch as a
# "BEGIN BATCH ... APPLY BATCH" statement, while these use the driver's
# BatchStatement, which sends a protocol BATCH message - and in
# check_batch_ttl_conditional_interaction, of prepared statements. Instead
# of sleeping 6 seconds for TTLs of 5 seconds to expire, these tests use
# the clock fixture. A value which the dtests wrote with a TTL of 10 seconds,
# and checked was still alive after 6 seconds, has a TTL of 20 seconds here:
# real time passes in addition to the clock's jumps, and a slow (e.g.,
# debug) build may take more than the 4 seconds that left.

# A sequence of batches mixing conditional and unconditional writes, some
# with TTLs, checking the table's contents after each batch. Many of the
# batches contain prepared statements. update is the prepared conditional
# update used in many of the batches.
def check_batch_ttl_conditional_interaction(cql, table, clock, update):
    insert = cql.prepare(f'INSERT INTO {table} (id, clustering1, clustering2, clustering3, val) VALUES (?, ?, ?, ?, ?)')
    conditional_update = cql.prepare(update)
    delete = cql.prepare(f'DELETE FROM {table} WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=?')
    get_rows = cql.prepare(f'SELECT * FROM {table} WHERE id=?')
    def rows():
        return sorted(tuple(r) for r in cql.execute(get_rows, [1]))
    def batch(*statements):
        b = BatchStatement()
        for s in statements:
            if isinstance(s, tuple):
                b.add(*s)
            else:
                b.add(s)
        cql.execute(b)

    cql.execute(insert, (1, 1, 1, 1, 1))
    assert rows() == [(1, 1, 1, 1, 1)]
    batch((insert, (1, 1, 1, 2, 2)), (conditional_update, (11, 1, 1, 1, 1, 1)))
    assert rows() == [(1, 1, 1, 1, 11), (1, 1, 1, 2, 2)]
    batch((insert, (1, 1, 2, 3, 23)), (conditional_update, (22, 1, 1, 1, 2, 2)), (delete, (1, 1, 1, 1)))
    assert rows() == [(1, 1, 1, 2, 22), (1, 1, 2, 3, 23)]
    batch((insert, (1, 2, 3, 4, 1234)), (conditional_update, (234, 1, 1, 1, 2, 22)))
    assert rows() == [(1, 1, 1, 2, 234), (1, 1, 2, 3, 23), (1, 2, 3, 4, 1234)]
    batch(f'DELETE FROM {table} WHERE id=1 AND clustering1=2', (conditional_update, (1234, 1, 1, 1, 2, 234)))
    assert rows() == [(1, 1, 1, 2, 1234), (1, 1, 2, 3, 23)]
    batch(f'UPDATE {table} SET val=345 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5',
          (conditional_update, (1, 1, 1, 1, 2, 1234)))
    assert rows() == [(1, 1, 1, 2, 1), (1, 1, 2, 3, 23), (1, 3, 4, 5, 345)]
    # If any condition is not fulfilled, the entire batch is skipped - so
    # nothing is deleted either
    batch((delete, (1, 3, 4, 5)), (conditional_update, (2300, 1, 1, 2, 3, 1)))
    assert rows() == [(1, 1, 1, 2, 1), (1, 1, 2, 3, 23), (1, 3, 4, 5, 345)]
    batch(f'DELETE FROM {table} WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=345',
          f'DELETE FROM {table} WHERE id=1 AND clustering1=1',
          (insert, (1, 2, 3, 4, 5)))
    assert rows() == [(1, 2, 3, 4, 5)]
    batch(f'INSERT INTO {table} (id, clustering1, clustering2, clustering3, val) VALUES (1, 3, 4, 5, 345) IF NOT EXISTS',
          (delete, (1, 2, 3, 4)))
    assert rows() == [(1, 3, 4, 5, 345)]
    batch(f'INSERT INTO {table} (id, clustering1, clustering2, clustering3, val) VALUES (1, 2, 3, 4, 5) USING TTL 5',
          f'UPDATE {table} USING TTL 20 SET val=5 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=345')
    assert rows() == [(1, 2, 3, 4, 5), (1, 3, 4, 5, 5)]
    clock.jump(6)
    assert rows() == [(1, 3, 4, 5, 5)]
    batch(f'INSERT INTO {table} (id, clustering1, clustering2, clustering3, val) VALUES (1, 2, 3, 4, 5) IF NOT EXISTS USING TTL 5',
          (insert, (1, 4, 5, 6, 7)))
    assert rows() == [(1, 2, 3, 4, 5), (1, 3, 4, 5, 5), (1, 4, 5, 6, 7)]
    clock.jump(15)
    assert rows() == [(1, 3, 4, 5, None), (1, 4, 5, 6, 7)]
    batch(f'UPDATE {table} USING TTL 5 SET val=5 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=NULL',
          f'UPDATE {table} USING TTL 5 SET val=8 WHERE id=1 AND clustering1=4 AND clustering2=5 AND clustering3=6')
    assert rows() == [(1, 3, 4, 5, 5), (1, 4, 5, 6, 8)]
    clock.jump(6)
    assert rows() == [(1, 3, 4, 5, None), (1, 4, 5, 6, None)]

CLUSTERING_SCHEMA = 'id int, clustering1 int, clustering2 int, clustering3 int, val int, PRIMARY KEY (id, clustering1, clustering2, clustering3)'
CONDITIONAL_UPDATE = 'UPDATE {} SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=? IF val=?'

# Translated from scylla-dtest's cassandra_batch_test.py::TestBatch::test_batch_ttl_conditional_interaction.
def test_batch_ttl_conditional_interaction(cql, test_keyspace, clock):
    with new_test_table(cql, test_keyspace, CLUSTERING_SCHEMA) as table:
        check_batch_ttl_conditional_interaction(cql, table, clock, CONDITIONAL_UPDATE.format(table))

# Test that the batches of check_batch_ttl_conditional_interaction work
# when the slow query log is enabled with a threshold of 0, so all queries
# are logged, and that the prepared conditional update in these batches is
# logged. Reproduces #5843, where the slow query log record of a batch was
# not written. This test is scylla_only because it enables the slow query
# log with Scylla's REST API.
# This test is slow - it takes one second on average, and up to two - because
# Scylla writes tracing records, including the slow query log, to its tables
# only every two seconds, so the test has to wait for the record to appear.
# Translated from scylla-dtest's cassandra_batch_test.py::TestBatch::test_batch_prepared_with_slow_query_log.
# The dtest also checked that the log doesn't have the message "No query
# parameter set for a session requesting a slow_query_log record", but since
# the fix of #5843, this message is logged only at trace level.
def test_batch_prepared_with_slow_query_log(cql, test_keyspace, clock, scylla_only):
    with new_test_table(cql, test_keyspace, CLUSTERING_SCHEMA) as table:
        update = CONDITIONAL_UPDATE.format(table)
        old = rest_api.get_request(cql, 'storage_service/slow_query')
        rest_api.post_request(cql, 'storage_service/slow_query?enable=true&ttl=604800&threshold=0')
        try:
            check_batch_ttl_conditional_interaction(cql, table, clock, update)
        finally:
            rest_api.post_request(cql, f"storage_service/slow_query?enable={str(old['enable']).lower()}&ttl={old['ttl']}&threshold={old['threshold']}")
        # The system_traces keyspace has RF=2, so it is read with CL=ONE
        select = SimpleStatement('SELECT command FROM system_traces.node_slow_log', consistency_level=ConsistencyLevel.ONE)
        deadline = time.time() + 30
        while time.time() < deadline:
            if any(r.command == update for r in cql.execute(select)):
                break
            time.sleep(0.1)
        else:
            pytest.fail(f'{update} not found in the slow query log')

# Like test_batch_ttl_conditional_interaction, on a table with a static
# column, with conditions on the static column, and without prepared
# statements.
# Translated from scylla-dtest's cassandra_batch_test.py::TestBatch::test_batch_static_ttl_conditional_interaction.
def test_batch_static_ttl_conditional_interaction(cql, test_keyspace, clock):
    with new_test_table(cql, test_keyspace, 'id int, clustering1 int, clustering2 int, clustering3 int, sval int static, val int, PRIMARY KEY (id, clustering1, clustering2, clustering3)') as table:
        def rows():
            return sorted(tuple(r) for r in cql.execute(f'SELECT * FROM {table} WHERE id=1'))
        def batch(*statements):
            b = BatchStatement()
            for s in statements:
                if isinstance(s, tuple):
                    b.add(*s)
                else:
                    b.add(s)
            cql.execute(b)
        def insert(c1, c2, c3, val, extra=''):
            return f'INSERT INTO {table} (id, clustering1, clustering2, clustering3, val) VALUES (1, {c1}, {c2}, {c3}, {val}) {extra}'
        def update(c1, c2, c3, val, extra='', using=''):
            return f'UPDATE {table} {using} SET val={val} WHERE id=1 AND clustering1={c1} AND clustering2={c2} AND clustering3={c3} {extra}'
        def delete(c1, c2, c3, extra=''):
            return f'DELETE FROM {table} WHERE id=1 AND clustering1={c1} AND clustering2={c2} AND clustering3={c3} {extra}'

        batch(f'INSERT INTO {table} (id, clustering1, clustering2, clustering3, sval, val) VALUES (1, 1, 1, 1, 1, 1)')
        assert rows() == [(1, 1, 1, 1, 1, 1)]
        batch(insert(1, 1, 2, 2), update(1, 1, 1, 11, 'IF sval=1'))
        assert rows() == [(1, 1, 1, 1, 1, 11), (1, 1, 1, 2, 1, 2)]
        batch(insert(1, 2, 3, 23), f'UPDATE {table} SET sval=22 WHERE id=1', delete(1, 1, 1))
        assert rows() == [(1, 1, 1, 2, 22, 2), (1, 1, 2, 3, 22, 23)]
        batch(insert(2, 3, 4, 1234), update(1, 1, 2, 234, 'IF sval=22', 'USING TTL 5'))
        assert rows() == [(1, 1, 1, 2, 22, 234), (1, 1, 2, 3, 22, 23), (1, 2, 3, 4, 22, 1234)]
        clock.jump(6)
        assert rows() == [(1, 1, 1, 2, 22, None), (1, 1, 2, 3, 22, 23), (1, 2, 3, 4, 22, 1234)]
        batch(f'DELETE FROM {table} WHERE id=1 AND clustering1=2', update(1, 1, 2, 1234, 'IF sval=22'))
        assert rows() == [(1, 1, 1, 2, 22, 1234), (1, 1, 2, 3, 22, 23)]
        batch(update(3, 4, 5, 345), update(1, 1, 2, 1, 'IF sval=22'))
        assert rows() == [(1, 1, 1, 2, 22, 1), (1, 1, 2, 3, 22, 23), (1, 3, 4, 5, 22, 345)]
        # The condition doesn't match, so the entire batch is skipped
        batch(delete(3, 4, 5), update(1, 2, 3, 2300, 'IF sval=1'))
        assert rows() == [(1, 1, 1, 2, 22, 1), (1, 1, 2, 3, 22, 23), (1, 3, 4, 5, 22, 345)]
        batch(delete(3, 4, 5, 'IF val=345'), f'DELETE FROM {table} WHERE id=1 AND clustering1=1', insert(2, 3, 4, 5))
        assert rows() == [(1, 2, 3, 4, 22, 5)]
        batch(insert(3, 4, 5, 345, 'IF NOT EXISTS'), delete(2, 3, 4))
        assert rows() == [(1, 3, 4, 5, 22, 345)]
        batch(insert(2, 3, 4, 5, 'USING TTL 5'), update(3, 4, 5, 5, 'IF val=345', 'USING TTL 20'))
        assert rows() == [(1, 2, 3, 4, 22, 5), (1, 3, 4, 5, 22, 5)]
        clock.jump(6)
        assert rows() == [(1, 3, 4, 5, 22, 5)]
        batch(insert(2, 3, 4, 5, 'IF NOT EXISTS USING TTL 5'), insert(4, 5, 6, 7))
        assert rows() == [(1, 2, 3, 4, 22, 5), (1, 3, 4, 5, 22, 5), (1, 4, 5, 6, 22, 7)]
        clock.jump(15)
        assert rows() == [(1, 3, 4, 5, 22, None), (1, 4, 5, 6, 22, 7)]
        batch(update(3, 4, 5, 5, 'IF val=NULL', 'USING TTL 5'), update(4, 5, 6, 8, '', 'USING TTL 5'))
        assert rows() == [(1, 3, 4, 5, 22, 5), (1, 4, 5, 6, 22, 8)]
        clock.jump(6)
        assert rows() == [(1, 3, 4, 5, 22, None), (1, 4, 5, 6, 22, None)]
        # A statement with parameters, which the driver substitutes
        batch((f'DELETE FROM {table} WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s', (1, 3, 4, 5, 22)),
              insert(2, 3, 4, 5))
        assert rows() == [(1, 2, 3, 4, 22, 5), (1, 4, 5, 6, 22, None)]
        batch(f'UPDATE {table} SET sval=23 WHERE id=1 IF sval=22', delete(4, 5, 6))
        assert rows() == [(1, 2, 3, 4, 23, 5)]
