# -*- coding: utf-8 -*-
# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Tests for batch operations
#############################################################################
from test import DEBUG_MODES
from cassandra import InvalidRequest
from cassandra import ConsistencyLevel
from cassandra.cluster import NoHostAvailable
from cassandra.query import BatchStatement, SimpleStatement
from .util import new_named_table, new_test_table, unique_name
from .nodetool import flush
import contextlib
from .rest_api import scylla_inject_error


import pytest
import time


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


# BEGIN BATCH USING TIMESTAMP applies the timestamp to every statement in
# the batch, and statements in a batch without a timestamp can each have
# their own USING TIMESTAMP. Setting a timestamp on both the batch and a
# statement in it is an error.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_batch_using_timestamp(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "id int PRIMARY KEY, a text, b text") as table:
        cql.execute(f"""BEGIN BATCH USING TIMESTAMP 1111111111111111
            INSERT INTO {table} (id, a, b) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO {table} (id, a, b) VALUES (1, 'Will', 'Turner')
            APPLY BATCH""")
        assert sorted(cql.execute(f"SELECT id, writetime(a), writetime(b) FROM {table}")) == [
            (0, 1111111111111111, 1111111111111111), (1, 1111111111111111, 1111111111111111)]

@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_batch_statements_using_timestamp(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "id int PRIMARY KEY, a text, b text") as table:
        cql.execute(f"""BEGIN BATCH
            INSERT INTO {table} (id, a, b) VALUES (0, 'Jack', 'Sparrow') USING TIMESTAMP 1111111111111111
            INSERT INTO {table} (id, a, b) VALUES (1, 'Will', 'Turner') USING TIMESTAMP 1111111111111112
            APPLY BATCH""")
        assert sorted(cql.execute(f"SELECT id, writetime(a), writetime(b) FROM {table}")) == [
            (0, 1111111111111111, 1111111111111111), (1, 1111111111111112, 1111111111111112)]

@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_batch_and_statement_timestamp_conflict(cql, table1):
    with pytest.raises(InvalidRequest, match='Timestamp must be set either on BATCH or individual statements'):
        cql.execute(f"""BEGIN BATCH USING TIMESTAMP 1111111111111111
            INSERT INTO {table1} (k, t) VALUES (0, 'Jack') USING TIMESTAMP 2
            INSERT INTO {table1} (k, t) VALUES (1, 'Will')
            APPLY BATCH""")


# Tests ported from scylla-dtest's cql_additional_tests.py. Like the dtests,
# they run in a keyspace with vnodes, and keep the dtests' table names.

# A logged batch of INSERTs, an UPDATE and a column DELETE on different
# partitions, at CL=QUORUM, is applied.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_batch_of_mixed_statements(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "users", "userid text PRIMARY KEY, name text, password text") as table:
        cql.execute(SimpleStatement(f"""
            BEGIN BATCH
                INSERT INTO {table} (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                UPDATE {table} SET password = 'ps22dhds' WHERE userid = 'user3';
                INSERT INTO {table} (userid, password) VALUES ('user4', 'ch@ngem3c');
                DELETE name FROM {table} WHERE userid = 'user1';
            APPLY BATCH;""", consistency_level=ConsistencyLevel.QUORUM))
        assert sorted(cql.execute(f"SELECT * FROM {table}")) == [
            ("user2", "second user", "ch@ngem3b"), ("user3", None, "ps22dhds"), ("user4", None, "ch@ngem3c")]

# List appends and prepends in one batch are applied in statement order.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_batch_of_list_appends_and_prepends(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int PRIMARY KEY, l list<int>") as table:
        cql.execute(f"""BEGIN BATCH
            UPDATE {table} SET l = l + [ 1 ] WHERE k = 0;
            UPDATE {table} SET l = l + [ 2 ] WHERE k = 0;
            UPDATE {table} SET l = l + [ 3 ] WHERE k = 0;
          APPLY BATCH""")
        assert list(cql.execute(f"SELECT l FROM {table} WHERE k = 0")) == [([1, 2, 3],)]
        cql.execute(f"""BEGIN BATCH
            UPDATE {table} SET l = [ 1 ] + l WHERE k = 1;
            UPDATE {table} SET l = [ 2 ] + l WHERE k = 1;
            UPDATE {table} SET l = [ 3 ] + l WHERE k = 1;
          APPLY BATCH""")
        assert list(cql.execute(f"SELECT l FROM {table} WHERE k = 1")) == [([3, 2, 1],)]

# A batch deleting one row and inserting another of the same partition
# (CASSANDRA-6115).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_batch_delete_one_row_insert_another(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int, v int, PRIMARY KEY (k, v)") as table:
        cql.execute(f"INSERT INTO {table} (k, v) VALUES (0, 1)")
        cql.execute(f"BEGIN BATCH DELETE FROM {table} WHERE k=0 AND v=1; INSERT INTO {table} (k, v) VALUES (0, 2); APPLY BATCH")
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(0, 2)]


# A keyspace and table created like dtest cassandra_batch_test.py did:
# CREATE KEYSPACE IF NOT EXISTS with the RF given as a string (and vnodes,
# the dtest node's default), and a table
# named `name` (the dtests used "clustering", a keyword) with `schema`.
@contextlib.contextmanager
def batch_dtest_table(cql, name, schema):
    ks = unique_name()
    cql.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': '1' }} AND tablets = {{'enabled': false}}")
    try:
        cql.execute(f'CREATE TABLE {ks}.{name} ({schema})')
        yield f'{ks}.{name}'
    finally:
        cql.execute(f'DROP KEYSPACE {ks}')

# Runs `steps` of the dtest cassandra_batch_test.py scenarios: each step is a
# batch of (statement, parameters) - statements with '?' parameters are
# prepared, those with '%s' ones formatted by the driver - and the rows partition 1 must then have; a step with
# `wait` set polls until the rows are as expected (some TTL expired).
def run_batch_steps(cql, table, steps):
    get_rows = cql.prepare(f'SELECT * FROM {table} WHERE id=?')
    prepared = {}
    for batch_stmts, expected, wait in steps:
        if batch_stmts:
            batch = BatchStatement()
            for stmt, params in batch_stmts:
                stmt = stmt.format(t=table)
                if params is not None and '%s' in stmt:
                    batch.add(stmt, params)
                elif params is not None:
                    if stmt not in prepared:
                        prepared[stmt] = cql.prepare(stmt)
                    batch.add(prepared[stmt], params)
                else:
                    batch.add(stmt)
            cql.execute(batch)
        def rows():
            return sorted(tuple(r) for r in cql.execute(get_rows, (1,)))
        if wait:
            deadline = time.time() + 30
            while rows() != sorted(expected):
                assert time.time() < deadline, rows()
                time.sleep(0.1)
        assert rows() == sorted(expected)
    # The expired data is written to sstables, as when a dtest node stops.
    flush(cql, table)
    assert sorted(tuple(r) for r in cql.execute(get_rows, (1,))) == sorted(steps[-1][1])

CLUSTERING_SCHEMA = 'id int, clustering1 int, clustering2 int, clustering3 int, val int, PRIMARY KEY(id, clustering1, clustering2, clustering3)'
CLUSTERING_COND_UPDATE = 'UPDATE {t} SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=? IF val=?'

# The dtest's batch_ttl_conditional_interaction() steps on the "clustering"
# table, with its conditional update statement.
def batch_ttl_conditional_interaction(cql, table, update_command=CLUSTERING_COND_UPDATE):
    ins = 'INSERT INTO {t}(id, clustering1, clustering2, clustering3, val) VALUES(?, ?, ?, ?, ?)'
    cond_upd = update_command
    delete = 'DELETE FROM {t} WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=?'
    cql.execute(cql.prepare(ins.format(t=table)), (1, 1, 1, 1, 1))
    r1, r2, r3 = (1, 1, 1, 1, 1), (1, 1, 1, 2, 2), (1, 1, 1, 1, 11)
    r4, r5 = (1, 1, 2, 3, 23), (1, 1, 1, 2, 22)
    r6, r7, r8 = (1, 2, 3, 4, 1234), (1, 1, 1, 2, 234), (1, 1, 1, 2, 1234)
    r9, r10, r11 = (1, 3, 4, 5, 345), (1, 1, 1, 2, 1), (1, 2, 3, 4, 5)
    r13, r14, r15 = (1, 3, 4, 5, 5), (1, 4, 5, 6, 7), (1, 3, 4, 5, None)
    r16, r17 = (1, 4, 5, 6, 8), (1, 4, 5, 6, None)
    run_batch_steps(cql, table, [
        ([], [r1], False),
        ([(ins, r2), (cond_upd, (11, *r1))], [r3, r2], False),
        ([(ins, r4), (cond_upd, (22, *r2)), (delete, r1[:-1])], [r4, r5], False),
        ([(ins, r6), (cond_upd, (234, *r5))], [r4, r6, r7], False),
        ([('DELETE FROM {t} WHERE id=1 AND clustering1=2', None), (cond_upd, (1234, *r7))], [r4, r8], False),
        ([('UPDATE {t} SET val=345 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5', None), (cond_upd, (1, *r8))], [r4, r9, r10], False),
        # A condition not met skips the whole batch.
        ([(delete, r9[:-1]), (cond_upd, (2300, 1, 1, 2, 3, 1))], [r4, r9, r10], False),
        ([('DELETE FROM {t} WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=345', None),
          ('DELETE FROM {t} WHERE id=1 AND clustering1=1', None), (ins, r11)], [r11], False),
        ([('INSERT INTO {t} (id, clustering1, clustering2, clustering3, val) VALUES(1, 3, 4, 5, 345) IF NOT EXISTS', None), (delete, r11[:-1])], [r9], False),
        ([('INSERT INTO {t}(id, clustering1, clustering2, clustering3, val) VALUES(1, 2, 3, 4, 5) USING TTL 2', None),
          ('UPDATE {t} USING TTL 4 SET val=5 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=345', None)], [r11, r13], False),
        ([], [r13], True),
        ([('INSERT INTO {t}(id, clustering1, clustering2, clustering3, val) VALUES(1, 2, 3, 4, 5)  IF NOT EXISTS USING TTL 2', None), (ins, r14)], [r11, r13, r14], False),
        ([], [r14, r15], True),
        ([('UPDATE {t} USING TTL 2 SET val=5 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=NULL', None),
          ('UPDATE {t} USING TTL 2 SET val=8 WHERE id=1 AND clustering1=4 AND clustering2=5 AND clustering3=6', None)], [r13, r16], False),
        ([], [r15, r17], True),
    ])


# Batches mixing conditional (LWT) and unconditional statements, with TTLs,
# on a table with three clustering columns: the conditional updates,
# deletes and inserts apply only if their condition holds, and then so does
# the whole batch; TTLs set in a batch apply to their statement only.
# Ported from dtest cassandra_batch_test.py with TTLs of 2 and 4 seconds
# instead of 5 and 10, waited for by polling.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_batch_ttl_conditional_interaction(cql):
    with batch_dtest_table(cql, 'clustering', CLUSTERING_SCHEMA) as table:
        batch_ttl_conditional_interaction(cql, table)

# The same with a static column, which conditions can test and batches can
# update.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_batch_static_ttl_conditional_interaction(cql):
    with batch_dtest_table(cql, 'clustering_static', 'id int, clustering1 int, clustering2 int, clustering3 int, sval int static, val int, PRIMARY KEY(id, clustering1, clustering2, clustering3)') as table:
        cql.execute(f'DELETE FROM {table} WHERE id=1')
        ins = 'INSERT INTO {t}(id, clustering1, clustering2, clustering3, val) VALUES'
        r1, r2, r3 = (1, 1, 1, 1, 1, 1), (1, 1, 1, 2, 1, 2), (1, 1, 1, 1, 1, 11)
        r4, r5 = (1, 1, 1, 2, 22, 2), (1, 1, 2, 3, 22, 23)
        r6, r7, r8 = (1, 2, 3, 4, 22, 1234), (1, 1, 1, 2, 22, 234), (1, 1, 1, 2, 22, None)
        r9, r10, r11 = (1, 1, 1, 2, 22, 1234), (1, 3, 4, 5, 22, 345), (1, 1, 1, 2, 22, 1)
        r12, r13, r14, r15 = (1, 2, 3, 4, 22, 5), (1, 3, 4, 5, 22, 345), (1, 2, 3, 4, 22, 5), (1, 3, 4, 5, 22, 5)
        r16, r17, r18 = (1, 2, 3, 4, 22, 5), (1, 4, 5, 6, 22, 7), (1, 3, 4, 5, 22, None)
        r19, r20 = (1, 3, 4, 5, 22, 5), (1, 4, 5, 6, 22, 8)
        r21, r22, r23, r24 = (1, 3, 4, 5, 22, None), (1, 4, 5, 6, 22, None), (1, 2, 3, 4, 22, 5), (1, 2, 3, 4, 23, 5)
        run_batch_steps(cql, table, [
            ([('INSERT INTO {t}(id, clustering1, clustering2, clustering3, sval, val) VALUES(1, 1, 1, 1, 1, 1)', None)], [r1], False),
            ([(ins + '(1, 1, 1, 2, 2)', None),
              ('UPDATE {t} SET val=11 WHERE id=1 AND clustering1=1 AND clustering2=1 AND clustering3=1 IF sval=1', None)], [r2, r3], False),
            ([(ins + '(1, 1, 2, 3, 23)', None), ('UPDATE {t} SET sval=22 WHERE id=1', None),
              ('DELETE FROM {t} WHERE id=1 AND clustering1=1 AND clustering2=1 AND clustering3=1', None)], [r4, r5], False),
            ([(ins + '(1, 2, 3, 4, 1234)', None),
              ('UPDATE {t} USING TTL 2 SET val=234 WHERE id=1 AND clustering1=1 AND clustering2=1 AND clustering3=2 IF sval=22', None)], [r5, r6, r7], False),
            ([], [r5, r6, r8], True),
            ([('DELETE FROM {t} WHERE id=1 AND clustering1=2', None),
              ('UPDATE {t} SET val=1234 WHERE id=1 AND clustering1=1 AND clustering2=1 AND clustering3=2 IF sval=22', None)], [r5, r9], False),
            ([('UPDATE {t} SET val=345 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5', None),
              ('UPDATE {t} SET val=1 WHERE id=1 AND clustering1=1 AND clustering2=1 AND clustering3=2 IF sval=22', None)], [r5, r10, r11], False),
            # A condition not met skips the whole batch.
            ([('DELETE FROM {t} WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5', None),
              ('UPDATE {t} SET val=2300 WHERE id=1 AND clustering1=1 AND clustering2=2 AND clustering3=3 IF sval=1', None)], [r5, r10, r11], False),
            ([('DELETE FROM {t} WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=345', None),
              ('DELETE FROM {t} WHERE id=1 AND clustering1=1', None), (ins + '(1, 2, 3, 4, 5)', None)], [r12], False),
            ([(ins + '(1, 3, 4, 5, 345) IF NOT EXISTS', None), ('DELETE FROM {t} WHERE id=1 AND clustering1=2 AND clustering2=3 AND clustering3=4', None)], [r13], False),
            ([(ins + '(1, 2, 3, 4, 5) USING TTL 2', None),
              ('UPDATE {t} USING TTL 4 SET val=5 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=345', None)], [r14, r15], False),
            ([], [r15], True),
            ([(ins + '(1, 2, 3, 4, 5)  IF NOT EXISTS USING TTL 2', None), (ins + '(1, 4, 5, 6, 7)', None)], [r15, r16, r17], False),
            ([], [r17, r18], True),
            ([('UPDATE {t} USING TTL 2 SET val=5 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=NULL', None),
              ('UPDATE {t} USING TTL 2 SET val=8 WHERE id=1 AND clustering1=4 AND clustering2=5 AND clustering3=6', None)], [r19, r20], False),
            ([], [r21, r22], True),
            ([('DELETE FROM {t} WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s', (1, 3, 4, 5, 22)),
              (ins + '(1, 2, 3, 4, 5)', None)], [r22, r23], False),
            ([('UPDATE {t} SET sval=23 WHERE id=1 IF sval=22', None),
              ('DELETE FROM {t} WHERE id=1 AND clustering1=4 AND clustering2=5 AND clustering3=6', None)], [r24], False),
        ])




# The tests below come from scylla-dtest batch_test.py's single-node TestBatch
# tests. Like the dtests, a session uses a vnodes keyspace with
# 'replication_factor': 1 holding the dtests' `clicks` (a counter) and `users`
# tables, and sends its statements without a keyspace.
@contextlib.contextmanager
def batch_dtest_session(cql):
    ks = unique_name()
    cql.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}")
    session = cql.cluster.connect()
    try:
        session.execute(f"USE {ks}")
        session.execute("CREATE TABLE clicks (userid int, url text, total counter, PRIMARY KEY (userid, url))")
        session.execute("CREATE TABLE users (id int, firstname text, lastname text, PRIMARY KEY (id))")
        yield session
    finally:
        session.shutdown()
        cql.execute(f"DROP KEYSPACE {ks}")

COUNTER_UPDATES = """
    UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
    UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://bar.com'
    UPDATE clicks SET total = total + 1 WHERE userid = 2 and url = 'http://baz.com'
"""

# Ported from scylla-dtest batch_test.py::TestBatch::test_empty_batch_throws_no_error (CASSANDRA-10711).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_empty_batch_throws_no_error(cql):
    with batch_dtest_session(cql) as session:
        session.execute("BEGIN BATCH APPLY BATCH")

# Ported from scylla-dtest batch_test.py::TestBatch::test_counter_batch_accepts_counter_mutations.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_counter_batch_accepts_counter_mutations(cql):
    with batch_dtest_session(cql) as session:
        session.execute(f"BEGIN COUNTER BATCH {COUNTER_UPDATES} APPLY BATCH")
        assert [list(r) for r in session.execute("SELECT total FROM clicks")] == [[1], [1], [1]]

# Ported from scylla-dtest batch_test.py::TestBatch::test_counter_batch_rejects_regular_mutations.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_counter_batch_rejects_regular_mutations(cql):
    with batch_dtest_session(cql) as session:
        with pytest.raises(InvalidRequest, match="Cannot include non-counter statement in a counter batch"):
            session.execute(f"BEGIN COUNTER BATCH {COUNTER_UPDATES} "
                            "INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow') APPLY BATCH")

# Ported from scylla-dtest batch_test.py::TestBatch::test_logged_batch_accepts_regular_mutations.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_logged_batch_accepts_regular_mutations(cql):
    with batch_dtest_session(cql) as session:
        session.execute("""BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH""")
        assert sorted(list(r) for r in session.execute("SELECT * FROM users")) == [[0, "Jack", "Sparrow"], [1, "Will", "Turner"]]


# Ported from scylla-dtest batch_test.py::TestBatch::test_logged_batch_rejects_counter_mutations.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_logged_batch_rejects_counter_mutations(cql):
    with batch_dtest_session(cql) as session:
        with pytest.raises(InvalidRequest, match="Cannot include a counter statement in a logged batch"):
            session.execute("""BEGIN BATCH
                INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
                INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
                UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
                APPLY BATCH""")

# Ported from scylla-dtest batch_test.py::TestBatch::test_unlogged_batch_accepts_regular_mutations.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_unlogged_batch_accepts_regular_mutations(cql):
    with batch_dtest_session(cql) as session:
        session.execute("""BEGIN UNLOGGED BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (2, 'Elizabeth', 'Swann')
            APPLY BATCH""")
        assert sorted(list(r) for r in session.execute("SELECT * FROM users")) == [[0, "Jack", "Sparrow"], [2, "Elizabeth", "Swann"]]
