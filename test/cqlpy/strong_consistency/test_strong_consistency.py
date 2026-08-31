# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Tests for strongly consistent tables, i.e., tables in a keyspace created
# with consistency = 'global'. Only tests which are happy with a single node
# and plain CQL belong here - the ones which need raft leadership, the node
# lifecycle or coordinated error injection live in test/cluster instead.
#
# The directory carries its own test_config.yaml so that only the server
# these tests run against gets the strongly-consistent-tables feature.
#############################################################################

import pytest
from cassandra.protocol import ConfigurationException, InvalidRequest
from cassandra.query import BatchStatement, BatchType

from test.pylib.skip_types import skip_env

from ..util import new_test_table, unique_name


# A keyspace whose tables are strongly consistent. Cassandra and the --vnodes
# mode have nothing to say about it, and neither has a build which runs without
# the strongly-consistent-tables experimental feature. Every other rejection of
# the keyspace is a regression, so it has to reach the test as a failure rather
# than as a skip.
@pytest.fixture(scope="module")
def sc_keyspace(cql, scylla_only, has_tablets):
    if not has_tablets:
        skip_env('Strongly consistent tables need a tablet based keyspace')
    keyspace = unique_name()
    try:
        cql.execute(f"CREATE KEYSPACE {keyspace} WITH replication = "
                    "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                    "AND tablets = {'initial': 1} AND consistency = 'global'")
    except ConfigurationException as e:
        if 'strongly_consistent_tables' not in str(e):
            raise
        skip_env('Strongly consistent tables need the strongly-consistent-tables feature on')
    yield keyspace
    cql.execute(f"DROP KEYSPACE {keyspace}")


def test_reject_user_provided_timestamps(cql, sc_keyspace):
    """
    A simple validation test that makes sure that we don't accept
    user-provided timestamps in queries to strongly consistent tables.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        error_msg = "Strongly consistent queries don't support user-provided timestamps"
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES (0, 13) USING TIMESTAMP 23")
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"UPDATE {table} USING TIMESTAMP 23 SET v = 13 WHERE pk = 0")
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"DELETE FROM {table} USING TIMESTAMP 23 WHERE pk = 0")
        # FIXME(SCYLLADB-977):
        # Add test cases for batches with timestamps. Remember to
        # handle both whole-batch timestamps, e.g.
        #   BEGIN BATCH USING TIMESTAMP ts
        #     ...
        #   APPLY BATCH
        # as well as timestamps for individual items, e.g.
        #   BEGIN BATCH
        #     INSERT INTO ... USING TIMESTAMP st;
        #     ...
        #   APPLY BATCH


@pytest.mark.parametrize("batch_mode", ["text", "prepared"], ids=["text", "prepared"])
def test_batch(cql, sc_keyspace, batch_mode):
    """
    Verify strongly consistent BATCH behavior for both paths:
    - textual CQL BATCH,
    - native protocol BATCH (prepared BatchStatement).

    Success cases:
    - same-partition batch succeeds (default logged for text, explicit logged for prepared),
    - mixed statement types in one partition succeed.

    Rejection cases:
    - batch touching multiple tables,
    - batch touching multiple partitions,
    - statement touching multiple partition keys,
    - counter batch.
    """

    def _prepared_batch_type(kind):
        if kind == "logged":
            return BatchType.LOGGED
        if kind == "unlogged":
            return BatchType.UNLOGGED
        if kind == "counter":
            return BatchType.COUNTER
        raise ValueError(f"Unexpected batch kind: {kind}")

    def _render_text_statement(table_name, op, args):
        if op == "insert":
            pk, ck, v = args
            return f"INSERT INTO {table_name} (pk, ck, v) VALUES ({pk}, {ck}, {v})"
        if op == "update":
            v, pk, ck = args
            return f"UPDATE {table_name} SET v = {v} WHERE pk = {pk} AND ck = {ck}"
        if op == "delete":
            pk, ck = args
            return f"DELETE FROM {table_name} WHERE pk = {pk} AND ck = {ck}"
        if op == "delete_in":
            pk1, pk2, ck = args
            return f"DELETE FROM {table_name} WHERE pk IN ({pk1}, {pk2}) AND ck = {ck}"
        if op == "counter_add":
            delta, pk = args
            return f"UPDATE {table_name} SET c = c + {delta} WHERE pk = {pk}"
        raise ValueError(f"Unexpected operation: {op}")

    def _make_batch_runner(table_name, *, counter_table=False):
        prepared_statements = {}
        if batch_mode == "prepared":
            if counter_table:
                prepared_statements = {
                    "counter_add": cql.prepare(f"UPDATE {table_name} SET c = c + ? WHERE pk = ?"),
                }
            else:
                prepared_statements = {
                    "insert": cql.prepare(f"INSERT INTO {table_name} (pk, ck, v) VALUES (?, ?, ?)"),
                    "update": cql.prepare(f"UPDATE {table_name} SET v = ? WHERE pk = ? AND ck = ?"),
                    "delete": cql.prepare(f"DELETE FROM {table_name} WHERE pk = ? AND ck = ?"),
                    "delete_in": cql.prepare(f"DELETE FROM {table_name} WHERE pk IN (?, ?) AND ck = ?"),
                }

        def _run(ops, *, kind):
            if batch_mode == "prepared":
                batch = BatchStatement(batch_type=_prepared_batch_type(kind))
                for op, args in ops:
                    batch.add(prepared_statements[op], args)
                return cql.execute(batch)

            if kind == "counter":
                begin = "BEGIN COUNTER BATCH"
            elif kind == "unlogged":
                begin = "BEGIN UNLOGGED BATCH"
            elif kind == "logged":
                begin = "BEGIN BATCH"
            else:
                raise ValueError(f"Unexpected batch kind: {kind}")
            lines = [begin]
            lines.extend(f"{_render_text_statement(table_name, op, args)};" for op, args in ops)
            lines.append("APPLY BATCH")
            return cql.execute("\n".join(lines))

        return _run

    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        run_batch = _make_batch_runner(table)

        run_batch([
            ("insert", (1, 1, 10)),
            ("insert", (1, 2, 20)),
            ("insert", (1, 3, 30)),
        ], kind="logged")

        rows = list(cql.execute(f"SELECT * FROM {table} WHERE pk = 1"))
        assert len(rows) == 3
        rows_by_ck = {r.ck: r.v for r in rows}
        assert rows_by_ck == {1: 10, 2: 20, 3: 30}

        run_batch([
            ("update", (99, 1, 1)),
            ("delete", (1, 3)),
        ], kind="unlogged")

        rows = list(cql.execute(f"SELECT * FROM {table} WHERE pk = 1"))
        assert len(rows) == 2
        rows_by_ck = {r.ck: r.v for r in rows}
        assert rows_by_ck == {1: 99, 2: 20}

        with pytest.raises(InvalidRequest, match="same partition"):
            run_batch([
                ("insert", (1, 1, 10)),
                ("insert", (2, 1, 20)),
            ], kind="unlogged")

        with pytest.raises(InvalidRequest, match="single partition"):
            run_batch([
                ("delete_in", (1, 2, 1)),
            ], kind="unlogged")

        with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as other_table:
            with pytest.raises(InvalidRequest, match="same table"):
                if batch_mode == "prepared":
                    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                    batch.add(cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"), (1, 1, 10))
                    batch.add(cql.prepare(f"INSERT INTO {other_table} (pk, ck, v) VALUES (?, ?, ?)"), (1, 1, 20))
                    cql.execute(batch)
                else:
                    cql.execute(f"""
                        BEGIN UNLOGGED BATCH
                        INSERT INTO {table} (pk, ck, v) VALUES (1, 1, 10);
                        INSERT INTO {other_table} (pk, ck, v) VALUES (1, 1, 20);
                        APPLY BATCH
                    """)

    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, c counter") as table:
        run_counter_batch = _make_batch_runner(table, counter_table=True)

        with pytest.raises(InvalidRequest, match="Counter batches are not supported"):
            run_counter_batch([
                ("counter_add", (1, 1)),
            ], kind="counter")
